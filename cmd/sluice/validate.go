package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
)

// validate refuses to start on a configuration that is broken, and warns loudly
// on one that is merely dangerous.
//
// Two of these checks exist because the corresponding misconfiguration breaks the
// APPLICATION's own writes, not just replication. Both were verified against a
// real PostgreSQL 18.4:
//
//	ERROR:  cannot update table "docs"
//	DETAIL: Column used in the publication WHERE expression is not part of the
//	        replica identity.
//
//	ERROR:  cannot delete from table "docs" because it does not have a replica
//	        identity and publishes deletes
//
// A server that let those slip through and only complained later would be
// actively unhelpful.
func validate(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, log *slog.Logger) error {
	// ---- wal_level -------------------------------------------------------
	var walLevel string
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&walLevel); err != nil {
		return fmt.Errorf("validate: read wal_level: %w", err)
	}
	if walLevel != "logical" {
		return fmt.Errorf("validate: wal_level is %q, must be \"logical\"; "+
			"this requires a server restart (add -c wal_level=logical)", walLevel)
	}

	// ---- slot headroom ---------------------------------------------------
	var maxSlots, usedSlots int
	if err := pool.QueryRow(ctx,
		`SELECT current_setting('max_replication_slots')::int,
		        (SELECT count(*) FROM pg_replication_slots)`).Scan(&maxSlots, &usedSlots); err != nil {
		return fmt.Errorf("validate: read slot counts: %w", err)
	}
	var slotExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		cfg.SlotName).Scan(&slotExists); err != nil {
		return fmt.Errorf("validate: check slot: %w", err)
	}
	if !slotExists && usedSlots >= maxSlots {
		return fmt.Errorf("validate: max_replication_slots=%d is fully used (%d) and slot %q does not exist yet",
			maxSlots, usedSlots, cfg.SlotName)
	}

	// ---- replication role ------------------------------------------------
	var canReplicate bool
	if err := pool.QueryRow(ctx, `
		SELECT bool_or(rolreplication OR rolsuper)
		  FROM pg_roles
		 WHERE rolname = split_part(split_part($1, '://', 2), ':', 1)`,
		cfg.ReplURL).Scan(&canReplicate); err != nil {
		log.Warn("validate: could not confirm the replication role's attributes", "err", err)
	} else if !canReplicate {
		log.Warn("validate: the replication role appears to lack the REPLICATION attribute; " +
			"START_REPLICATION will fail")
	}

	// ---- publication -----------------------------------------------------
	var pubExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		cfg.Publication).Scan(&pubExists); err != nil {
		return fmt.Errorf("validate: check publication: %w", err)
	}
	if !pubExists {
		return fmt.Errorf("validate: publication %q does not exist; create it with: CREATE PUBLICATION %s",
			cfg.Publication, cfg.Publication)
	}

	// ---- FATAL: publication row filters over non-replica-identity columns --
	rows, err := pool.Query(ctx, `
		SELECT c.oid::regclass::text,
		       pg_get_expr(pr.prqual, pr.prrelid)
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		 WHERE p.pubname = $1 AND pr.prqual IS NOT NULL`, cfg.Publication)
	if err != nil {
		return fmt.Errorf("validate: inspect publication row filters: %w", err)
	}
	var filtered []string
	for rows.Next() {
		var rel, qual string
		if err := rows.Scan(&rel, &qual); err != nil {
			rows.Close()
			return err
		}
		filtered = append(filtered, rel+" WHERE "+qual)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(filtered) > 0 {
		return fmt.Errorf("validate: publication %q has row filters (%v). "+
			"Sluice does not support them: any column used in a publication WHERE expression must be "+
			"part of the replica identity, and when it is not, the application's own UPDATE and DELETE "+
			"statements FAIL. Remove the filters and let Sluice filter per subscriber instead: "+
			"ALTER PUBLICATION %s DROP TABLE ...; ALTER PUBLICATION %s ADD TABLE ...;",
			cfg.Publication, filtered, cfg.Publication, cfg.Publication)
	}

	// ---- FATAL: inadequate replica identity on a table publishing deletes --
	rows, err = pool.Query(ctx, `
		SELECT c.oid::regclass::text, c.relreplident
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		 WHERE p.pubname = $1
		   AND (p.pubdelete OR p.pubupdate)
		   AND NOT (
		     CASE c.relreplident
		       WHEN 'f' THEN true
		       WHEN 'd' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)
		       WHEN 'i' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisreplident)
		       ELSE false
		     END)`, cfg.Publication)
	if err != nil {
		return fmt.Errorf("validate: inspect replica identities: %w", err)
	}
	var broken []string
	for rows.Next() {
		var rel string
		var ri byte
		if err := rows.Scan(&rel, &ri); err != nil {
			rows.Close()
			return err
		}
		broken = append(broken, fmt.Sprintf("%s (relreplident=%q)", rel, string(ri)))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	if len(broken) > 0 {
		return fmt.Errorf("validate: these published tables have an inadequate replica identity: %v. "+
			"The application's own UPDATE and DELETE statements on them are already failing with "+
			"\"cannot delete from table ... because it does not have a replica identity and publishes "+
			"deletes\". Fix with ALTER TABLE ... REPLICA IDENTITY USING INDEX <unique index> "+
			"(preferred) or REPLICA IDENTITY FULL", broken)
	}

	// ---- WARN: unbounded WAL retention -----------------------------------
	var keep string
	if err := pool.QueryRow(ctx, `SHOW max_slot_wal_keep_size`).Scan(&keep); err == nil {
		if keep == "-1" {
			metrics.ConfigWarnings.WithLabelValues("unbounded_wal_retention").Set(1)
			log.Warn("max_slot_wal_keep_size is -1 (unlimited): an unconsumed replication slot can " +
				"fill the disk and, in the extreme, force a shutdown to prevent transaction ID " +
				"wraparound. Set a bound.")
		}
	}

	// ---- WARN: idle slot invalidation ------------------------------------
	var idleTimeout string
	if err := pool.QueryRow(ctx, `SHOW idle_replication_slot_timeout`).Scan(&idleTimeout); err == nil {
		if idleTimeout != "0" {
			metrics.ConfigWarnings.WithLabelValues("idle_slot_timeout").Set(1)
			log.Warn("idle_replication_slot_timeout is set; if Sluice is offline for longer than this, "+
				"PostgreSQL DESTROYS the slot at the next checkpoint and the change stream gains an "+
				"unrecoverable gap", "value", idleTimeout)
		}
	}

	// ---- WARN: published tables without a primary key --------------------
	rows, err = pool.Query(ctx, `
		SELECT c.oid::regclass::text
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		 WHERE p.pubname = $1
		   AND NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary)`,
		cfg.Publication)
	if err == nil {
		var noPK []string
		for rows.Next() {
			var rel string
			if err := rows.Scan(&rel); err == nil {
				noPK = append(noPK, rel)
			}
		}
		rows.Close()
		if len(noPK) > 0 {
			metrics.ConfigWarnings.WithLabelValues("no_primary_key").Set(1)
			log.Warn("published tables without a primary key: clients cannot reliably identify rows, "+
				"and at-least-once delivery becomes impossible to dedupe", "tables", noPK)
		}
	}

	// ---- WARN: REPLICA IDENTITY FULL over TOAST-able columns -------------
	rows, err = pool.Query(ctx, `
		SELECT c.oid::regclass::text
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		 WHERE p.pubname = $1 AND c.relreplident = 'f'
		   AND EXISTS (SELECT 1 FROM pg_attribute a
		                WHERE a.attrelid = c.oid AND a.attnum > 0
		                  AND NOT a.attisdropped AND a.attstorage IN ('x','e'))`,
		cfg.Publication)
	if err == nil {
		var heavy []string
		for rows.Next() {
			var rel string
			if err := rows.Scan(&rel); err == nil {
				heavy = append(heavy, rel)
			}
		}
		rows.Close()
		if len(heavy) > 0 {
			metrics.ConfigWarnings.WithLabelValues("replica_identity_full_with_toast").Set(1)
			log.Warn("REPLICA IDENTITY FULL on tables with TOAST-able columns: every UPDATE and DELETE "+
				"inlines the whole out-of-line value into the old tuple. Measured 15x more WAL and "+
				"3200x larger DELETE messages. Prefer REPLICA IDENTITY USING INDEX over just the "+
				"columns you filter on.", "tables", heavy)
		}
	}

	// ---- impersonation capability ----------------------------------------
	// RLS mode assumes application roles via SET LOCAL ROLE. Issuer mode does
	// not impersonate: it reads with the pool role and refuses to start unless
	// that role can SELECT published tables and bypass RLS (or RLS is off).
	if cfg.IssuerMode() {
		if err := validateIssuerPrivileges(ctx, pool, cfg); err != nil {
			return err
		}
	} else {
		for _, role := range cfg.AllowedRoles {
			if role == "service_role" {
				continue // BYPASSRLS; never impersonated
			}
			var canSet bool
			// MEMBER, not USAGE. USAGE asks whether the role's privileges are
			// available WITHOUT SET ROLE, which is false by design for a NOINHERIT
			// role -- and NOINHERIT is exactly what we want, so that the authz role
			// can only ever act as an application role deliberately.
			if err := pool.QueryRow(ctx,
				`SELECT pg_has_role(current_user, $1, 'MEMBER')`, role).Scan(&canSet); err != nil {
				log.Warn("validate: could not check role membership", "role", role, "err", err)
				continue
			}
			if !canSet {
				return fmt.Errorf("validate: the authz role cannot assume %q; "+
					"grant it with: GRANT %s TO <authz role>", role, role)
			}
		}
	}

	log.Info("startup validation passed",
		"wal_level", walLevel, "publication", cfg.Publication, "slot", cfg.SlotName,
		"shape_oracle", cfg.ShapeOracle)
	return nil
}

func validateIssuerPrivileges(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) error {
	var bypass bool
	if err := pool.QueryRow(ctx, `
		SELECT rolbypassrls OR rolsuper
		  FROM pg_roles WHERE rolname = current_user`).Scan(&bypass); err != nil {
		return fmt.Errorf("validate: read BYPASSRLS for current_user: %w", err)
	}

	rows, err := pool.Query(ctx, `
		SELECT n.nspname, c.relname, c.relrowsecurity,
		       has_table_privilege(c.oid, 'SELECT')
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE p.pubname = $1`, cfg.Publication)
	if err != nil {
		return fmt.Errorf("validate: inspect issuer table privileges: %w", err)
	}
	defer rows.Close()

	var rlsBlocked, noSelect []string
	for rows.Next() {
		var schema, name string
		var rls, canSelect bool
		if err := rows.Scan(&schema, &name, &rls, &canSelect); err != nil {
			return err
		}
		rel := schema + "." + name
		if !canSelect {
			noSelect = append(noSelect, rel)
		}
		if rls && !bypass {
			rlsBlocked = append(rlsBlocked, rel)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if len(noSelect) > 0 {
		return fmt.Errorf("validate: issuer mode: the pool role cannot SELECT published table(s) %v; "+
			"GRANT SELECT ON those tables to the Sluice role", noSelect)
	}
	if len(rlsBlocked) > 0 {
		return fmt.Errorf("validate: issuer mode: published table(s) %v have row-level security enabled "+
			"and the Sluice role does not bypass it. Snapshots and hold EXISTS would be judged by RLS, "+
			"which is a second oracle. ALTER ROLE <sluice> BYPASSRLS, or DISABLE ROW LEVEL SECURITY on those tables",
			rlsBlocked)
	}
	return nil
}
