package main

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/server"
)

// published lists every table the publication covers, however it was added
// (explicitly, FOR TABLES IN SCHEMA, or FOR ALL TABLES).
const published = `
WITH pub AS (
  SELECT c.oid, c.oid::regclass::text AS rel, c.relreplident, c.relrowsecurity,
         n.nspname, c.relname
    FROM pg_publication_tables pt
    JOIN pg_namespace n ON n.nspname = pt.schemaname
    JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename
   WHERE pt.pubname = $1
)`

// validate refuses to start on a configuration that is broken, and returns the
// findings that are merely dangerous so /diagnostics keeps reporting them.
//
// Two of these checks exist because the corresponding misconfiguration breaks the
// APPLICATION's own writes, not just replication:
//
//	ERROR:  cannot update table "docs"
//	DETAIL: Column used in the publication WHERE expression is not part of the
//	        replica identity.
//
//	ERROR:  cannot delete from table "docs" because it does not have a replica
//	        identity and publishes deletes
func validate(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config, log *slog.Logger) ([]server.Diagnostic, error) {
	var warnings []server.Diagnostic
	warn := func(d server.Diagnostic, args ...any) {
		warnings = append(warnings, d)
		log.Warn(d.Reason, args...)
	}

	// ---- wal_level -------------------------------------------------------
	var walLevel string
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&walLevel); err != nil {
		return nil, fmt.Errorf("validate: read wal_level: %w", err)
	}
	if walLevel != "logical" {
		return nil, fmt.Errorf("validate: wal_level is %q, must be \"logical\"; "+
			"this requires a server restart (add -c wal_level=logical)", walLevel)
	}

	// ---- slot headroom ---------------------------------------------------
	var maxSlots, usedSlots int
	var slotExists bool
	if err := pool.QueryRow(ctx,
		`SELECT current_setting('max_replication_slots')::int,
		        (SELECT count(*) FROM pg_replication_slots),
		        EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		cfg.SlotName).Scan(&maxSlots, &usedSlots, &slotExists); err != nil {
		return nil, fmt.Errorf("validate: read slot counts: %w", err)
	}
	if !slotExists && usedSlots >= maxSlots {
		return nil, fmt.Errorf("validate: max_replication_slots=%d is fully used (%d) and slot %q does not exist yet",
			maxSlots, usedSlots, cfg.SlotName)
	}

	// ---- replication role ------------------------------------------------
	// Best effort: the role name is read from a URL-form SLUICE_DB_REPL_URL.
	var canReplicate *bool
	if err := pool.QueryRow(ctx, `
		SELECT bool_or(rolreplication OR rolsuper)
		  FROM pg_roles
		 WHERE rolname = split_part(split_part($1, '://', 2), ':', 1)`,
		cfg.ReplURL).Scan(&canReplicate); err == nil && canReplicate != nil && !*canReplicate {
		warn(server.Diagnostic{Code: "replication_role_attribute", Severity: "high",
			Reason: "the replication role appears to lack the REPLICATION attribute; START_REPLICATION will fail",
			Remedy: "ALTER ROLE <replication role> REPLICATION;"})
	}

	// ---- publication -----------------------------------------------------
	var pubExists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`,
		cfg.Publication).Scan(&pubExists); err != nil {
		return nil, fmt.Errorf("validate: check publication: %w", err)
	}
	if !pubExists {
		return nil, fmt.Errorf("validate: publication %q does not exist; create it with: CREATE PUBLICATION %s",
			cfg.Publication, cfg.Publication)
	}

	// ---- FATAL: publication row filters ----------------------------------
	// Only explicitly listed tables can carry a row filter, so pg_publication_rel
	// sees all of them.
	filtered, err := queryStrings(ctx, pool, `
		SELECT c.oid::regclass::text || ' WHERE ' || pg_get_expr(pr.prqual, pr.prrelid)
		  FROM pg_publication p
		  JOIN pg_publication_rel pr ON pr.prpubid = p.oid
		  JOIN pg_class c ON c.oid = pr.prrelid
		 WHERE p.pubname = $1 AND pr.prqual IS NOT NULL`, cfg.Publication)
	if err != nil {
		return nil, fmt.Errorf("validate: inspect publication row filters: %w", err)
	}
	if len(filtered) > 0 {
		return nil, fmt.Errorf("validate: publication %q has row filters (%v). "+
			"Sluice does not support them: any column used in a publication WHERE expression must be "+
			"part of the replica identity, and when it is not, the application's own UPDATE and DELETE "+
			"statements FAIL. Remove the filters and let Sluice filter per subscriber instead: "+
			"ALTER PUBLICATION %s SET TABLE ... (without WHERE)",
			cfg.Publication, filtered, cfg.Publication)
	}

	// ---- FATAL: inadequate replica identity on a table publishing updates/deletes
	broken, err := queryStrings(ctx, pool, published+`
		SELECT pub.rel || ' (relreplident=' || pub.relreplident::text || ')'
		  FROM pub, pg_publication p
		 WHERE p.pubname = $1
		   AND (p.pubdelete OR p.pubupdate)
		   AND NOT (
		     CASE pub.relreplident
		       WHEN 'f' THEN true
		       WHEN 'd' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = pub.oid AND i.indisprimary)
		       WHEN 'i' THEN EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = pub.oid AND i.indisreplident)
		       ELSE false
		     END)`, cfg.Publication)
	if err != nil {
		return nil, fmt.Errorf("validate: inspect replica identities: %w", err)
	}
	if len(broken) > 0 {
		return nil, fmt.Errorf("validate: these published tables have an inadequate replica identity: %v. "+
			"The application's own UPDATE and DELETE statements on them are already failing with "+
			"\"cannot delete from table ... because it does not have a replica identity and publishes "+
			"deletes\". Fix with ALTER TABLE ... REPLICA IDENTITY USING INDEX <unique index> "+
			"(preferred) or REPLICA IDENTITY FULL", broken)
	}

	// ---- WARN: unbounded WAL retention -----------------------------------
	var keep string
	if err := pool.QueryRow(ctx, `SHOW max_slot_wal_keep_size`).Scan(&keep); err == nil && keep == "-1" {
		warn(server.Diagnostic{Code: "unbounded_wal_retention", Severity: "high",
			Reason: "max_slot_wal_keep_size is -1 (unlimited): an unconsumed replication slot can fill the disk",
			Remedy: "ALTER SYSTEM SET max_slot_wal_keep_size = '4GB'; -- or another bound, then reload"})
	}

	// ---- WARN: idle slot invalidation (PostgreSQL 18+) --------------------
	var idleTimeout string
	if err := pool.QueryRow(ctx, `SHOW idle_replication_slot_timeout`).Scan(&idleTimeout); err == nil && idleTimeout != "0" {
		warn(server.Diagnostic{Code: "idle_slot_timeout", Severity: "medium",
			Reason: "idle_replication_slot_timeout is " + idleTimeout + ": if Sluice is offline longer than " +
				"that, PostgreSQL invalidates the slot and Sluice refuses to stream until it is recreated " +
				"(by hand, or at startup with SLUICE_SLOT_RECREATE=true)",
			Remedy: "ALTER SYSTEM SET idle_replication_slot_timeout = 0; -- or a value longer than any outage"})
	}

	// ---- WARN: published tables without a primary key --------------------
	if noPK, err := queryStrings(ctx, pool, published+`
		SELECT pub.rel FROM pub
		 WHERE NOT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = pub.oid AND i.indisprimary)`,
		cfg.Publication); err == nil && len(noPK) > 0 {
		warn(server.Diagnostic{Code: "no_primary_key", Severity: "low",
			Reason: fmt.Sprintf("published tables without a primary key: %v; clients cannot reliably "+
				"identify or deduplicate their rows", noPK),
			Remedy: "ALTER TABLE <table> ADD PRIMARY KEY (...);"})
	}

	// ---- FATAL: session lookup without read access ------------------------
	if cfg.SessionLookup {
		var readable *bool
		if err := pool.QueryRow(ctx,
			`SELECT has_column_privilege(to_regclass($1), 'id', 'SELECT')`,
			cfg.SessionsTable).Scan(&readable); err != nil || readable == nil || !*readable {
			return nil, fmt.Errorf("validate: SLUICE_REVOCATION_SESSION_LOOKUP=true needs the pool role "+
				"to read the id column of %s: GRANT SELECT (id) ON %s TO <authz role>",
				cfg.SessionsTable, cfg.SessionsTable)
		}
	}

	// ---- impersonation capability ----------------------------------------
	// Issuer mode never impersonates, so it checks no roles. That includes
	// SLUICE_JWT_REQUIRE_ROLE=false, which config only accepts in issuer mode.
	if cfg.IssuerMode() {
		if err := validateIssuerPrivileges(ctx, pool, cfg); err != nil {
			return nil, err
		}
	} else {
		// RLS mode assumes the JWT's role with SET LOCAL ROLE for snapshots and
		// Tier C probes. MEMBER, not USAGE: USAGE asks whether the role's
		// privileges are available WITHOUT SET ROLE, which is false by design
		// for a NOINHERIT role.
		for _, role := range cfg.AllowedRoles {
			var exists, canSet, bypass bool
			if err := pool.QueryRow(ctx, `
				SELECT true, pg_has_role(current_user, r.oid, 'MEMBER'), r.rolbypassrls OR r.rolsuper
				  FROM pg_roles r WHERE r.rolname = $1`, role).Scan(&exists, &canSet, &bypass); err != nil || !exists {
				return nil, fmt.Errorf("validate: SLUICE_ALLOWED_ROLES names %q, which is not a role in this database", role)
			}
			if canSet {
				continue
			}
			if !bypass {
				return nil, fmt.Errorf("validate: the authz role cannot assume %q; "+
					"grant it with: GRANT %s TO <authz role>", role, role)
			}
			// A role that bypasses RLS needs no policy evaluation, but snapshots
			// still SET ROLE to it to apply its table grants.
			warn(server.Diagnostic{Code: "role_not_assumable", Severity: "medium",
				Reason: fmt.Sprintf("the authz role cannot assume %q, so initial snapshots for %s tokens fail", role, role),
				Impact: "granting it lets the authz role bypass row-level security with SET ROLE, which Sluice only does for a verified token of that role",
				Remedy: fmt.Sprintf("GRANT %s TO <authz role>; -- or remove %s from SLUICE_ALLOWED_ROLES if it never needs snapshots", role, role)})
		}
	}

	log.Info("startup validation passed",
		"wal_level", walLevel, "publication", cfg.Publication, "slot", cfg.SlotName,
		"shape_oracle", cfg.ShapeOracle, "warnings", len(warnings))
	return warnings, nil
}

func validateIssuerPrivileges(ctx context.Context, pool *pgxpool.Pool, cfg *config.Config) error {
	var bypass bool
	if err := pool.QueryRow(ctx, `
		SELECT rolbypassrls OR rolsuper
		  FROM pg_roles WHERE rolname = current_user`).Scan(&bypass); err != nil {
		return fmt.Errorf("validate: read BYPASSRLS for current_user: %w", err)
	}

	rows, err := pool.Query(ctx, published+`
		SELECT pub.nspname || '.' || pub.relname, pub.relrowsecurity, has_table_privilege(pub.oid, 'SELECT')
		  FROM pub`, cfg.Publication)
	if err != nil {
		return fmt.Errorf("validate: inspect issuer table privileges: %w", err)
	}
	defer rows.Close()

	var rlsBlocked, noSelect []string
	for rows.Next() {
		var rel string
		var rls, canSelect bool
		if err := rows.Scan(&rel, &rls, &canSelect); err != nil {
			return err
		}
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

// queryStrings runs a query returning one text column.
func queryStrings(ctx context.Context, pool *pgxpool.Pool, sql string, args ...any) ([]string, error) {
	rows, err := pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
