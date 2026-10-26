package server

import (
	"cmp"
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/reader"
)

// Diagnostic is one actionable configuration finding.
type Diagnostic struct {
	Code     string `json:"code"`
	Severity string `json:"severity"` // high | medium | low
	Relation string `json:"relation,omitempty"`
	Policy   string `json:"policy,omitempty"`
	Reason   string `json:"reason"`
	Impact   string `json:"impact,omitempty"`
	Remedy   string `json:"remedy,omitempty"`
}

// handleDiagnostics answers "what is wrong with my setup?" -- the endpoint the
// incumbent does not have, and the reason a slow policy cannot stay invisible.
func (s *Server) handleDiagnostics(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil || id.Role != "service_role" {
		s.refuse(w, r, http.StatusForbidden, "forbidden", "service_role required")
		return
	}

	out := map[string]any{}

	if s.oracle != nil {
		out["oracle"] = s.oracle.Name()
	} else {
		out["oracle"] = oracle.NameRLS
	}
	if s.cfg != nil && s.cfg.IssuerMode() {
		out["issuer"] = map[string]any{
			"url":     s.cfg.IssuerURL,
			"timeout": s.cfg.IssuerTimeout.String(),
			"holds":   s.holds.Count(),
		}
	}

	if s.reader != nil {
		out["slot"] = s.slotInfo(r.Context())
	}

	stats := s.reg.Stats()
	subs := map[string]any{
		"total":     stats.Subscriptions,
		"streams":   stats.Streams,
		"relations": stats.Relations,
		"unindexed": stats.Unindexed,
	}
	if s.oracle == nil || s.oracle.Name() == oracle.NameRLS {
		subs["by_tier"] = map[string]int{
			"A": stats.ByTier[authz.TierA],
			"B": stats.ByTier[authz.TierB],
			"C": stats.ByTier[authz.TierC],
		}
	}
	out["subscriptions"] = subs

	diags := s.diagnostics()
	out["warnings"] = diags
	setWarningMetrics(diags)

	out["publication"] = map[string]any{
		"name":   s.cfg.Publication,
		"tables": len(s.cat.All()),
	}
	out["replication_options"] = map[string]any{
		"proto_version": s.cfg.ProtoVersion,
		"messages":      s.cfg.Messages,
	}

	writeJSON(w, http.StatusOK, out)
}

// RefreshHealth runs on every catalog tick. It keeps the slot and warning
// metrics current without anyone calling /diagnostics, and bounds the memory
// the resume buffer and the session lookup hold.
func (s *Server) RefreshHealth(ctx context.Context) {
	if s.reader != nil && s.pool != nil {
		if st, err := s.readSlot(ctx); err == nil {
			setSlotMetrics(st)
			s.noteRetention(ctx, st)
		}
	}
	setWarningMetrics(s.diagnostics())
	s.hub.Rings().Sweep()
	if s.sessions != nil {
		s.sessions.sweep()
	}
}

// setWarningMetrics resets then sets sluice_config_warnings, so a resolved
// warning stops being reported.
func setWarningMetrics(diags []Diagnostic) {
	metrics.ConfigWarnings.Reset()
	for _, d := range diags {
		metrics.ConfigWarnings.WithLabelValues(d.Code).Set(1)
	}
}

func (s *Server) slotInfo(ctx context.Context) map[string]any {
	info := map[string]any{
		"name":          s.cfg.SlotName,
		"confirmed_lsn": reader.FormatLSN(s.reader.ConfirmedLSN()),
		"received_lsn":  reader.FormatLSN(s.reader.ReceivedLSN()),
	}
	st, err := s.readSlot(ctx)
	if err != nil {
		return info
	}
	setSlotMetrics(st)
	info["active"] = st.active
	info["wal_status"] = st.walStatus
	info["retained_bytes"] = st.retained
	if st.safe != nil {
		info["safe_wal_bytes"] = *st.safe
	}
	if st.invalidation != "" {
		info["invalidation_reason"] = st.invalidation
	}
	return info
}

// slotStatus is the slot as pg_replication_slots reports it.
type slotStatus struct {
	active       bool
	walStatus    string
	invalidation string
	// retained is the WAL from the slot's restart point to the WAL end, which
	// PostgreSQL keeps for it; unconfirmed, from the position this process has
	// confirmed. Not the slot's confirmed_flush_lsn, which only moves when the
	// reader reports it (SLUICE_STATUS_INTERVAL) and so lags a burst of WAL.
	retained, unconfirmed int64
	// safe is how much more WAL can be written before the slot is lost
	// (safe_wal_size), nil when max_slot_wal_keep_size is -1 or the slot is
	// already lost. Below zero, the next checkpoint invalidates it.
	safe *int64
}

func (s *Server) readSlot(ctx context.Context) (slotStatus, error) {
	var st slotStatus
	// invalidation_reason exists from PostgreSQL 17; through to_jsonb it reads
	// as NULL on 16 rather than failing the query.
	err := s.pool.QueryRow(ctx, `
		SELECT active,
		       coalesce(wal_status, 'unknown'),
		       coalesce(to_jsonb(s) ->> 'invalidation_reason', ''),
		       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint,
		       greatest(pg_wal_lsn_diff(pg_current_wal_lsn(), $2::pg_lsn), 0)::bigint,
		       safe_wal_size
		  FROM pg_replication_slots s WHERE slot_name = $1`,
		s.cfg.SlotName, reader.FormatLSN(s.reader.ConfirmedLSN())).
		Scan(&st.active, &st.walStatus, &st.invalidation, &st.retained, &st.unconfirmed, &st.safe)
	return st, err
}

func setSlotMetrics(st slotStatus) {
	metrics.SlotRetainedBytes.Set(float64(st.retained))
	if st.safe != nil {
		metrics.SlotSafeWALBytes.WithLabelValues().Set(float64(*st.safe))
	} else {
		metrics.SlotSafeWALBytes.Reset()
	}
}

// retention is how much of the WAL max_slot_wal_keep_size lets the slot keep
// it retains, as of the last check, and what holds it back.
type retention struct {
	share float64 // retained / (retained + safe_wal_size)
	level int     // 1 from half of it, 2 from 80%; 0 below, or without a bound
	// byTransaction is set when the slot's restart point, not what Sluice
	// confirmed, holds the WAL: an open transaction anywhere in the database.
	// holder names the oldest one, when it was found. Otherwise Sluice is
	// unconfirmed bytes behind the WAL end.
	byTransaction bool
	holder        string
	unconfirmed   int64
}

// assessRetention measures a slot against max_slot_wal_keep_size. A level is
// left 10 points below the share it starts at, so a share hovering on a
// threshold is reported once.
func assessRetention(st slotStatus, prev *retention) *retention {
	r := &retention{}
	if st.safe != nil && st.retained+*st.safe > 0 {
		r.share = float64(st.retained) / float64(st.retained+*st.safe)
	}
	was := 0
	if prev != nil {
		was = prev.level
	}
	switch {
	case r.share >= 0.8 || was == 2 && r.share >= 0.7:
		r.level = 2
	case r.share >= 0.5 || was >= 1 && r.share >= 0.4:
		r.level = 1
	}
	r.byTransaction = st.unconfirmed < st.retained/2
	r.unconfirmed = st.unconfirmed
	return r
}

// noteRetention follows how close the slot is to max_slot_wal_keep_size: past
// it PostgreSQL invalidates the slot, and Sluice stops with a gap every client
// reads its state again for. The slot retains WAL from its restart point,
// which the oldest transaction open in the database holds back, whatever it
// wrote and even while Sluice confirms everything. The process reading the
// slot logs each change of level; /diagnostics reports a level above 0.
func (s *Server) noteRetention(ctx context.Context, st slotStatus) {
	prev := s.retention.Load()
	r := assessRetention(st, prev)
	if r.level > 0 && r.byTransaction {
		r.holder = s.oldestTransaction(ctx)
	}
	s.retention.Store(r)
	if prev != nil && prev.level == r.level || prev == nil && r.level == 0 || !s.replicated.Load() {
		return
	}
	args := []any{"slot", s.cfg.SlotName, "share", math.Round(r.share*100) / 100,
		"retained_bytes", st.retained}
	if st.safe != nil {
		args = append(args, "safe_wal_bytes", *st.safe)
	}
	switch {
	case r.level == 0:
		s.log.Info("replication slot is back under half of max_slot_wal_keep_size", args...)
		return
	case r.byTransaction:
		args = append(args, "held_by", r.heldBy())
	default:
		args = append(args, "unconfirmed_bytes", r.unconfirmed)
	}
	level := slog.LevelWarn
	if r.level == 2 {
		level = slog.LevelError
	}
	s.log.Log(ctx, level, "replication slot nears max_slot_wal_keep_size; past it the slot is invalidated and Sluice stops", args...)
}

// oldestTransaction names the oldest transaction open in the database, which
// holds back the restart point of every logical slot: a session's pid, or a
// prepared transaction's gid. Any role can read both; how long a session has
// been in its transaction shows only to a role that may see that session
// (pg_read_all_stats). "" when none is open or the query fails.
func (s *Server) oldestTransaction(ctx context.Context) string {
	var who string
	var secs *float64
	err := s.pool.QueryRow(ctx, `
		SELECT who, secs FROM (
		  SELECT 'pid ' || pid AS who, age(backend_xid) AS age,
		         extract(epoch FROM now() - xact_start)::float8 AS secs
		    FROM pg_stat_activity WHERE backend_xid IS NOT NULL
		  UNION ALL
		  SELECT 'prepared transaction ' || quote_literal(gid), age(transaction),
		         extract(epoch FROM now() - prepared)::float8
		    FROM pg_prepared_xacts) t
		 ORDER BY age DESC LIMIT 1`).Scan(&who, &secs)
	if err != nil {
		return ""
	}
	if secs != nil {
		who += ", open for " + time.Duration(*secs*float64(time.Second)).Round(time.Second).String()
	}
	return who
}

func (r *retention) diagnostic() Diagnostic {
	d := Diagnostic{
		Code: "slot_wal_retention", Severity: "medium",
		Reason: fmt.Sprintf("the replication slot retains %.0f%% of the WAL max_slot_wal_keep_size lets it keep", r.share*100),
		Impact: "at all of it PostgreSQL invalidates the slot: Sluice stops, and every client reads its state again",
	}
	if r.level == 2 {
		d.Severity = "high"
	}
	if r.byTransaction {
		d.Reason += "; the oldest open transaction holds it back: " + r.heldBy()
		d.Remedy = "end that transaction (pg_terminate_backend for a session, ROLLBACK PREPARED for a prepared transaction); " +
			"bound them with idle_in_transaction_session_timeout and transaction_timeout; or raise max_slot_wal_keep_size"
	} else {
		d.Reason += fmt.Sprintf("; Sluice has not confirmed the last %d MiB of it", r.unconfirmed>>20)
		d.Remedy = "give Sluice more CPU (sluice_reader_lag_seconds says how late changes arrive), or raise max_slot_wal_keep_size"
	}
	return d
}

// heldBy names the transaction holding the slot's restart point. Once it has
// ended the point moves with PostgreSQL's next record of running transactions,
// a few seconds later.
func (r *retention) heldBy() string {
	return cmp.Or(r.holder, "none open any more, so it moves shortly")
}

// diagnostics builds the warning list from startup validation, the catalog and
// the live registry.
func (s *Server) diagnostics() []Diagnostic {
	out := append([]Diagnostic(nil), s.startupWarnings...)
	if r := s.retention.Load(); r != nil && r.level > 0 {
		out = append(out, r.diagnostic())
	}

	// Per-relation findings from the catalog.
	for _, rel := range s.cat.All() {
		if !rel.ReplicaIdentityOK {
			out = append(out, Diagnostic{
				Code: "replica_identity_broken", Severity: "high",
				Relation: rel.FullName(),
				Reason: fmt.Sprintf("relreplident is %q but no adequate key is available",
					string(rel.ReplicaIdentity)),
				Impact: "the APPLICATION's UPDATE and DELETE statements on this table are failing, " +
					"not just replication",
				Remedy: "ALTER TABLE " + rel.FullName() + " REPLICA IDENTITY FULL; " +
					"-- or recreate the unique index named by relreplident",
			})
		}
		if rel.ReplicaIdentity == 'f' && rel.HasToastableColumn {
			out = append(out, Diagnostic{
				Code: "replica_identity_full_with_toast", Severity: "medium",
				Relation: rel.FullName(),
				Reason:   "REPLICA IDENTITY FULL combined with a TOAST-able column",
				Impact: "every UPDATE and DELETE carries the whole out-of-line value in the old tuple, " +
					"even when that column did not change, inflating WAL and the replication stream",
				Remedy: "CREATE UNIQUE INDEX " + rel.Name + "_ri ON " + rel.FullName() +
					" (<filter columns>, <pk>); ALTER TABLE " + rel.FullName() +
					" REPLICA IDENTITY USING INDEX " + rel.Name + "_ri;",
			})
		}

		if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
			continue
		}

		// Report uncompilable policies even when nobody is subscribed yet, so the
		// problem is visible before it costs anything.
		for _, p := range rel.Policies {
			node := p.Parsed
			if node == nil {
				out = append(out, Diagnostic{
					Code: "policy_unparseable", Severity: "high",
					Relation: rel.FullName(), Policy: p.Name,
					Reason: p.ParseErr,
					Impact: "subscriptions on this relation fall to Tier C, which runs one impersonated " +
						"query per subscriber per change",
					Remedy: "simplify the policy, or accept the cost and set alerts on " +
						"sluice_authz_tier_c_probes_total",
				})
				continue
			}
			info := expr.Analyze(node, rel.Name)

			// Applies to PUBLIC, i.e. to anon as well. PostgreSQL evaluates the
			// whole predicate before discovering the caller was never eligible.
			if len(p.Roles) == 0 {
				out = append(out, Diagnostic{
					Code: "policy_applies_to_public", Severity: "medium",
					Relation: rel.FullName(), Policy: p.Name,
					Reason: "the policy has no TO clause, so it applies to PUBLIC",
					Impact: "every role, including anon, evaluates this predicate in full before " +
						"being rejected, and Sluice must resolve it for anonymous subscriptions too",
					Remedy: fmt.Sprintf(
						"ALTER POLICY %s ON %s TO authenticated;  -- or the roles that should actually match",
						catalog.QuoteIdent(p.Name), rel.FullName()),
				})
			}

			// Per-query functions that are not wrapped in a scalar subquery.
			// Sluice folds them once regardless; PostgreSQL does not, and it is
			// PostgreSQL that runs this predicate for every snapshot, every
			// Tier C probe and every ordinary application query.
			if len(info.UncachedCalls) > 0 {
				calls := make([]string, 0, len(info.UncachedCalls))
				for _, c := range info.UncachedCalls {
					calls = append(calls, c+"()")
				}
				out = append(out, Diagnostic{
					Code: "policy_function_not_wrapped", Severity: "medium",
					Relation: rel.FullName(), Policy: p.Name,
					Reason: "calls " + strings.Join(calls, ", ") + " directly rather than as a scalar subquery",
					Impact: "PostgreSQL re-invokes the function for every row it scans instead of once " +
						"per query as an InitPlan, in snapshots, Tier C probes and the application's own queries",
					Remedy: fmt.Sprintf(
						"rewrite the policy wrapping each call, e.g. `(select %s)`, then "+
							"ALTER POLICY %s ON %s USING (...);",
						calls[0], catalog.QuoteIdent(p.Name), rel.FullName()),
				})
			}

			// A predicate column with no index makes PostgreSQL filter row by
			// row on every snapshot and every probe.
			for _, col := range info.Columns {
				if rel.IndexedColumns[col] {
					continue
				}
				out = append(out, Diagnostic{
					Code: "unindexed_policy_column", Severity: "low",
					Relation: rel.FullName(), Policy: p.Name,
					Reason: fmt.Sprintf("the policy reads %q, which is not the leading column of any index", col),
					Impact: "snapshots, Tier C probes and the application's own queries scan the table " +
						"instead of seeking",
					Remedy: fmt.Sprintf("CREATE INDEX ON %s (%s);", rel.FullName(), catalog.QuoteIdent(col)),
				})
			}

			if info.Compilable() {
				continue
			}
			out = append(out, Diagnostic{
				Code: "tier_c_policy", Severity: "high",
				Relation: rel.FullName(), Policy: p.Name,
				Reason: info.Unsupported,
				Impact: "every change to this table runs one impersonated query per Tier C subscriber, " +
					"on the replication path, so throughput falls as subscribers grow",
				Remedy: remedyForTierC(rel, info),
			})
		}
	}

	// Live registry findings.
	stats := s.reg.Stats()
	if stats.Unindexed > 0 {
		byRel := map[string]int{}
		for _, sub := range s.reg.All() {
			if !sub.Indexed() {
				byRel[sub.Relation.FullName()]++
			}
		}
		for name, n := range byRel {
			out = append(out, Diagnostic{
				Code: "unindexed_shape", Severity: "low",
				Relation: name,
				Reason:   fmt.Sprintf("%d subscription(s) have no equality filter on an indexed column", n),
				Impact: "these are scanned for every change to the relation instead of being found " +
					"by a map lookup",
				Remedy: "add an equality filter on an indexed column, or index the filtered column",
			})
		}
	}
	if n := stats.ByTier[authz.TierC]; n > 0 && (s.oracle == nil || s.oracle.Name() == oracle.NameRLS) {
		out = append(out, Diagnostic{
			Code: "tier_c_subscriptions", Severity: "high",
			Reason: fmt.Sprintf("%d subscription(s) are authorizing per change", n),
			Impact: "this is the one code path Sluice exists to avoid",
			Remedy: "see the tier_c_policy findings above; or set SLUICE_TIER_C=deny to refuse " +
				"such subscriptions outright",
		})
	}

	if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
		for _, w := range s.holds.All() {
			for _, h := range w.Holds {
				if h.Rel == nil || h.Filter == nil {
					continue
				}
				if missing := hold.MissingReplicaIdentity(h.Rel, h.Filter); len(missing) > 0 {
					out = append(out, Diagnostic{
						Code: "hold_replica_identity", Severity: "high",
						Relation: h.Rel.FullName(),
						Reason: fmt.Sprintf("hold filter column(s) %s are not in the replica identity",
							strings.Join(missing, ", ")),
						Impact: "a DELETE of this hold cannot be cut from the WAL; this is PostgreSQL REPLICA IDENTITY",
						Remedy: "CREATE UNIQUE INDEX on the hold filter columns and ALTER TABLE ... REPLICA IDENTITY USING INDEX",
					})
				}
			}
		}
	}
	return out
}

func remedyForTierC(rel *catalog.Relation, info expr.Info) string {
	if len(info.Subqueries) > 0 {
		return fmt.Sprintf(
			"denormalise the joined column onto %s so the policy becomes a direct comparison "+
				"(for example `team_id = (auth.jwt() ->> 'team')::uuid`), which lets shapes filtering "+
				"on that column resolve to Tier A",
			rel.FullName())
	}
	if len(info.ForeignQualifiers) > 0 {
		return "rewrite the policy to reference only " + rel.FullName() + "'s own columns, " +
			"denormalising if necessary"
	}
	return "rewrite the policy using only comparisons, boolean operators, and the auth.* helpers"
}
