package server

import (
	"fmt"
	"net/http"
	"strings"

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
		writeErr(w, http.StatusForbidden, "forbidden", "service_role required")
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
		out["slot"] = s.slotInfo(r)
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

	// Reset then set, so a resolved warning stops being reported.
	metrics.ConfigWarnings.Reset()
	for _, d := range diags {
		metrics.ConfigWarnings.WithLabelValues(d.Code).Set(1)
	}

	out["publication"] = map[string]any{
		"name":   s.cfg.Publication,
		"tables": len(s.cat.All()),
	}
	out["replication_options"] = map[string]any{
		"proto_version": s.cfg.ProtoVersion,
		"streaming":     s.cfg.Streaming,
		"binary":        s.cfg.Binary,
		"messages":      s.cfg.Messages,
	}

	writeJSON(w, http.StatusOK, out)
}

func (s *Server) slotInfo(r *http.Request) map[string]any {
	info := map[string]any{
		"name":          s.cfg.SlotName,
		"confirmed_lsn": reader.FormatLSN(s.reader.ConfirmedLSN()),
		"received_lsn":  reader.FormatLSN(s.reader.ReceivedLSN()),
	}
	var (
		active   bool
		wal      string
		retained int64
		invalid  *string
	)
	err := s.pool.QueryRow(r.Context(), `
		SELECT active,
		       coalesce(wal_status, 'unknown'),
		       coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn), 0)::bigint,
		       invalidation_reason
		  FROM pg_replication_slots WHERE slot_name = $1`, s.cfg.SlotName).
		Scan(&active, &wal, &retained, &invalid)
	if err == nil {
		info["active"] = active
		info["wal_status"] = wal
		info["retained_bytes"] = retained
		metrics.SlotRetainedBytes.Set(float64(retained))
		if invalid != nil {
			info["invalidation_reason"] = *invalid
		}
	}
	return info
}

// diagnostics builds the warning list from the catalog and the live registry.
func (s *Server) diagnostics() []Diagnostic {
	var out []Diagnostic

	if s.cfg.Streaming != "off" {
		out = append(out, Diagnostic{
			Code: "streaming_enabled", Severity: "low",
			Reason: "streaming is set to " + s.cfg.Streaming,
			Impact: "uncommitted transactions must be buffered in the Go heap until Stream Commit, " +
				"instead of being buffered by PostgreSQL where the spill is disk-backed and observable",
			Remedy: "set SLUICE_STREAMING=off unless a measured workload requires otherwise",
		})
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
				Impact: "toast_flatten_tuple inlines the whole out-of-line value into every old tuple; " +
					"measured 15x more WAL and 3200x larger DELETE messages",
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
						"per query as an InitPlan; measured at 179 ms versus 9 ms over 100,000 rows",
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
					Impact: "snapshots and Tier C probes scan the table instead of seeking; " +
						"measured at 171 ms versus under 0.1 ms over 100,000 rows",
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
				Impact: "measured at roughly 13 microseconds per subscriber per change, which caps " +
					"throughput near 100 changes/sec at 1,000 subscribers",
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
