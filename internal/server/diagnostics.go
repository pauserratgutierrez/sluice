package server

import (
	"fmt"
	"net/http"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
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

	if s.reader != nil {
		out["slot"] = s.slotInfo(r)
	}

	stats := s.reg.Stats()
	out["subscriptions"] = map[string]any{
		"total":     stats.Subscriptions,
		"streams":   stats.Streams,
		"relations": stats.Relations,
		"unindexed": stats.Unindexed,
		"by_tier": map[string]int{
			"A": stats.ByTier[authz.TierA],
			"B": stats.ByTier[authz.TierB],
			"C": stats.ByTier[authz.TierC],
		},
	}

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
	if n := stats.ByTier[authz.TierC]; n > 0 {
		out = append(out, Diagnostic{
			Code: "tier_c_subscriptions", Severity: "high",
			Reason: fmt.Sprintf("%d subscription(s) are authorizing per change", n),
			Impact: "this is the one code path Sluice exists to avoid",
			Remedy: "see the tier_c_policy findings above; or set SLUICE_TIER_C=deny to refuse " +
				"such subscriptions outright",
		})
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
