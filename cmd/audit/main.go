// Command audit is a production-readiness battery for Sluice.
//
// It applies a broad SQL/RLS spectrum, classifies expected Tier A/B/C outcomes,
// exercises live subscribe+DML delivery, probes rare WAL edge cases, and
// scrapes /diagnostics + /metrics for Tier-C remediation quality.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

var (
	authURL   = envOr("SMOKE_AUTH_URL", "http://auth:9999")
	restURL   = envOr("SMOKE_REST_URL", "http://rest:3000")
	sluiceURL = envOr("SMOKE_SLUICE_URL", "http://sluice:4000/sluice/v1")
	dbURL     = envOr("SMOKE_DB_URL", "")
	svcKey    = os.Getenv("SERVICE_ROLE_KEY")

	// runKey keeps rows written by one run from colliding with the last one, so
	// the suite is rerunnable without reapplying the fixtures.
	runKey = int(time.Now().Unix() % 1000000)

	checks, passes, fails int
	findings              []Finding
	tierResults           []TierResult
	dmlResults            []DMLResult
	diagBefore, diagAfter map[string]any
	metricsSnap           map[string]float64
	offlineResults        []OfflineResult
	timing                = map[string]time.Duration{}
)

type Finding struct {
	Severity string `json:"severity"`
	Code     string `json:"code"`
	Title    string `json:"title"`
	Detail   string `json:"detail"`
}

type TierResult struct {
	Sub      string `json:"sub"`
	Table    string `json:"table"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Indexed  bool   `json:"indexed"`
	OK       bool   `json:"ok"`
	Detail   string `json:"detail,omitempty"`
	Warnings []any  `json:"warnings,omitempty"`
}

type DMLResult struct {
	Name     string `json:"name"`
	Tier     string `json:"tier"`
	Op       string `json:"op"`
	OK       bool   `json:"ok"`
	Expected string `json:"expected"`
	Got      string `json:"got"`
	Latency  int64  `json:"latency_ms"`
}

type OfflineResult struct {
	Name        string `json:"name"`
	SQL         string `json:"sql"`
	Expected    string `json:"expected"` // A|B|C (compilable+cols vs not)
	Compilable  bool   `json:"compilable"`
	Unsupported string `json:"unsupported,omitempty"`
	ParseErr    string `json:"parse_err,omitempty"`
	OK          bool   `json:"ok"`
}

type Report struct {
	StartedAt      time.Time          `json:"started_at"`
	FinishedAt     time.Time          `json:"finished_at"`
	DurationMS     int64              `json:"duration_ms"`
	Checks         int                `json:"checks"`
	Passes         int                `json:"passes"`
	Fails          int                `json:"fails"`
	PassRate       float64            `json:"pass_rate"`
	Findings       []Finding          `json:"findings"`
	Offline        []OfflineResult    `json:"offline_policy_spectrum"`
	TierLive       []TierResult       `json:"tier_live"`
	DML            []DMLResult        `json:"dml"`
	Diagnostics    map[string]any     `json:"diagnostics_after"`
	DiagnosticsPre map[string]any     `json:"diagnostics_before"`
	Metrics        map[string]float64 `json:"metrics"`
	TimingsMS      map[string]int64   `json:"timings_ms"`
	PGVersion      string             `json:"pg_version"`
	PubTables      int                `json:"publication_tables"`
	Verdict        string             `json:"verdict"`
}

func main() {
	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
	defer cancel()

	fmt.Println("== Sluice production-readiness audit ==")

	if dbURL == "" {
		dbURL = "postgres://postgres:" + os.Getenv("POSTGRES_PASSWORD") + "@db:5432/postgres"
	}

	pool, err := pgxpool.New(ctx, dbURL)
	must(err, "connect to postgres")
	defer pool.Close()

	var pgVer string
	must(pool.QueryRow(ctx, "select version()").Scan(&pgVer), "pg version")
	fmt.Printf("postgres  %s\n", truncate(pgVer, 90))

	// Offline spectrum first — no network dependency on Sluice behaviour.
	t0 := time.Now()
	offlineResults = runOfflineSpectrum()
	timing["offline_spectrum"] = time.Since(t0)
	fmt.Printf("\n-- offline policy spectrum: %d cases --\n", len(offlineResults))
	offPass, offFail := 0, 0
	for _, r := range offlineResults {
		if r.OK {
			offPass++
			fmt.Printf("  PASS  [%s] %s → compilable=%v\n", r.Expected, r.Name, r.Compilable)
		} else {
			offFail++
			fmt.Printf("  FAIL  [%s] %s → compilable=%v parse=%q unsupported=%q\n",
				r.Expected, r.Name, r.Compilable, r.ParseErr, truncate(r.Unsupported, 80))
		}
	}
	checks += offPass + offFail
	passes += offPass
	fails += offFail

	if svcKey != "" {
		diagBefore, _ = fetchDiagnostics(ctx)
	}

	t0 = time.Now()
	fmt.Println("\n-- applying audit fixtures (expect external psql apply before run) --")
	var pubN int
	must(pool.QueryRow(ctx, `select count(*) from pg_publication_tables where pubname='sluice'`).Scan(&pubN), "count pub")
	fmt.Printf("publication tables: %d\n", pubN)
	if pubN < 20 {
		flag("critical", "fixtures_missing",
			"Audit fixtures do not appear to be published",
			fmt.Sprintf("only %d publication tables; apply deploy/db/audit_fixtures.sql via psql first", pubN))
	}
	// The Tier C fixtures reference public.memberships, and the base fixtures
	// drop that table with CASCADE -- which drops those policies with it. Re-run
	// the compose stack and the audit tables silently lose their RLS. Catch it
	// here rather than reporting it as a misclassification forty checks later.
	var unprotected []string
	if err := pool.QueryRow(ctx, `
		SELECT coalesce(array_agg(c.relname ORDER BY c.relname), '{}')
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = 'public'
		 WHERE c.relrowsecurity
		   AND (c.relname LIKE 'a\_%' OR c.relname LIKE 'b\_%' OR c.relname LIKE 'c\_%'
		     OR c.relname LIKE 'w\_%' OR c.relname LIKE 'e\_%')
		   AND NOT EXISTS (SELECT 1 FROM pg_policy p WHERE p.polrelid = c.oid)`,
	).Scan(&unprotected); err == nil && len(unprotected) > 0 {
		fatal("these fixture tables have RLS enabled but no policy: %v\n"+
			"re-apply deploy/db/audit_fixtures.sql (the base fixtures drop public.memberships CASCADE, "+
			"which drops every policy that references it)", unprotected)
	}
	timing["apply_fixtures"] = time.Since(t0)

	// Catalog refresh is 30s by default; wait a bit then also poke via DML so
	// Relation messages fire for newly published tables after refresh.
	fmt.Println("waiting for catalog refresh (35s)…")
	t0 = time.Now()
	select {
	case <-ctx.Done():
		fatal("context cancelled waiting for catalog")
	case <-time.After(35 * time.Second):
	}
	timing["catalog_wait"] = time.Since(t0)

	email := fmt.Sprintf("audit-%d@example.test", time.Now().UnixNano())
	tok, userID, err := signUp(ctx, email, "sluice-audit-password-1")
	must(err, "signup")
	fmt.Printf("user  %s\n", userID)

	// Ensure membership for Tier C team tables.
	mustExec(ctx, pool, fmt.Sprintf(
		`insert into memberships (user_id, team_id) values ('%s', 1) on conflict do nothing`, userID))

	if svcKey != "" {
		diagAfter, _ = fetchDiagnostics(ctx)
	}

	t0 = time.Now()
	runLiveTier(ctx, pool, tok, userID)
	timing["live_tiers"] = time.Since(t0)

	t0 = time.Now()
	runDMLBattery(ctx, pool, tok, userID)
	timing["dml_battery"] = time.Since(t0)

	t0 = time.Now()
	runRareCases(ctx, pool, tok, userID)
	timing["rare_cases"] = time.Since(t0)

	t0 = time.Now()
	runBypassRLS(ctx, pool, tok)
	timing["bypassrls"] = time.Since(t0)

	t0 = time.Now()
	runRevocation(ctx, pool, tok, userID)
	timing["revocation"] = time.Since(t0)

	t0 = time.Now()
	runSecurityNegatives(ctx, tok)
	timing["security"] = time.Since(t0)

	t0 = time.Now()
	runLoadProbe(ctx, pool, tok, userID)
	timing["load"] = time.Since(t0)

	if svcKey != "" {
		diagAfter, _ = fetchDiagnostics(ctx)
		analyzeDiagnostics(diagAfter)
	}
	metricsSnap = scrapeMetrics(ctx)

	analyzeCodeFindings()

	finished := time.Now()
	passRate := 0.0
	if checks > 0 {
		passRate = float64(passes) / float64(checks) * 100
	}
	verdict := "NOT PRODUCTION READY"
	switch {
	case fails == 0 && countFindings("critical")+countFindings("high") == 0:
		verdict = "PRODUCTION CANDIDATE (pending burn-in)"
	case fails == 0:
		verdict = "FUNCTIONALLY SOUND — OPERATIONAL GAPS REMAIN"
	case fails > 0 && passRate >= 90:
		verdict = "MOSTLY SOUND — FIX FAILING ASSERTIONS BEFORE PRODUCTION"
	}

	timingsMS := map[string]int64{}
	for k, v := range timing {
		timingsMS[k] = v.Milliseconds()
	}

	rep := Report{
		StartedAt: start, FinishedAt: finished, DurationMS: finished.Sub(start).Milliseconds(),
		Checks: checks, Passes: passes, Fails: fails, PassRate: passRate,
		Findings: findings, Offline: offlineResults, TierLive: tierResults, DML: dmlResults,
		Diagnostics: diagAfter, DiagnosticsPre: diagBefore, Metrics: metricsSnap,
		TimingsMS: timingsMS, PGVersion: pgVer, PubTables: pubN, Verdict: verdict,
	}

	raw, _ := json.MarshalIndent(rep, "", "  ")
	outPath := envOr("AUDIT_OUT", "/out/audit-report.json")
	_ = os.WriteFile(outPath, raw, 0o644)
	fmt.Printf("\n== %d checks, %d pass, %d fail (%.1f%%) ==\n", checks, passes, fails, passRate)
	fmt.Printf("verdict: %s\n", verdict)
	fmt.Printf("report:  %s\n", outPath)
	if fails > 0 {
		os.Exit(1)
	}
}

// ---------------------------------------------------------------------------
// Offline spectrum: parse + analyze against PostgreSQL-shaped policy texts.
// Expected: "A/B" means compilable (Tier A or B depending on filter coverage),
// "C" means not compilable.
// ---------------------------------------------------------------------------

func runOfflineSpectrum() []OfflineResult {
	type tc struct {
		name, sql, expect string // expect: AB | C
	}
	cases := []tc{
		// --- should be compilable (AB) ---
		{"eq_auth_uid", `(owner_id = auth.uid())`, "AB"},
		{"expanded_jwt_sub", `(owner_id = ((current_setting('request.jwt.claims'::text, true))::jsonb ->> 'sub'::text)::uuid)`, "AB"},
		{"auth_role", `(auth.role() = 'authenticated'::text)`, "AB"},
		{"auth_email_lower", `(lower(email) = lower(auth.email()))`, "AB"},
		{"auth_jwt_arrow", `(owner_id = ((auth.jwt() ->> 'sub'::text))::uuid)`, "AB"},
		{"or_public", `((visibility = 'public'::text) OR (owner_id = auth.uid()))`, "AB"},
		{"and_is_null", `((owner_id = auth.uid()) AND (deleted_at IS NULL))`, "AB"},
		{"in_list", `((owner_id = auth.uid()) AND (status IN ('open'::text, 'pending'::text)))`, "AB"},
		{"any_array", `(visibility = ANY (ARRAY['public'::text, 'unlisted'::text]))`, "AB"},
		{"between", `((owner_id = auth.uid()) AND (score BETWEEN 1 AND 100))`, "AB"},
		{"coalesce", `(coalesce(owner_id, '00000000-0000-4000-8000-000000000000'::uuid) = auth.uid())`, "AB"},
		{"nullif", `((owner_id = auth.uid()) OR (nullif(status, 'draft'::text) IS NOT NULL))`, "AB"},
		{"is_distinct", `((owner_id IS NOT DISTINCT FROM auth.uid()) OR (flag = 'open'::text))`, "AB"},
		{"not_and", `((NOT hidden) AND ((visibility = 'public'::text) OR (owner_id = auth.uid())))`, "AB"},
		{"ilike", `((title ILIKE 'public-%'::text) OR (owner_id = auth.uid()))`, "AB"},
		{"like", `((title LIKE 'x%'::text) OR (owner_id = auth.uid()))`, "AB"},
		{"arith", `((owner_id = auth.uid()) OR ((a + b) > 100))`, "AB"},
		{"concat", `((owner_id = auth.uid()) OR ((prefix || suffix) = 'okok'::text))`, "AB"},
		{"length", `((owner_id = auth.uid()) OR (length(body) > 10))`, "AB"},
		{"jsonb_path", `((owner_id = auth.uid()) AND ((meta ->> 'kind'::text) = 'ok'::text))`, "AB"},
		{"neq", `((status <> 'draft'::text) OR (owner_id = auth.uid()))`, "AB"},
		{"lte_gte", `((score >= 50) OR (owner_id = auth.uid()))`, "AB"},
		{"volatile_now", `((owner_id = auth.uid()) OR (expires_at > now()))`, "AB"},
		{"trim", `(btrim(status) = 'ok'::text)`, "AB"},
		{"upper", `(upper(code) = 'ABC'::text)`, "AB"},
		{"is_true", `(active IS TRUE)`, "AB"},
		{"is_not_false", `(active IS NOT FALSE)`, "AB"},
		{"cast_numeric", `(amount > (0)::numeric)`, "AB"},

		// --- the recommended InitPlan wrapper, as PostgreSQL stores it ---
		{"wrapped_uid", `(owner_id = ( SELECT auth.uid() AS uid))`, "AB"},
		{"wrapped_jwt", `(owner_id = (( SELECT (auth.jwt() ->> 'sub'::text)))::uuid)`, "AB"},
		{"wrapped_role", `((( SELECT auth.role() AS role)) = 'authenticated'::text)`, "AB"},
		{"wrapped_setting", `(owner_id = ((( SELECT current_setting('request.jwt.claims'::text, true) AS current_setting))::jsonb ->> 'sub'::text)::uuid)`, "AB"},
		{"wrapped_or", `((visibility = 'public'::text) OR (owner_id = ( SELECT auth.uid() AS uid)))`, "AB"},
		{"wrapped_in_scalar", `(owner_id IN ( SELECT auth.uid() AS uid))`, "AB"},
		{"wrapped_any_scalar", `(owner_id = ANY ( SELECT auth.uid() AS uid))`, "AB"},
		{"wrapped_nested", `((( SELECT auth.uid() AS uid) = owner_id) AND (deleted_at IS NULL))`, "AB"},

		// --- must force Tier C ---
		{"exists_subquery", `(EXISTS ( SELECT 1 FROM memberships m WHERE ((m.team_id = invoices.team_id) AND (m.user_id = auth.uid()))))`, "C"},
		{"in_select", `(team_id IN ( SELECT m.team_id FROM memberships m WHERE (m.user_id = auth.uid())))`, "C"},
		{"any_select", `(team_id = ANY ( SELECT m.team_id FROM memberships m WHERE (m.user_id = auth.uid())))`, "C"},
		{"scalar_subquery", `(team_id = ( SELECT m.team_id FROM memberships m WHERE (m.user_id = auth.uid()) LIMIT 1))`, "C"},
		{"case_expr", `(CASE WHEN (status = 'open'::text) THEN (owner_id = auth.uid()) ELSE false END)`, "C"},
		{"udf_custom", `(team_id = public.audit_team_of(auth.uid()))`, "C"},
		{"current_user", `((CURRENT_USER = 'authenticated'::name) AND (owner_id = auth.uid()))`, "C"},
		{"has_table_privilege", `(has_table_privilege('authenticated'::text, 'public.t'::text, 'SELECT'::text))`, "C"},
		{"all_array", `((owner_id = auth.uid()) AND (score = ALL (ARRAY[1, 1, 1])))`, "C"},
		{"regexp_match_op", `((owner_id = auth.uid()) AND (code ~ '^[A-Z]+$'::text))`, "C"},
		{"extract", `((owner_id = auth.uid()) AND (EXTRACT(epoch FROM ts) > (0)::numeric))`, "C"},
		{"greatest", `((owner_id = auth.uid()) AND (greatest(a, b) > 0))`, "C"},
		{"pg_has_role", `(pg_has_role('authenticated'::name, 'member'::text))`, "C"},
		{"array_literal_contains", `(tags @> ARRAY['x'::text])`, "C"},
		{"array_contained_by", `(tags <@ ARRAY['x'::text])`, "C"},
		{"json_path_op", `((meta #>> '{a,b}'::text[]) = 'x'::text)`, "C"},
		{"wrapped_real_subquery", `(team_id IN ( SELECT m.team_id FROM memberships m WHERE (m.user_id = ( SELECT auth.uid() AS uid))))`, "C"},
		{"wrapped_exists", `(EXISTS ( SELECT 1 FROM memberships m WHERE (m.user_id = ( SELECT auth.uid() AS uid))))`, "C"},
		{"version_fn", `(version() IS NOT NULL)`, "C"},
		{"current_database", `(current_database() = 'postgres'::name)`, "C"},
		{"to_jsonb", `(to_jsonb(owner_id) IS NOT NULL)`, "C"},
		{"date_trunc", `(date_trunc('day'::text, created_at) < now())`, "C"},
		{"age_fn", `(age(created_at) < '1 day'::interval)`, "C"},
		{"array_length", `(array_length(tags, 1) > 0)`, "C"},
		{"cardinality", `(cardinality(tags) > 0)`, "C"},
		{"substring", `(substring(title from 1 for 3) = 'abc'::text)`, "C"},
		{"overlay", `(overlay(title placing 'x' from 1 for 1) = 'x'::text)`, "C"},
		{"position", `(position('a'::text in title) > 0)`, "C"},
		{"strpos", `(strpos(title, 'a'::text) > 0)`, "C"},
		{"abs", `(abs(score) > 0)`, "C"},
		{"round", `(round(amount) > (0)::numeric)`, "C"},
		{"ceil", `(ceil(amount) > (0)::numeric)`, "C"},
		{"bitwise_and", `((flags & 1) = 1)`, "C"},
		{"fts_match", `(to_tsvector('english'::regconfig, body) @@ plainto_tsquery('english'::regconfig, 'x'::text))`, "C"},
		{"gen_random_uuid_cmp", `(owner_id <> gen_random_uuid())`, "C"},
		{"row_security_active", `row_security_active()`, "C"},
		{"session_user", `(session_user = 'authenticated'::name)`, "C"},
		{"inet_client_addr", `(inet_client_addr() IS NULL)`, "C"},
		{"jsonb_path_query", `(jsonb_path_query_first(meta, '$.a'::jsonpath) IS NOT NULL)`, "C"},
		{"least", `(least(a, b) > 0)`, "C"},
		{"nullif_nested_udf", `(nullif(public.audit_team_of(auth.uid()), 0) IS NOT NULL)`, "C"},
		{"correlated_foreign", `(other.team_id = team_id)`, "C"},
		{"with_select", `(EXISTS ( WITH x AS (SELECT 1 AS n) SELECT 1 FROM x ))`, "C"},
		{"union_subquery", `(EXISTS ( SELECT 1 UNION SELECT 2 ))`, "C"},
		{"limit_offset_subq", `(EXISTS ( SELECT 1 FROM memberships LIMIT 1 OFFSET 0 ))`, "C"},
		{"order_by_subq", `(EXISTS ( SELECT 1 FROM memberships ORDER BY team_id ))`, "C"},
		{"group_by_subq", `(EXISTS ( SELECT 1 FROM memberships GROUP BY team_id HAVING count(*) > 0 ))`, "C"},
		{"window_subq", `(EXISTS ( SELECT count(*) OVER () FROM memberships ))`, "C"},
		{"lateral_subq", `(EXISTS ( SELECT 1 FROM memberships m, LATERAL (SELECT m.team_id) s ))`, "C"},
		{"distinct_subq", `(EXISTS ( SELECT DISTINCT team_id FROM memberships ))`, "C"},
		{"except_subq", `(EXISTS ( SELECT 1 EXCEPT SELECT 2 ))`, "C"},
		{"intersect_subq", `(EXISTS ( SELECT 1 INTERSECT SELECT 1 ))`, "C"},
		{"values_subq", `(EXISTS ( VALUES (1) ))`, "C"},
		{"cast_regclass", `(has_table_privilege('authenticated', 'public.t'::regclass, 'select'))`, "C"},
		{"timezone_fn", `(timezone('UTC'::text, ts) < now())`, "C"},
		{"make_interval", `(expires_at < (now() + make_interval(days => 1)))`, "C"},
		{"json_array_length", `(json_array_length(meta::json) > 0)`, "C"},
		{"starts_with", `(starts_with(title, 'x'::text))`, "C"},
		{"left_fn", `(left(title, 1) = 'x'::text)`, "C"},
		{"right_fn", `(right(title, 1) = 'x'::text)`, "C"},
		{"md5", `(md5(title) = md5('x'::text))`, "C"},
		{"encode", `(encode(decode('xx','hex'),'hex') IS NOT NULL)`, "C"},
	}

	var out []OfflineResult
	for _, c := range cases {
		r := OfflineResult{Name: c.name, SQL: c.sql, Expected: c.expect}
		n, err := expr.Parse(c.sql)
		if err != nil {
			r.ParseErr = err.Error()
			r.Compilable = false
			// Parse failure is fail-closed → Tier C behaviour.
			r.OK = c.expect == "C"
			out = append(out, r)
			continue
		}
		info := expr.Analyze(n, "t")
		r.Compilable = info.Compilable()
		r.Unsupported = info.Unsupported
		if c.expect == "AB" {
			r.OK = r.Compilable
		} else {
			r.OK = !r.Compilable
		}
		out = append(out, r)
	}
	return out
}

// ---------------------------------------------------------------------------
// Live tier classification against published audit tables.
// ---------------------------------------------------------------------------

type liveCase struct {
	sub, table, filter, expect string
}

func liveCases(userID, email string) []liveCase {
	return []liveCase{
		{"a_owner", "a_owner", "owner_id=eq." + userID, "A"},
		{"a_role", "a_role", "owner_id=eq." + userID, "A"},
		{"a_jwt", "a_jwt_claim", "owner_id=eq." + userID, "A"},
		{"a_coal", "a_coalesce", "owner_id=eq." + userID, "A"},
		{"a_email", "a_email_lower", "email=eq." + strings.ToLower(email), "A"},
		{"a_norls", "a_norls", "", "A"},

		// Tier A requires the shape to pin EVERY column the predicate reads, not
		// just the owner column. These three pin the second column too, so the
		// whole predicate reduces to a constant.
		{"a_in", "a_in_list", "owner_id=eq." + userID + ",status=eq.open", "A"},
		{"a_any", "a_any_array", "owner_id=eq." + userID + ",visibility=eq.public", "A"},
		{"a_bet", "a_between", "owner_id=eq." + userID + ",score=eq.50", "A"},

		// These two read a column no equality filter can pin -- `deleted_at IS
		// NULL` and a jsonb path -- so Tier B is the correct answer, not a defect.
		{"a_null", "a_is_null", "owner_id=eq." + userID, "B"},
		{"a_json", "a_jsonb_path", "owner_id=eq." + userID, "B"},
		{"b_or", "b_or_public", "owner_id=eq." + userID, "B"},
		{"b_thr", "b_threshold", "owner_id=eq." + userID, "B"},
		{"b_not", "b_not_and", "owner_id=eq." + userID, "B"},
		{"b_like", "b_like", "owner_id=eq." + userID, "B"},
		{"b_dist", "b_is_distinct", "owner_id=eq." + userID, "B"},
		{"b_nullif", "b_nullif", "owner_id=eq." + userID, "B"},
		{"b_arith", "b_arith", "owner_id=eq." + userID, "B"},
		{"b_concat", "b_concat", "owner_id=eq." + userID, "B"},
		{"b_len", "b_length", "owner_id=eq." + userID, "B"},
		{"b_now", "b_volatile_now", "owner_id=eq." + userID, "B"},
		{"c_ex", "c_exists", "team_id=eq.1", "C"},
		{"c_in", "c_in_select", "team_id=eq.1", "C"},
		{"c_any", "c_any_select", "team_id=eq.1", "C"},
		{"c_udf", "c_udf", "team_id=eq.1", "C"},
		{"c_case", "c_case", "owner_id=eq." + userID, "C"},
		{"c_cu", "c_current_user", "owner_id=eq." + userID, "C"},
		{"c_hp", "c_has_priv", "owner_id=eq." + userID, "C"},
		{"c_sc", "c_scalar_subq", "team_id=eq.1", "C"},
		{"c_all", "c_all_array", "owner_id=eq." + userID, "C"},
		{"c_re", "c_regexp", "owner_id=eq." + userID, "C"},
		{"c_ext", "c_extract", "owner_id=eq." + userID, "C"},
		{"c_gr", "c_greatest", "owner_id=eq." + userID, "C"},
		{"c_rc", "c_restrictive_combo", "team_id=eq.1", "C"},
		{"c_mp", "c_multi_perm", "owner_id=eq." + userID, "C"},
		{"e_pk", "e_composite_pk", "owner_id=eq." + userID, "A"},
		{"e_gen", "e_generated", "owner_id=eq." + userID, "A"},
		{"e_toast", "e_toast", "owner_id=eq." + userID, "A"},
		{"e_nopk", "e_no_pk", "owner_id=eq." + userID, "A"},

		// The Supabase-recommended spelling: every per-query call wrapped in a
		// scalar subquery. Must classify identically to the direct spelling.
		{"w_owner", "w_owner", "owner_id=eq." + userID, "A"},
		{"w_role", "w_role", "owner_id=eq." + userID, "A"},
		{"w_jwt", "w_jwt", "owner_id=eq." + userID, "A"},
		{"w_or", "w_or_public", "owner_id=eq." + userID, "B"},
		{"w_in", "w_in_scalar", "owner_id=eq." + userID, "A"},
		{"w_any", "w_any_scalar", "owner_id=eq." + userID, "A"},
		{"w_bad", "w_bad_practice", "owner_id=eq." + userID, "A"},
	}
}

// runBypassRLS checks the one authorization path with no policy behind it.
//
// PostgreSQL's check_enable_rls() skips row security entirely for a role holding
// BYPASSRLS, so Sluice must too, or it contradicts what the same JWT gets from
// PostgREST -- the oracle the rest of this audit measures against.
//
// It is also the path where the two halves of Sluice could disagree with each
// other. The initial snapshot impersonates through set_config('role'), so
// PostgreSQL applies the bypass to it whether or not Sluice models it; the
// streaming path evaluates catalog.Predicate in process, so it does not. Until
// the bypass was modeled, service_role was refused at subscribe time on any
// table whose policies did not happen to name it, while a snapshot of that same
// table would have returned every row.
//
// The negative matters as much as the positive: the grant has to be earned from
// pg_roles, never inferred from the string "service_role".
func runBypassRLS(ctx context.Context, pool *pgxpool.Pool, userTok string) {
	fmt.Println("\n-- BYPASSRLS (service_role) --")
	if svcKey == "" {
		flag("medium", "bypassrls_unverified",
			"BYPASSRLS behaviour was not exercised",
			"SERVICE_ROLE_KEY was unset, so the audit could not open a service_role stream.")
		return
	}

	// The table must have RLS on and no policy naming service_role, or the test
	// proves nothing: a grant could come from a policy rather than the bypass.
	const table = "documents"
	var rlsOn bool
	var namesService bool
	if err := pool.QueryRow(ctx, `
		SELECT c.relrowsecurity,
		       EXISTS (SELECT 1 FROM pg_policy p
		                WHERE p.polrelid = c.oid
		                  AND 'service_role' = ANY (SELECT pg_get_userbyid(r)
		                                              FROM unnest(p.polroles) r))
		  FROM pg_class c
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relname = $1`, table).Scan(&rlsOn, &namesService); err != nil {
		check(false, "read RLS state for public."+table, err.Error())
		return
	}
	check(rlsOn && !namesService,
		fmt.Sprintf("public.%s has RLS on and no policy naming service_role", table),
		fmt.Sprintf("rls=%v names_service_role=%v", rlsOn, namesService))

	var hasAttr bool
	if err := pool.QueryRow(ctx,
		`SELECT rolbypassrls OR rolsuper FROM pg_roles WHERE rolname = 'service_role'`,
	).Scan(&hasAttr); err != nil {
		check(false, "read service_role attributes from pg_roles", err.Error())
		return
	}
	check(hasAttr, "service_role holds BYPASSRLS in this deployment",
		fmt.Sprintf("rolbypassrls or rolsuper = %v", hasAttr))

	body := fmt.Sprintf(
		`{"subscriptions":[{"sub":"bypass","shape":{"schema":"public","table":%q}}]}`, table)

	svcStream, err := openStream(ctx, svcKey, body)
	if err != nil {
		check(false, "open a service_role stream on an RLS-protected table", err.Error())
		return
	}
	defer svcStream.Close()

	ready, err := svcStream.next(20 * time.Second)
	if err != nil || ready.Name != "ready" {
		check(false, "ready event for the service_role stream", fmt.Sprintf("%v %s", err, ready.Name))
		return
	}
	got := readySubs(ready)["bypass"]

	check(got.Err == "",
		"service_role is not refused on a table whose policies do not name it",
		fmt.Sprintf("err=%q reason=%q", got.Err, got.Reason))
	check(got.Tier == "A",
		"service_role resolves to Tier A, with no per-change work to do",
		fmt.Sprintf("got tier=%q", got.Tier))
	// The reason is what an operator reads in /diagnostics when a protected
	// table shows an unrestricted subscription. "no row-level security applies"
	// would be a lie here: RLS is on, the role simply outranks it.
	check(strings.Contains(got.Reason, "bypasses row-level security"),
		"the grant explains itself as a bypass rather than as an absence of RLS",
		fmt.Sprintf("reason=%q", got.Reason))

	// Negative control: the same shape for a role without the attribute must
	// still be constrained by the policy.
	userStream, err := openStream(ctx, userTok, body)
	if err != nil {
		check(false, "open an authenticated stream on the same table", err.Error())
		return
	}
	defer userStream.Close()

	uReady, err := userStream.next(20 * time.Second)
	if err != nil || uReady.Name != "ready" {
		check(false, "ready event for the authenticated stream", fmt.Sprintf("%v %s", err, uReady.Name))
		return
	}
	uGot := readySubs(uReady)["bypass"]
	check(uGot.Tier != "A" || uGot.Err != "",
		"an authenticated caller on the same shape is still subject to the policy",
		fmt.Sprintf("got tier=%q err=%q reason=%q", uGot.Tier, uGot.Err, uGot.Reason))
}

// catalogSettle is how long to wait for a catalog change to reach open streams.
// Revocation is not pushed: it lands on the SLUICE_CATALOG_REFRESH tick, which
// the harness sets to 5s.
const catalogSettle = 12 * time.Second

// runRevocation checks the other half of a subscribe-time authorization model.
//
// Resolving once is only sound if the resolution can be withdrawn. Nothing in
// PostgreSQL pushes a notification when a policy is dropped or a role attribute
// changes, so Sluice re-resolves on a timer and compares a fingerprint of
// everything an authorization decision reads. These two cases are the ones that
// fingerprint exists for, and both were silently unenforceable before it: a
// stable Tier A or Tier B decision carries no lease, so nothing ever revisited
// it, and a dropped policy stayed enforceable-but-unenforced for the life of the
// stream.
//
// Each case restores what it changed on the way out, including on failure. A
// half-reverted harness fails later tests for reasons that have nothing to do
// with them.
func runRevocation(ctx context.Context, pool *pgxpool.Pool, userTok, userID string) {
	fmt.Println("\n-- revocation reaches open streams --")
	revokeByPolicy(ctx, pool, userTok, userID)
	if svcKey != "" {
		revokeByRoleAttribute(ctx, pool)
	}
}

// revokeByPolicy drops every SELECT policy on a table and expects open streams
// to lose access.
func revokeByPolicy(ctx context.Context, pool *pgxpool.Pool, userTok, userID string) {
	const table = "documents"

	type policy struct{ name, cmd, roles, using, check string }
	var saved []policy
	rows, err := pool.Query(ctx, `
		SELECT p.polname, p.polcmd::text,
		       COALESCE((SELECT string_agg(quote_ident(pg_get_userbyid(r)), ',')
		                   FROM unnest(p.polroles) r WHERE r <> 0), 'public'),
		       COALESCE(pg_get_expr(p.polqual, p.polrelid), ''),
		       COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
		  FROM pg_policy p JOIN pg_class c ON c.oid = p.polrelid
		  JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relname = $1`, table)
	if err != nil {
		check(false, "read policies on public."+table, err.Error())
		return
	}
	for rows.Next() {
		var p policy
		if err := rows.Scan(&p.name, &p.cmd, &p.roles, &p.using, &p.check); err != nil {
			rows.Close()
			check(false, "scan policies on public."+table, err.Error())
			return
		}
		saved = append(saved, p)
	}
	rows.Close()
	if len(saved) == 0 {
		check(false, "public."+table+" has policies to drop", "none found")
		return
	}

	restore := func() {
		for _, p := range saved {
			cmd := map[string]string{"r": "SELECT", "a": "INSERT", "w": "UPDATE", "d": "DELETE", "*": "ALL"}[p.cmd]
			stmt := fmt.Sprintf("CREATE POLICY %q ON public.%s FOR %s TO %s", p.name, table, cmd, p.roles)
			if p.using != "" {
				stmt += " USING (" + p.using + ")"
			}
			if p.check != "" {
				stmt += " WITH CHECK (" + p.check + ")"
			}
			if _, err := pool.Exec(ctx, stmt); err != nil {
				check(false, "restore policy "+p.name, err.Error())
			}
		}
	}
	defer restore()

	body := fmt.Sprintf(
		`{"subscriptions":[{"sub":"r","shape":{"schema":"public","table":%q,"filter":"owner_id=eq.%s"}}]}`,
		table, userID)
	st, err := openStream(ctx, userTok, body)
	if err != nil {
		check(false, "open a stream before dropping the policy", err.Error())
		return
	}
	defer st.Close()

	ready, err := st.next(20 * time.Second)
	if err != nil || ready.Name != "ready" {
		check(false, "ready event before dropping the policy", fmt.Sprintf("%v %s", err, ready.Name))
		return
	}
	got := readySubs(ready)["r"]
	check(got.Err == "" && got.Tier != "",
		"the subscription is authorized before the policy is dropped",
		fmt.Sprintf("tier=%q err=%q", got.Tier, got.Err))
	// A leased decision would be re-resolved anyway; the point of this test is
	// the stable case that nothing used to revisit.
	check(got.Tier == "A" || got.Tier == "B",
		"the decision is a stable one, which is the case that carries no lease",
		fmt.Sprintf("tier=%q", got.Tier))

	for _, p := range saved {
		if _, err := pool.Exec(ctx, fmt.Sprintf("DROP POLICY %q ON public.%s", p.name, table)); err != nil {
			check(false, "drop policy "+p.name, err.Error())
			return
		}
	}

	// Assert on the error the server sends, not merely on silence: an absent
	// change event is also what a broken test looks like.
	check(waitForError(st, "shape_not_authorized", catalogSettle),
		"dropping every SELECT policy revokes an already-resolved subscription",
		"expected a shape_not_authorized error within "+catalogSettle.String())
}

// revokeByRoleAttribute takes BYPASSRLS away from service_role and expects the
// grant that attribute produced to be withdrawn. The bypass set is part of the
// authorization fingerprint precisely so that this works.
func revokeByRoleAttribute(ctx context.Context, pool *pgxpool.Pool) {
	defer func() {
		if _, err := pool.Exec(ctx, "ALTER ROLE service_role BYPASSRLS"); err != nil {
			check(false, "restore BYPASSRLS on service_role", err.Error())
		}
	}()

	body := `{"subscriptions":[{"sub":"b","shape":{"schema":"public","table":"documents"}}]}`
	st, err := openStream(ctx, svcKey, body)
	if err != nil {
		check(false, "open a service_role stream before revoking BYPASSRLS", err.Error())
		return
	}
	defer st.Close()

	ready, err := st.next(20 * time.Second)
	if err != nil || ready.Name != "ready" {
		check(false, "ready event for the service_role stream", fmt.Sprintf("%v %s", err, ready.Name))
		return
	}
	check(readySubs(ready)["b"].Tier == "A",
		"service_role is granted Tier A while it holds BYPASSRLS",
		fmt.Sprintf("tier=%q", readySubs(ready)["b"].Tier))

	if _, err := pool.Exec(ctx, "ALTER ROLE service_role NOBYPASSRLS"); err != nil {
		check(false, "revoke BYPASSRLS from service_role", err.Error())
		return
	}
	check(waitForError(st, "shape_not_authorized", catalogSettle),
		"ALTER ROLE NOBYPASSRLS revokes an already-resolved service_role stream",
		"expected a shape_not_authorized error within "+catalogSettle.String())
}

// waitForError reports whether the stream delivers an error with this code
// before the deadline. Other events are ignored rather than treated as failure:
// a change already in flight when the policy was dropped is not a counterexample.
func waitForError(st *stream, code string, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return false
		}
		ev, err := st.next(remaining)
		if err != nil {
			return false
		}
		if ev.Name != "error" {
			continue
		}
		var payload struct {
			Code string `json:"code"`
		}
		if json.Unmarshal(ev.Data, &payload) == nil && payload.Code == code {
			return true
		}
	}
}

// maxShapesPerStream mirrors SLUICE_MAX_SHAPES_PER_STREAM. The matrix is split
// into streams no larger than this, because exceeding it is refused -- which is
// the point of the limit.
const maxShapesPerStream = 20

type subReady struct {
	Tier, Err, Reason string
	Indexed           bool
	Warnings          []any
}

// readySubs decodes the per-subscription results carried by a ready event.
func readySubs(ready sseEvent) map[string]subReady {
	var payload struct {
		Subscriptions []struct {
			Sub      string `json:"sub"`
			Tier     string `json:"tier"`
			Indexed  bool   `json:"indexed"`
			Warnings []any  `json:"warnings"`
			Reason   string `json:"reason"`
			Error    *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"subscriptions"`
	}
	_ = json.Unmarshal(ready.Data, &payload)
	out := map[string]subReady{}
	for _, s := range payload.Subscriptions {
		r := subReady{Tier: s.Tier, Reason: s.Reason, Indexed: s.Indexed, Warnings: s.Warnings}
		if s.Error != nil {
			r.Err = s.Error.Code + ": " + s.Error.Message
		}
		out[s.Sub] = r
	}
	return out
}

func runLiveTier(ctx context.Context, pool *pgxpool.Pool, tok, userID string) {
	fmt.Println("\n-- live tier classification --")
	email := ""
	_ = pool.QueryRow(ctx, `select email from auth.users where id=$1`, userID).Scan(&email)
	cases := liveCases(userID, email)

	bySub := map[string]subReady{}
	for start := 0; start < len(cases); start += maxShapesPerStream {
		end := min(start+maxShapesPerStream, len(cases))

		var subs []string
		for _, c := range cases[start:end] {
			shape := fmt.Sprintf(`{"schema":"public","table":%q`, c.table)
			if c.filter != "" {
				shape += fmt.Sprintf(`,"filter":%q`, c.filter)
			}
			shape += `}`
			subs = append(subs, fmt.Sprintf(`{"sub":%q,"shape":%s}`, c.sub, shape))
		}
		body := fmt.Sprintf(`{"subscriptions":[%s]}`, strings.Join(subs, ","))
		stream, err := openStream(ctx, tok, body)
		if err != nil {
			check(false, fmt.Sprintf("open tier-matrix stream for shapes %d..%d", start, end), err.Error())
			return
		}
		defer stream.Close()

		ready, err := stream.next(20 * time.Second)
		if err != nil || ready.Name != "ready" {
			check(false, "ready event for tier matrix", fmt.Sprintf("%v %s", err, ready.Name))
			return
		}
		for sub, r := range readySubs(ready) {
			bySub[sub] = r
		}
	}
	check(len(bySub) == len(cases), "every shape in the matrix produced a result",
		fmt.Sprintf("got %d of %d", len(bySub), len(cases)))

	for _, c := range cases {
		got := bySub[c.sub]
		ok := got.Tier == c.expect
		detail := ""
		if !ok {
			detail = fmt.Sprintf("got tier=%q err=%q reason=%q", got.Tier, got.Err, got.Reason)
			// Under SLUICE_TIER_C=deny a Tier C shape is refused outright, which
			// is the configured behaviour rather than a misclassification.
			if c.expect == "C" && strings.Contains(got.Err, "policy_requires_impersonation") {
				ok = true
				detail = "refused as configured: " + got.Err
			}
		}
		tierResults = append(tierResults, TierResult{
			Sub: c.sub, Table: c.table, Expected: c.expect, Actual: got.Tier,
			Indexed: got.Indexed, OK: ok, Detail: detail, Warnings: got.Warnings,
		})
		check(ok, fmt.Sprintf("tier %s.%s → %s", c.table, c.sub, c.expect), detail)
	}

	// Keep stream for a moment so diagnostics sees Tier C live subscriptions.
	if svcKey != "" {
		d, _ := fetchDiagnostics(ctx)
		analyzeDiagnostics(d)
		diagAfter = d
	}
}

// ---------------------------------------------------------------------------
// DML battery across tiers
// ---------------------------------------------------------------------------

func runDMLBattery(ctx context.Context, pool *pgxpool.Pool, tok, userID string) {
	fmt.Println("\n-- DML delivery battery --")
	email := ""
	_ = pool.QueryRow(ctx, `select email from auth.users where id=$1`, userID).Scan(&email)

	body := fmt.Sprintf(`{"subscriptions":[
		{"sub":"d_a","shape":{"schema":"public","table":"a_owner","filter":"owner_id=eq.%s","columns":["id","title","owner_id"]}},
		{"sub":"d_b","shape":{"schema":"public","table":"b_or_public"}},
		{"sub":"d_c","shape":{"schema":"public","table":"c_exists","filter":"team_id=eq.1"}},
		{"sub":"d_toast","shape":{"schema":"public","table":"e_toast","filter":"owner_id=eq.%s"}},
		{"sub":"d_gen","shape":{"schema":"public","table":"e_generated","filter":"owner_id=eq.%s"}},
		{"sub":"d_pk","shape":{"schema":"public","table":"e_composite_pk","filter":"owner_id=eq.%s"}},
		{"sub":"d_norls","shape":{"schema":"public","table":"a_norls"}},
		{"sub":"d_room","channel":"room:audit"}
	]}`, userID, userID, userID, userID)

	stream, err := openStream(ctx, tok, body)
	must(err, "open DML stream")
	defer stream.Close()
	ready, err := stream.next(15 * time.Second)
	must(err, "DML ready")
	if ready.Name != "ready" {
		fatal("expected ready, got %s", ready.Name)
	}
	// A refused subscription here would make every assertion below vacuous.
	for sub, r := range readySubs(ready) {
		if r.Err != "" {
			check(false, "DML subscription "+sub+" was accepted", r.Err)
		}
	}

	type step struct {
		name, tier, op, sql string
		wantSub             string
		wantTitle           string
		wantAbsent          string
		checkUnchanged      bool
	}
	steps := []step{
		{"A insert own", "A", "INSERT",
			fmt.Sprintf(`insert into a_owner (owner_id, title) values ('%s','a-mine')`, userID),
			"d_a", "a-mine", "", false},
		{"A insert foreign withheld", "A", "INSERT",
			`insert into a_owner (owner_id, title) values (gen_random_uuid(),'a-theirs')`,
			"d_a", "", "a-theirs", false},
		{"A update own", "A", "UPDATE",
			fmt.Sprintf(`update a_owner set title='a-upd' where owner_id='%s' and title='a-mine'`, userID),
			"d_a", "a-upd", "", false},
		{"A delete own", "A", "DELETE",
			fmt.Sprintf(`delete from a_owner where owner_id='%s' and title='a-upd'`, userID),
			"d_a", "", "", false},
		{"B insert own private", "B", "INSERT",
			fmt.Sprintf(`insert into b_or_public (owner_id, visibility, title) values ('%s','private','b-mine')`, userID),
			"d_b", "b-mine", "", false},
		{"B insert public foreign", "B", "INSERT",
			`insert into b_or_public (owner_id, visibility, title) values (gen_random_uuid(),'public','b-pub')`,
			"d_b", "b-pub", "", false},
		{"B insert private foreign withheld", "B", "INSERT",
			`insert into b_or_public (owner_id, visibility, title) values (gen_random_uuid(),'private','b-hid')`,
			"d_b", "", "b-hid", false},
		{"B delete own", "B", "DELETE",
			fmt.Sprintf(`delete from b_or_public where owner_id='%s' and title='b-mine'`, userID),
			"d_b", "", "", false},
		{"C insert team", "C", "INSERT",
			`insert into c_exists (team_id, note) values (1,'c-mine')`,
			"d_c", "", "", false},
		{"C insert other team withheld", "C", "INSERT",
			`insert into c_exists (team_id, note) values (9999,'c-other')`,
			"d_c", "", "c-other", false},
		{"toast insert", "A", "INSERT",
			fmt.Sprintf(`insert into e_toast (owner_id, body) values ('%s', repeat('Z', 180000))`, userID),
			"d_toast", "", "", false},
		{"toast update unchanged", "A", "UPDATE",
			fmt.Sprintf(`update e_toast set views = views + 1 where owner_id='%s'`, userID),
			"d_toast", "", "", true},
		{"generated insert", "A", "INSERT",
			fmt.Sprintf(`insert into e_generated (owner_id, price, qty) values ('%s', 3.5, 4)`, userID),
			"d_gen", "", "", false},
		{"composite pk insert", "A", "INSERT",
			fmt.Sprintf(`insert into e_composite_pk (a,b,owner_id,payload) values (%d,%d,'%s','pk')`,
				runKey, runKey, userID),
			"d_pk", "", "", false},
		{"norls insert", "A", "INSERT",
			`insert into a_norls (name, value) values ('n', 1.5)`,
			"d_norls", "", "", false},
		{"multi-row insert", "A", "INSERT",
			fmt.Sprintf(`insert into a_owner (owner_id, title) values ('%s','m1'), ('%s','m2')`, userID, userID),
			"d_a", "m1", "", false},
		{"on conflict", "A", "INSERT",
			fmt.Sprintf(`insert into e_composite_pk (a,b,owner_id,payload) values (%d,%d,'%s','pk2')
			  on conflict (a,b) do update set payload=excluded.payload, owner_id=excluded.owner_id`,
				runKey, runKey, userID),
			"d_pk", "", "", false},
		{"merge", "A", "MERGE",
			fmt.Sprintf(`merge into a_owner t using (select '%s'::uuid as owner_id, 'merged' as title) s
			  on t.owner_id=s.owner_id and t.title='m1'
			  when matched then update set title=s.title
			  when not matched then insert (owner_id, title) values (s.owner_id, s.title)`, userID),
			"d_a", "merged", "", false},
	}

	for _, st := range steps {
		drain(stream, 200*time.Millisecond)
		t0 := time.Now()
		if err := execSQL(ctx, pool, st.sql); err != nil {
			dmlResults = append(dmlResults, DMLResult{
				Name: st.name, Tier: st.tier, Op: st.op, OK: false,
				Expected: "exec ok", Got: err.Error(), Latency: time.Since(t0).Milliseconds(),
			})
			check(false, "DML "+st.name, err.Error())
			continue
		}
		time.Sleep(300 * time.Millisecond)
		evs := stream.collect(2 * time.Second)
		lat := time.Since(t0).Milliseconds()

		ok := true
		gotDesc := summarizeChanges(evs, st.wantSub)
		if st.wantTitle != "" {
			ok = titlesContain(evs, st.wantSub, st.wantTitle)
		}
		if st.wantAbsent != "" && titlesContain(evs, st.wantSub, st.wantAbsent) {
			ok = false
			gotDesc += " leaked withheld row"
		}
		if st.op == "DELETE" && st.wantTitle == "" && st.wantAbsent == "" {
			ok = hasOp(evs, st.wantSub, "DELETE")
			if !ok {
				gotDesc = "no DELETE event"
			}
		}
		if st.name == "C insert team" {
			ok = countSub(evs, "d_c") >= 1
		}
		if st.checkUnchanged {
			ok = hasUnchanged(evs, st.wantSub, "body")
			if !ok {
				gotDesc = "missing unchanged:[body]"
			}
		}
		if st.name == "multi-row insert" {
			ok = titlesContain(evs, "d_a", "m1") && titlesContain(evs, "d_a", "m2")
		}
		if st.name == "generated insert" {
			ok = countSub(evs, "d_gen") >= 1
		}
		if st.name == "composite pk insert" || st.name == "on conflict" {
			ok = countSub(evs, "d_pk") >= 1
		}
		if st.name == "norls insert" {
			ok = countSub(evs, "d_norls") >= 1
		}
		if st.name == "toast insert" {
			ok = countSub(evs, "d_toast") >= 1
		}

		dmlResults = append(dmlResults, DMLResult{
			Name: st.name, Tier: st.tier, Op: st.op, OK: ok,
			Expected: st.wantTitle, Got: gotDesc, Latency: lat,
		})
		check(ok, "DML "+st.name, gotDesc)
	}

	// Transactional broadcast + rollback. The prefix after `sluice:` is the
	// channel, and the stream must have joined it to receive anything.
	drain(stream, 100*time.Millisecond)
	mustExec(ctx, pool, `begin; select pg_logical_emit_message(true, 'sluice:room:audit', '{"event":"ok"}'); commit;`)
	evs := stream.collect(2 * time.Second)
	sawBC := false
	for _, e := range evs {
		if e.Name == "broadcast" {
			sawBC = true
		}
	}
	check(sawBC, "transactional pg_logical_emit_message delivered", summarizeNames(evs))

	drain(stream, 100*time.Millisecond)
	mustExec(ctx, pool, `begin; select pg_logical_emit_message(true, 'sluice:room:audit', '{"event":"rolled"}'); rollback;`)
	evs = stream.collect(2 * time.Second)
	sawBad := false
	for _, e := range evs {
		if e.Name == "broadcast" && bytes.Contains(e.Data, []byte("rolled")) {
			sawBad = true
		}
	}
	check(!sawBad, "rolled-back logical message never delivered", "")
}

func runRareCases(ctx context.Context, pool *pgxpool.Pool, tok, userID string) {
	fmt.Println("\n-- rare / edge cases --")

	// TRUNCATE on a subscribed table — should not panic the reader.
	body := fmt.Sprintf(`{"subscriptions":[{"sub":"r_a","shape":{"schema":"public","table":"a_norls"}}]}`)
	stream, err := openStream(ctx, tok, body)
	must(err, "rare stream")
	defer stream.Close()
	_, _ = stream.next(10 * time.Second)

	mustExec(ctx, pool, `insert into a_norls (name, value) values ('t',1)`)
	_ = stream.collect(1 * time.Second)
	err = execSQL(ctx, pool, `truncate a_norls`)
	check(err == nil, "TRUNCATE on published table succeeds", fmt.Sprint(err))
	// Stream should still be alive.
	mustExec(ctx, pool, `insert into a_norls (name, value) values ('after-trunc',2)`)
	evs := stream.collect(2 * time.Second)
	check(countSub(evs, "r_a") >= 1, "stream survives TRUNCATE and delivers later INSERT", summarizeChanges(evs, "r_a"))

	// UNLOGGED table must not be publishable / must not stream.
	err = execSQL(ctx, pool, `alter publication sluice add table public.e_unlogged`)
	check(err != nil, "UNLOGGED table rejected from publication", fmt.Sprint(err))

	// Snapshot floor LSN check. The snapshot is requested with
	// `"initial":"snapshot"` on the shape, not a boolean.
	body = fmt.Sprintf(`{"subscriptions":[{"sub":"snap","shape":{"schema":"public","table":"a_owner","filter":"owner_id=eq.%s","initial":"snapshot"}}]}`, userID)
	s2, err := openStream(ctx, tok, body)
	if err != nil {
		check(false, "snapshot stream opens", err.Error())
	} else {
		defer s2.Close()
		ready, err := s2.next(15 * time.Second)
		check(err == nil && ready.Name == "ready", "snapshot ready", fmt.Sprint(err))
		evs := s2.collect(5 * time.Second)
		sawEnd := false
		floor := ""
		for _, e := range evs {
			if e.Name == "snapshot_end" {
				sawEnd = true
				var se struct {
					FloorLSN string `json:"floor_lsn"`
				}
				_ = json.Unmarshal(e.Data, &se)
				floor = se.FloorLSN
			}
		}
		check(sawEnd, "snapshot_end received", "")
		if floor == "0/0" {
			flag("high", "snapshot_floor_zero",
				"Snapshot floor_lsn is 0/0 after quiet restart",
				"reader.ConfirmedLSN stays 0 until the first Commit after process start; snapshots and /diagnostics report 0/0 even though pg_replication_slots.confirmed_flush_lsn is advanced. Risk: misleading ops signal; ring replay floor may be wrong until first commit.")
		}
		check(floor != "" && floor != "0/0", "snapshot floor_lsn is non-zero", "floor="+floor)
	}

	// Filter grammar edges
	neg := []struct {
		name, filter string
		wantReject   bool
	}{
		{"sql injection attempt", "owner_id=eq.1;drop table a_owner", true},
		{"or injection", "owner_id=eq." + userID + ",or,true", true},
		{"unknown column", "not_a_col=eq.1", true},
		{"empty in", "owner_id=in.()", true},
	}
	for _, n := range neg {
		raw, _ := subscribe(ctx, tok, stream.id, fmt.Sprintf(
			`{"sub":"bad_%s","shape":{"schema":"public","table":"a_owner","filter":%q}}`, n.name, n.filter))
		rejected := strings.Contains(raw, "error") || strings.Contains(raw, "invalid") ||
			strings.Contains(raw, "unknown") || strings.Contains(strings.ToLower(raw), "reject")
		// Also HTTP non-200 surfaces as error string from subscribe helper.
		if strings.Contains(raw, `"tier"`) && !strings.Contains(raw, `"error"`) {
			rejected = false
		}
		ok := rejected == n.wantReject
		check(ok, "filter reject: "+n.name, truncate(raw, 120))
	}

	// Partitioned insert
	err = execSQL(ctx, pool, fmt.Sprintf(
		`insert into e_partitioned (owner_id, region, note) values ('%s','eu','p1')`, userID))
	if err != nil {
		flag("medium", "partition_insert", "Partitioned table DML failed", err.Error())
		check(false, "insert into partitioned published table", err.Error())
	} else {
		check(true, "insert into partitioned published table", "")
	}

	// Confirmed LSN diagnostics mismatch
	if svcKey != "" {
		d, _ := fetchDiagnostics(ctx)
		if slot, ok := d["slot"].(map[string]any); ok {
			cl, _ := slot["confirmed_lsn"].(string)
			var pgFlush string
			_ = pool.QueryRow(ctx, `select confirmed_flush_lsn::text from pg_replication_slots where slot_name='sluice'`).Scan(&pgFlush)
			if cl == "0/0" && pgFlush != "" && pgFlush != "0/0" {
				flag("high", "diagnostics_confirmed_lsn_stale",
					"diagnostics confirmed_lsn is 0/0 while PostgreSQL confirmed_flush_lsn is advanced",
					fmt.Sprintf("sluice=%s pg=%s — in-memory reader.confirmed is not seeded from the slot on startup", cl, pgFlush))
			}
			check(cl != "0/0" || pgFlush == "0/0",
				"diagnostics confirmed_lsn tracks slot (or both zero)",
				fmt.Sprintf("sluice=%s pg=%s", cl, pgFlush))
		}
	}

	// Concurrent transactions
	var wg sync.WaitGroup
	errs := make(chan error, 10)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			errs <- execSQL(ctx, pool, fmt.Sprintf(
				`insert into a_owner (owner_id, title) values ('%s','conc-%d')`, userID, i))
		}()
	}
	wg.Wait()
	close(errs)
	nerr := 0
	for e := range errs {
		if e != nil {
			nerr++
		}
	}
	check(nerr == 0, "10 concurrent INSERTs succeed", fmt.Sprintf("%d errors", nerr))
}

func runSecurityNegatives(ctx context.Context, tok string) {
	fmt.Println("\n-- security negatives --")
	code, _ := httpCode(ctx, http.MethodGet, sluiceURL+"/diagnostics", tok, "")
	check(code == http.StatusForbidden || code == http.StatusUnauthorized,
		"diagnostics forbidden to authenticated user", fmt.Sprintf("status=%d", code))

	code, _ = httpCode(ctx, http.MethodPost, sluiceURL+"/stream", "", `{"subscriptions":[]}`)
	check(code == http.StatusUnauthorized, "unauthenticated stream rejected", fmt.Sprintf("status=%d", code))

	code, body := httpCode(ctx, http.MethodPost, sluiceURL+"/stream",
		"eyJhbGciOiJub25lIn0.eyJyb2xlIjoiYXV0aGVudGljYXRlZCJ9.", `{"subscriptions":[]}`)
	check(code == http.StatusUnauthorized, "alg=none rejected", truncate(body, 80))

	if svcKey != "" {
		code, _ = httpCode(ctx, http.MethodGet, sluiceURL+"/diagnostics", svcKey, "")
		check(code == http.StatusOK, "diagnostics allowed for service_role", fmt.Sprintf("status=%d", code))
	}
}

func runLoadProbe(ctx context.Context, pool *pgxpool.Pool, tok, userID string) {
	fmt.Println("\n-- micro load probe (50 changes) --")
	body := fmt.Sprintf(`{"subscriptions":[{"sub":"load","shape":{"schema":"public","table":"a_owner","filter":"owner_id=eq.%s"}}]}`, userID)
	stream, err := openStream(ctx, tok, body)
	must(err, "load stream")
	defer stream.Close()
	_, _ = stream.next(10 * time.Second)
	drain(stream, 200*time.Millisecond)

	const n = 50
	t0 := time.Now()
	for i := 0; i < n; i++ {
		must(execSQL(ctx, pool, fmt.Sprintf(
			`insert into a_owner (owner_id, title) values ('%s','load-%d')`, userID, i)), "load insert")
	}
	deadline := time.After(15 * time.Second)
	got := 0
	for got < n {
		select {
		case ev, ok := <-stream.events:
			if !ok {
				goto done
			}
			if ev.Name == "change" && bytes.Contains(ev.Data, []byte(`"sub":"load"`)) {
				got++
			}
		case <-deadline:
			goto done
		}
	}
done:
	elapsed := time.Since(t0)
	rate := float64(got) / elapsed.Seconds()
	ok := float64(got) >= 0.9*float64(n)
	check(ok, fmt.Sprintf("load probe delivered ≥90%% of %d events (got %d in %s, %.0f evt/s)",
		n, got, elapsed.Round(time.Millisecond), rate), "")
	timing["load_probe"] = elapsed
}

func analyzeDiagnostics(d map[string]any) {
	if d == nil {
		return
	}
	warns, _ := d["warnings"].([]any)
	codes := map[string]int{}
	for _, w := range warns {
		m, _ := w.(map[string]any)
		code, _ := m["code"].(string)
		codes[code]++
		if remedy, _ := m["remedy"].(string); remedy == "" {
			flag("high", "warning_without_remedy",
				code+" warning carries no remedy", fmt.Sprint(m))
		}
		// The wrapped fixtures are the recommended spelling; if any of them is
		// reported as Tier C, the classifier has regressed.
		if code == "tier_c_policy" {
			if rel, _ := m["relation"].(string); strings.HasPrefix(rel, "public.w_") {
				flag("critical", "wrapped_policy_demoted",
					"a policy using the recommended (select auth.uid()) wrapper fell to Tier C",
					fmt.Sprint(m))
			}
		}
	}
	if codes["tier_c_policy"] == 0 {
		flag("medium", "tier_c_not_diagnosed",
			"Expected tier_c_policy warnings after the audit fixtures",
			"the catalog may not have refreshed, or policies were unexpectedly compilable")
	}
	// The two RLS best-practice checks must fire on the table that deliberately
	// violates them, and only on it.
	for _, want := range []string{"policy_applies_to_public", "policy_function_not_wrapped", "unindexed_policy_column"} {
		check(codes[want] > 0, "diagnostics reports "+want,
			fmt.Sprintf("count=%d", codes[want]))
	}
}

// analyzeCodeFindings records the issues that are structural rather than
// observable from a single run, so the report carries them alongside the
// measured results.
func analyzeCodeFindings() {
	flag("low", "owner_rls_bypass_not_modeled",
		"A table owner's RLS bypass is not modeled in catalog.Predicate",
		"BYPASSRLS and superuser are now read from pg_roles and short-circuit the predicate. The owner bypass (owner of a table without FORCE ROW LEVEL SECURITY) is not, because PostgreSQL decides it with has_privs_of_role, which means expanding role membership. It stays fail-closed, and in a Supabase layout the owner is a superuser anyway.")

	flag("medium", "column_grants_not_revocable",
		"REVOKE SELECT (col) does not reach an open stream",
		"Column privileges are checked once, at subscribe time, with a live has_column_privilege query. Policy changes and role attributes now re-resolve on the catalog tick, but column grants are not part of that fingerprint: they are not in the catalog snapshot, so noticing a change costs a round trip per subscription per tick rather than a hash comparison. Deliberately deferred as a cost decision, not an oversight.")

	flag("medium", "tier_c_snapshot_skew",
		"Tier C probes the live table, not the WAL commit snapshot",
		"Documented residual: between commit and probe the joined rows can change, so visibility can disagree with what PostgREST would have returned at commit time. Fail-closed on probe error mitigates crashes, not skew.")

	flag("medium", "no_ci_pipeline",
		"No CI pipeline in the repository",
		"Build, vet, -race and the harness suites all run by hand today. -race additionally needs CGO, which the Windows dev host does not have.")

	flag("low", "sdk_unreleased",
		"packages/sluice-js is still 0.0.0",
		"No published versioned client or container image yet.")

	flag("high", "no_horizontal_scale",
		"Multi-node Bus not implemented",
		"Single process, single slot. The advisory lock elects one reader, so a second node serves streams but cannot take over fan-out; a restart is a reconnect stampede.")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func summarizeNames(evs []sseEvent) string {
	if len(evs) == 0 {
		return "no events"
	}
	var names []string
	for _, e := range evs {
		names = append(names, e.Name)
	}
	return strings.Join(names, ",")
}

func fetchDiagnostics(ctx context.Context) (map[string]any, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, sluiceURL+"/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer "+svcKey)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func scrapeMetrics(ctx context.Context) map[string]float64 {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, sluiceURL+"/metrics", nil)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]float64{}
	for _, line := range strings.Split(string(raw), "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) < 2 {
			continue
		}
		var v float64
		if _, err := fmt.Sscanf(parts[len(parts)-1], "%f", &v); err == nil {
			out[parts[0]] = v
		}
	}
	return out
}

func flag(sev, code, title, detail string) {
	findings = append(findings, Finding{Severity: sev, Code: code, Title: title, Detail: detail})
	fmt.Printf("  FLAG  [%s] %s — %s\n", sev, code, title)
}

func countFindings(sev string) int {
	n := 0
	for _, f := range findings {
		if f.Severity == sev {
			n++
		}
	}
	return n
}

func check(ok bool, what, detail string) {
	checks++
	if ok {
		passes++
		fmt.Printf("  PASS  %s\n", what)
		return
	}
	fails++
	if detail != "" {
		fmt.Printf("  FAIL  %s (%s)\n", what, detail)
	} else {
		fmt.Printf("  FAIL  %s\n", what)
	}
}

func must(err error, what string) {
	if err != nil {
		fatal("%s: %v", what, err)
	}
}

func fatal(f string, args ...any) {
	fmt.Printf("FATAL: "+f+"\n", args...)
	os.Exit(2)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func execSQL(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	_, err := pool.Exec(ctx, sql)
	return err
}

func mustExec(ctx context.Context, pool *pgxpool.Pool, sql string) {
	must(execSQL(ctx, pool, sql), truncate(sql, 60))
	time.Sleep(200 * time.Millisecond)
}

func signUp(ctx context.Context, email, password string) (string, string, error) {
	body := fmt.Sprintf(`{"email":%q,"password":%q}`, email, password)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, authURL+"/signup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("signup %d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	var out struct {
		AccessToken string `json:"access_token"`
		User        struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", "", err
	}
	if out.AccessToken == "" || out.User.ID == "" {
		return "", "", fmt.Errorf("bad signup body: %s", truncate(string(raw), 200))
	}
	return out.AccessToken, out.User.ID, nil
}

type sseEvent struct {
	Name string
	ID   string
	Data []byte
}

type stream struct {
	id     string
	body   io.ReadCloser
	events chan sseEvent
}

func openStream(ctx context.Context, token, body string) (*stream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/stream", strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("stream %d: %s", resp.StatusCode, b)
	}
	s := &stream{id: resp.Header.Get("Sluice-Stream-Id"), body: resp.Body, events: make(chan sseEvent, 1024)}
	go func() {
		sc := bufio.NewScanner(s.body)
		sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
		var cur sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if cur.Name != "" || len(cur.Data) > 0 {
					s.events <- cur
				}
				cur = sseEvent{}
			case strings.HasPrefix(line, ":"):
			case strings.HasPrefix(line, "event:"):
				cur.Name = strings.TrimSpace(line[len("event:"):])
			case strings.HasPrefix(line, "id:"):
				cur.ID = strings.TrimSpace(line[len("id:"):])
			case strings.HasPrefix(line, "data:"):
				cur.Data = append(cur.Data, []byte(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))...)
			}
		}
		close(s.events)
	}()
	return s, nil
}

func (s *stream) next(timeout time.Duration) (sseEvent, error) {
	select {
	case ev, ok := <-s.events:
		if !ok {
			return sseEvent{}, fmt.Errorf("stream closed")
		}
		return ev, nil
	case <-time.After(timeout):
		return sseEvent{}, fmt.Errorf("timeout")
	}
}

func (s *stream) collect(window time.Duration) []sseEvent {
	var out []sseEvent
	deadline := time.After(window)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return out
			}
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
}

func (s *stream) Close() { _ = s.body.Close() }

func drain(s *stream, window time.Duration) { _ = s.collect(window) }

func subscribe(ctx context.Context, token, streamID, sub string) (string, error) {
	body := fmt.Sprintf(`{"stream_id":%q,"subscriptions":[%s]}`, streamID, sub)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/subscribe", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sluice-Stream-Id", streamID)
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw), nil
}

func httpCode(ctx context.Context, method, url, token, body string) (int, string) {
	var rdr io.Reader
	if body != "" {
		rdr = strings.NewReader(body)
	}
	req, _ := http.NewRequestWithContext(ctx, method, url, rdr)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	return resp.StatusCode, string(raw)
}

func titlesContain(evs []sseEvent, sub, title string) bool {
	for _, e := range evs {
		if e.Name != "change" {
			continue
		}
		var c struct {
			Sub    string         `json:"sub"`
			Record map[string]any `json:"record"`
			Old    map[string]any `json:"old"`
		}
		if json.Unmarshal(e.Data, &c) != nil || c.Sub != sub {
			continue
		}
		if t, _ := c.Record["title"].(string); t == title {
			return true
		}
		if t, _ := c.Old["title"].(string); t == title {
			return true
		}
		if t, _ := c.Record["note"].(string); t == title {
			return true
		}
	}
	return false
}

func hasOp(evs []sseEvent, sub, op string) bool {
	for _, e := range evs {
		var c struct {
			Sub string `json:"sub"`
			Op  string `json:"op"`
		}
		if e.Name == "change" && json.Unmarshal(e.Data, &c) == nil && c.Sub == sub && c.Op == op {
			return true
		}
	}
	return false
}

func hasUnchanged(evs []sseEvent, sub, col string) bool {
	for _, e := range evs {
		var c struct {
			Sub       string   `json:"sub"`
			Unchanged []string `json:"unchanged"`
		}
		if e.Name == "change" && json.Unmarshal(e.Data, &c) == nil && c.Sub == sub && slices.Contains(c.Unchanged, col) {
			return true
		}
	}
	return false
}

func countSub(evs []sseEvent, sub string) int {
	n := 0
	for _, e := range evs {
		var c struct {
			Sub string `json:"sub"`
		}
		if e.Name == "change" && json.Unmarshal(e.Data, &c) == nil && c.Sub == sub {
			n++
		}
	}
	return n
}

func summarizeChanges(evs []sseEvent, sub string) string {
	var parts []string
	for _, e := range evs {
		if e.Name != "change" {
			continue
		}
		var c struct {
			Sub    string         `json:"sub"`
			Op     string         `json:"op"`
			Record map[string]any `json:"record"`
		}
		if json.Unmarshal(e.Data, &c) != nil || (sub != "" && c.Sub != sub) {
			continue
		}
		title, _ := c.Record["title"].(string)
		note, _ := c.Record["note"].(string)
		parts = append(parts, fmt.Sprintf("%s:%s/%s", c.Sub, c.Op, title+note))
	}
	if len(parts) == 0 {
		return "no changes"
	}
	if len(parts) > 6 {
		parts = parts[:6]
	}
	return strings.Join(parts, ",")
}
