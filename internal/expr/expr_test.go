package expr

import (
	"encoding/json"
	"slices"
	"testing"
	"time"
)

// The predicate texts below are the real output of
// pg_get_expr(polqual, polrelid) for the policies in deploy/db/fixtures.sql,
// captured from PostgreSQL 18.4. Testing against normalised PostgreSQL output
// rather than hand-written SQL is the point: that is what the catalog delivers.
const (
	predTierA = `(owner_id = auth.uid())`

	predTierAExpanded = `(owner_id = ((current_setting('request.jwt.claims'::text, true))::jsonb ->> 'sub'::text)::uuid)`

	predTierB = `((visibility = 'public'::text) OR (owner_id = auth.uid()))`

	predTierC = `(EXISTS ( SELECT 1
   FROM memberships m
  WHERE ((m.team_id = invoices.team_id) AND (m.user_id = auth.uid()))))`

	predRestrictive = `(amount > (0)::numeric)`

	predAnyArray = `(visibility = ANY (ARRAY['public'::text, 'unlisted'::text]))`
)

type row map[string]Value

func (r row) Column(name string) (Value, bool) {
	v, ok := r[name]
	return v, ok
}

// unknownRow reports a column as present-but-unavailable, which is what an
// unchanged TOASTed value looks like.
type unknownRow struct {
	row     row
	unknown map[string]bool
}

func (u unknownRow) Column(name string) (Value, bool) {
	if u.unknown[name] {
		return Null, false
	}
	return u.row.Column(name)
}

func ctxFor(t *testing.T, claims map[string]any) *Context {
	t.Helper()
	raw, err := json.Marshal(claims)
	if err != nil {
		t.Fatal(err)
	}
	return &Context{Claims: claims, ClaimsJSON: string(raw), Now: time.Now()}
}

const alice = "7f3a1c2e-0000-4000-8000-000000000001"
const bob = "7f3a1c2e-0000-4000-8000-000000000002"

func TestParseRealPolicies(t *testing.T) {
	for name, sql := range map[string]string{
		"tier_a":       predTierA,
		"tier_a_expan": predTierAExpanded,
		"tier_b":       predTierB,
		"tier_c":       predTierC,
		"restrictive":  predRestrictive,
		"any_array":    predAnyArray,
	} {
		if _, err := Parse(sql); err != nil {
			t.Errorf("%s: Parse(%q) = %v", name, sql, err)
		}
	}
}

func TestAnalyzeColumnsAndCompilability(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		relation   string
		wantCols   []string
		compilable bool
	}{
		{"tier_a", predTierA, "documents", []string{"owner_id"}, true},
		{"tier_a_expanded", predTierAExpanded, "documents", []string{"owner_id"}, true},
		{"tier_b", predTierB, "posts", []string{"owner_id", "visibility"}, true},
		{"restrictive", predRestrictive, "invoices", []string{"amount"}, true},
		{"any_array", predAnyArray, "posts", []string{"visibility"}, true},
		// A subquery must NEVER be reported as compilable: that is the fail-closed
		// boundary between "evaluate in process" and "impersonate".
		{"tier_c", predTierC, "invoices", nil, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse: %v", err)
			}
			info := Analyze(n, tc.relation)
			if info.Compilable() != tc.compilable {
				t.Fatalf("Compilable() = %v (reason %q), want %v",
					info.Compilable(), info.Unsupported, tc.compilable)
			}
			if !tc.compilable {
				if len(info.Subqueries) == 0 {
					t.Error("expected a subquery to be recorded for diagnostics")
				}
				return
			}
			if got := info.Columns; !slices.Equal(got, tc.wantCols) {
				t.Errorf("Columns = %v, want %v", got, tc.wantCols)
			}
		})
	}
}

// TestTierAReduction is the test that matters most: it proves the soundness
// argument behind Tier A. If the shape pins every column the predicate reads,
// substituting those constants must leave an expression with no column
// references whose value decides the whole shape.
func TestTierAReduction(t *testing.T) {
	for _, sql := range []string{predTierA, predTierAExpanded} {
		n, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		ctx := ctxFor(t, map[string]any{"sub": alice, "role": "authenticated"})
		folded := Fold(n, ctx)

		// Folding must collapse the claim-derived side to a literal, so the
		// per-change path never parses JSON.
		if info := Analyze(folded, "documents"); len(info.Columns) != 1 || info.Columns[0] != "owner_id" {
			t.Fatalf("after folding, columns = %v, want [owner_id]", info.Columns)
		}

		// Alice subscribing to her own rows: reduces to TRUE.
		reduced := Substitute(folded, map[string]Value{"owner_id": Text(alice)})
		if info := Analyze(reduced, "documents"); len(info.Columns) != 0 {
			t.Fatalf("after substitution, columns = %v, want none", info.Columns)
		}
		if vis, unk := Visible(reduced, ctx); !vis || unk {
			t.Fatalf("%s: alice on her own shape: visible=%v unknown=%v, want true/false", sql, vis, unk)
		}

		// Alice subscribing to Bob's rows: reduces to FALSE, so the subscription
		// must be refused outright rather than filtered per row.
		reduced = Substitute(folded, map[string]Value{"owner_id": Text(bob)})
		if vis, unk := Visible(reduced, ctx); vis || unk {
			t.Fatalf("%s: alice on bob's shape: visible=%v unknown=%v, want false/false", sql, vis, unk)
		}
	}
}

func TestTierBPerRowEvaluation(t *testing.T) {
	n, err := Parse(predTierB)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ctx := ctxFor(t, map[string]any{"sub": alice, "role": "authenticated"})
	pred := Fold(n, ctx)

	cases := []struct {
		name    string
		row     row
		visible bool
	}{
		{"own private row", row{"owner_id": Text(alice), "visibility": Text("private")}, true},
		{"someone else public", row{"owner_id": Text(bob), "visibility": Text("public")}, true},
		{"someone else private", row{"owner_id": Text(bob), "visibility": Text("private")}, false},
		{"someone else unlisted", row{"owner_id": Text(bob), "visibility": Text("unlisted")}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx.Row = tc.row
			vis, unk := Visible(pred, ctx)
			if unk {
				t.Fatalf("unexpected unknown")
			}
			if vis != tc.visible {
				t.Errorf("visible = %v, want %v", vis, tc.visible)
			}
		})
	}
}

// TestDeleteAuthorizationOnOldTuple is the correctness claim Supabase documents
// as impossible: "RLS policies are not applied to DELETE statements, because
// there is no way for Postgres to verify that a user has access to a deleted
// record."
//
// Evaluating the predicate against the OLD TUPLE THE WAL ALREADY DELIVERED needs
// no table access at all, so a deleted row is authorized exactly like a live one.
func TestDeleteAuthorizationOnOldTuple(t *testing.T) {
	n, err := Parse(predTierB)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ctx := ctxFor(t, map[string]any{"sub": alice, "role": "authenticated"})
	pred := Fold(n, ctx)

	// The row is gone from the table; this is purely the old tuple.
	ctx.Row = row{"owner_id": Text(alice), "visibility": Text("private")}
	if vis, unk := Visible(pred, ctx); !vis || unk {
		t.Errorf("alice's deleted row: visible=%v unknown=%v, want true/false", vis, unk)
	}

	ctx.Row = row{"owner_id": Text(bob), "visibility": Text("private")}
	if vis, unk := Visible(pred, ctx); vis || unk {
		t.Errorf("bob's deleted row: visible=%v unknown=%v, want false/false", vis, unk)
	}
}

// TestUnchangedToastIsNotNull guards the single worst failure mode available to a
// realtime server. An unchanged TOASTed value must surface as UNKNOWN, never as
// NULL and never as visible.
func TestUnchangedToastIsNotNull(t *testing.T) {
	n, err := Parse(predTierB)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ctx := ctxFor(t, map[string]any{"sub": bob, "role": "authenticated"})
	pred := Fold(n, ctx)

	ctx.Row = unknownRow{
		row:     row{"owner_id": Text(alice)},
		unknown: map[string]bool{"visibility": true},
	}
	vis, unk := Visible(pred, ctx)
	if !unk {
		t.Fatal("an unavailable column must produce unknown, not a decision")
	}
	if vis {
		t.Fatal("unknown must never grant visibility")
	}
}

func TestThreeValuedLogic(t *testing.T) {
	cases := []struct {
		sql     string
		row     row
		visible bool
		null    bool
	}{
		{`(a = 1)`, row{"a": Null}, false, true},
		{`(a IS NULL)`, row{"a": Null}, true, false},
		{`(a IS NOT NULL)`, row{"a": Null}, false, false},
		// FALSE AND unknown is FALSE, which is what lets a short-circuited
		// predicate stay decidable.
		{`((a = 1) AND (b = 2))`, row{"a": Int(9), "b": Null}, false, false},
		// TRUE OR unknown is TRUE.
		{`((a = 1) OR (b = 2))`, row{"a": Int(1), "b": Null}, true, false},
		// unknown OR unknown is unknown, so it must not grant.
		{`((a = 1) OR (b = 2))`, row{"a": Null, "b": Null}, false, true},
	}
	for _, tc := range cases {
		n, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		ctx := ctxFor(t, map[string]any{"sub": alice})
		ctx.Row = tc.row
		v, err := Eval(n, ctx)
		if err != nil {
			t.Fatalf("Eval(%q): %v", tc.sql, err)
		}
		if v.IsNull() != tc.null {
			t.Errorf("%q with %v: IsNull = %v, want %v", tc.sql, tc.row, v.IsNull(), tc.null)
		}
		vis, _ := Visible(n, ctx)
		if vis != tc.visible {
			t.Errorf("%q with %v: visible = %v, want %v", tc.sql, tc.row, vis, tc.visible)
		}
	}
}

func TestAnyArrayBecomesInList(t *testing.T) {
	n, err := Parse(predAnyArray)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	ctx := ctxFor(t, nil)
	for _, tc := range []struct {
		v    string
		want bool
	}{{"public", true}, {"unlisted", true}, {"private", false}} {
		ctx.Row = row{"visibility": Text(tc.v)}
		if vis, _ := Visible(n, ctx); vis != tc.want {
			t.Errorf("visibility=%q: visible = %v, want %v", tc.v, vis, tc.want)
		}
	}
}

func TestLikeMatching(t *testing.T) {
	cases := []struct {
		s, p string
		ci   bool
		want bool
	}{
		{"draft report", "draft%", false, true},
		{"final report", "draft%", false, false},
		{"DRAFT report", "draft%", true, true},
		{"DRAFT report", "draft%", false, false},
		{"abc", "a_c", false, true},
		{"abbc", "a_c", false, false},
		{"a%b", `a\%b`, false, true},
		{"axb", `a\%b`, false, false},
		{"anything", "%", false, true},
		{"", "%", false, true},
		// Consecutive wildcards must not blow up.
		{"aaaaaaaaaaaaaaaaaaaab", "%%%%%b", false, true},
	}
	for _, tc := range cases {
		if got := likeMatch(tc.s, tc.p, tc.ci); got != tc.want {
			t.Errorf("likeMatch(%q, %q, ci=%v) = %v, want %v", tc.s, tc.p, tc.ci, got, tc.want)
		}
	}
}

func TestNoPermissivePolicyDenies(t *testing.T) {
	// PostgreSQL default-denies once RLS is enabled and no policy grants access.
	// Getting this backwards would be a security hole, so it is asserted.
	if !IsAlwaysFalse(Or()) {
		t.Fatal("Or() with no permissive policies must be constant FALSE")
	}
	if !IsAlwaysTrue(And()) {
		t.Fatal("And() with no restrictive policies must be constant TRUE")
	}
	if !IsAlwaysTrue(And(Or(TrueNode), And())) {
		t.Fatal("a single permissive TRUE with no restrictions must be TRUE")
	}
	if !IsAlwaysFalse(And(Or(), And(TrueNode))) {
		t.Fatal("no permissive policy must remain FALSE regardless of restrictions")
	}
}

func TestUnsupportedConstructsFailClosed(t *testing.T) {
	// Every one of these must be reported as uncompilable so the caller falls
	// back to an impersonated probe instead of guessing.
	for _, sql := range []string{
		`(EXISTS ( SELECT 1 FROM t))`,
		`(a = (SELECT max(b) FROM t))`,
		`(a IN ( SELECT b FROM t))`,
		`(some_unknown_function(a) = 1)`,
		`(a = other_table.b)`,
	} {
		n, err := Parse(sql)
		if err != nil {
			// A parse error is also fail-closed, which is acceptable.
			continue
		}
		if info := Analyze(n, "t"); info.Compilable() {
			t.Errorf("Analyze(%q) reported compilable; must fail closed", sql)
		}
	}
}

func TestCastNormalisesUUIDCase(t *testing.T) {
	// PostgreSQL emits canonical lowercase UUIDs and JWT `sub` is lowercase, but a
	// mixed-case literal in a policy must still compare equal.
	v, err := Cast(Text("7F3A1C2E-0000-4000-8000-000000000001"), "uuid")
	if err != nil {
		t.Fatal(err)
	}
	if v.Str != alice {
		t.Errorf("Cast to uuid = %q, want %q", v.Str, alice)
	}
}

func TestParseTextTypes(t *testing.T) {
	cases := []struct {
		typ, in string
		kind    Kind
	}{
		{"bool", "t", KindBool},
		{"int4", "42", KindInt},
		{"int8", "9007199254740993", KindInt},
		{"numeric", "1.5", KindFloat},
		{"numeric", "NaN", KindText}, // must not become a bogus JSON number
		{"jsonb", `{"k":1}`, KindJSON},
		{"uuid", "ABC", KindText},
		{"text", "hello", KindText},
	}
	for _, tc := range cases {
		if got := ParseText(tc.typ, tc.in).Kind; got != tc.kind {
			t.Errorf("ParseText(%q, %q).Kind = %v, want %v", tc.typ, tc.in, got, tc.kind)
		}
	}
}
