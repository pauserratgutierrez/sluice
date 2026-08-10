package expr

import (
	"slices"
	"testing"
)

// The predicate texts below are pg_get_expr output captured from PostgreSQL
// 18.4 for policies written the way Supabase's RLS performance guide
// recommends: every stable function wrapped in a scalar subquery so the planner
// evaluates it once as an InitPlan instead of once per row.
//
//	create policy p on t for select to authenticated
//	  using (owner_id = (select auth.uid()));
//
// PostgreSQL stores that as `(owner_id = ( SELECT auth.uid() AS uid))`. If the
// wrapper is treated as an opaque subquery, the single most important policy
// shape in the ecosystem lands in Tier C -- the one path Sluice exists to
// avoid -- and /diagnostics tells the operator to denormalise a policy that is
// already optimal.
const (
	predWrappedUID  = `(owner_id = ( SELECT auth.uid() AS uid))`
	predWrappedJWT  = `(owner_id = (( SELECT (auth.jwt() ->> 'sub'::text)))::uuid)`
	predWrappedBoth = `(((( SELECT auth.role() AS role)) = 'authenticated'::text) AND (owner_id = ( SELECT auth.uid() AS uid)))`
	predWrappedOr   = `((visibility = 'public'::text) OR (owner_id = ( SELECT auth.uid() AS uid)))`
	predWrappedIn   = `(team_id IN ( SELECT (( SELECT auth.jwt() AS jwt) ->> 'team'::text)))`
	predWrappedAny  = `(owner_id = ANY ( SELECT auth.uid() AS uid))`

	// A subquery that actually reads another relation stays Tier C: its value is
	// not a function of the WAL tuple plus the claim set.
	predRealSubquery = `(EXISTS ( SELECT 1 FROM memberships m WHERE ((m.team_id = invoices.team_id) AND (m.user_id = ( SELECT auth.uid() AS uid)))))`
)

func TestInitPlanWrapperIsCompilable(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		wantCols []string
	}{
		{"wrapped_uid", predWrappedUID, []string{"owner_id"}},
		{"wrapped_jwt", predWrappedJWT, []string{"owner_id"}},
		{"wrapped_both", predWrappedBoth, []string{"owner_id"}},
		{"wrapped_or", predWrappedOr, []string{"owner_id", "visibility"}},
		{"wrapped_in", predWrappedIn, []string{"team_id"}},
		{"wrapped_any", predWrappedAny, []string{"owner_id"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%s): %v", tc.sql, err)
			}
			info := Analyze(n, "t")
			if !info.Compilable() {
				t.Fatalf("not compilable: %s", info.Unsupported)
			}
			if !slices.Equal(info.Columns, tc.wantCols) {
				t.Errorf("columns = %v, want %v", info.Columns, tc.wantCols)
			}
		})
	}
}

// A wrapped policy must reduce to a constant exactly like the unwrapped one, or
// Tier A is unreachable for anybody following the performance guide.
func TestWrappedPolicyReducesToTierA(t *testing.T) {
	n, err := Parse(predWrappedUID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := ctxFor(t, map[string]any{"sub": alice, "role": "authenticated"})
	folded := Fold(n, ctx)

	mine := Substitute(folded, map[string]Value{"owner_id": Text(alice)})
	if ok, unknown := Visible(mine, ctx); !ok || unknown {
		t.Errorf("own row: visible=%v unknown=%v, want true/false", ok, unknown)
	}
	theirs := Substitute(folded, map[string]Value{"owner_id": Text(bob)})
	if ok, _ := Visible(theirs, ctx); ok {
		t.Error("another user's row must not be visible")
	}
}

func TestRealSubqueryStaysTierC(t *testing.T) {
	n, err := Parse(predRealSubquery)
	if err != nil {
		t.Fatal(err)
	}
	if info := Analyze(n, "invoices"); info.Compilable() {
		t.Error("a subquery reading another relation must never be compilable")
	}
}

// Session identifiers are not columns of the row. Treating them as such makes a
// predicate look compilable, and then every WAL tuple is missing the "column",
// so every row is withheld -- a silent, undiagnosed denial.
func TestSessionIdentifiersAreNotColumns(t *testing.T) {
	for _, sql := range []string{
		`((CURRENT_USER = 'authenticated'::name) AND (owner_id = auth.uid()))`,
		`(SESSION_USER = 'authenticated'::name)`,
		`(CURRENT_ROLE = 'authenticated'::name)`,
		`(owner_id = auth.uid() AND created_at < CURRENT_DATE)`,
	} {
		n, err := Parse(sql)
		if err != nil {
			continue // a parse failure is already fail-closed
		}
		info := Analyze(n, "t")
		if info.Compilable() {
			t.Errorf("%s: compilable with columns %v; must fall to Tier C", sql, info.Columns)
		}
	}
}

// Every operator the parser accepts must either be evaluable or be reported as
// unsupported. An operator that parses but has no evaluator produces ErrUnknown
// for every row, which withholds silently and shows nothing in /diagnostics.
func TestParsedOperatorsAreEitherEvaluableOrRejected(t *testing.T) {
	rowCtx := func() *Context {
		return &Context{Row: testRow{
			"code":  Text("ABC"),
			"tags":  Text("{a,b}"),
			"meta":  JSON(`{"a":{"b":1}}`),
			"score": Int(5),
		}}
	}
	for _, sql := range []string{
		`(code ~ '^[A-Z]+$'::text)`,
		`(code ~* '^[a-z]+$'::text)`,
		`(code !~ '^[0-9]+$'::text)`,
		`(code !~* '^[0-9]+$'::text)`,
		`((meta #> '{a}'::text[]) IS NOT NULL)`,
		`((meta #>> '{a,b}'::text[]) = '1'::text)`,
		`(tags <@ ARRAY['a'::text, 'b'::text])`,
	} {
		n, err := Parse(sql)
		if err != nil {
			continue // rejected at parse time: fail-closed, fine
		}
		info := Analyze(n, "t")
		if !info.Compilable() {
			continue // reported as Tier C: fine
		}
		if _, unknown := Visible(n, rowCtx()); unknown {
			t.Errorf("%s: analyzed as compilable but evaluates to unknown", sql)
		}
	}
}

// A relation with RLS on, no permissive policy for the caller's role, and one
// or more restrictive policies must combine to a constant FALSE. Leaving it as
// `false AND <restrictive>` is not a leak -- evaluation still short-circuits --
// but it resolves to a Tier B subscription that withholds every row forever
// instead of being refused with a reason at subscribe time.
func TestRestrictiveOnlyCombinesToFalse(t *testing.T) {
	amountPositive, err := Parse(`(amount > (0)::numeric)`)
	if err != nil {
		t.Fatal(err)
	}
	combined := And(Or(), And(amountPositive))
	if !IsAlwaysFalse(combined) {
		t.Errorf("no permissive policy with a restrictive one = %s, want constant false",
			Format(combined))
	}
}

type testRow map[string]Value

func (m testRow) Column(name string) (Value, bool) {
	v, ok := m[name]
	return v, ok
}
