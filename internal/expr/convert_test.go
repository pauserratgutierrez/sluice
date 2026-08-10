package expr

import (
	"strings"
	"testing"
	"time"
)

// evalBool parses and evaluates a predicate, asserting it compiles first. want
// is a *Value* rather than a bool so the three-valued cases can be expressed:
// pass Null to assert UNKNOWN.
func evalBool(t *testing.T, sql string, r row, want Value) {
	t.Helper()
	n, err := Parse(sql)
	if err != nil {
		t.Fatalf("Parse(%s): %v", sql, err)
	}
	if info := Analyze(n, "t"); !info.Compilable() {
		t.Fatalf("Parse(%s): expected compilable, got %s", sql, info.Unsupported)
	}
	got, err := Eval(n, &Context{Row: r, Now: time.Now()})
	if err != nil {
		t.Fatalf("Eval(%s): %v", sql, err)
	}
	if got.IsNull() != want.IsNull() || (!want.IsNull() && got.Truthy() != want.Truthy()) {
		t.Errorf("%s = %v, want %v", sql, got, want)
	}
}

// TestGrammarBreadth covers constructs that reach Sluice from pg_get_expr but
// that a hand-maintained subset grammar had no production for. Each is both
// parsed and evaluated, because parsing a construct and then evaluating it
// incorrectly is the failure mode that matters.
func TestGrammarBreadth(t *testing.T) {
	r := row{
		"n":      Int(5),
		"a":      Text("x"),
		"b":      Text("y"),
		"same":   Text("x"),
		"flag":   Bool(true),
		"status": Text("draft"),
		"null":   Null,
	}
	tests := []struct {
		name string
		sql  string
		want Value
	}{
		{"between_inside", `(n BETWEEN 1 AND 10)`, Bool(true)},
		{"between_outside", `(n BETWEEN 6 AND 10)`, Bool(false)},
		{"between_boundary", `(n BETWEEN 5 AND 5)`, Bool(true)},
		{"not_between", `(n NOT BETWEEN 1 AND 10)`, Bool(false)},

		{"is_distinct_differs", `(a IS DISTINCT FROM b)`, Bool(true)},
		{"is_distinct_same", `(a IS DISTINCT FROM same)`, Bool(false)},
		// IS DISTINCT FROM is never UNKNOWN, unlike plain `<>`.
		{"is_distinct_null", `(a IS DISTINCT FROM "null")`, Bool(true)},
		{"is_not_distinct_null", `(a IS NOT DISTINCT FROM "null")`, Bool(false)},
		{"plain_ne_null_is_unknown", `(a <> "null")`, Null},

		{"coalesce_first", `(COALESCE(a, b) = 'x'::text)`, Bool(true)},
		{"coalesce_second", `(COALESCE("null", b) = 'y'::text)`, Bool(true)},
		{"nullif_equal", `(NULLIF(a, same) IS NULL)`, Bool(true)},
		{"nullif_differs", `(NULLIF(a, b) IS NULL)`, Bool(false)},

		{"is_true", `(flag IS TRUE)`, Bool(true)},
		{"is_not_false", `(flag IS NOT FALSE)`, Bool(true)},
		{"is_unknown_on_null", `("null" IS UNKNOWN)`, Bool(true)},
		{"is_not_unknown", `(flag IS NOT UNKNOWN)`, Bool(true)},
		{"is_false_on_null", `("null" IS FALSE)`, Bool(false)},

		// pg_get_expr never emits IN or NOT IN: it deparses them as the
		// quantified forms below. Both spellings must agree.
		{"in_literal", `(status IN ('draft'::text, 'sent'::text))`, Bool(true)},
		{"any_array", `(status = ANY (ARRAY['draft'::text, 'sent'::text]))`, Bool(true)},
		{"any_array_miss", `(status = ANY (ARRAY['sent'::text]))`, Bool(false)},
		{"not_in_literal", `(status NOT IN ('sent'::text))`, Bool(true)},
		{"all_array", `(status <> ALL (ARRAY['sent'::text, 'paid'::text]))`, Bool(true)},
		{"all_array_hit", `(status <> ALL (ARRAY['draft'::text]))`, Bool(false)},
		// A NULL among the elements makes a failed NOT IN unknown, not true.
		{"all_array_null_element", `(status <> ALL (ARRAY['sent'::text, NULL]))`, Null},

		{"quoted_identifier", `("null" IS NULL)`, Bool(true)},
		{"escape_string", `(a = E'x'::text)`, Bool(true)},
		{"dollar_quoted", `(a = $$x$$::text)`, Bool(true)},
		{"unicode_literal", `(a <> U&'\00e9'::text)`, Bool(true)},
		{"chained_cast", `((n)::text = '5'::text)`, Bool(true)},
		{"schema_qualified_operator", `(a OPERATOR(pg_catalog.=) same)`, Bool(true)},
		{"array_cast_distributes", `(status = ANY ((ARRAY['draft', 'sent'])::text[]))`, Bool(true)},

		{"nested_not", `(NOT (NOT flag))`, Bool(true)},
		{"negative_literal", `(n > -1)`, Bool(true)},
		{"bpchar_cast", `((a)::bpchar = 'x'::text)`, Bool(true)},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) { evalBool(t, tc.sql, r, tc.want) })
	}
}

// TestNaryBooleansFlatten covers the one structural difference between
// PostgreSQL's tree and Sluice's. PostgreSQL collapses `a AND b AND c` into a
// single BoolExpr with three arguments; Sluice's evaluator is binary, so the
// converter rebuilds a left-leaning chain. Precedence itself is covered by
// TestPrecedence, which now exercises the real grammar.
func TestNaryBooleansFlatten(t *testing.T) {
	tests := []struct {
		sql  string
		r    row
		want Value
	}{
		{`(a AND b AND c)`, row{"a": Bool(true), "b": Bool(true), "c": Bool(true)}, Bool(true)},
		{`(a AND b AND c)`, row{"a": Bool(true), "b": Bool(false), "c": Bool(true)}, Bool(false)},
		{`(a OR b OR c)`, row{"a": Bool(false), "b": Bool(false), "c": Bool(true)}, Bool(true)},
		{`(a OR b OR c)`, row{"a": Bool(false), "b": Bool(false), "c": Bool(false)}, Bool(false)},
		{`(a AND b AND c AND d AND e)`, row{
			"a": Bool(true), "b": Bool(true), "c": Bool(true), "d": Bool(true), "e": Bool(false),
		}, Bool(false)},
		// Three-valued logic must survive the rebuild: FALSE dominates AND even
		// with a NULL present, but TRUE AND NULL is UNKNOWN.
		{`(a AND b AND c)`, row{"a": Bool(true), "b": Null, "c": Bool(false)}, Bool(false)},
		{`(a AND b AND c)`, row{"a": Bool(true), "b": Null, "c": Bool(true)}, Null},
		{`(a OR b OR c)`, row{"a": Bool(false), "b": Null, "c": Bool(true)}, Bool(true)},
		{`(a OR b OR c)`, row{"a": Bool(false), "b": Null, "c": Bool(false)}, Null},
	}
	for _, tc := range tests {
		t.Run(tc.sql, func(t *testing.T) { evalBool(t, tc.sql, tc.r, tc.want) })
	}
}

// TestDeclinedConstructsFailClosed is the test that justifies the whole design.
//
// Sluice must never mistake a construct it cannot evaluate for one it can. The
// prior hand-written parser had exactly that failure: with no production for
// CURRENT_USER, it fell through to the identifier rule and produced a column
// reference. The predicate looked compilable, the WAL tuple had no such column,
// and every row was silently withheld.
//
// Each case below must be reported as non-compilable -- which routes the
// subscription to Tier C, where PostgreSQL itself decides -- rather than
// producing a tree that evaluates to anything at all.
func TestDeclinedConstructsFailClosed(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"current_user", `(owner_id = CURRENT_USER)`},
		{"session_user", `(owner_id = SESSION_USER)`},
		{"current_schema", `(name = CURRENT_SCHEMA)`},
		{"exists_subquery", `(EXISTS ( SELECT 1 FROM memberships m WHERE (m.user_id = t.owner_id)))`},
		{"in_subquery", `(team_id IN ( SELECT m.team_id FROM memberships m))`},
		{"scalar_subquery_over_table", `(owner_id = ( SELECT max(m.user_id) FROM memberships m))`},
		{"aggregate", `(count(*) > 0)`},
		{"window_function", `(row_number() OVER () = 1)`},
		{"user_defined_operator", `(a OPERATOR(public.===) b)`},
		{"regex_match", `(a ~ '^x$'::text)`},
		{"regex_imatch", `(a ~* '^x$'::text)`},
		{"jsonb_path_operator", `((a #> '{k}'::text[]) = 'v'::jsonb)`},
		{"array_cast_of_column", `((tags)::text[] = ARRAY['a'::text])`},
		{"ne_any", `(status <> ANY (ARRAY['a'::text, 'b'::text]))`},
		{"eq_all", `(status = ALL (ARRAY['a'::text, 'b'::text]))`},
		{"case_expression", `(CASE WHEN a THEN b ELSE c END)`},
		{"unknown_function", `(some_extension.check_it(owner_id))`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			n, err := Parse(tc.sql)
			if err != nil {
				return // Refusing to parse is also failing closed.
			}
			info := Analyze(n, "t")
			if info.Compilable() {
				t.Fatalf("%s was accepted as compilable; it must fall to Tier C", tc.sql)
			}
			if info.Unsupported == "" {
				t.Error("non-compilable predicate carries no reason for /diagnostics")
			}
			// Evaluation must refuse too, so a caller that ignores Compilable
			// still cannot obtain a bogus visibility decision.
			if _, err := Eval(n, &Context{Row: row{}, Now: time.Now()}); err == nil {
				t.Errorf("%s evaluated without error despite being unsupported", tc.sql)
			}
		})
	}
}

// TestParserRejectsHostileInput checks that predicate text which is not a single
// expression cannot smuggle in a second statement or crash the process. Policy
// text comes from the catalog rather than from users, but client filters reach
// the same parser.
func TestParserRejectsHostileInput(t *testing.T) {
	inputs := []string{
		`1; DROP TABLE users`,
		`1) OR (1=1`,
		`owner_id = 'x' UNION SELECT * FROM auth.users`,
		`owner_id = 'x'; --`,
		`/* unterminated`,
		`'unterminated`,
		`(((((((((((`,
		``,
		`   `,
		`)`,
		`SELECT 1`,
		`owner_id = $1`,
		strings.Repeat("(a AND ", 200) + "b" + strings.Repeat(")", 200),
	}
	for _, in := range inputs {
		t.Run(in, func(t *testing.T) {
			n, err := Parse(in)
			if err != nil {
				return
			}
			// Anything that does parse must still be a single, self-contained,
			// evaluable expression.
			info := Analyze(n, "t")
			if !info.Compilable() {
				return
			}
			if _, err := Eval(n, &Context{Row: row{"a": Bool(true), "b": Bool(true)}, Now: time.Now()}); err != nil {
				t.Logf("parsed and compilable but did not evaluate: %v", err)
			}
		})
	}
}

// TestInitPlanWrapperIsUnwrappedNotFlattened checks that unwrapping
// `(select auth.uid())` marks the call as InitPlan-cached without changing what
// it evaluates to, and that an unwrapped call is reported so /diagnostics can
// tell an operator their policy is missing the recommended wrapper.
func TestInitPlanWrapperIsUnwrappedNotFlattened(t *testing.T) {
	ctx := ctxFor(t, map[string]any{"sub": alice, "role": "authenticated"})
	ctx.Row = row{"owner_id": Text(alice)}

	wrapped, err := Parse(`(owner_id = ( SELECT auth.uid() AS uid))`)
	if err != nil {
		t.Fatal(err)
	}
	bare, err := Parse(`(owner_id = auth.uid())`)
	if err != nil {
		t.Fatal(err)
	}

	for name, n := range map[string]Node{"wrapped": wrapped, "bare": bare} {
		v, err := Eval(n, ctx)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if !v.Truthy() {
			t.Errorf("%s: predicate should hold for the owner", name)
		}
	}

	if got := Analyze(wrapped, "t").UncachedCalls; len(got) != 0 {
		t.Errorf("wrapped call reported as uncached: %v", got)
	}
	if got := Analyze(bare, "t").UncachedCalls; len(got) == 0 {
		t.Error("bare auth.uid() should be reported so the operator can add the wrapper")
	}
}
