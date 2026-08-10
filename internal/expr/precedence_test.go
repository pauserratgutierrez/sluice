package expr

import (
	"encoding/json"
	"testing"
)

// Precedence now comes from PostgreSQL's own grammar rather than from a
// precedence table maintained here, so these cases are no longer guarding
// against an arithmetic slip. They are kept because a mistake in this area does
// not fail loudly: it yields an expression the evaluator is happy to evaluate
// and that quietly disagrees with PostgreSQL, which is the one class of bug the
// Tier B cross-check exists to catch. Pinning the behaviour means it never has
// to.
//
// They also pin the conversion from PostgreSQL's tree to Sluice's, which is
// where a precedence-equivalent mistake could still be introduced.
func TestPrecedence(t *testing.T) {
	cases := []struct {
		sql  string
		row  row
		want bool
	}{
		// AND binds tighter than OR.
		{`a = 1 OR b = 1 AND c = 1`, row{"a": Int(1), "b": Int(0), "c": Int(0)}, true},
		{`a = 1 OR b = 1 AND c = 1`, row{"a": Int(0), "b": Int(1), "c": Int(0)}, false},
		{`a = 1 OR b = 1 AND c = 1`, row{"a": Int(0), "b": Int(1), "c": Int(1)}, true},
		// Parentheses override it.
		{`(a = 1 OR b = 1) AND c = 1`, row{"a": Int(1), "b": Int(0), "c": Int(0)}, false},
		{`(a = 1 OR b = 1) AND c = 1`, row{"a": Int(1), "b": Int(0), "c": Int(1)}, true},

		// NOT binds tighter than AND, looser than comparison.
		{`NOT a = 1 AND b = 1`, row{"a": Int(2), "b": Int(1)}, true},
		{`NOT a = 1 AND b = 1`, row{"a": Int(1), "b": Int(1)}, false},
		{`NOT (a = 1 AND b = 1)`, row{"a": Int(1), "b": Int(2)}, true},

		// Arithmetic binds tighter than comparison.
		{`a + 1 = 3`, row{"a": Int(2)}, true},
		{`a - 1 = 3`, row{"a": Int(4)}, true},
		{`a * 2 = 6`, row{"a": Int(3)}, true},
		// Multiplication before addition.
		{`a + b * 2 = 7`, row{"a": Int(1), "b": Int(3)}, true},
		{`(a + b) * 2 = 8`, row{"a": Int(1), "b": Int(3)}, true},

		// Casts bind tighter than everything.
		{`a::int = 5`, row{"a": Text("5")}, true},
		{`a::text = '5'`, row{"a": Int(5)}, true},

		// Chained comparisons through parentheses, as pg_get_expr emits them.
		{`((a > 1) AND (a < 10))`, row{"a": Int(5)}, true},
		{`((a > 1) AND (a < 10))`, row{"a": Int(50)}, false},

		// BETWEEN desugars correctly and negates as a whole.
		{`a BETWEEN 1 AND 10`, row{"a": Int(5)}, true},
		{`a BETWEEN 1 AND 10`, row{"a": Int(50)}, false},
		{`a NOT BETWEEN 1 AND 10`, row{"a": Int(50)}, true},

		// IS binds tighter than AND.
		{`a IS NULL AND b = 1`, row{"a": Null, "b": Int(1)}, true},
		{`a IS NOT NULL AND b = 1`, row{"a": Null, "b": Int(1)}, false},

		// IN and its negation.
		{`a IN (1, 2, 3)`, row{"a": Int(2)}, true},
		{`a NOT IN (1, 2, 3)`, row{"a": Int(9)}, true},
		{`a NOT IN (1, 2, 3)`, row{"a": Int(2)}, false},

		// String concatenation before comparison.
		{`a || 'x' = 'yx'`, row{"a": Text("y")}, true},

		// json ->> binds tighter than comparison.
		{`j ->> 'k' = 'v'`, row{"j": JSON(`{"k":"v"}`)}, true},
		{`j ->> 'k' = 'v'`, row{"j": JSON(`{"k":"w"}`)}, false},

		// Unary minus.
		{`-a = -5`, row{"a": Int(5)}, true},
		{`a > -1`, row{"a": Int(0)}, true},
	}

	for _, tc := range cases {
		t.Run(tc.sql, func(t *testing.T) {
			n, err := Parse(tc.sql)
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.sql, err)
			}
			ctx := &Context{Row: tc.row, Claims: Claims{}, ClaimsJSON: "{}"}
			got, unknown := Visible(n, ctx)
			if unknown {
				t.Fatalf("unexpected unknown for %q with %v", tc.sql, tc.row)
			}
			if got != tc.want {
				t.Errorf("%q with %v = %v, want %v (parsed as %s)",
					tc.sql, tc.row, got, tc.want, Format(n))
			}
		})
	}
}

// A policy value is attacker-influenced in exactly one way: it can contain SQL
// syntax. Because Tier B never builds SQL from it -- it compiles to an in-process
// evaluator -- there is nothing to inject into. These pin that the parser treats
// such content as data, and that a string literal survives quoting intact.
func TestLiteralsAreData(t *testing.T) {
	cases := []struct {
		sql  string
		row  row
		want bool
	}{
		{`a = 'x'' OR 1=1 --'`, row{"a": Text("x' OR 1=1 --")}, true},
		{`a = 'x'' OR 1=1 --'`, row{"a": Text("anything else")}, false},
		{`a = ''`, row{"a": Text("")}, true},
		{`a = 'multi
line'`, row{"a": Text("multi\nline")}, true},
		{`a = 'émoji 🎉'`, row{"a": Text("émoji 🎉")}, true},
		{`"weird column" = 1`, row{"weird column": Int(1)}, true},
		{`a = ';DROP TABLE users;'`, row{"a": Text(";DROP TABLE users;")}, true},
	}
	for _, tc := range cases {
		n, err := Parse(tc.sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", tc.sql, err)
		}
		ctx := &Context{Row: tc.row, Claims: Claims{}, ClaimsJSON: "{}"}
		got, _ := Visible(n, ctx)
		if got != tc.want {
			t.Errorf("%q = %v, want %v", tc.sql, got, tc.want)
		}
	}
}

// Fold must never change what a predicate means. It is only allowed to collapse
// subtrees that contain no column references and no volatile functions.
func TestFoldPreservesSemantics(t *testing.T) {
	claims := map[string]any{"sub": alice, "role": "authenticated", "tier": "gold"}
	raw, _ := json.Marshal(claims)
	ctx := &Context{Claims: claims, ClaimsJSON: string(raw)}

	sqls := []string{
		predTierA,
		predTierAExpanded,
		predTierB,
		`((owner_id = auth.uid()) AND (tier = (auth.jwt() ->> 'tier'::text)))`,
		`(lower(name) = lower('ALICE'))`,
		`(coalesce(nickname, name) = 'x')`,
	}
	rows := []row{
		{"owner_id": Text(alice), "visibility": Text("private"), "tier": Text("gold"), "name": Text("alice"), "nickname": Null},
		{"owner_id": Text(bob), "visibility": Text("public"), "tier": Text("silver"), "name": Text("bob"), "nickname": Text("x")},
		{"owner_id": Text(bob), "visibility": Text("private"), "tier": Text("gold"), "name": Text("ALICE"), "nickname": Null},
	}

	for _, sql := range sqls {
		n, err := Parse(sql)
		if err != nil {
			t.Fatalf("Parse(%q): %v", sql, err)
		}
		folded := Fold(n, ctx)
		for i, r := range rows {
			ctx.Row = r
			before, unkA := Visible(n, ctx)
			after, unkB := Visible(folded, ctx)
			if before != after || unkA != unkB {
				t.Errorf("%q row %d: unfolded=(%v,%v) folded=(%v,%v)\n  folded to: %s",
					sql, i, before, unkA, after, unkB, Format(folded))
			}
		}
	}
}

// Substitute must be equivalent to evaluating with those values in the row.
func TestSubstituteEquivalentToRow(t *testing.T) {
	n, err := Parse(predTierB)
	if err != nil {
		t.Fatal(err)
	}
	claims := map[string]any{"sub": alice}
	raw, _ := json.Marshal(claims)

	for _, r := range []row{
		{"owner_id": Text(alice), "visibility": Text("private")},
		{"owner_id": Text(bob), "visibility": Text("public")},
		{"owner_id": Text(bob), "visibility": Text("private")},
	} {
		consts := map[string]Value{}
		for k, v := range r {
			consts[k] = v
		}
		viaRow, _ := Visible(n, &Context{Row: r, Claims: claims, ClaimsJSON: string(raw)})
		viaSub, _ := Visible(Substitute(n, consts), &Context{Claims: claims, ClaimsJSON: string(raw)})
		if viaRow != viaSub {
			t.Errorf("row %v: via row = %v, via substitution = %v", r, viaRow, viaSub)
		}
	}
}

func TestParserRejectsMalformedInput(t *testing.T) {
	// Every one of these must produce an error or a non-compilable node. What it
	// must never do is panic or silently produce something evaluable.
	for _, sql := range []string{
		``, `(`, `)`, `((a = 1)`, `a =`, `= 1`, `a = = 1`,
		`'unterminated`, `"unterminated`, `a ~~~ 'x'`, `a IS`, `a IS NOT`,
		`a IN ()`, `a IN (,)`, `ARRAY[`, `CASE WHEN a THEN 1 END`,
		`a = ANY(b)`, `a >= ALL (ARRAY[1])`, `1 2 3`,
	} {
		t.Run(sql, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("Parse(%q) panicked: %v", sql, r)
				}
			}()
			n, err := Parse(sql)
			if err != nil {
				return
			}
			if info := Analyze(n, "t"); info.Compilable() {
				// Parsing successfully is acceptable only if evaluating it is safe.
				ctx := &Context{Row: row{}, Claims: Claims{}, ClaimsJSON: "{}"}
				if vis, _ := Visible(n, ctx); vis {
					t.Errorf("Parse(%q) produced a compilable, TRUE-evaluating node: %s", sql, Format(n))
				}
			}
		})
	}
}

func TestDeeplyNestedInputTerminates(t *testing.T) {
	// A pathological policy must not blow the stack or hang. It may fail; it may
	// not take the process with it.
	deep := "a = 1"
	for i := 0; i < 500; i++ {
		deep = "(" + deep + " AND a = 1)"
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() { _ = recover() }()
		if n, err := Parse(deep); err == nil {
			ctx := &Context{Row: row{"a": Int(1)}, Claims: Claims{}, ClaimsJSON: "{}"}
			Visible(n, ctx)
		}
	}()
	<-done
}
