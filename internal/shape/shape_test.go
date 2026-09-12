package shape

import (
	"strings"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

func rel() *catalog.Relation {
	return &catalog.Relation{
		Schema: "public", Name: "docs",
		ReplicaIdentity:        'i',
		ReplicaIdentityColumns: []string{"owner_id", "id"},
		Columns: []catalog.Column{
			{Name: "id", TypeName: "bigint", AttNum: 1, NotNull: true},
			{Name: "owner_id", TypeName: "uuid", AttNum: 2, NotNull: true},
			{Name: "title", TypeName: "text", AttNum: 3},
			{Name: "score", TypeName: "integer", AttNum: 4},
			{Name: "archived", TypeName: "boolean", AttNum: 5},
		},
		IndexedColumns: map[string]bool{"id": true, "owner_id": true},
	}
}

type row map[string]expr.Value

func (r row) Column(name string) (expr.Value, bool) { v, ok := r[name]; return v, ok }

func eval(t *testing.T, f *Filter, r row) bool {
	t.Helper()
	v, unknown := expr.Visible(f.Node, &expr.Context{Row: r, Claims: expr.Claims{}, ClaimsJSON: "{}"})
	if unknown {
		t.Fatalf("unexpected unknown evaluating %q against %v", f.Raw, r)
	}
	return v
}

func TestParseAndEvaluate(t *testing.T) {
	cases := []struct {
		filter string
		row    row
		want   bool
	}{
		{"owner_id=eq.abc", row{"owner_id": expr.Text("abc")}, true},
		{"owner_id=eq.abc", row{"owner_id": expr.Text("xyz")}, false},
		{"owner_id=not.eq.abc", row{"owner_id": expr.Text("xyz")}, true},
		{"score=gte.10", row{"score": expr.Int(10)}, true},
		{"score=gt.10", row{"score": expr.Int(10)}, false},
		{"score=lt.10,score=gt.1", row{"score": expr.Int(5)}, true},
		{"score=lt.10,score=gt.1", row{"score": expr.Int(50)}, false},
		{"title=in.(a,b,c)", row{"title": expr.Text("b")}, true},
		{"title=in.(a,b,c)", row{"title": expr.Text("z")}, false},
		{"title=not.in.(a,b)", row{"title": expr.Text("z")}, true},
		{"title=like.draft*", row{"title": expr.Text("draft one")}, true},
		{"title=like.draft*", row{"title": expr.Text("final")}, false},
		{"title=ilike.DRAFT*", row{"title": expr.Text("draft one")}, true},
		{"archived=is.false", row{"archived": expr.Bool(false)}, true},
		{"archived=is.true", row{"archived": expr.Bool(false)}, false},
		{"title=is.null", row{"title": expr.Null}, true},
		// An empty filter matches everything, which is what an unfiltered shape means.
		{"", row{}, true},
	}
	for _, tc := range cases {
		t.Run(tc.filter, func(t *testing.T) {
			f, err := Parse(tc.filter, rel())
			if err != nil {
				t.Fatalf("Parse(%q): %v", tc.filter, err)
			}
			if got := eval(t, f, tc.row); got != tc.want {
				t.Errorf("%q against %v = %v, want %v", tc.filter, tc.row, got, tc.want)
			}
		})
	}
}

func TestParseRejectsBadInput(t *testing.T) {
	for _, filter := range []string{
		"nosuchcolumn=eq.1", // unknown column
		"owner_id",          // no operator
		"owner_id=abc",      // no operator separator
		"owner_id=bogus.1",  // unknown operator
		"owner_id=in.a,b",   // in without parentheses
		"owner_id=is.maybe", // is only takes null/true/false
		"owner_id=eq.a,,",   // empty clause
		"owner_id=eq.(unbalanced",
	} {
		if _, err := Parse(filter, rel()); err == nil {
			t.Errorf("Parse(%q) should have failed", filter)
		}
	}
}

func TestInListCappedAt100(t *testing.T) {
	var vals []string
	for i := 0; i < 101; i++ {
		vals = append(vals, "v")
	}
	_, err := Parse("title=in.("+strings.Join(vals, ",")+")", rel())
	if err == nil {
		t.Fatal("an in.() list over 100 values must be rejected: it would become a linear scan per change")
	}
}

func TestSplitRespectsParentheses(t *testing.T) {
	// The comma inside in.() must not split the filter into two clauses.
	f, err := Parse("title=in.(a,b,c),score=gt.1", rel())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Terms) != 2 {
		t.Fatalf("terms = %d, want 2: %v", len(f.Terms), f.Terms)
	}
	if len(f.Terms[0].Values) != 3 {
		t.Errorf("in list = %v, want 3 values", f.Terms[0].Values)
	}
}

// Only non-negated equalities may become constants: they are what makes Tier A
// sound and what the routing index keys on. Anything else must not.
func TestEqualitiesOnlyFromPlainEq(t *testing.T) {
	f, err := Parse("owner_id=eq.abc,score=gt.5,title=not.eq.x,archived=is.true", rel())
	if err != nil {
		t.Fatal(err)
	}
	if len(f.Equalities) != 1 {
		t.Fatalf("equalities = %v, want only owner_id", f.Equalities)
	}
	if v, ok := f.Equalities["owner_id"]; !ok || v.String() != "abc" {
		t.Errorf("owner_id equality = %v", v)
	}
}

func TestRoutingKeyPrefersIndexedAndReplicaIdentity(t *testing.T) {
	r := rel()

	// owner_id is both indexed and in the replica identity: the best key, because
	// DELETE events can be routed by it too.
	f, _ := Parse("owner_id=eq.abc,title=eq.x", r)
	if got := f.RoutingKey(r); got != "owner_id" {
		t.Errorf("routing key = %q, want owner_id", got)
	}

	// title is neither: usable as a last resort, but it means unindexed lookups.
	f, _ = Parse("title=eq.x", r)
	if got := f.RoutingKey(r); got != "title" {
		t.Errorf("routing key = %q, want title", got)
	}

	// No equality at all means no routing key, i.e. the shape is scanned for
	// every change to the relation.
	f, _ = Parse("score=gt.1", r)
	if got := f.RoutingKey(r); got != "" {
		t.Errorf("routing key = %q, want empty", got)
	}
}

func TestSQLRendersBoundParameters(t *testing.T) {
	f, err := Parse("owner_id=eq.abc,score=gte.5,title=in.(a,b),title=like.d*,archived=is.false", rel())
	if err != nil {
		t.Fatal(err)
	}
	sql, args := f.SQL(1)

	// Values must never appear in the SQL text: they are always parameters.
	for _, forbidden := range []string{"abc", "'a'", "'b'"} {
		if strings.Contains(sql, forbidden) {
			t.Errorf("SQL contains a literal value %q: %s", forbidden, sql)
		}
	}
	if want := 4; len(args) != want {
		t.Errorf("args = %d (%v), want %d", len(args), args, want)
	}
	for i := 1; i <= len(args); i++ {
		if !strings.Contains(sql, "$"+string(rune('0'+i))) {
			t.Errorf("SQL is missing placeholder $%d: %s", i, sql)
		}
	}
	if !strings.Contains(sql, "IS FALSE") {
		t.Errorf("is.false should render as IS FALSE: %s", sql)
	}
}

func TestSQLQuotesIdentifiers(t *testing.T) {
	r := rel()
	r.Columns = append(r.Columns, catalog.Column{Name: `we"ird`, TypeName: "text", AttNum: 6})
	f, err := Parse(`we"ird=eq.x`, r)
	if err != nil {
		t.Fatal(err)
	}
	sql, _ := f.SQL(1)
	if !strings.Contains(sql, `"we""ird"`) {
		t.Errorf("identifier not doubled-quote-escaped: %s", sql)
	}
}

func TestParseOps(t *testing.T) {
	all, err := ParseOps(nil)
	if err != nil || !all.Insert || !all.Update || !all.Delete || all.Truncate {
		t.Errorf("default ops = %+v, want I/U/D but not TRUNCATE", all)
	}
	only, err := ParseOps([]string{"insert", "DELETE"})
	if err != nil || !only.Insert || only.Update || !only.Delete {
		t.Errorf("parsed ops = %+v", only)
	}
	if _, err := ParseOps([]string{"nope"}); err == nil {
		t.Error("an unknown op must be rejected")
	}
}

func TestNarrowFillsAuthorizedEqualitiesAndKeepsClientTerms(t *testing.T) {
	r := rel()
	auth, err := Parse("owner_id=eq.abc", r)
	if err != nil {
		t.Fatal(err)
	}
	client, err := Parse("score=gte.10", r)
	if err != nil {
		t.Fatal(err)
	}
	got, err := Narrow(auth, client, r)
	if err != nil {
		t.Fatal(err)
	}
	if got.Equalities["owner_id"].String() != "abc" {
		t.Fatalf("authorized equality was dropped: %v", got.Equalities)
	}
	if len(got.Terms) != 2 {
		t.Fatalf("terms = %d, want 2 (authorized + client)", len(got.Terms))
	}
}

func TestNarrowAllowsOmittingAuthorizedEquality(t *testing.T) {
	r := rel()
	auth, _ := Parse("owner_id=eq.abc", r)
	got, err := Narrow(auth, nil, r)
	if err != nil {
		t.Fatal(err)
	}
	if got.Equalities["owner_id"].String() != "abc" {
		t.Fatalf("omitting the equality must not widen: %v", got.Equalities)
	}
}

func TestNarrowRejectsConflictingEquality(t *testing.T) {
	r := rel()
	auth, _ := Parse("owner_id=eq.abc", r)
	client, _ := Parse("owner_id=eq.xyz", r)
	if _, err := Narrow(auth, client, r); err == nil {
		t.Fatal("a conflicting equality must be denied, not become an empty shape")
	}
}

func TestNarrowRejectsWholeTableGrant(t *testing.T) {
	r := rel()
	auth, _ := Parse("", r)
	if _, err := Narrow(auth, nil, r); err == nil {
		t.Fatal("an authorized filter with no equality is a whole-table grant and must be refused")
	}
}

func TestConcrete(t *testing.T) {
	r := rel()
	empty, _ := Parse("", r)
	if empty.Concrete() {
		t.Fatal("empty filter is not concrete")
	}
	eq, _ := Parse("owner_id=eq.abc", r)
	if !eq.Concrete() {
		t.Fatal("eq filter must be concrete")
	}
	neg, _ := Parse("owner_id=not.eq.abc", r)
	if neg.Concrete() {
		t.Fatal("a negated equality must not count as concrete")
	}
}

func TestColumnsNeededDeduplicates(t *testing.T) {
	f, _ := Parse("score=gt.1,score=lt.9,owner_id=eq.x", rel())
	got := f.ColumnsNeeded()
	if len(got) != 2 {
		t.Errorf("columns needed = %v, want 2 distinct", got)
	}
}
