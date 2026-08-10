// Package shape parses and validates client filters, and compiles them into the
// same expression tree used for RLS predicates.
//
// The grammar is deliberately narrow and AND-only. `OR` is not an oversight: it
// destroys the constant indexing that lets Sluice route a change to its
// interested subscribers in O(1) instead of O(subscribers). A client needing OR
// registers two subscriptions.
package shape

import (
	"fmt"
	"strings"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// Op is a filter operator, spelled as in PostgREST so that anyone who has used
// the Data API already knows the syntax.
type Op string

const (
	OpEq    Op = "eq"
	OpNeq   Op = "neq"
	OpLt    Op = "lt"
	OpLte   Op = "lte"
	OpGt    Op = "gt"
	OpGte   Op = "gte"
	OpIn    Op = "in"
	OpLike  Op = "like"
	OpILike Op = "ilike"
	OpIs    Op = "is"
)

// Term is one parsed filter clause.
type Term struct {
	Column string
	Op     Op
	Negate bool
	Value  string
	Values []string // for OpIn
}

// Filter is a conjunction of terms plus the compiled expression.
type Filter struct {
	Raw   string
	Terms []Term
	Node  expr.Node

	// Equalities maps column -> constant for every non-negated `eq` term. This
	// is the set the authorizer uses for Tier A constant reduction and the
	// registry uses to build its routing key.
	Equalities map[string]expr.Value
}

// Ops enumerates which DML operations a subscription wants.
type Ops struct {
	Insert, Update, Delete, Truncate bool
}

func (o Ops) Any() bool { return o.Insert || o.Update || o.Delete || o.Truncate }

// ParseOps turns []string{"INSERT","UPDATE"} into an Ops. An empty list means
// all row operations.
func ParseOps(list []string) (Ops, error) {
	if len(list) == 0 {
		return Ops{Insert: true, Update: true, Delete: true}, nil
	}
	var o Ops
	for _, s := range list {
		switch strings.ToUpper(strings.TrimSpace(s)) {
		case "INSERT":
			o.Insert = true
		case "UPDATE":
			o.Update = true
		case "DELETE":
			o.Delete = true
		case "TRUNCATE":
			o.Truncate = true
		case "*", "ALL":
			o.Insert, o.Update, o.Delete = true, true, true
		default:
			return Ops{}, fmt.Errorf("unknown op %q", s)
		}
	}
	if !o.Any() {
		return Ops{}, fmt.Errorf("at least one op is required")
	}
	return o, nil
}

// Parse parses a filter string against a relation's columns.
//
// Type-checking against pg_attribute here is what stops a malformed value from
// ever reaching the database, and means the compiled comparison uses the right
// coercion. The value is never interpolated into SQL.
func Parse(raw string, rel *catalog.Relation) (*Filter, error) {
	f := &Filter{Raw: raw, Equalities: map[string]expr.Value{}}
	if strings.TrimSpace(raw) == "" {
		f.Node = expr.TrueNode
		return f, nil
	}

	parts, err := splitTop(raw)
	if err != nil {
		return nil, err
	}

	var nodes []expr.Node
	for _, p := range parts {
		t, err := parseTerm(p)
		if err != nil {
			return nil, err
		}
		col, ok := rel.Column(t.Column)
		if !ok {
			return nil, fmt.Errorf("filter references unknown column %q on %s", t.Column, rel.FullName())
		}
		node, err := compileTerm(t, col)
		if err != nil {
			return nil, err
		}
		f.Terms = append(f.Terms, t)
		nodes = append(nodes, node)

		if t.Op == OpEq && !t.Negate {
			f.Equalities[t.Column] = expr.ParseText(col.TypeName, t.Value)
		}
	}
	f.Node = expr.And(nodes...)
	return f, nil
}

// splitTop splits on commas that are not inside parentheses, so that
// `status=in.(open,pending),priority=gte.3` yields two terms.
func splitTop(s string) ([]string, error) {
	var out []string
	depth, start := 0, 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return nil, fmt.Errorf("unbalanced ) in filter")
			}
		case ',':
			if depth == 0 {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	if depth != 0 {
		return nil, fmt.Errorf("unbalanced ( in filter")
	}
	out = append(out, s[start:])
	for i := range out {
		out[i] = strings.TrimSpace(out[i])
		if out[i] == "" {
			return nil, fmt.Errorf("empty filter clause")
		}
	}
	return out, nil
}

// parseTerm parses `column=op.value` or `column=not.op.value`.
func parseTerm(s string) (Term, error) {
	eq := strings.IndexByte(s, '=')
	if eq <= 0 {
		return Term{}, fmt.Errorf("filter clause %q must be column=op.value", s)
	}
	t := Term{Column: strings.TrimSpace(s[:eq])}
	rest := s[eq+1:]

	if strings.HasPrefix(rest, "not.") {
		t.Negate = true
		rest = rest[len("not."):]
	}

	dot := strings.IndexByte(rest, '.')
	if dot < 0 {
		return Term{}, fmt.Errorf("filter clause %q is missing the operator separator", s)
	}
	t.Op = Op(strings.ToLower(rest[:dot]))
	t.Value = rest[dot+1:]

	if t.Op == OpIn {
		v := strings.TrimSpace(t.Value)
		if !strings.HasPrefix(v, "(") || !strings.HasSuffix(v, ")") {
			return Term{}, fmt.Errorf("in.() requires parentheses: %q", s)
		}
		inner := v[1 : len(v)-1]
		if strings.TrimSpace(inner) != "" {
			for _, item := range strings.Split(inner, ",") {
				t.Values = append(t.Values, unquote(strings.TrimSpace(item)))
			}
		}
		// An empty list matches nothing, so accepting it would register a
		// subscription that can never deliver -- almost always a client bug, and
		// indistinguishable from a working one once it is live.
		if len(t.Values) == 0 {
			return Term{}, fmt.Errorf("in.() needs at least one value: %q", s)
		}
		// PostgREST caps this at 100; the same cap keeps a single subscription
		// from turning into an unbounded linear scan per change.
		if len(t.Values) > 100 {
			return Term{}, fmt.Errorf("in.() accepts at most 100 values, got %d", len(t.Values))
		}
	} else {
		t.Value = unquote(t.Value)
	}
	return t, nil
}

func unquote(s string) string {
	if len(s) >= 2 && s[0] == '"' && s[len(s)-1] == '"' {
		return strings.ReplaceAll(s[1:len(s)-1], `\"`, `"`)
	}
	return s
}

func compileTerm(t Term, col catalog.Column) (expr.Node, error) {
	ref := &expr.ColumnRef{Name: t.Column}

	var node expr.Node
	switch t.Op {
	case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
		op := map[Op]string{OpEq: "=", OpNeq: "<>", OpLt: "<", OpLte: "<=", OpGt: ">", OpGte: ">="}[t.Op]
		node = &expr.Binary{Op: op, Left: ref, Right: &expr.Literal{Value: expr.ParseText(col.TypeName, t.Value)}}

	case OpIn:
		items := make([]expr.Node, 0, len(t.Values))
		for _, v := range t.Values {
			items = append(items, &expr.Literal{Value: expr.ParseText(col.TypeName, v)})
		}
		node = &expr.InList{Arg: ref, Items: items}

	case OpLike, OpILike:
		op := "~~"
		if t.Op == OpILike {
			op = "~~*"
		}
		// PostgREST spells the wildcard `*`; translate to SQL's `%`.
		pattern := strings.ReplaceAll(t.Value, "*", "%")
		node = &expr.Binary{Op: op, Left: ref, Right: &expr.Literal{Value: expr.Text(pattern)}}

	case OpIs:
		switch strings.ToLower(t.Value) {
		case "null":
			node = &expr.IsTest{Arg: ref, Test: "null"}
		case "true":
			node = &expr.IsTest{Arg: ref, Test: "true"}
		case "false":
			node = &expr.IsTest{Arg: ref, Test: "false"}
		default:
			return nil, fmt.Errorf("is.%s is not supported; use null, true or false", t.Value)
		}

	default:
		return nil, fmt.Errorf("unknown filter operator %q", t.Op)
	}

	if t.Negate {
		node = &expr.Unary{Op: "not", Arg: node}
	}
	return node, nil
}

// RoutingKey picks the column Sluice will index this subscription by.
//
// Preference order: an equality on a column that is BOTH indexed in PostgreSQL
// and present in the replica identity (so DELETE events can be routed too),
// then any indexed column, then any equality at all. Returning "" means the
// shape is unindexed and will be scanned for every change to the relation --
// exactly the case ElectricSQL measured at 140 changes/sec versus 5,000.
func (f *Filter) RoutingKey(rel *catalog.Relation) string {
	var indexedAndRI, indexed, any string
	for col := range f.Equalities {
		if any == "" {
			any = col
		}
		if rel.IndexedColumns[col] {
			if indexed == "" {
				indexed = col
			}
			if rel.InReplicaIdentity(col) && indexedAndRI == "" {
				indexedAndRI = col
			}
		}
	}
	switch {
	case indexedAndRI != "":
		return indexedAndRI
	case indexed != "":
		return indexed
	default:
		return any
	}
}

// ColumnsNeeded lists every column the filter reads, so the caller can verify
// they survive into old tuples before promising DELETE support.
func (f *Filter) ColumnsNeeded() []string {
	seen := map[string]bool{}
	var out []string
	for _, t := range f.Terms {
		if !seen[t.Column] {
			seen[t.Column] = true
			out = append(out, t.Column)
		}
	}
	return out
}

// SQL renders the filter as a WHERE clause with bound parameters, numbered from
// `next`. Values are never interpolated; identifiers were validated against
// pg_attribute at parse time.
//
// Used only for the initial snapshot, where PostgreSQL has to apply the same
// filter Sluice applies in process to the live stream.
func (f *Filter) SQL(next int) (string, []any) {
	if len(f.Terms) == 0 {
		return "true", nil
	}
	var parts []string
	var args []any
	for _, t := range f.Terms {
		col := `"` + strings.ReplaceAll(t.Column, `"`, `""`) + `"`
		var clause string
		switch t.Op {
		case OpEq, OpNeq, OpLt, OpLte, OpGt, OpGte:
			op := map[Op]string{OpEq: "=", OpNeq: "<>", OpLt: "<", OpLte: "<=", OpGt: ">", OpGte: ">="}[t.Op]
			clause = fmt.Sprintf("%s %s $%d", col, op, next)
			args = append(args, t.Value)
			next++
		case OpIn:
			// A single array parameter keeps the statement shape stable however
			// many values the client sent, so the plan cache is not thrashed.
			clause = fmt.Sprintf("%s::text = ANY($%d::text[])", col, next)
			args = append(args, t.Values)
			next++
		case OpLike, OpILike:
			op := "LIKE"
			if t.Op == OpILike {
				op = "ILIKE"
			}
			clause = fmt.Sprintf("%s::text %s $%d", col, op, next)
			args = append(args, strings.ReplaceAll(t.Value, "*", "%"))
			next++
		case OpIs:
			switch strings.ToLower(t.Value) {
			case "null":
				clause = col + " IS NULL"
			case "true":
				clause = col + " IS TRUE"
			case "false":
				clause = col + " IS FALSE"
			}
		}
		if clause == "" {
			continue
		}
		if t.Negate {
			clause = "NOT (" + clause + ")"
		}
		parts = append(parts, clause)
	}
	if len(parts) == 0 {
		return "true", nil
	}
	return strings.Join(parts, " AND "), args
}

// Describe renders the filter for diagnostics.
func (f *Filter) Describe() string {
	if len(f.Terms) == 0 {
		return "(none)"
	}
	parts := make([]string, 0, len(f.Terms))
	for _, t := range f.Terms {
		neg := ""
		if t.Negate {
			neg = "not."
		}
		v := t.Value
		if t.Op == OpIn {
			v = "(" + strings.Join(t.Values, ",") + ")"
		}
		parts = append(parts, t.Column+"="+neg+string(t.Op)+"."+v)
	}
	return strings.Join(parts, ",")
}
