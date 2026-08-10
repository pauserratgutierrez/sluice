package expr

import "strings"

// Node is a parsed expression.
type Node interface{ node() }

// ColumnRef is a reference to a column of the target relation.
//
// Qualifier is the table alias or name if the source wrote one (`posts.owner_id`).
// pg_get_expr qualifies references to the policy's own relation, so the
// qualifier is usually the relation name and can be ignored -- but a qualifier
// naming a *different* relation means a correlated subquery, which the analyzer
// rejects.
type ColumnRef struct {
	Qualifier string
	Name      string
}

// Literal is a constant.
type Literal struct{ Value Value }

// Cast is `expr::type`.
type CastExpr struct {
	Arg  Node
	Type string
}

// FuncCall is `name(args...)`, with Schema set for `auth.uid()` style calls.
type FuncCall struct {
	Schema string
	Name   string
	Args   []Node

	// Cached is true when the call arrived wrapped in a scalar subquery --
	// `(select auth.uid())` -- which makes PostgreSQL evaluate it once per query
	// as an InitPlan instead of once per row. It changes nothing about how
	// Sluice evaluates the call; it is reported in /diagnostics so an operator
	// can be told which calls are still costing a per-row invocation inside
	// PostgreSQL itself.
	Cached bool
}

// Unary is a prefix operator: NOT, -, +.
type Unary struct {
	Op  string
	Arg Node
}

// Binary is an infix operator.
type Binary struct {
	Op          string
	Left, Right Node
}

// IsTest is `expr IS [NOT] NULL|TRUE|FALSE|UNKNOWN`.
type IsTest struct {
	Arg    Node
	Negate bool
	Test   string // "null", "true", "false", "unknown"
}

// InList is `expr [NOT] IN (a, b, c)` and also `expr = ANY (ARRAY[...])`, which
// pg_get_expr emits for CHECK-style membership tests.
type InList struct {
	Arg    Node
	Items  []Node
	Negate bool
}

// ArrayExpr is `ARRAY[a, b, c]`.
type ArrayExpr struct{ Items []Node }

// SubqueryExpr is any EXISTS(...) / (SELECT ...) / IN (SELECT ...) construct.
//
// It is parsed rather than rejected at parse time so that the analyzer can
// report *why* a policy fell to Tier C, with the offending SQL text, in the
// diagnostics endpoint. It is never evaluated.
type SubqueryExpr struct{ SQL string }

func (*ColumnRef) node()    {}
func (*Literal) node()      {}
func (*CastExpr) node()     {}
func (*FuncCall) node()     {}
func (*Unary) node()        {}
func (*Binary) node()       {}
func (*IsTest) node()       {}
func (*InList) node()       {}
func (*ArrayExpr) node()    {}
func (*SubqueryExpr) node() {}

// TrueNode is the predicate for "no restriction", used when RLS is disabled or
// the role holds BYPASSRLS.
var TrueNode Node = &Literal{Value: Bool(true)}

// And combines predicates, dropping literal TRUEs so that a relation with one
// permissive policy does not produce a needlessly nested tree.
func And(nodes ...Node) Node {
	var out []Node
	for _, n := range nodes {
		if n == nil || isTrueLiteral(n) {
			continue
		}
		// A constant FALSE anywhere in a conjunction decides it. This is the
		// relation whose permissive policy set is empty but which still carries
		// restrictive policies: PostgreSQL shows that caller nothing, and saying
		// so here refuses the subscription at subscribe time instead of
		// accepting one that silently withholds every row for its whole life.
		if IsAlwaysFalse(n) {
			return &Literal{Value: Bool(false)}
		}
		out = append(out, n)
	}
	switch len(out) {
	case 0:
		return TrueNode
	case 1:
		return out[0]
	}
	acc := out[0]
	for _, n := range out[1:] {
		acc = &Binary{Op: "and", Left: acc, Right: n}
	}
	return acc
}

// Or combines permissive policies. Unlike And, a literal TRUE here short-circuits
// the whole disjunction.
func Or(nodes ...Node) Node {
	var out []Node
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if isTrueLiteral(n) {
			return TrueNode
		}
		// A permissive policy that can never match contributes nothing.
		if IsAlwaysFalse(n) {
			continue
		}
		out = append(out, n)
	}
	switch len(out) {
	case 0:
		// No permissive policy means no rows are visible. This is PostgreSQL's
		// default-deny behaviour once RLS is enabled, and getting it wrong in
		// the permissive direction would be a security hole.
		return &Literal{Value: Bool(false)}
	case 1:
		return out[0]
	}
	acc := out[0]
	for _, n := range out[1:] {
		acc = &Binary{Op: "or", Left: acc, Right: n}
	}
	return acc
}

func isTrueLiteral(n Node) bool {
	l, ok := n.(*Literal)
	return ok && l.Value.Kind == KindBool && l.Value.Bool
}

// IsAlwaysTrue reports whether the node is the constant TRUE, which lets the
// authorizer skip evaluation entirely.
func IsAlwaysTrue(n Node) bool { return isTrueLiteral(n) }

// IsAlwaysFalse reports whether the node is the constant FALSE.
func IsAlwaysFalse(n Node) bool {
	l, ok := n.(*Literal)
	return ok && l.Value.Kind == KindBool && !l.Value.Bool
}

// markCached records that every function call beneath n arrived wrapped in a
// scalar subquery -- `(select auth.uid())`.
//
// It changes nothing about how Sluice evaluates the call. It exists so that
// /diagnostics can distinguish a policy written the way Supabase's RLS
// performance guide prescribes, where PostgreSQL hoists the call into an
// InitPlan and evaluates it once per query, from one where PostgreSQL still
// invokes it once per row.
func markCached(n Node) {
	Walk(n, func(c Node) {
		if f, ok := c.(*FuncCall); ok {
			f.Cached = true
		}
	})
}

// Walk calls fn for n and every node beneath it, in pre-order.
func Walk(n Node, fn func(Node)) {
	if n == nil {
		return
	}
	fn(n)
	switch t := n.(type) {
	case *CastExpr:
		Walk(t.Arg, fn)
	case *FuncCall:
		for _, a := range t.Args {
			Walk(a, fn)
		}
	case *Unary:
		Walk(t.Arg, fn)
	case *Binary:
		Walk(t.Left, fn)
		Walk(t.Right, fn)
	case *IsTest:
		Walk(t.Arg, fn)
	case *InList:
		Walk(t.Arg, fn)
		for _, i := range t.Items {
			Walk(i, fn)
		}
	case *ArrayExpr:
		for _, i := range t.Items {
			Walk(i, fn)
		}
	}
}

// Format renders a node back to approximate SQL. Used only for diagnostics, so
// it favours readability over exact round-tripping.
func Format(n Node) string {
	var b strings.Builder
	format(&b, n)
	return b.String()
}

func format(b *strings.Builder, n Node) {
	switch t := n.(type) {
	case *ColumnRef:
		if t.Qualifier != "" {
			b.WriteString(t.Qualifier)
			b.WriteByte('.')
		}
		b.WriteString(t.Name)
	case *Literal:
		if t.Value.Kind == KindText || t.Value.Kind == KindJSON {
			b.WriteByte('\'')
			b.WriteString(strings.ReplaceAll(t.Value.Str, "'", "''"))
			b.WriteByte('\'')
		} else {
			b.WriteString(t.Value.String())
		}
	case *CastExpr:
		format(b, t.Arg)
		b.WriteString("::")
		b.WriteString(t.Type)
	case *FuncCall:
		if t.Schema != "" {
			b.WriteString(t.Schema)
			b.WriteByte('.')
		}
		b.WriteString(t.Name)
		b.WriteByte('(')
		for i, a := range t.Args {
			if i > 0 {
				b.WriteString(", ")
			}
			format(b, a)
		}
		b.WriteByte(')')
	case *Unary:
		b.WriteString(strings.ToUpper(t.Op))
		b.WriteByte(' ')
		format(b, t.Arg)
	case *Binary:
		b.WriteByte('(')
		format(b, t.Left)
		b.WriteByte(' ')
		b.WriteString(strings.ToUpper(t.Op))
		b.WriteByte(' ')
		format(b, t.Right)
		b.WriteByte(')')
	case *IsTest:
		format(b, t.Arg)
		b.WriteString(" IS ")
		if t.Negate {
			b.WriteString("NOT ")
		}
		b.WriteString(strings.ToUpper(t.Test))
	case *InList:
		format(b, t.Arg)
		if t.Negate {
			b.WriteString(" NOT")
		}
		b.WriteString(" IN (")
		for i, it := range t.Items {
			if i > 0 {
				b.WriteString(", ")
			}
			format(b, it)
		}
		b.WriteByte(')')
	case *ArrayExpr:
		b.WriteString("ARRAY[")
		for i, it := range t.Items {
			if i > 0 {
				b.WriteString(", ")
			}
			format(b, it)
		}
		b.WriteByte(']')
	case *SubqueryExpr:
		b.WriteString(t.SQL)
	default:
		b.WriteString("<?>")
	}
}
