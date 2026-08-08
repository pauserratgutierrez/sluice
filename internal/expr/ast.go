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
