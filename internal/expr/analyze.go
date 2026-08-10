package expr

import (
	"sort"
	"strings"
)

// Info describes what a predicate needs in order to be evaluated, and is what
// the authorizer uses to pick a tier.
type Info struct {
	// Columns of the target relation the predicate reads, sorted and deduped.
	Columns []string

	// Qualifiers seen on column references other than the target relation. A
	// non-empty value means a correlated reference, i.e. a subquery.
	ForeignQualifiers []string

	// Subqueries is the list of subquery fragments found, for diagnostics.
	Subqueries []string

	// Volatile is true when the predicate's truth value can change without any
	// row or claim changing -- `now()`, or a subquery over another table. Those
	// subscriptions need a lease; stable ones do not.
	Volatile bool

	// Unsupported is non-empty when the predicate cannot be evaluated in
	// process. It is the human-readable reason shown in /diagnostics, so it is
	// written to be actionable rather than terse.
	Unsupported string

	// UncachedCalls names the per-query functions -- auth.uid() and friends --
	// that are NOT wrapped in a scalar subquery. Sluice evaluates them once
	// either way; PostgreSQL does not, and re-invokes them for every row it
	// scans whenever the policy is evaluated in the database, which is every
	// snapshot, every Tier C probe, and every ordinary query the application
	// makes. Reported, never a reason to refuse.
	UncachedCalls []string
}

// Compilable reports whether the predicate can be evaluated in process, i.e.
// whether Tier A or Tier B is available.
func (i Info) Compilable() bool { return i.Unsupported == "" }

// Analyze walks a predicate and reports what it needs.
//
// `relation` is the unqualified name of the target relation, used to tell
// self-references (fine) from correlated references into another table (not
// fine). Pass "" to accept any qualifier.
func Analyze(n Node, relation string) Info {
	a := &analyzer{
		relation: strings.ToLower(relation),
		cols:     map[string]bool{},
		quals:    map[string]bool{},
		uncached: map[string]bool{},
	}
	a.walk(n)

	info := Info{
		Subqueries:  a.subqueries,
		Volatile:    a.volatile,
		Unsupported: a.unsupported,
	}
	for f := range a.uncached {
		info.UncachedCalls = append(info.UncachedCalls, f)
	}
	sort.Strings(info.UncachedCalls)
	for c := range a.cols {
		info.Columns = append(info.Columns, c)
	}
	sort.Strings(info.Columns)
	for q := range a.quals {
		info.ForeignQualifiers = append(info.ForeignQualifiers, q)
	}
	sort.Strings(info.ForeignQualifiers)

	if info.Unsupported == "" && len(info.ForeignQualifiers) > 0 {
		info.Unsupported = "references columns of another relation (" +
			strings.Join(info.ForeignQualifiers, ", ") + "), which requires a join"
	}
	return info
}

type analyzer struct {
	relation    string
	cols        map[string]bool
	quals       map[string]bool
	uncached    map[string]bool
	subqueries  []string
	volatile    bool
	unsupported string
}

func (a *analyzer) fail(reason string) {
	if a.unsupported == "" {
		a.unsupported = reason
	}
}

// Functions whose value depends only on their arguments and the claim set.
// Everything outside this set makes the predicate uncompilable.
var stableFuncs = map[string]bool{
	"auth.uid": true, "auth.role": true, "auth.email": true, "auth.jwt": true,
	"current_setting": true,
	"coalesce":        true, "nullif": true,
	"lower": true, "upper": true, "length": true,
	"btrim": true, "trim": true, "ltrim": true, "rtrim": true,
	"jsonb_extract_path_text": true, "json_extract_path_text": true,
}

// Functions that are evaluable but whose result changes over time, so the
// decision needs a lease.
var volatileFuncs = map[string]bool{
	"now": true, "current_timestamp": true,
	"statement_timestamp": true, "transaction_timestamp": true,
	"clock_timestamp": true, "random": true,
}

// perQueryFuncs are the calls whose value is fixed for a whole query, and which
// therefore belong inside a `(select ...)` wrapper so PostgreSQL evaluates them
// once rather than per scanned row.
var perQueryFuncs = map[string]bool{
	"auth.uid": true, "auth.role": true, "auth.email": true, "auth.jwt": true,
	"current_setting": true,
}

// evaluableBinaryOps is exactly the set evalBinary implements.
//
// Analyze checks against it so that an operator the parser accepts but the
// evaluator does not is reported as Tier C. Without this check such an operator
// compiles to a predicate that returns unknown for every row -- which withholds
// every row, silently, with nothing in /diagnostics to explain it. Adding a case
// to evalBinary means adding it here too.
var evaluableBinaryOps = map[string]bool{
	"and": true, "or": true,
	"=": true, "<>": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
	"isdistinct": true, "isnotdistinct": true,
	"~~": true, "~~*": true, "!~~": true, "!~~*": true,
	"->": true, "->>": true,
	"||": true,
	"+":  true, "-": true, "*": true, "/": true, "%": true,
}

// describeOp names an operator the way an operator would recognise it.
func describeOp(op string) string {
	switch op {
	case "~", "~*", "!~", "!~*":
		return "POSIX regular-expression operator " + op
	case "@>", "<@":
		return "containment operator " + op
	case "#>", "#>>":
		return "JSON path operator " + op
	}
	return "operator " + op
}

func (a *analyzer) walk(n Node) {
	switch t := n.(type) {
	case nil:
		return

	case *Literal:

	case *ColumnRef:
		q := strings.ToLower(t.Qualifier)
		if q != "" && a.relation != "" && q != a.relation {
			a.quals[q] = true
			return
		}
		a.cols[strings.ToLower(t.Name)] = true

	case *CastExpr:
		a.walk(t.Arg)

	case *FuncCall:
		name := strings.ToLower(t.Name)
		if t.Schema != "" {
			name = strings.ToLower(t.Schema) + "." + name
		}
		switch {
		case stableFuncs[name]:
		case volatileFuncs[name]:
			a.volatile = true
		default:
			a.fail("calls function " + name + "(), which is not in the compilable whitelist")
		}
		if perQueryFuncs[name] && !t.Cached {
			a.uncached[name] = true
		}
		for _, arg := range t.Args {
			a.walk(arg)
		}

	case *Unary:
		a.walk(t.Arg)

	case *Binary:
		if !evaluableBinaryOps[t.Op] {
			a.fail("uses the " + describeOp(t.Op) +
				", which Sluice cannot evaluate against the WAL tuple")
		}
		a.walk(t.Left)
		a.walk(t.Right)

	case *IsTest:
		a.walk(t.Arg)

	case *InList:
		a.walk(t.Arg)
		for _, it := range t.Items {
			a.walk(it)
		}

	case *ArrayExpr:
		for _, it := range t.Items {
			a.walk(it)
		}

	case *SubqueryExpr:
		a.subqueries = append(a.subqueries, t.SQL)
		a.volatile = true
		a.fail("contains a subquery (" + t.SQL + "), which cannot be evaluated against the WAL tuple")

	default:
		a.fail("contains an expression node Sluice does not recognise")
	}
}

// Substitute replaces column references with literals.
//
// This is the mechanism behind Tier A: if the shape pins every column the
// predicate reads to an equality constant, substituting them leaves an
// expression with no column references at all, whose value is therefore the
// same for every row in the shape.
func Substitute(n Node, consts map[string]Value) Node {
	if ref, ok := n.(*ColumnRef); ok {
		if v, ok := consts[strings.ToLower(ref.Name)]; ok {
			return &Literal{Value: v}
		}
		return ref
	}
	return mapChildren(n, func(c Node) Node { return Substitute(c, consts) })
}

// Fold evaluates every subtree that contains no column references and no
// volatile functions, replacing it with a literal.
//
// This is a large win, not a micro-optimisation. The canonical Supabase policy
// is
//
//	owner_id = (current_setting('request.jwt.claims')::jsonb ->> 'sub')::uuid
//
// and the entire right-hand side is constant for a given subscription. Folding
// it once at subscribe time turns the per-change evaluation into a single string
// comparison, and removes JSON parsing from the hot path entirely.
func Fold(n Node, ctx *Context) Node {
	if n == nil {
		return nil
	}
	// Recurse first so that inner constants are available to outer nodes.
	n = mapChildren(n, func(c Node) Node { return Fold(c, ctx) })

	if _, isLit := n.(*Literal); isLit {
		return n
	}
	info := Analyze(n, "")
	if !info.Compilable() || info.Volatile || len(info.Columns) > 0 {
		return n
	}
	v, err := Eval(n, ctx)
	if err != nil {
		return n
	}
	if v.Kind == KindArray {
		// Keep arrays as ArrayExpr so InList continues to see literal items.
		items := make([]Node, len(v.Arr))
		for i, e := range v.Arr {
			items[i] = &Literal{Value: e}
		}
		return &ArrayExpr{Items: items}
	}
	return &Literal{Value: v}
}

// mapChildren rebuilds a node with each child passed through f. Leaves are
// returned unchanged, so both Fold and Substitute are one-liners over it rather
// than two copies of the same traversal.
func mapChildren(n Node, f func(Node) Node) Node {
	switch t := n.(type) {
	case *CastExpr:
		return &CastExpr{Arg: f(t.Arg), Type: t.Type}
	case *FuncCall:
		out := &FuncCall{Schema: t.Schema, Name: t.Name, Cached: t.Cached, Args: make([]Node, len(t.Args))}
		for i, a := range t.Args {
			out.Args[i] = f(a)
		}
		return out
	case *Unary:
		return &Unary{Op: t.Op, Arg: f(t.Arg)}
	case *Binary:
		return &Binary{Op: t.Op, Left: f(t.Left), Right: f(t.Right)}
	case *IsTest:
		return &IsTest{Arg: f(t.Arg), Negate: t.Negate, Test: t.Test}
	case *InList:
		out := &InList{Arg: f(t.Arg), Negate: t.Negate, Items: make([]Node, len(t.Items))}
		for i, it := range t.Items {
			out.Items[i] = f(it)
		}
		return out
	case *ArrayExpr:
		out := &ArrayExpr{Items: make([]Node, len(t.Items))}
		for i, it := range t.Items {
			out.Items[i] = f(it)
		}
		return out
	}
	return n
}

// Visible collapses an evaluation result to a visibility decision.
//
// Only a definite TRUE grants visibility. NULL (SQL unknown) and ErrUnknown
// (the WAL did not carry a needed value) both deny, and ErrUnknown is reported
// separately so the caller can mark the event degraded rather than pretending
// it made a clean decision.
func Visible(n Node, ctx *Context) (visible bool, unknown bool) {
	v, err := Eval(n, ctx)
	if err != nil {
		return false, true
	}
	if v.IsNull() {
		return false, false
	}
	return v.Truthy(), false
}
