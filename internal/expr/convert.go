package expr

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/pgplex/pgparser/nodes"
	pgparser "github.com/pgplex/pgparser/parser"
)

// Parse parses the SQL expression text that `pg_get_expr(polqual, polrelid)`
// produces.
//
// The grammar is PostgreSQL's own. `pgplex/pgparser` is a port of
// `src/backend/parser/gram.y` through goyacc, with node types that map 1:1 onto
// `parsenodes.h`, so what is parsed here is what PostgreSQL parses -- no
// hand-maintained subset, no operator-precedence table of our own to get wrong.
// It is pure Go, so the binary stays static and cgo-free.
//
// What Sluice still owns is the SEMANTICS: which nodes it is willing to evaluate
// in process. That decision lives in convert() below, and it fails closed. A
// node the converter does not recognise becomes a SubqueryExpr, which Analyze
// reports as non-compilable, which sends the subscription to Tier C -- correct
// but slow, and loudly reported.
//
// That distinction matters. When Sluice hand-parsed a subset, a gap in the
// grammar could silently produce the WRONG tree: `CURRENT_USER` parsed as a
// column reference looked perfectly compilable while the WAL tuple could never
// supply it, so every row was withheld without a word. Now an unsupported
// construct is structurally unrepresentable rather than quietly misread.
func Parse(sql string) (Node, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, unsupported("empty expression")
	}

	// pg_get_expr returns a bare expression, and the grammar only accepts
	// statements. Wrapping it in a trivial SELECT and taking the target-list
	// entry back out is the standard way to parse a fragment.
	stmts, err := pgparser.Parse("SELECT " + sql)
	if err != nil {
		return nil, unsupported("%v", err)
	}

	raw, err := targetExpr(stmts)
	if err != nil {
		return nil, err
	}
	return convert(raw), nil
}

// targetExpr digs the single expression back out of `SELECT <expr>`.
func targetExpr(stmts *nodes.List) (nodes.Node, error) {
	if stmts == nil || len(stmts.Items) != 1 {
		return nil, unsupported("expected exactly one statement")
	}
	sel, ok := stmts.Items[0].(*nodes.SelectStmt)
	if !ok {
		return nil, unsupported("expected a SELECT, got %T", stmts.Items[0])
	}
	if sel.TargetList == nil || len(sel.TargetList.Items) != 1 {
		return nil, unsupported("expected exactly one target expression")
	}
	rt, ok := sel.TargetList.Items[0].(*nodes.ResTarget)
	if !ok || rt.Val == nil {
		return nil, unsupported("expected a value in the target list")
	}
	return rt.Val, nil
}

// unsupportedNode is the fail-closed result: everything the converter declines
// becomes one of these, and Analyze treats it as non-compilable.
func unsupportedNode(format string, args ...any) Node {
	return &SubqueryExpr{SQL: fmt.Sprintf(format, args...)}
}

// convert maps a PostgreSQL parse node onto Sluice's evaluable AST.
//
// Every case here is a deliberate statement that Sluice reproduces PostgreSQL's
// semantics for that construct. Everything else is declined.
func convert(n nodes.Node) Node {
	switch t := n.(type) {
	case nil:
		return unsupportedNode("empty node")

	case *nodes.A_Const:
		return convertConst(t)

	case *nodes.ColumnRef:
		return convertColumnRef(t)

	case *nodes.A_Expr:
		return convertAExpr(t)

	case *nodes.BoolExpr:
		return convertBoolExpr(t)

	case *nodes.NullTest:
		return &IsTest{
			Arg:    convert(t.Arg),
			Test:   "null",
			Negate: t.Nulltesttype == nodes.IS_NOT_NULL,
		}

	case *nodes.BooleanTest:
		return convertBooleanTest(t)

	case *nodes.TypeCast:
		return convertCast(t)

	case *nodes.FuncCall:
		return convertFuncCall(t)

	case *nodes.CoalesceExpr:
		// PostgreSQL parses COALESCE into its own node rather than a FuncCall.
		return &FuncCall{Name: "coalesce", Args: convertList(t.Args)}

	case *nodes.SQLValueFunction:
		// CURRENT_USER and friends: keyword-spelled functions with no parens.
		name, ok := sqlValueFuncName(t.Op)
		if !ok {
			return unsupportedNode("SQL value function %d", t.Op)
		}
		return &FuncCall{Name: name}

	case *nodes.A_ArrayExpr:
		return &ArrayExpr{Items: convertList(t.Elements)}

	case *nodes.SubLink:
		return convertSubLink(t)

	case *nodes.List:
		// A bare list appears as the right-hand side of IN. Represent it as an
		// array so the caller can fold it into an InList.
		return &ArrayExpr{Items: convertList(t)}
	}

	return unsupportedNode("%T is not evaluable in process", n)
}

func convertList(l *nodes.List) []Node {
	if l == nil {
		return nil
	}
	out := make([]Node, 0, len(l.Items))
	for _, item := range l.Items {
		out = append(out, convert(item))
	}
	return out
}

func convertConst(c *nodes.A_Const) Node {
	if c.Isnull || c.Val == nil {
		return &Literal{Value: Null}
	}
	switch v := c.Val.(type) {
	case *nodes.Integer:
		return &Literal{Value: Int(v.Ival)}
	case *nodes.Float:
		// PostgreSQL keeps floats as strings to preserve precision, and so does
		// Sluice when the value will not round-trip as a JSON number.
		if f, err := strconv.ParseFloat(v.Fval, 64); err == nil {
			return &Literal{Value: Float(f)}
		}
		return &Literal{Value: Text(v.Fval)}
	case *nodes.String:
		return &Literal{Value: Text(v.Str)}
	case *nodes.Boolean:
		return &Literal{Value: Bool(v.Boolval)}
	}
	return unsupportedNode("constant of type %T", c.Val)
}

func convertColumnRef(c *nodes.ColumnRef) Node {
	if c.Fields == nil || len(c.Fields.Items) == 0 {
		return unsupportedNode("column reference with no name")
	}
	parts := make([]string, 0, len(c.Fields.Items))
	for _, f := range c.Fields.Items {
		s, ok := f.(*nodes.String)
		if !ok {
			// A_Star, i.e. `t.*`. Never meaningful in a policy predicate.
			return unsupportedNode("wildcard column reference")
		}
		parts = append(parts, s.Str)
	}
	switch len(parts) {
	case 1:
		return &ColumnRef{Name: parts[0]}
	default:
		// Keep only the last qualifier, matching how pg_get_expr writes
		// self-references as `relation.column`.
		return &ColumnRef{Qualifier: parts[len(parts)-2], Name: parts[len(parts)-1]}
	}
}

// operatorName renders a possibly-qualified operator name. Anything in a schema
// other than pg_catalog is a user-defined operator whose semantics Sluice cannot
// know.
func operatorName(l *nodes.List) (string, bool) {
	if l == nil || len(l.Items) == 0 {
		return "", false
	}
	parts := make([]string, 0, len(l.Items))
	for _, item := range l.Items {
		s, ok := item.(*nodes.String)
		if !ok {
			return "", false
		}
		parts = append(parts, s.Str)
	}
	if len(parts) == 2 && parts[0] != "pg_catalog" {
		return "", false
	}
	return parts[len(parts)-1], true
}

func convertAExpr(e *nodes.A_Expr) Node {
	op, ok := operatorName(e.Name)
	if !ok {
		return unsupportedNode("user-defined operator")
	}

	switch e.Kind {
	case nodes.AEXPR_OP:
		// A unary operator has no left argument, e.g. `-x`.
		if e.Lexpr == nil {
			if op != "-" && op != "+" {
				return unsupportedNode("unary operator %q", op)
			}
			return &Unary{Op: op, Arg: convert(e.Rexpr)}
		}
		// Whether Sluice can actually evaluate this operator is not decided
		// here. Analyze checks it against evaluableBinaryOps, which is the one
		// list kept in step with evalBinary, and reports it in the operator's
		// own vocabulary.
		return &Binary{Op: op, Left: convert(e.Lexpr), Right: convert(e.Rexpr)}

	case nodes.AEXPR_DISTINCT:
		return &Binary{Op: "isdistinct", Left: convert(e.Lexpr), Right: convert(e.Rexpr)}
	case nodes.AEXPR_NOT_DISTINCT:
		return &Binary{Op: "isnotdistinct", Left: convert(e.Lexpr), Right: convert(e.Rexpr)}

	case nodes.AEXPR_NULLIF:
		return &FuncCall{Name: "nullif", Args: []Node{convert(e.Lexpr), convert(e.Rexpr)}}

	case nodes.AEXPR_IN:
		// The grammar guarantees the operator is "=" for IN and "<>" for NOT IN.
		items := convert(e.Rexpr)
		arr, isArr := items.(*ArrayExpr)
		if !isArr {
			return unsupportedNode("IN over a non-list")
		}
		return &InList{Arg: convert(e.Lexpr), Items: arr.Items, Negate: op == "<>"}

	case nodes.AEXPR_LIKE, nodes.AEXPR_ILIKE:
		// The operator name already encodes negation and case sensitivity:
		// ~~ / !~~ / ~~* / !~~*.
		return &Binary{Op: op, Left: convert(e.Lexpr), Right: convert(e.Rexpr)}

	case nodes.AEXPR_BETWEEN, nodes.AEXPR_NOT_BETWEEN:
		bounds, ok := twoBounds(e.Rexpr)
		if !ok {
			return unsupportedNode("BETWEEN with malformed bounds")
		}
		arg := convert(e.Lexpr)
		var out Node = &Binary{
			Op:    "and",
			Left:  &Binary{Op: ">=", Left: arg, Right: bounds[0]},
			Right: &Binary{Op: "<=", Left: arg, Right: bounds[1]},
		}
		if e.Kind == nodes.AEXPR_NOT_BETWEEN {
			out = &Unary{Op: "not", Arg: out}
		}
		return out

	case nodes.AEXPR_OP_ANY, nodes.AEXPR_OP_ALL:
		return convertQuantified(e, op)
	}

	return unsupportedNode("expression kind %d", e.Kind)
}

// convertQuantified maps the two quantified forms that membership tests
// actually arrive in.
//
// pg_get_expr does not preserve `IN`: it deparses `x IN (a, b)` as
// `x = ANY (ARRAY[a, b])` and `x NOT IN (a, b)` as `x <> ALL (ARRAY[a, b])`.
// Those two spellings are therefore the ones that matter, and both reduce to an
// InList, whose evaluator already implements SQL's three-valued semantics for a
// NULL among the elements.
//
// The other two combinations do not reduce: `x <> ANY (...)` is true when any
// element differs, and `x = ALL (...)` when every element matches. Neither is a
// membership test, so both are declined rather than approximated.
func convertQuantified(e *nodes.A_Expr, op string) Node {
	negate := e.Kind == nodes.AEXPR_OP_ALL
	quantifier, want := "ANY", "="
	if negate {
		quantifier, want = "ALL", "<>"
	}
	if op != want {
		return unsupportedNode("%s %s (...)", op, quantifier)
	}
	arr, isArray := convert(e.Rexpr).(*ArrayExpr)
	if !isArray {
		return unsupportedNode("%s %s (...) over a non-literal array", op, quantifier)
	}
	return &InList{Arg: convert(e.Lexpr), Items: arr.Items, Negate: negate}
}

func twoBounds(n nodes.Node) ([2]Node, bool) {
	l, ok := n.(*nodes.List)
	if !ok || len(l.Items) != 2 {
		return [2]Node{}, false
	}
	return [2]Node{convert(l.Items[0]), convert(l.Items[1])}, true
}

func convertBoolExpr(b *nodes.BoolExpr) Node {
	args := convertList(b.Args)
	switch b.Boolop {
	case nodes.NOT_EXPR:
		if len(args) != 1 {
			return unsupportedNode("NOT with %d arguments", len(args))
		}
		return &Unary{Op: "not", Arg: args[0]}
	case nodes.AND_EXPR, nodes.OR_EXPR:
		if len(args) == 0 {
			return unsupportedNode("boolean expression with no arguments")
		}
		op := "and"
		if b.Boolop == nodes.OR_EXPR {
			op = "or"
		}
		// PostgreSQL flattens nested AND/OR into an n-ary node; rebuild it as a
		// left-leaning chain, which is what the evaluator expects.
		acc := args[0]
		for _, a := range args[1:] {
			acc = &Binary{Op: op, Left: acc, Right: a}
		}
		return acc
	}
	return unsupportedNode("boolean operator %d", b.Boolop)
}

func convertBooleanTest(t *nodes.BooleanTest) Node {
	arg := convert(t.Arg)
	switch t.Booltesttype {
	case nodes.IS_TRUE:
		return &IsTest{Arg: arg, Test: "true"}
	case nodes.IS_NOT_TRUE:
		return &IsTest{Arg: arg, Negate: true, Test: "true"}
	case nodes.IS_FALSE:
		return &IsTest{Arg: arg, Test: "false"}
	case nodes.IS_NOT_FALSE:
		return &IsTest{Arg: arg, Negate: true, Test: "false"}
	case nodes.IS_UNKNOWN:
		return &IsTest{Arg: arg, Test: "unknown"}
	case nodes.IS_NOT_UNKNOWN:
		return &IsTest{Arg: arg, Negate: true, Test: "unknown"}
	}
	return unsupportedNode("boolean test %d", t.Booltesttype)
}

func convertFuncCall(f *nodes.FuncCall) Node {
	if f.AggStar || f.AggDistinct || f.AggOrder != nil || f.AggFilter != nil ||
		f.Over != nil || f.FuncVariadic {
		return unsupportedNode("aggregate or window function call")
	}
	parts, ok := nameParts(f.Funcname)
	if !ok {
		return unsupportedNode("function with an unnameable name")
	}
	call := &FuncCall{Name: strings.ToLower(parts[len(parts)-1]), Args: convertList(f.Args)}
	if len(parts) > 1 {
		call.Schema = strings.ToLower(parts[len(parts)-2])
	}
	return call
}

func nameParts(l *nodes.List) ([]string, bool) {
	if l == nil || len(l.Items) == 0 {
		return nil, false
	}
	out := make([]string, 0, len(l.Items))
	for _, item := range l.Items {
		s, ok := item.(*nodes.String)
		if !ok {
			return nil, false
		}
		out = append(out, s.Str)
	}
	return out, true
}

// convertSubLink handles the one subquery shape Sluice unwraps.
//
// `(select auth.uid())` is the InitPlan wrapper the Supabase RLS performance
// guide prescribes: it tells the planner the value is constant for the whole
// query, so PostgreSQL computes it once instead of once per row. It carries no
// other meaning, and unwrapping it is what keeps a policy written the
// recommended way in Tier A instead of putting the best-practice spelling on the
// slowest path.
//
// Anything with a FROM clause is a real subquery over real data and is declined.
func convertSubLink(s *nodes.SubLink) Node {
	switch nodes.SubLinkType(s.SubLinkType) {
	case nodes.EXPR_SUBLINK:
		if inner, ok := scalarSelect(s.Subselect); ok {
			n := convert(inner)
			markCached(n)
			return n
		}
		return unsupportedNode("(SELECT ...) over a table")

	case nodes.ANY_SUBLINK:
		// `x = ANY (select f())` and `x IN (select f())` both arrive here. A
		// FROM-less select yields exactly one row, so this is a plain comparison.
		inner, ok := scalarSelect(s.Subselect)
		if !ok {
			return unsupportedNode("IN (SELECT ...) over a table")
		}
		op := "="
		if name, ok := operatorName(s.OperName); ok && name != "" {
			op = name
		}
		right := convert(inner)
		markCached(right)
		if s.Testexpr == nil {
			return unsupportedNode("ANY (SELECT ...) with no test expression")
		}
		return &Binary{Op: op, Left: convert(s.Testexpr), Right: right}

	case nodes.EXISTS_SUBLINK:
		return unsupportedNode("EXISTS (SELECT ...), which cannot be evaluated against the WAL tuple")
	}
	return unsupportedNode("subquery of type %d", s.SubLinkType)
}

// scalarSelect recognises `SELECT <expr>` with nothing else attached, and
// returns the single expression.
func scalarSelect(n nodes.Node) (nodes.Node, bool) {
	sel, ok := n.(*nodes.SelectStmt)
	if !ok {
		return nil, false
	}
	// Anything beyond a bare target list means the value depends on stored data.
	if sel.FromClause != nil && len(sel.FromClause.Items) > 0 {
		return nil, false
	}
	if sel.WhereClause != nil || sel.HavingClause != nil || sel.WithClause != nil ||
		sel.GroupClause != nil || sel.WindowClause != nil || sel.DistinctClause != nil ||
		sel.SortClause != nil || sel.LimitCount != nil || sel.LimitOffset != nil ||
		sel.ValuesLists != nil || sel.Larg != nil || sel.Rarg != nil {
		return nil, false
	}
	if sel.TargetList == nil || len(sel.TargetList.Items) != 1 {
		return nil, false
	}
	rt, ok := sel.TargetList.Items[0].(*nodes.ResTarget)
	if !ok || rt.Val == nil {
		return nil, false
	}
	return rt.Val, true
}

// convertCast maps `expr::type`. Alias spellings are normalised downstream by
// Cast, so the type name is passed through as written.
//
// An array cast is only accepted where Sluice can push it down: PostgreSQL
// treats `ARRAY[...]::t[]` as casting each element, and that is reproduced
// exactly. Casting anything else to an array type is declined rather than
// evaluated as though the `[]` were absent.
func convertCast(t *nodes.TypeCast) Node {
	if t.TypeName == nil {
		return unsupportedNode("cast with no target type")
	}
	parts, ok := nameParts(t.TypeName.Names)
	if !ok {
		return unsupportedNode("cast to an unnameable type")
	}
	typ := strings.ToLower(parts[len(parts)-1])
	arg := convert(t.Arg)

	if t.TypeName.ArrayBounds != nil && len(t.TypeName.ArrayBounds.Items) > 0 {
		arr, isArray := arg.(*ArrayExpr)
		if !isArray {
			return unsupportedNode("cast to array type %s[]", typ)
		}
		for i, item := range arr.Items {
			arr.Items[i] = &CastExpr{Arg: item, Type: typ}
		}
		return arr
	}
	return &CastExpr{Arg: arg, Type: typ}
}

func sqlValueFuncName(op nodes.SVFOp) (string, bool) {
	switch op {
	case nodes.SVFOP_CURRENT_DATE:
		return "current_date", true
	case nodes.SVFOP_CURRENT_TIME, nodes.SVFOP_CURRENT_TIME_N:
		return "current_time", true
	case nodes.SVFOP_CURRENT_TIMESTAMP, nodes.SVFOP_CURRENT_TIMESTAMP_N:
		return "current_timestamp", true
	case nodes.SVFOP_LOCALTIME, nodes.SVFOP_LOCALTIME_N:
		return "localtime", true
	case nodes.SVFOP_LOCALTIMESTAMP, nodes.SVFOP_LOCALTIMESTAMP_N:
		return "localtimestamp", true
	case nodes.SVFOP_CURRENT_ROLE:
		return "current_role", true
	case nodes.SVFOP_CURRENT_USER:
		return "current_user", true
	case nodes.SVFOP_USER:
		return "user", true
	case nodes.SVFOP_SESSION_USER:
		return "session_user", true
	case nodes.SVFOP_CURRENT_CATALOG:
		return "current_catalog", true
	case nodes.SVFOP_CURRENT_SCHEMA:
		return "current_schema", true
	}
	return "", false
}
