package expr

import (
	"encoding/json"
	"strings"
	"time"
)

// Row supplies column values for evaluation.
//
// The second return value distinguishes "this column is NULL" from "the WAL did
// not carry a value for this column". The latter happens for unchanged TOASTed
// values (pgoutput's 'u' marker) and for columns absent from an old tuple under
// a narrow replica identity, and it must surface as ErrUnknown rather than being
// silently coerced to NULL.
type Row interface {
	Column(name string) (v Value, known bool)
}

// Claims is the verified JWT claim set.
type Claims map[string]any

// Context carries everything evaluation needs besides the AST.
type Context struct {
	Row        Row
	Claims     Claims
	ClaimsJSON string // raw JSON, so `current_setting('request.jwt.claims')` is exact
	Now        time.Time
}

// Eval evaluates a node to a Value.
//
// SQL three-valued logic is respected: comparisons involving NULL yield NULL,
// AND/OR follow the standard truth tables. Callers deciding visibility must
// treat anything other than a definite TRUE as "not visible".
func Eval(n Node, ctx *Context) (Value, error) {
	switch t := n.(type) {
	case *Literal:
		return t.Value, nil

	case *ColumnRef:
		if ctx.Row == nil {
			return Null, ErrUnknown
		}
		v, known := ctx.Row.Column(t.Name)
		if !known {
			return Null, ErrUnknown
		}
		return v, nil

	case *CastExpr:
		v, err := Eval(t.Arg, ctx)
		if err != nil {
			return Null, err
		}
		return Cast(v, t.Type)

	case *FuncCall:
		return evalFunc(t, ctx)

	case *ArrayExpr:
		out := make([]Value, 0, len(t.Items))
		for _, it := range t.Items {
			v, err := Eval(it, ctx)
			if err != nil {
				return Null, err
			}
			out = append(out, v)
		}
		return Array(out), nil

	case *Unary:
		return evalUnary(t, ctx)

	case *Binary:
		return evalBinary(t, ctx)

	case *IsTest:
		return evalIsTest(t, ctx)

	case *InList:
		return evalInList(t, ctx)

	case *SubqueryExpr:
		return Null, unsupported("subquery cannot be evaluated in process: %s", t.SQL)
	}
	return Null, unsupported("unknown node type %T", n)
}

func evalUnary(t *Unary, ctx *Context) (Value, error) {
	v, err := Eval(t.Arg, ctx)
	if err != nil {
		return Null, err
	}
	switch t.Op {
	case "not":
		if v.IsNull() {
			return Null, nil // NOT NULL is NULL in SQL
		}
		return Bool(!v.Truthy()), nil
	case "-":
		switch v.Kind {
		case KindInt:
			return Int(-v.Int), nil
		case KindFloat:
			return Float(-v.Float), nil
		case KindNull:
			return Null, nil
		}
		return Null, unsupported("unary minus on %v", v.Kind)
	}
	return Null, unsupported("unary operator %q", t.Op)
}

func evalBinary(t *Binary, ctx *Context) (Value, error) {
	switch t.Op {
	case "and":
		// Short-circuit on a definite FALSE, which is also what lets a policy
		// like `false AND <subquery>` stay evaluable in process.
		l, lerr := Eval(t.Left, ctx)
		if lerr == nil && !l.IsNull() && !l.Truthy() {
			return Bool(false), nil
		}
		r, rerr := Eval(t.Right, ctx)
		if rerr == nil && !r.IsNull() && !r.Truthy() {
			return Bool(false), nil
		}
		if lerr != nil {
			return Null, lerr
		}
		if rerr != nil {
			return Null, rerr
		}
		if l.IsNull() || r.IsNull() {
			return Null, nil
		}
		return Bool(l.Truthy() && r.Truthy()), nil

	case "or":
		l, lerr := Eval(t.Left, ctx)
		if lerr == nil && !l.IsNull() && l.Truthy() {
			return Bool(true), nil
		}
		r, rerr := Eval(t.Right, ctx)
		if rerr == nil && !r.IsNull() && r.Truthy() {
			return Bool(true), nil
		}
		if lerr != nil {
			return Null, lerr
		}
		if rerr != nil {
			return Null, rerr
		}
		if l.IsNull() || r.IsNull() {
			return Null, nil
		}
		return Bool(false), nil
	}

	l, err := Eval(t.Left, ctx)
	if err != nil {
		return Null, err
	}
	r, err := Eval(t.Right, ctx)
	if err != nil {
		return Null, err
	}

	switch t.Op {
	case "=", "<>", "!=", "<", "<=", ">", ">=":
		c, err := compare(l, r)
		if err != nil {
			if err == ErrUnknown {
				return Null, nil // comparison with NULL is NULL
			}
			return Null, err
		}
		switch t.Op {
		case "=":
			return Bool(c == 0), nil
		case "<>", "!=":
			return Bool(c != 0), nil
		case "<":
			return Bool(c < 0), nil
		case "<=":
			return Bool(c <= 0), nil
		case ">":
			return Bool(c > 0), nil
		case ">=":
			return Bool(c >= 0), nil
		}

	case "isdistinct", "isnotdistinct":
		eq := false
		switch {
		case l.IsNull() && r.IsNull():
			eq = true
		case l.IsNull() || r.IsNull():
			eq = false
		default:
			c, err := compare(l, r)
			if err != nil {
				return Null, err
			}
			eq = c == 0
		}
		if t.Op == "isdistinct" {
			return Bool(!eq), nil
		}
		return Bool(eq), nil

	case "~~", "~~*", "!~~", "!~~*":
		if l.IsNull() || r.IsNull() {
			return Null, nil
		}
		ci := strings.HasSuffix(t.Op, "*")
		m := likeMatch(l.String(), r.String(), ci)
		if strings.HasPrefix(t.Op, "!") {
			m = !m
		}
		return Bool(m), nil

	case "->", "->>":
		return jsonExtract(l, r, ctx, t.Op == "->>")

	case "||":
		if l.IsNull() || r.IsNull() {
			return Null, nil
		}
		return Text(l.String() + r.String()), nil

	case "+", "-", "*", "/", "%":
		return arith(t.Op, l, r)

	case "@>":
		// array/jsonb containment: only the literal-array form is supported
		if l.Kind == KindArray && r.Kind == KindArray {
			for _, want := range r.Arr {
				found := false
				for _, have := range l.Arr {
					if c, err := compare(have, want); err == nil && c == 0 {
						found = true
						break
					}
				}
				if !found {
					return Bool(false), nil
				}
			}
			return Bool(true), nil
		}
		return Null, unsupported("@> outside literal arrays")
	}

	return Null, unsupported("binary operator %q", t.Op)
}

func arith(op string, l, r Value) (Value, error) {
	if l.IsNull() || r.IsNull() {
		return Null, nil
	}
	lf, lok := asFloat(l)
	rf, rok := asFloat(r)
	if !lok || !rok {
		return Null, unsupported("arithmetic on non-numeric values")
	}
	bothInt := l.Kind == KindInt && r.Kind == KindInt
	var out float64
	switch op {
	case "+":
		out = lf + rf
	case "-":
		out = lf - rf
	case "*":
		out = lf * rf
	case "/":
		if rf == 0 {
			return Null, unsupported("division by zero")
		}
		if bothInt {
			return Int(l.Int / r.Int), nil
		}
		out = lf / rf
	case "%":
		if rf == 0 {
			return Null, unsupported("modulo by zero")
		}
		if bothInt {
			return Int(l.Int % r.Int), nil
		}
		return Null, unsupported("modulo on non-integers")
	}
	if bothInt {
		return Int(int64(out)), nil
	}
	return Float(out), nil
}

func evalIsTest(t *IsTest, ctx *Context) (Value, error) {
	v, err := Eval(t.Arg, ctx)
	if err != nil {
		// IS NULL over an unavailable value is genuinely unknown, not true.
		return Null, err
	}
	var res bool
	switch t.Test {
	case "null":
		res = v.IsNull()
	case "true":
		res = !v.IsNull() && v.Truthy()
	case "false":
		res = !v.IsNull() && !v.Truthy()
	case "unknown":
		res = v.IsNull()
	default:
		return Null, unsupported("IS %s", t.Test)
	}
	if t.Negate {
		res = !res
	}
	return Bool(res), nil
}

func evalInList(t *InList, ctx *Context) (Value, error) {
	v, err := Eval(t.Arg, ctx)
	if err != nil {
		return Null, err
	}
	if v.IsNull() {
		return Null, nil
	}
	sawNull := false
	for _, it := range t.Items {
		iv, err := Eval(it, ctx)
		if err != nil {
			return Null, err
		}
		if iv.IsNull() {
			sawNull = true
			continue
		}
		if c, err := compare(v, iv); err == nil && c == 0 {
			return Bool(!t.Negate), nil
		}
	}
	// SQL: `x IN (a, NULL)` with no match is NULL, not FALSE.
	if sawNull {
		return Null, nil
	}
	return Bool(t.Negate), nil
}

// evalFunc implements the whitelisted function set. Anything not listed here is
// unsupported, which sends the subscription to Tier C. Adding a function means
// deliberately accepting responsibility for matching PostgreSQL's semantics.
func evalFunc(t *FuncCall, ctx *Context) (Value, error) {
	name := t.Name
	if t.Schema != "" {
		name = strings.ToLower(t.Schema) + "." + name
	}

	switch name {
	case "auth.uid":
		return claimText(ctx, "sub", true)
	case "auth.role":
		return claimText(ctx, "role", false)
	case "auth.email":
		return claimText(ctx, "email", false)
	case "auth.jwt":
		return JSON(ctx.ClaimsJSON), nil

	case "current_setting":
		if len(t.Args) == 0 {
			return Null, unsupported("current_setting with no argument")
		}
		key, err := Eval(t.Args[0], ctx)
		if err != nil {
			return Null, err
		}
		name := key.String()
		switch name {
		case "request.jwt.claims", "request.jwt.claim":
			return Text(ctx.ClaimsJSON), nil
		}
		// GoTrue's own migration defines auth.uid() using the LEGACY singular
		// form, `current_setting('request.jwt.claim.sub')`. A deployment that
		// never upgraded those helpers must still resolve to Tier A, so the
		// per-claim spelling is supported too.
		if rest, ok := strings.CutPrefix(name, "request.jwt.claim."); ok && rest != "" {
			return claimText(ctx, rest, rest == "sub")
		}
		// A policy reading some other GUC depends on session state Sluice does
		// not reproduce. Refuse rather than guess.
		return Null, unsupported("current_setting(%q)", name)

	case "coalesce":
		for _, a := range t.Args {
			v, err := Eval(a, ctx)
			if err != nil {
				return Null, err
			}
			if !v.IsNull() {
				return v, nil
			}
		}
		return Null, nil

	case "nullif":
		if len(t.Args) != 2 {
			return Null, unsupported("nullif expects 2 arguments")
		}
		a, err := Eval(t.Args[0], ctx)
		if err != nil {
			return Null, err
		}
		b, err := Eval(t.Args[1], ctx)
		if err != nil {
			return Null, err
		}
		if c, err := compare(a, b); err == nil && c == 0 {
			return Null, nil
		}
		return a, nil

	case "lower", "upper", "length", "btrim", "trim", "ltrim", "rtrim":
		if len(t.Args) < 1 {
			return Null, unsupported("%s expects an argument", name)
		}
		v, err := Eval(t.Args[0], ctx)
		if err != nil {
			return Null, err
		}
		if v.IsNull() {
			return Null, nil
		}
		s := v.String()
		switch name {
		case "lower":
			return Text(strings.ToLower(s)), nil
		case "upper":
			return Text(strings.ToUpper(s)), nil
		case "length":
			return Int(int64(len([]rune(s)))), nil
		case "btrim", "trim":
			return Text(strings.TrimSpace(s)), nil
		case "ltrim":
			return Text(strings.TrimLeft(s, " ")), nil
		case "rtrim":
			return Text(strings.TrimRight(s, " ")), nil
		}

	case "now", "current_timestamp", "statement_timestamp", "transaction_timestamp":
		if ctx.Now.IsZero() {
			return Null, ErrUnknown
		}
		return Text(ctx.Now.UTC().Format(time.RFC3339Nano)), nil

	case "jsonb_extract_path_text", "json_extract_path_text":
		if len(t.Args) < 2 {
			return Null, unsupported("%s expects at least 2 arguments", name)
		}
		cur, err := Eval(t.Args[0], ctx)
		if err != nil {
			return Null, err
		}
		for _, a := range t.Args[1:] {
			k, err := Eval(a, ctx)
			if err != nil {
				return Null, err
			}
			cur, err = jsonExtract(cur, k, ctx, true)
			if err != nil {
				return Null, err
			}
		}
		return cur, nil
	}

	return Null, unsupported("function %s()", name)
}

func claimText(ctx *Context, key string, lower bool) (Value, error) {
	if ctx.Claims == nil {
		return Null, ErrUnknown
	}
	raw, ok := ctx.Claims[key]
	if !ok || raw == nil {
		return Null, nil
	}
	s, ok := raw.(string)
	if !ok {
		return Null, nil
	}
	if s == "" {
		return Null, nil
	}
	if lower {
		s = strings.ToLower(s)
	}
	return Text(s), nil
}

// jsonExtract implements -> and ->>.
//
// The hot path never reaches this function: Fold collapses claim-derived
// subtrees to literals once per subscription, because they contain no column
// references. What remains here is jsonb columns, which are rare in policies.
func jsonExtract(left, key Value, ctx *Context, asText bool) (Value, error) {
	if left.IsNull() || key.IsNull() {
		return Null, nil
	}

	// Fast path: the claims object, already parsed.
	if ctx != nil && ctx.Claims != nil && left.Str == ctx.ClaimsJSON && ctx.ClaimsJSON != "" {
		return fromAny(ctx.Claims[key.String()], asText), nil
	}

	var m any
	if err := json.Unmarshal([]byte(left.Str), &m); err != nil {
		return Null, unsupported("json extraction over a non-JSON value")
	}
	switch container := m.(type) {
	case map[string]any:
		return fromAny(container[key.String()], asText), nil
	case []any:
		i := int(key.Int)
		if key.Kind != KindInt || i < 0 || i >= len(container) {
			return Null, nil
		}
		return fromAny(container[i], asText), nil
	}
	return Null, nil
}

func fromAny(v any, asText bool) Value {
	switch x := v.(type) {
	case nil:
		return Null
	case string:
		if asText {
			return Text(x)
		}
		b, _ := json.Marshal(x)
		return JSON(string(b))
	case bool:
		if asText {
			if x {
				return Text("true")
			}
			return Text("false")
		}
		return Bool(x)
	case float64:
		if asText {
			return Text(Float(x).String())
		}
		return Float(x)
	default:
		b, _ := json.Marshal(x)
		if asText {
			return Text(string(b))
		}
		return JSON(string(b))
	}
}

// likeMatch implements SQL LIKE / ILIKE without compiling a regexp, so it stays
// allocation-free on the per-change path.
func likeMatch(s, pattern string, ci bool) bool {
	if ci {
		s = strings.ToLower(s)
		pattern = strings.ToLower(pattern)
	}
	return likeAt([]rune(s), []rune(pattern), 0, 0)
}

func likeAt(s, p []rune, si, pi int) bool {
	for pi < len(p) {
		switch p[pi] {
		case '%':
			// collapse consecutive % so that "a%%b" is not exponential
			for pi < len(p) && p[pi] == '%' {
				pi++
			}
			if pi == len(p) {
				return true
			}
			for i := si; i <= len(s); i++ {
				if likeAt(s, p, i, pi) {
					return true
				}
			}
			return false
		case '_':
			if si >= len(s) {
				return false
			}
			si++
			pi++
		case '\\':
			pi++
			if pi >= len(p) {
				return false
			}
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		default:
			if si >= len(s) || s[si] != p[pi] {
				return false
			}
			si++
			pi++
		}
	}
	return si == len(s)
}
