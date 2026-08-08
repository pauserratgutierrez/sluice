// Package expr implements the expression engine Sluice uses to evaluate RLS
// policy predicates and client filters in process, against the tuple the WAL
// already delivered.
//
// This is what makes Tier A and Tier B possible, and therefore what makes
// Sluice scale with the write rate instead of the subscriber count. It is also
// what makes DELETE authorization correct: the predicate is evaluated against
// the old tuple, so there is nothing to look up in a table the row has already
// left.
//
// The grammar is deliberately a subset of PostgreSQL's. Anything outside it is
// reported as unsupported and the caller falls back to an impersonated probe
// (Tier C). Failing closed is the only acceptable direction here.
package expr

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// ErrUnknown means the expression could not be evaluated with the information
// available -- almost always because it references a column whose value the WAL
// did not carry (an unchanged TOASTed value, or a column absent from the old
// tuple under a narrow replica identity).
//
// It is emphatically NOT "false". A caller that treats it as false would leak
// rows; a caller that treats it as true would leak rows too. It must be handled
// explicitly.
var ErrUnknown = errors.New("expr: value not available")

// ErrUnsupported means the expression contains something the compiler declines
// to reason about. The caller must fall back to Tier C.
type ErrUnsupported struct{ Reason string }

func (e *ErrUnsupported) Error() string { return "expr: unsupported: " + e.Reason }

func unsupported(format string, args ...any) error {
	return &ErrUnsupported{Reason: fmt.Sprintf(format, args...)}
}

// Kind is the dynamic type of a Value.
type Kind uint8

const (
	KindNull Kind = iota
	KindBool
	KindInt
	KindFloat
	KindText
	KindJSON  // raw JSON text
	KindArray // heterogeneous list, used for IN / ANY / ARRAY[...]
)

// Value is a dynamically typed SQL value.
//
// Deliberately a value type with no pointers except the array slice: a fan-out
// server holds millions of these transiently, and Go's GC marking cost scales
// with live pointer-bearing objects.
type Value struct {
	Kind  Kind
	Bool  bool
	Int   int64
	Float float64
	Str   string
	Arr   []Value
}

var Null = Value{Kind: KindNull}

func Bool(b bool) Value     { return Value{Kind: KindBool, Bool: b} }
func Int(i int64) Value     { return Value{Kind: KindInt, Int: i} }
func Float(f float64) Value { return Value{Kind: KindFloat, Float: f} }
func Text(s string) Value   { return Value{Kind: KindText, Str: s} }
func JSON(s string) Value   { return Value{Kind: KindJSON, Str: s} }
func Array(v []Value) Value { return Value{Kind: KindArray, Arr: v} }

func (v Value) IsNull() bool { return v.Kind == KindNull }

// Truthy reports the SQL boolean interpretation. SQL three-valued logic means
// NULL is neither true nor false, so callers must check IsNull first when it
// matters.
func (v Value) Truthy() bool {
	switch v.Kind {
	case KindBool:
		return v.Bool
	case KindInt:
		return v.Int != 0
	case KindFloat:
		return v.Float != 0
	case KindText:
		s := strings.ToLower(strings.TrimSpace(v.Str))
		return s == "t" || s == "true" || s == "y" || s == "yes" || s == "on" || s == "1"
	}
	return false
}

func (v Value) String() string {
	switch v.Kind {
	case KindNull:
		return "NULL"
	case KindBool:
		if v.Bool {
			return "true"
		}
		return "false"
	case KindInt:
		return strconv.FormatInt(v.Int, 10)
	case KindFloat:
		return strconv.FormatFloat(v.Float, 'g', -1, 64)
	default:
		return v.Str
	}
}

// ParseText converts a PostgreSQL text-format datum into a Value, guided by the
// declared type name. Sluice requests `binary 'false'` from pgoutput precisely
// so that this is the only decoding path it needs: measured, the binary format
// was actually *larger* (112 vs 88 bytes for a representative row) and would
// require per-type decoders including array headers and jsonb's version byte.
func ParseText(typeName, s string) Value {
	switch normalizeType(typeName) {
	case "bool":
		return Bool(s == "t" || s == "true" || s == "1")
	case "int2", "int4", "int8", "oid":
		if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			return Int(i)
		}
		return Text(s)
	case "float4", "float8", "numeric":
		// numeric and float can hold NaN and +/-Infinity. strconv.ParseFloat
		// accepts those spellings, but they are not valid JSON numbers, so
		// emitting them as floats would produce a payload no client can parse.
		// Keep them as text instead of silently corrupting the stream.
		if f, err := strconv.ParseFloat(s, 64); err == nil &&
			!math.IsNaN(f) && !math.IsInf(f, 0) {
			return Float(f)
		}
		return Text(s)
	case "json", "jsonb":
		return JSON(s)
	case "uuid":
		// PostgreSQL emits canonical lowercase; JWT `sub` is lowercase too.
		// Normalising here means a policy comparing them never fails on case.
		return Text(strings.ToLower(s))
	default:
		return Text(s)
	}
}

func normalizeType(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	t = strings.TrimSuffix(t, "[]")
	if i := strings.IndexByte(t, '('); i >= 0 {
		t = t[:i]
	}
	switch t {
	case "boolean":
		return "bool"
	case "smallint":
		return "int2"
	case "integer", "int":
		return "int4"
	case "bigint":
		return "int8"
	case "real":
		return "float4"
	case "double precision":
		return "float8"
	case "decimal":
		return "numeric"
	case "character varying", "varchar", "character", "char", "citext", "name":
		return "text"
	case "timestamp with time zone", "timestamptz":
		return "timestamptz"
	case "timestamp without time zone", "timestamp":
		return "timestamp"
	}
	return t
}

// Cast applies an explicit `::type` cast.
func Cast(v Value, typeName string) (Value, error) {
	if v.IsNull() {
		return Null, nil
	}
	switch normalizeType(typeName) {
	case "bool":
		if v.Kind == KindBool {
			return v, nil
		}
		return Bool(v.Truthy()), nil
	case "int2", "int4", "int8", "oid":
		switch v.Kind {
		case KindInt:
			return v, nil
		case KindFloat:
			return Int(int64(math.Round(v.Float))), nil
		case KindBool:
			if v.Bool {
				return Int(1), nil
			}
			return Int(0), nil
		default:
			i, err := strconv.ParseInt(strings.Trim(v.Str, `"`), 10, 64)
			if err != nil {
				return Null, unsupported("cannot cast %q to %s", v.Str, typeName)
			}
			return Int(i), nil
		}
	case "float4", "float8", "numeric":
		switch v.Kind {
		case KindFloat:
			return v, nil
		case KindInt:
			return Float(float64(v.Int)), nil
		default:
			f, err := strconv.ParseFloat(strings.Trim(v.Str, `"`), 64)
			if err != nil {
				return Null, unsupported("cannot cast %q to %s", v.Str, typeName)
			}
			return Float(f), nil
		}
	case "json", "jsonb":
		if v.Kind == KindJSON {
			return v, nil
		}
		return JSON(v.Str), nil
	case "uuid":
		return Text(strings.ToLower(strings.Trim(v.String(), `"`))), nil
	case "text", "timestamptz", "timestamp", "date", "time", "inet", "interval":
		// A JSON string arriving from ->> is already unquoted; a JSON scalar
		// from -> still carries its quotes and must lose them.
		return Text(strings.Trim(v.String(), `"`)), nil
	}
	return Text(v.String()), nil
}

// compare returns -1, 0 or 1, or ErrUnknown when either side is NULL.
//
// Coercion follows the practical rule that if either side is numeric and the
// other parses as a number, compare numerically; otherwise compare as text.
// This matches what PostgreSQL does for the operator forms `pg_get_expr`
// actually emits, because it has already inserted explicit casts where the
// types differ.
func compare(a, b Value) (int, error) {
	if a.IsNull() || b.IsNull() {
		return 0, ErrUnknown
	}
	if a.Kind == KindBool || b.Kind == KindBool {
		ab, bb := a.Truthy(), b.Truthy()
		switch {
		case ab == bb:
			return 0, nil
		case !ab:
			return -1, nil
		default:
			return 1, nil
		}
	}
	if af, aok := asFloat(a); aok {
		if bf, bok := asFloat(b); bok {
			switch {
			case af < bf:
				return -1, nil
			case af > bf:
				return 1, nil
			default:
				return 0, nil
			}
		}
	}
	return strings.Compare(a.String(), b.String()), nil
}

func asFloat(v Value) (float64, bool) {
	switch v.Kind {
	case KindInt:
		return float64(v.Int), true
	case KindFloat:
		return v.Float, true
	case KindText:
		f, err := strconv.ParseFloat(v.Str, 64)
		return f, err == nil
	}
	return 0, false
}
