// Package encode turns PostgreSQL text-format values into the JSON values
// to_jsonb produces for them.
//
// Change events and snapshot rows both go through here, so a column has one
// encoding on the wire whichever path delivered it, and it is the one PostgREST
// clients and generated `Database` types already expect: booleans, numbers,
// embedded JSON, ISO 8601 timestamps in UTC, arrays and composites as JSON
// arrays and objects, and everything else as PostgreSQL's text output.
//
// The input must be text output in DateStyle ISO. The replication connection
// and snapshot queries set it.
package encode

import (
	"encoding/json"
	"strings"
	"time"
)

// Kind is how a type is encoded, after resolving domains to their base type.
type Kind uint8

const (
	Text Kind = iota
	Bool
	Number
	JSON
	Timestamp
	TimestampTZ
	Array
	Composite
)

// Type is the encoding of one PostgreSQL type.
type Type struct {
	Kind Kind
	// Elem and Delim describe an array's elements.
	Elem  *Type
	Delim byte
	// Fields describes a composite's attributes, in order.
	Fields []Field
}

// Field is one attribute of a composite type.
type Field struct {
	Name string
	Type *Type
}

// TextType is the encoding of any type Sluice has no specific rule for.
var TextType = &Type{Kind: Text}

var (
	boolType        = &Type{Kind: Bool}
	numberType      = &Type{Kind: Number}
	jsonType        = &Type{Kind: JSON}
	timestampType   = &Type{Kind: Timestamp}
	timestampTZType = &Type{Kind: TimestampTZ}
)

// Builtin is the encoding of a base type by OID; anything but the booleans,
// numbers, JSON and timestamps is encoded as text. oid is text too, as in
// to_jsonb. It knows nothing of domains, arrays or composites, which need the
// catalog.
func Builtin(oid uint32) *Type {
	switch oid {
	case 16: // bool
		return boolType
	case 20, 21, 23, 700, 701, 1700: // int8, int2, int4, float4, float8, numeric
		return numberType
	case 114, 3802: // json, jsonb
		return jsonType
	case 1114: // timestamp
		return timestampType
	case 1184: // timestamptz
		return timestampTZType
	}
	return TextType
}

// Value encodes one non-null text value. A value that does not parse as its
// type is sent as the text itself rather than dropped.
func (t *Type) Value(s string) any {
	switch t.Kind {
	case Bool:
		switch s {
		case "t":
			return true
		case "f":
			return false
		}
	case Number:
		// NaN and Infinity are not JSON numbers; to_jsonb sends them as strings.
		if json.Valid([]byte(s)) {
			return json.Number(s)
		}
	case JSON:
		if json.Valid([]byte(s)) {
			return json.RawMessage(s)
		}
	case Timestamp:
		return isoTimestamp(s)
	case TimestampTZ:
		return isoTimestampTZ(s)
	case Array:
		if v, ok := parseArray(s, t); ok {
			return v
		}
	case Composite:
		if v, ok := parseComposite(s, t); ok {
			return v
		}
	}
	return s
}

// isoTimestamp turns `2026-09-29 20:24:42.5` into `2026-09-29T20:24:42.5`.
// `infinity` and a trailing ` BC` pass through, as they do in to_jsonb.
func isoTimestamp(s string) string {
	if i := strings.IndexByte(s, ' '); i > 0 && i+1 < len(s) && s[i+1] != 'B' {
		return s[:i] + "T" + s[i+1:]
	}
	return s
}

var tzLayouts = []string{
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05-07:00",
	"2006-01-02 15:04:05-07:00:00",
}

// ParseTimestampTZ parses a timestamptz in PostgreSQL's ISO text output, such
// as `2026-09-29 22:24:42.39+02`. It reports false for `infinity`, BC dates and
// years past 9999, which time.Time cannot hold in that form.
func ParseTimestampTZ(s string) (time.Time, bool) {
	for _, layout := range tzLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

// isoTimestampTZ turns `2026-09-29 22:24:42.39+02` into
// `2026-09-29T20:24:42.39+00:00`: the same instant in UTC, whatever time zone
// the session printed it in. Values Go cannot represent (BC, years past 9999)
// keep their offset and only gain the `T`.
func isoTimestampTZ(s string) string {
	if t, ok := ParseTimestampTZ(s); ok {
		return t.UTC().Format("2006-01-02T15:04:05.999999-07:00")
	}
	out := isoTimestamp(s)
	// PostgreSQL omits the minutes of a whole-hour offset; to_jsonb does not.
	body, bc := strings.CutSuffix(out, " BC")
	if n := len(body); n > 3 && (body[n-3] == '+' || body[n-3] == '-') {
		body += ":00"
	}
	if bc {
		return body + " BC"
	}
	return body
}

// parseArray parses an array literal such as `{a,"b c",NULL}` or
// `[0:1]={{1,2},{3,4}}` into nested []any. Bounds are dropped, as in to_jsonb.
func parseArray(s string, t *Type) (any, bool) {
	if strings.HasPrefix(s, "[") {
		i := strings.IndexByte(s, '=')
		if i < 0 {
			return nil, false
		}
		s = s[i+1:]
	}
	p := &literal{s: s}
	v, ok := p.array(t)
	if !ok || p.i != len(p.s) {
		return nil, false
	}
	return v, true
}

type literal struct {
	s string
	i int
}

func (p *literal) peek() byte {
	if p.i < len(p.s) {
		return p.s[p.i]
	}
	return 0
}

func (p *literal) array(t *Type) ([]any, bool) {
	if p.peek() != '{' {
		return nil, false
	}
	p.i++
	out := []any{}
	if p.peek() == '}' {
		p.i++
		return out, true
	}
	for {
		switch c := p.peek(); {
		case c == '{':
			sub, ok := p.array(t)
			if !ok {
				return nil, false
			}
			out = append(out, sub)
		case c == '"':
			v, ok := p.quoted('\\')
			if !ok {
				return nil, false
			}
			out = append(out, t.Elem.Value(v))
		default:
			start := p.i
			for p.i < len(p.s) && p.s[p.i] != t.Delim && p.s[p.i] != '}' {
				p.i++
			}
			if v := p.s[start:p.i]; v == "NULL" {
				out = append(out, nil)
			} else {
				out = append(out, t.Elem.Value(v))
			}
		}
		switch p.peek() {
		case t.Delim:
			p.i++
		case '}':
			p.i++
			return out, true
		default:
			return nil, false
		}
	}
}

// quoted reads a double-quoted element. Arrays escape with a backslash;
// composites also double a quote inside a quoted field.
func (p *literal) quoted(escape byte) (string, bool) {
	p.i++ // opening quote
	var b strings.Builder
	for p.i < len(p.s) {
		c := p.s[p.i]
		switch {
		case c == escape && p.i+1 < len(p.s):
			b.WriteByte(p.s[p.i+1])
			p.i += 2
		case c == '"' && p.i+1 < len(p.s) && p.s[p.i+1] == '"':
			b.WriteByte('"')
			p.i += 2
		case c == '"':
			p.i++
			return b.String(), true
		default:
			b.WriteByte(c)
			p.i++
		}
	}
	return "", false
}

// parseComposite parses a row literal such as `(1,"a b",)` into an object keyed
// by attribute name. An empty unquoted field is NULL.
func parseComposite(s string, t *Type) (any, bool) {
	p := &literal{s: s}
	if p.peek() != '(' {
		return nil, false
	}
	p.i++
	out := make(map[string]any, len(t.Fields))
	for n, f := range t.Fields {
		if n > 0 {
			if p.peek() != ',' {
				return nil, false
			}
			p.i++
		}
		switch c := p.peek(); {
		case c == '"':
			v, ok := p.quoted('\\')
			if !ok {
				return nil, false
			}
			out[f.Name] = f.Type.Value(v)
		default:
			start := p.i
			for p.i < len(p.s) && p.s[p.i] != ',' && p.s[p.i] != ')' {
				p.i++
			}
			if v := p.s[start:p.i]; v == "" {
				out[f.Name] = nil
			} else {
				out[f.Name] = f.Type.Value(v)
			}
		}
	}
	if p.peek() != ')' || p.i+1 != len(p.s) {
		return nil, false
	}
	return out, true
}
