package encode

import (
	"encoding/json"
	"testing"
)

// The expected strings are what PostgreSQL 18's to_jsonb produced for the same
// values in a session with TimeZone UTC.
func TestValueMatchesToJSONB(t *testing.T) {
	num := &Type{Kind: Number}
	text := TextType
	pair := &Type{Kind: Composite, Fields: []Field{{"a", num}, {"b", text}}}
	for _, tc := range []struct {
		name string
		typ  *Type
		in   string
		want string
	}{
		{"bool", &Type{Kind: Bool}, "t", `true`},
		{"float", num, "0.1", `0.1`},
		{"numeric keeps its scale", num, "12.50", `12.50`},
		{"numeric keeps its digits", num, "12345678901234567.89", `12345678901234567.89`},
		{"NaN is a string", num, "NaN", `"NaN"`},
		{"Infinity is a string", num, "Infinity", `"Infinity"`},
		{"jsonb embedded", &Type{Kind: JSON}, `{"a": [1, 2]}`, `{"a":[1,2]}`},
		{"timestamp", &Type{Kind: Timestamp}, "2026-09-29 20:24:42.5", `"2026-09-29T20:24:42.5"`},
		{"timestamp BC", &Type{Kind: Timestamp}, "0044-03-15 12:00:00 BC", `"0044-03-15T12:00:00 BC"`},
		{"timestamptz to UTC", &Type{Kind: TimestampTZ}, "2026-09-29 22:24:42.39579+02", `"2026-09-29T20:24:42.39579+00:00"`},
		{"timestamptz whole second", &Type{Kind: TimestampTZ}, "2026-09-29 20:24:42+00", `"2026-09-29T20:24:42+00:00"`},
		{"timestamptz half-hour offset", &Type{Kind: TimestampTZ}, "2026-09-29 20:24:42+05:30", `"2026-09-29T14:54:42+00:00"`},
		{"timestamptz BC", &Type{Kind: TimestampTZ}, "0044-03-15 12:00:00+00 BC", `"0044-03-15T12:00:00+00:00 BC"`},
		{"timestamptz infinity", &Type{Kind: TimestampTZ}, "infinity", `"infinity"`},
		{"date stays", text, "2026-09-29", `"2026-09-29"`},
		{"text array", &Type{Kind: Array, Elem: text, Delim: ','}, `{a,"b c",NULL,"NULL"}`, `["a","b c",null,"NULL"]`},
		{"escaped elements", &Type{Kind: Array, Elem: text, Delim: ','}, `{"a\"b","c\\d"}`, `["a\"b","c\\d"]`},
		{"two dimensions", &Type{Kind: Array, Elem: num, Delim: ','}, `{{1,2},{3,4}}`, `[[1,2],[3,4]]`},
		{"bounds dropped", &Type{Kind: Array, Elem: num, Delim: ','}, `[0:1]={1,2}`, `[1,2]`},
		{"empty array", &Type{Kind: Array, Elem: num, Delim: ','}, `{}`, `[]`},
		{"bool array", &Type{Kind: Array, Elem: &Type{Kind: Bool}, Delim: ','}, `{t,f}`, `[true,false]`},
		{"timestamptz array", &Type{Kind: Array, Elem: &Type{Kind: TimestampTZ}, Delim: ','}, `{"2026-09-29 20:00:00+00"}`, `["2026-09-29T20:00:00+00:00"]`},
		{"jsonb array", &Type{Kind: Array, Elem: &Type{Kind: JSON}, Delim: ','}, `{"{\"x\": 1}"}`, `[{"x":1}]`},
		{"box array uses ;", &Type{Kind: Array, Elem: text, Delim: ';'}, `{(1,1),(0,0);(2,2),(1,1)}`, `["(1,1),(0,0)","(2,2),(1,1)"]`},
		{"composite", pair, `(1,x)`, `{"a":1,"b":"x"}`},
		{"composite quoting and NULL", pair, `(,"a ""q"" \\ b")`, `{"a":null,"b":"a \"q\" \\ b"}`},
		{"malformed array is sent as text", &Type{Kind: Array, Elem: text, Delim: ','}, `{a,`, `"{a,"`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, err := json.Marshal(tc.typ.Value(tc.in))
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != tc.want {
				t.Errorf("%s: got %s, want %s", tc.in, b, tc.want)
			}
		})
	}
}
