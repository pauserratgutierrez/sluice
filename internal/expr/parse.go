package expr

import (
	"strconv"
	"strings"
	"unicode"
)

// Parse parses the SQL expression text that `pg_get_expr(polqual, polrelid)`
// produces.
//
// It is a hand-written recursive-descent parser over a whitelisted subset
// rather than a binding to libpg_query. That keeps Sluice pure Go with a static
// binary and no cgo, and costs nothing in safety: anything the parser does not
// recognise becomes a SubqueryExpr or an error, and either way the authorizer
// falls back to an impersonated probe. The design document records this as the
// deliberate v1 choice.
func Parse(sql string) (Node, error) {
	p := &parser{lex: newLexer(sql)}
	if err := p.lex.err; err != nil {
		return nil, err
	}
	n, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if !p.at(tokEOF) {
		return nil, unsupported("unexpected trailing input near %q", p.cur().text)
	}
	return n, nil
}

// ---------------------------------------------------------------------------
// Lexer
// ---------------------------------------------------------------------------

type tokKind uint8

const (
	tokEOF tokKind = iota
	tokIdent
	tokQIdent // "quoted identifier"
	tokNumber
	tokString
	tokOp
	tokPunct
)

type token struct {
	kind tokKind
	text string
	// upper is the uppercased text, precomputed because keyword comparison is
	// the hottest thing the parser does.
	upper string
}

type lexer struct {
	toks []token
	pos  int
	err  error
}

// ':' is in this set because PostgreSQL's cast operator is '::' and pg_get_expr
// emits it constantly.
var operatorRunes = "+-*/<>=~!@#%^&|`?:"

// multi-char operators, longest first so that greedy matching works
var multiOps = []string{
	"#>>", "->>", "!~~*", "!~~", "~~*", "#>", "->", "<@", "@>", "||",
	"<>", "!=", "<=", ">=", "::", "~~", "~*", "!~",
}

func newLexer(s string) *lexer {
	l := &lexer{}
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '\'':
			// standard SQL string; '' is an escaped quote
			j := i + 1
			var sb strings.Builder
			for j < len(s) {
				if s[j] == '\'' {
					if j+1 < len(s) && s[j+1] == '\'' {
						sb.WriteByte('\'')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			if j >= len(s) {
				l.err = unsupported("unterminated string literal")
				return l
			}
			l.push(token{kind: tokString, text: sb.String()})
			i = j + 1
		case c == '"':
			j := i + 1
			var sb strings.Builder
			for j < len(s) {
				if s[j] == '"' {
					if j+1 < len(s) && s[j+1] == '"' {
						sb.WriteByte('"')
						j += 2
						continue
					}
					break
				}
				sb.WriteByte(s[j])
				j++
			}
			if j >= len(s) {
				l.err = unsupported("unterminated quoted identifier")
				return l
			}
			l.push(token{kind: tokQIdent, text: sb.String()})
			i = j + 1
		case c >= '0' && c <= '9', c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9':
			j := i
			seenDot, seenExp := false, false
			for j < len(s) {
				d := s[j]
				if d >= '0' && d <= '9' {
					j++
				} else if d == '.' && !seenDot && !seenExp {
					seenDot = true
					j++
				} else if (d == 'e' || d == 'E') && !seenExp && j+1 < len(s) {
					seenExp = true
					j++
					if j < len(s) && (s[j] == '+' || s[j] == '-') {
						j++
					}
				} else {
					break
				}
			}
			l.push(token{kind: tokNumber, text: s[i:j]})
			i = j
		case c == '_' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(s) && (s[j] == '_' || s[j] == '$' || s[j] == '.' && false ||
				unicode.IsLetter(rune(s[j])) || unicode.IsDigit(rune(s[j]))) {
				j++
			}
			l.push(token{kind: tokIdent, text: s[i:j]})
			i = j
		case c == '(' || c == ')' || c == ',' || c == '[' || c == ']' || c == '.':
			l.push(token{kind: tokPunct, text: string(c)})
			i++
		case strings.IndexByte(operatorRunes, c) >= 0:
			matched := ""
			for _, m := range multiOps {
				if strings.HasPrefix(s[i:], m) {
					matched = m
					break
				}
			}
			if matched == "" {
				matched = string(c)
			}
			l.push(token{kind: tokOp, text: matched})
			i += len(matched)
		default:
			l.err = unsupported("unexpected character %q", string(c))
			return l
		}
	}
	l.push(token{kind: tokEOF})
	return l
}

func (l *lexer) push(t token) {
	t.upper = strings.ToUpper(t.text)
	l.toks = append(l.toks, t)
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

type parser struct{ lex *lexer }

func (p *parser) cur() token  { return p.lex.toks[p.lex.pos] }
func (p *parser) next() token { t := p.cur(); p.lex.pos++; return t }

func (p *parser) at(k tokKind) bool { return p.cur().kind == k }

func (p *parser) atKeyword(kw ...string) bool {
	c := p.cur()
	if c.kind != tokIdent {
		return false
	}
	for _, k := range kw {
		if c.upper == k {
			return true
		}
	}
	return false
}

func (p *parser) atPunct(s string) bool {
	return p.cur().kind == tokPunct && p.cur().text == s
}

func (p *parser) atOp(s ...string) bool {
	if p.cur().kind != tokOp {
		return false
	}
	for _, o := range s {
		if p.cur().text == o {
			return true
		}
	}
	return false
}

func (p *parser) expectPunct(s string) error {
	if !p.atPunct(s) {
		return unsupported("expected %q, found %q", s, p.cur().text)
	}
	p.lex.pos++
	return nil
}

// parseExpr  := parseOr
func (p *parser) parseExpr() (Node, error) { return p.parseOr() }

func (p *parser) parseOr() (Node, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.atKeyword("OR") {
		p.lex.pos++
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: "or", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseAnd() (Node, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}
	for p.atKeyword("AND") {
		p.lex.pos++
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: "and", Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseNot() (Node, error) {
	if p.atKeyword("NOT") {
		p.lex.pos++
		arg, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &Unary{Op: "not", Arg: arg}, nil
	}
	return p.parseCompare()
}

var compareOps = map[string]bool{
	"=": true, "<>": true, "!=": true, "<": true, "<=": true, ">": true, ">=": true,
	"~~": true, "~~*": true, "!~~": true, "!~~*": true,
	"~": true, "~*": true, "!~": true, "!~*": true,
	"@>": true, "<@": true,
}

func (p *parser) parseCompare() (Node, error) {
	left, err := p.parseAdditive()
	if err != nil {
		return nil, err
	}

	// IS [NOT] NULL / TRUE / FALSE / UNKNOWN / DISTINCT FROM
	if p.atKeyword("IS") {
		p.lex.pos++
		negate := false
		if p.atKeyword("NOT") {
			negate = true
			p.lex.pos++
		}
		switch {
		case p.atKeyword("NULL"):
			p.lex.pos++
			return &IsTest{Arg: left, Negate: negate, Test: "null"}, nil
		case p.atKeyword("TRUE"):
			p.lex.pos++
			return &IsTest{Arg: left, Negate: negate, Test: "true"}, nil
		case p.atKeyword("FALSE"):
			p.lex.pos++
			return &IsTest{Arg: left, Negate: negate, Test: "false"}, nil
		case p.atKeyword("UNKNOWN"):
			p.lex.pos++
			return &IsTest{Arg: left, Negate: negate, Test: "unknown"}, nil
		case p.atKeyword("DISTINCT"):
			p.lex.pos++
			if !p.atKeyword("FROM") {
				return nil, unsupported("expected FROM after IS DISTINCT")
			}
			p.lex.pos++
			right, err := p.parseAdditive()
			if err != nil {
				return nil, err
			}
			op := "isdistinct"
			if negate {
				op = "isnotdistinct"
			}
			return &Binary{Op: op, Left: left, Right: right}, nil
		}
		return nil, unsupported("unsupported IS test near %q", p.cur().text)
	}

	// [NOT] IN / LIKE / ILIKE / BETWEEN
	negate := false
	if p.atKeyword("NOT") {
		// only meaningful before IN/LIKE/ILIKE/BETWEEN
		save := p.lex.pos
		p.lex.pos++
		if p.atKeyword("IN", "LIKE", "ILIKE", "BETWEEN") {
			negate = true
		} else {
			p.lex.pos = save
		}
	}

	switch {
	case p.atKeyword("IN"):
		p.lex.pos++
		if err := p.expectPunct("("); err != nil {
			return nil, err
		}
		// IN (SELECT ...) is a subquery; bail out to Tier C rather than guess.
		if p.atKeyword("SELECT") {
			return &SubqueryExpr{SQL: "IN (SELECT ...)"}, p.skipParens()
		}
		var items []Node
		for {
			it, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			items = append(items, it)
			if p.atPunct(",") {
				p.lex.pos++
				continue
			}
			break
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
		return &InList{Arg: left, Items: items, Negate: negate}, nil

	case p.atKeyword("LIKE", "ILIKE"):
		op := "~~"
		if p.cur().upper == "ILIKE" {
			op = "~~*"
		}
		if negate {
			op = "!" + op
		}
		p.lex.pos++
		right, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: op, Left: left, Right: right}, nil

	case p.atKeyword("BETWEEN"):
		p.lex.pos++
		lo, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		if !p.atKeyword("AND") {
			return nil, unsupported("expected AND in BETWEEN")
		}
		p.lex.pos++
		hi, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		n := Node(&Binary{
			Op:    "and",
			Left:  &Binary{Op: ">=", Left: left, Right: lo},
			Right: &Binary{Op: "<=", Left: left, Right: hi},
		})
		if negate {
			n = &Unary{Op: "not", Arg: n}
		}
		return n, nil
	}

	if p.cur().kind == tokOp && compareOps[p.cur().text] {
		op := p.next().text

		// `= ANY (ARRAY[...])` is what pg_get_expr emits for membership tests.
		if p.atKeyword("ANY", "ALL") {
			all := p.cur().upper == "ALL"
			p.lex.pos++
			if err := p.expectPunct("("); err != nil {
				return nil, err
			}
			if p.atKeyword("SELECT") {
				return &SubqueryExpr{SQL: op + " ANY (SELECT ...)"}, p.skipParens()
			}
			inner, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			if err := p.expectPunct(")"); err != nil {
				return nil, err
			}
			if all {
				return nil, unsupported("%s ALL (...) is not supported", op)
			}
			arr, ok := inner.(*ArrayExpr)
			if !ok {
				// e.g. `= ANY (some_array_column)`: needs runtime array
				// decoding, which the text-format path does not do yet.
				return nil, unsupported("%s ANY over a non-literal array", op)
			}
			if op != "=" {
				return nil, unsupported("%s ANY (ARRAY[...]) is not supported", op)
			}
			return &InList{Arg: left, Items: arr.Items}, nil
		}

		right, err := p.parseAdditive()
		if err != nil {
			return nil, err
		}
		return &Binary{Op: op, Left: left, Right: right}, nil
	}

	if negate {
		return nil, unsupported("dangling NOT near %q", p.cur().text)
	}
	return left, nil
}

func (p *parser) parseAdditive() (Node, error) {
	left, err := p.parseJSONOps()
	if err != nil {
		return nil, err
	}
	for p.atOp("+", "-", "||") {
		op := p.next().text
		right, err := p.parseJSONOps()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseJSONOps() (Node, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}
	for p.atOp("->", "->>", "#>", "#>>", "*", "/", "%") {
		op := p.next().text
		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		left = &Binary{Op: op, Left: left, Right: right}
	}
	return left, nil
}

func (p *parser) parseUnary() (Node, error) {
	if p.atOp("-", "+") {
		op := p.next().text
		arg, err := p.parseUnary()
		if err != nil {
			return nil, err
		}
		if op == "+" {
			return arg, nil
		}
		return &Unary{Op: "-", Arg: arg}, nil
	}
	return p.parsePostfix()
}

func (p *parser) parsePostfix() (Node, error) {
	n, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for p.atOp("::") {
		p.lex.pos++
		t, err := p.parseTypeName()
		if err != nil {
			return nil, err
		}
		n = &CastExpr{Arg: n, Type: t}
	}
	return n, nil
}

// parseTypeName consumes the loose, possibly multi-word type names PostgreSQL
// emits: `text`, `character varying(20)`, `timestamp with time zone`, `text[]`.
func (p *parser) parseTypeName() (string, error) {
	if p.cur().kind != tokIdent && p.cur().kind != tokQIdent {
		return "", unsupported("expected a type name, found %q", p.cur().text)
	}
	var parts []string
	parts = append(parts, p.next().text)
	// multi-word type names
	for p.atKeyword("VARYING", "PRECISION", "WITH", "WITHOUT", "TIME", "ZONE", "INT", "INTEGER") {
		parts = append(parts, p.next().text)
	}
	name := strings.Join(parts, " ")
	// schema-qualified type
	if p.atPunct(".") {
		p.lex.pos++
		if p.cur().kind != tokIdent && p.cur().kind != tokQIdent {
			return "", unsupported("expected a type name after '.'")
		}
		name = p.next().text
	}
	// precision / scale
	if p.atPunct("(") {
		depth := 0
		for {
			if p.at(tokEOF) {
				return "", unsupported("unterminated type modifier")
			}
			if p.atPunct("(") {
				depth++
			} else if p.atPunct(")") {
				depth--
			}
			p.lex.pos++
			if depth == 0 {
				break
			}
		}
	}
	// array suffix
	for p.atPunct("[") {
		p.lex.pos++
		if err := p.expectPunct("]"); err != nil {
			return "", err
		}
		name += "[]"
	}
	return name, nil
}

func (p *parser) parsePrimary() (Node, error) {
	c := p.cur()

	switch {
	case p.atPunct("("):
		p.lex.pos++
		// A parenthesised SELECT is a scalar subquery: not evaluable in process.
		if p.atKeyword("SELECT") {
			return &SubqueryExpr{SQL: "(SELECT ...)"}, p.skipParens()
		}
		n, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
		return n, nil

	case c.kind == tokString:
		p.lex.pos++
		return &Literal{Value: Text(c.text)}, nil

	case c.kind == tokNumber:
		p.lex.pos++
		if i, err := strconv.ParseInt(c.text, 10, 64); err == nil {
			return &Literal{Value: Int(i)}, nil
		}
		f, err := strconv.ParseFloat(c.text, 64)
		if err != nil {
			return nil, unsupported("bad numeric literal %q", c.text)
		}
		return &Literal{Value: Float(f)}, nil

	case c.kind == tokQIdent:
		p.lex.pos++
		return p.maybeQualified(ColumnRef{Name: c.text})

	case c.kind == tokIdent:
		switch c.upper {
		case "TRUE":
			p.lex.pos++
			return &Literal{Value: Bool(true)}, nil
		case "FALSE":
			p.lex.pos++
			return &Literal{Value: Bool(false)}, nil
		case "NULL":
			p.lex.pos++
			return &Literal{Value: Null}, nil
		case "EXISTS":
			p.lex.pos++
			if !p.atPunct("(") {
				return nil, unsupported("expected ( after EXISTS")
			}
			p.lex.pos++
			return &SubqueryExpr{SQL: "EXISTS (SELECT ...)"}, p.skipParens()
		case "ARRAY":
			p.lex.pos++
			if !p.atPunct("[") {
				return nil, unsupported("expected [ after ARRAY")
			}
			p.lex.pos++
			var items []Node
			if !p.atPunct("]") {
				for {
					it, err := p.parseExpr()
					if err != nil {
						return nil, err
					}
					items = append(items, it)
					if p.atPunct(",") {
						p.lex.pos++
						continue
					}
					break
				}
			}
			if err := p.expectPunct("]"); err != nil {
				return nil, err
			}
			return &ArrayExpr{Items: items}, nil
		case "SELECT":
			return &SubqueryExpr{SQL: "SELECT ..."}, nil
		case "CASE":
			return nil, unsupported("CASE expressions are not supported")
		}
		p.lex.pos++
		return p.maybeQualified(ColumnRef{Name: c.text})
	}

	return nil, unsupported("unexpected token %q", c.text)
}

// maybeQualified resolves `a`, `a.b`, `a.b()` and `a.b.c` after the first
// identifier has been consumed.
func (p *parser) maybeQualified(ref ColumnRef) (Node, error) {
	for p.atPunct(".") {
		p.lex.pos++
		if p.cur().kind != tokIdent && p.cur().kind != tokQIdent {
			return nil, unsupported("expected an identifier after '.'")
		}
		nxt := p.next().text
		if ref.Qualifier == "" {
			ref.Qualifier = ref.Name
		} else {
			// three-part name (db.schema.table): keep only the last two parts
			ref.Qualifier = ref.Name
		}
		ref.Name = nxt
	}

	if p.atPunct("(") {
		p.lex.pos++
		fn := &FuncCall{Schema: ref.Qualifier, Name: strings.ToLower(ref.Name)}
		if !p.atPunct(")") {
			for {
				a, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				fn.Args = append(fn.Args, a)
				if p.atPunct(",") {
					p.lex.pos++
					continue
				}
				break
			}
		}
		if err := p.expectPunct(")"); err != nil {
			return nil, err
		}
		return fn, nil
	}

	return &ColumnRef{Qualifier: ref.Qualifier, Name: ref.Name}, nil
}

// skipParens consumes tokens until the currently-open parenthesis closes. Used
// to swallow subqueries whole so that the rest of the predicate still parses
// and the analyzer can report a precise reason for falling to Tier C.
func (p *parser) skipParens() error {
	depth := 1
	for depth > 0 {
		if p.at(tokEOF) {
			return unsupported("unterminated subquery")
		}
		if p.atPunct("(") {
			depth++
		} else if p.atPunct(")") {
			depth--
		}
		p.lex.pos++
	}
	return nil
}
