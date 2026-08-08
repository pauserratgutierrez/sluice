// Package pgoutput decodes PostgreSQL's built-in logical replication protocol.
//
// Sluice decodes pgoutput itself rather than using pglogrepl's message parser,
// for one decisive reason and two supporting ones. The decisive one: correct
// handling of the TupleData 'u' byte, which marks an unchanged TOASTed value.
// wal2json omits such columns from its JSON with no marker at all, so a
// consumer cannot tell "unchanged" from "not selected" from "dropped column" --
// and a realtime server that guesses wrong silently blanks large columns on
// every subscriber whenever an unrelated counter is incremented. Here the
// distinction is explicit in the type system.
//
// The supporting reasons: pglogrepl parses only protocol versions 1 and 2 (no
// ParseV3/ParseV4 exists), and it has never had a tagged release. Sluice needs
// the full v4 message set, so it owns this code.
//
// Reference: https://www.postgresql.org/docs/18/protocol-logicalrep-message-formats.html
package pgoutput

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// Message type discriminators, the first byte of every message.
const (
	MsgBegin          byte = 'B'
	MsgMessage        byte = 'M'
	MsgCommit         byte = 'C'
	MsgOrigin         byte = 'O'
	MsgRelation       byte = 'R'
	MsgType           byte = 'Y'
	MsgInsert         byte = 'I'
	MsgUpdate         byte = 'U'
	MsgDelete         byte = 'D'
	MsgTruncate       byte = 'T'
	MsgStreamStart    byte = 'S'
	MsgStreamStop     byte = 'E'
	MsgStreamCommit   byte = 'c'
	MsgStreamAbort    byte = 'A'
	MsgBeginPrepare   byte = 'b'
	MsgPrepare        byte = 'P'
	MsgCommitPrepared byte = 'K'
	MsgRollbackPrep   byte = 'r'
	MsgStreamPrepare  byte = 'p'
)

// TupleData column kinds. These are the four bytes PostgreSQL uses per column.
type ColumnKind byte

const (
	// ColNull is 'n': the value is SQL NULL.
	ColNull ColumnKind = 'n'
	// ColUnchanged is 'u': the column holds an unchanged TOASTed value which was
	// NOT transmitted. This is not NULL and not an empty string. Treating it as
	// either is a data-loss bug.
	ColUnchanged ColumnKind = 'u'
	// ColText is 't': the value is in the type's text output format.
	ColText ColumnKind = 't'
	// ColBinary is 'b': the value is in the type's binary output format. Sluice
	// requests binary='false', but pgoutput can still emit 'b' if a future
	// configuration enables it, so the decoder carries the bytes through rather
	// than silently mangling them.
	ColBinary ColumnKind = 'b'
)

// Column flag bits from a Relation message.
const relColumnIsKey = 1

// ColumnMeta describes one column of a relation, from the Relation message.
type ColumnMeta struct {
	Name     string
	TypeOID  uint32
	TypeMod  int32
	IsKey    bool // part of the replica identity
	TypeName string
}

// Relation is the cached schema for one relation OID.
//
// Per the protocol, a Relation message arrives before the first DML for that
// OID and is re-sent whenever the definition changes -- which makes it the only
// in-band signal a consumer gets that the schema moved, since DDL is not
// replicated.
type Relation struct {
	OID             uint32
	Namespace       string
	Name            string
	ReplicaIdentity byte // 'd' default, 'n' nothing, 'f' full, 'i' using index
	Columns         []ColumnMeta

	// fullName is computed once when the Relation message is decoded, not lazily.
	// This value is read on the per-change path, and building it there would mean
	// an allocation per change per relation -- and a lazily-filled cache would be
	// a data race besides.
	fullName string

	byName map[string]int
}

// FullName returns schema.table.
func (r *Relation) FullName() string { return r.fullName }

// ColumnIndex returns the position of a column, or -1.
func (r *Relation) ColumnIndex(name string) int {
	if r.byName == nil {
		return -1
	}
	if i, ok := r.byName[name]; ok {
		return i
	}
	return -1
}

// KeyColumns returns the names of the replica-identity columns. These are the
// only columns guaranteed to be present in an old tuple, which is why a shape
// filtering on anything else needs REPLICA IDENTITY USING INDEX or FULL.
func (r *Relation) KeyColumns() []string {
	var out []string
	for _, c := range r.Columns {
		if c.IsKey {
			out = append(out, c.Name)
		}
	}
	return out
}

// Column is one decoded column value.
type Column struct {
	Kind ColumnKind
	Data []byte // valid for ColText and ColBinary only
}

// Tuple is a decoded row: one Column per relation column, in relation order.
type Tuple struct{ Columns []Column }

// Message is any decoded pgoutput message.
type Message struct {
	Type byte

	// Begin / Commit
	FinalLSN   uint64
	CommitLSN  uint64
	EndLSN     uint64
	CommitTime time.Time
	XID        uint32

	// Relation
	Relation *Relation

	// Type ('Y')
	TypeOID       uint32
	TypeNamespace string
	TypeName      string

	// Insert / Update / Delete
	RelationOID uint32
	New         *Tuple
	Old         *Tuple
	// OldIsKey distinguishes the 'K' (replica identity key) form from the 'O'
	// (full old tuple) form. Per the protocol a message carries at most one of
	// them, and DEFAULT replica identity with an unchanged key carries neither.
	OldIsKey bool

	// Truncate
	TruncateRelations []uint32
	TruncateCascade   bool
	TruncateRestart   bool

	// Message ('M') -- pg_logical_emit_message
	MessageTransactional bool
	MessagePrefix        string
	MessageContent       []byte
	MessageLSN           uint64

	// Origin
	OriginName string

	// Stream* (protocol >= 2)
	StreamXID   uint32
	StreamFirst bool
	AbortLSN    uint64 // protocol 4, streaming=parallel only
	AbortTime   time.Time
	AbortSubXID uint32
}

// pgEpoch is PostgreSQL's timestamp origin: 2000-01-01 00:00:00 UTC.
var pgEpoch = time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)

// Decoder holds the relation cache across messages.
//
// The protocol explicitly makes this the client's responsibility: "The protocol
// assumes that the client is capable of remembering this metadata for as many
// relations as needed."
type Decoder struct {
	relations map[uint32]*Relation
	typeNames map[uint32]string

	// inStream tracks whether we are between StreamStart and StreamStop, which
	// changes nothing about the message bodies but is required state when
	// streaming is enabled.
	inStream bool

	// OnRelation, if set, is called whenever a Relation message arrives --
	// including a re-send after a schema change. Sluice uses this to revalidate
	// replica identity and invalidate the policy cache.
	OnRelation func(old, new *Relation)
}

func NewDecoder() *Decoder {
	return &Decoder{
		relations: make(map[uint32]*Relation),
		typeNames: make(map[uint32]string),
	}
}

// Relation returns the cached relation for an OID.
func (d *Decoder) Relation(oid uint32) (*Relation, bool) {
	r, ok := d.relations[oid]
	return r, ok
}

// InStream reports whether a streamed (uncommitted) transaction is in progress.
// With streaming='off', which is Sluice's default, this is always false and
// every message the decoder emits belongs to an already-committed transaction.
func (d *Decoder) InStream() bool { return d.inStream }

type reader struct {
	b   []byte
	i   int
	err error
}

func (r *reader) fail(what string) {
	if r.err == nil {
		r.err = fmt.Errorf("pgoutput: truncated message reading %s", what)
	}
}

func (r *reader) u8() byte {
	if r.i+1 > len(r.b) {
		r.fail("uint8")
		return 0
	}
	v := r.b[r.i]
	r.i++
	return v
}

func (r *reader) u16() uint16 {
	if r.i+2 > len(r.b) {
		r.fail("uint16")
		return 0
	}
	v := binary.BigEndian.Uint16(r.b[r.i:])
	r.i += 2
	return v
}

func (r *reader) u32() uint32 {
	if r.i+4 > len(r.b) {
		r.fail("uint32")
		return 0
	}
	v := binary.BigEndian.Uint32(r.b[r.i:])
	r.i += 4
	return v
}

func (r *reader) i32() int32 { return int32(r.u32()) }

func (r *reader) u64() uint64 {
	if r.i+8 > len(r.b) {
		r.fail("uint64")
		return 0
	}
	v := binary.BigEndian.Uint64(r.b[r.i:])
	r.i += 8
	return v
}

// str reads a NUL-terminated string.
func (r *reader) str() string {
	start := r.i
	for r.i < len(r.b) && r.b[r.i] != 0 {
		r.i++
	}
	if r.i >= len(r.b) {
		r.fail("cstring")
		return ""
	}
	s := string(r.b[start:r.i])
	r.i++ // consume NUL
	return s
}

func (r *reader) timestamp() time.Time {
	micros := int64(r.u64())
	if micros == math.MinInt64 {
		return time.Time{}
	}
	return pgEpoch.Add(time.Duration(micros) * time.Microsecond)
}

func (r *reader) remaining() int { return len(r.b) - r.i }

// Decode parses one pgoutput message.
func (d *Decoder) Decode(data []byte) (*Message, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("pgoutput: empty message")
	}
	r := &reader{b: data}
	m := &Message{Type: r.u8()}

	switch m.Type {
	case MsgBegin:
		m.FinalLSN = r.u64()
		m.CommitTime = r.timestamp()
		m.XID = r.u32()

	case MsgCommit:
		_ = r.u8() // flags, currently unused
		m.CommitLSN = r.u64()
		m.EndLSN = r.u64()
		m.CommitTime = r.timestamp()

	case MsgOrigin:
		m.CommitLSN = r.u64()
		m.OriginName = r.str()

	case MsgRelation:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		rel := &Relation{}
		rel.OID = r.u32()
		rel.Namespace = r.str()
		if rel.Namespace == "" {
			rel.Namespace = "pg_catalog"
		}
		rel.Name = r.str()
		rel.fullName = rel.Namespace + "." + rel.Name
		rel.ReplicaIdentity = r.u8()
		n := int(r.u16())
		rel.Columns = make([]ColumnMeta, 0, n)
		rel.byName = make(map[string]int, n)
		for i := 0; i < n && r.err == nil; i++ {
			var c ColumnMeta
			c.IsKey = r.u8()&relColumnIsKey != 0
			c.Name = r.str()
			c.TypeOID = r.u32()
			c.TypeMod = r.i32()
			c.TypeName = d.typeNameFor(c.TypeOID)
			rel.byName[c.Name] = len(rel.Columns)
			rel.Columns = append(rel.Columns, c)
		}
		if r.err == nil {
			old := d.relations[rel.OID]
			d.relations[rel.OID] = rel
			if d.OnRelation != nil {
				d.OnRelation(old, rel)
			}
		}
		m.Relation = rel
		m.RelationOID = rel.OID

	case MsgType:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		m.TypeOID = r.u32()
		m.TypeNamespace = r.str()
		m.TypeName = r.str()
		if r.err == nil {
			d.typeNames[m.TypeOID] = m.TypeName
		}

	case MsgInsert:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		m.RelationOID = r.u32()
		if tag := r.u8(); tag != 'N' {
			return nil, fmt.Errorf("pgoutput: insert: expected 'N', got %q", tag)
		}
		m.New = d.tuple(r, m.RelationOID)

	case MsgUpdate:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		m.RelationOID = r.u32()
		// The protocol: at most one of 'K' (replica identity key) or 'O' (full
		// old tuple), never both, and possibly neither.
		tag := r.u8()
		if tag == 'K' || tag == 'O' {
			m.OldIsKey = tag == 'K'
			m.Old = d.tuple(r, m.RelationOID)
			tag = r.u8()
		}
		if tag != 'N' {
			return nil, fmt.Errorf("pgoutput: update: expected 'N', got %q", tag)
		}
		m.New = d.tuple(r, m.RelationOID)

	case MsgDelete:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		m.RelationOID = r.u32()
		tag := r.u8()
		if tag != 'K' && tag != 'O' {
			return nil, fmt.Errorf("pgoutput: delete: expected 'K' or 'O', got %q", tag)
		}
		m.OldIsKey = tag == 'K'
		m.Old = d.tuple(r, m.RelationOID)

	case MsgTruncate:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		n := int(r.u32())
		opts := r.u8()
		m.TruncateCascade = opts&1 != 0
		m.TruncateRestart = opts&2 != 0
		for i := 0; i < n && r.err == nil; i++ {
			m.TruncateRelations = append(m.TruncateRelations, r.u32())
		}

	case MsgMessage:
		if d.inStream {
			m.StreamXID = r.u32()
		}
		m.MessageTransactional = r.u8()&1 != 0
		m.MessageLSN = r.u64()
		m.MessagePrefix = r.str()
		n := int(r.u32())
		if r.err == nil {
			if n < 0 || n > r.remaining() {
				r.fail("message content")
			} else {
				m.MessageContent = append([]byte(nil), r.b[r.i:r.i+n]...)
				r.i += n
			}
		}

	case MsgStreamStart:
		m.StreamXID = r.u32()
		m.StreamFirst = r.u8() == 1
		d.inStream = true

	case MsgStreamStop:
		d.inStream = false

	case MsgStreamCommit:
		m.StreamXID = r.u32()
		_ = r.u8() // flags
		m.CommitLSN = r.u64()
		m.EndLSN = r.u64()
		m.CommitTime = r.timestamp()
		d.inStream = false

	case MsgStreamAbort:
		m.StreamXID = r.u32()
		m.AbortSubXID = r.u32()
		// Protocol 4 with streaming='parallel' appends the abort LSN and
		// timestamp. Length-checking rather than tracking the negotiated options
		// keeps the decoder honest either way.
		if r.remaining() >= 16 {
			m.AbortLSN = r.u64()
			m.AbortTime = r.timestamp()
		}
		d.inStream = false

	case MsgBeginPrepare, MsgPrepare, MsgCommitPrepared, MsgRollbackPrep, MsgStreamPrepare:
		// Two-phase messages (protocol >= 3). Sluice does not request
		// two_phase, so these should never arrive; decode the common prefix so
		// that an unexpected one is logged rather than desynchronising the
		// stream.
		if m.Type == MsgStreamPrepare {
			m.StreamXID = r.u32()
		}
		_ = r.u8() // flags
		m.FinalLSN = r.u64()
		m.EndLSN = r.u64()
		m.CommitTime = r.timestamp()
		m.XID = r.u32()

	default:
		return nil, fmt.Errorf("pgoutput: unknown message type %q", m.Type)
	}

	if r.err != nil {
		return nil, r.err
	}
	return m, nil
}

func (d *Decoder) typeNameFor(oid uint32) string {
	if n, ok := d.typeNames[oid]; ok {
		return n
	}
	return builtinTypeName(oid)
}

func (d *Decoder) tuple(r *reader, relOID uint32) *Tuple {
	n := int(r.u16())
	t := &Tuple{Columns: make([]Column, 0, n)}
	for i := 0; i < n && r.err == nil; i++ {
		kind := ColumnKind(r.u8())
		switch kind {
		case ColNull, ColUnchanged:
			t.Columns = append(t.Columns, Column{Kind: kind})
		case ColText, ColBinary:
			ln := int(r.u32())
			if ln < 0 || ln > r.remaining() {
				r.fail("tuple column data")
				return t
			}
			t.Columns = append(t.Columns, Column{Kind: kind, Data: r.b[r.i : r.i+ln]})
			r.i += ln
		default:
			r.err = fmt.Errorf("pgoutput: unknown tuple column kind %q", byte(kind))
			return t
		}
	}
	return t
}

// builtinTypeName maps the type OIDs Sluice cares about to names, so that text
// values can be coerced without a catalog round trip. Anything unlisted is
// treated as text, which is correct for comparison purposes because PostgreSQL
// already inserted explicit casts into the policy expression.
func builtinTypeName(oid uint32) string {
	switch oid {
	case 16:
		return "bool"
	case 20:
		return "int8"
	case 21:
		return "int2"
	case 23:
		return "int4"
	case 25:
		return "text"
	case 26:
		return "oid"
	case 114:
		return "json"
	case 700:
		return "float4"
	case 701:
		return "float8"
	case 1042:
		return "bpchar"
	case 1043:
		return "varchar"
	case 1082:
		return "date"
	case 1083:
		return "time"
	case 1114:
		return "timestamp"
	case 1184:
		return "timestamptz"
	case 1186:
		return "interval"
	case 1700:
		return "numeric"
	case 2950:
		return "uuid"
	case 3802:
		return "jsonb"
	case 869:
		return "inet"
	}
	return "text"
}
