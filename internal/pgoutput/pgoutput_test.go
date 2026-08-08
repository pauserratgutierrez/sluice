package pgoutput

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"
)

// builder assembles pgoutput messages the way the walsender does, so the decoder
// is tested against the real byte layout rather than against itself.
type builder struct{ b bytes.Buffer }

func (w *builder) u8(v byte) *builder { w.b.WriteByte(v); return w }
func (w *builder) u16(v uint16) *builder {
	binary.Write(&w.b, binary.BigEndian, v)
	return w
}
func (w *builder) u32(v uint32) *builder {
	binary.Write(&w.b, binary.BigEndian, v)
	return w
}
func (w *builder) i32(v int32) *builder {
	binary.Write(&w.b, binary.BigEndian, v)
	return w
}
func (w *builder) u64(v uint64) *builder {
	binary.Write(&w.b, binary.BigEndian, v)
	return w
}
func (w *builder) str(s string) *builder {
	w.b.WriteString(s)
	w.b.WriteByte(0)
	return w
}
func (w *builder) bytes() []byte { return w.b.Bytes() }

// relation builds a Relation message for a table with the given columns.
func relation(oid uint32, ns, name string, replident byte, cols ...ColumnMeta) []byte {
	w := (&builder{}).u8(MsgRelation).u32(oid).str(ns).str(name).u8(replident).u16(uint16(len(cols)))
	for _, c := range cols {
		flag := byte(0)
		if c.IsKey {
			flag = 1
		}
		w.u8(flag).str(c.Name).u32(c.TypeOID).i32(c.TypeMod)
	}
	return w.bytes()
}

// tuple builds a TupleData block.
func tuple(cols ...Column) []byte {
	w := (&builder{}).u16(uint16(len(cols)))
	for _, c := range cols {
		w.u8(byte(c.Kind))
		if c.Kind == ColText || c.Kind == ColBinary {
			w.u32(uint32(len(c.Data)))
			w.b.Write(c.Data)
		}
	}
	return w.bytes()
}

func text(s string) Column { return Column{Kind: ColText, Data: []byte(s)} }

var testCols = []ColumnMeta{
	{Name: "id", TypeOID: 23, TypeMod: -1, IsKey: true},
	{Name: "owner_id", TypeOID: 2950, TypeMod: -1},
	{Name: "body", TypeOID: 25, TypeMod: -1},
}

func decoderWithRelation(t *testing.T) *Decoder {
	t.Helper()
	d := NewDecoder()
	if _, err := d.Decode(relation(16400, "public", "docs", 'd', testCols...)); err != nil {
		t.Fatalf("decode relation: %v", err)
	}
	return d
}

func TestDecodeRelation(t *testing.T) {
	d := NewDecoder()
	m, err := d.Decode(relation(16400, "public", "docs", 'i', testCols...))
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != MsgRelation {
		t.Fatalf("type = %q", m.Type)
	}
	rel := m.Relation
	if rel.FullName() != "public.docs" {
		t.Errorf("FullName = %q", rel.FullName())
	}
	if rel.ReplicaIdentity != 'i' {
		t.Errorf("ReplicaIdentity = %q", rel.ReplicaIdentity)
	}
	if len(rel.Columns) != 3 {
		t.Fatalf("columns = %d", len(rel.Columns))
	}
	if rel.ColumnIndex("owner_id") != 1 || rel.ColumnIndex("nope") != -1 {
		t.Errorf("ColumnIndex wrong: owner_id=%d nope=%d",
			rel.ColumnIndex("owner_id"), rel.ColumnIndex("nope"))
	}
	if got := rel.KeyColumns(); len(got) != 1 || got[0] != "id" {
		t.Errorf("KeyColumns = %v, want [id]", got)
	}
	// Built-in OIDs must resolve to names without a catalog round trip, since the
	// text decoder needs them to coerce values.
	if rel.Columns[1].TypeName != "uuid" || rel.Columns[2].TypeName != "text" {
		t.Errorf("type names = %q, %q", rel.Columns[1].TypeName, rel.Columns[2].TypeName)
	}
}

func TestRelationResendFiresCallback(t *testing.T) {
	d := NewDecoder()
	var oldSeen, newSeen *Relation
	var calls int
	d.OnRelation = func(o, n *Relation) { oldSeen, newSeen, calls = o, n, calls+1 }

	d.Decode(relation(16400, "public", "docs", 'd', testCols...))
	if calls != 1 || oldSeen != nil {
		t.Fatalf("first relation: calls=%d old=%v", calls, oldSeen)
	}
	// A schema change re-sends Relation. It is the only in-band signal there is,
	// because DDL is not replicated.
	d.Decode(relation(16400, "public", "docs", 'f', append(testCols, ColumnMeta{Name: "extra", TypeOID: 23, TypeMod: -1})...))
	if calls != 2 || oldSeen == nil {
		t.Fatalf("re-send: calls=%d old=%v", calls, oldSeen)
	}
	if len(oldSeen.Columns) != 3 || len(newSeen.Columns) != 4 {
		t.Errorf("old=%d new=%d columns", len(oldSeen.Columns), len(newSeen.Columns))
	}
	if newSeen.ReplicaIdentity != 'f' {
		t.Errorf("new replica identity = %q", newSeen.ReplicaIdentity)
	}
}

func TestDecodeInsert(t *testing.T) {
	d := decoderWithRelation(t)
	msg := append([]byte{MsgInsert}, u32b(16400)...)
	msg = append(msg, 'N')
	msg = append(msg, tuple(text("1"), text("abc"), text("hello"))...)

	m, err := d.Decode(msg)
	if err != nil {
		t.Fatal(err)
	}
	if m.New == nil || m.Old != nil {
		t.Fatalf("New=%v Old=%v", m.New, m.Old)
	}
	if got := string(m.New.Columns[2].Data); got != "hello" {
		t.Errorf("body = %q", got)
	}
}

// The unchanged-TOAST marker is the single most important thing this decoder
// gets right. It must never be conflated with NULL.
func TestDecodeUnchangedToast(t *testing.T) {
	d := decoderWithRelation(t)
	msg := append([]byte{MsgUpdate}, u32b(16400)...)
	msg = append(msg, 'N')
	msg = append(msg, tuple(text("1"), text("abc"), Column{Kind: ColUnchanged})...)

	m, err := d.Decode(msg)
	if err != nil {
		t.Fatal(err)
	}
	body := m.New.Columns[2]
	if body.Kind != ColUnchanged {
		t.Fatalf("body kind = %q, want 'u'", byte(body.Kind))
	}
	if body.Data != nil {
		t.Errorf("an unchanged column must carry no data, got %q", body.Data)
	}
	// And it must be distinguishable from a real NULL.
	if body.Kind == ColNull {
		t.Fatal("unchanged TOAST must not compare equal to NULL")
	}
}

// The protocol allows an Update to carry a 'K' key tuple, an 'O' full old tuple,
// or neither -- and never both. All three shapes must decode.
func TestDecodeUpdateOldTupleVariants(t *testing.T) {
	newTup := tuple(text("1"), text("abc"), text("v2"))

	t.Run("neither", func(t *testing.T) {
		d := decoderWithRelation(t)
		msg := append(append([]byte{MsgUpdate}, u32b(16400)...), 'N')
		m, err := d.Decode(append(msg, newTup...))
		if err != nil {
			t.Fatal(err)
		}
		if m.Old != nil {
			t.Error("expected no old tuple with REPLICA IDENTITY DEFAULT and unchanged key")
		}
	})

	for _, tc := range []struct {
		tag   byte
		isKey bool
	}{{'K', true}, {'O', false}} {
		t.Run(string(tc.tag), func(t *testing.T) {
			d := decoderWithRelation(t)
			msg := append(append([]byte{MsgUpdate}, u32b(16400)...), tc.tag)
			msg = append(msg, tuple(text("1"), text("abc"), text("v1"))...)
			msg = append(msg, 'N')
			m, err := d.Decode(append(msg, newTup...))
			if err != nil {
				t.Fatal(err)
			}
			if m.Old == nil {
				t.Fatal("expected an old tuple")
			}
			if m.OldIsKey != tc.isKey {
				t.Errorf("OldIsKey = %v, want %v", m.OldIsKey, tc.isKey)
			}
			if got := string(m.Old.Columns[2].Data); got != "v1" {
				t.Errorf("old body = %q", got)
			}
		})
	}
}

func TestDecodeDelete(t *testing.T) {
	d := decoderWithRelation(t)
	msg := append(append([]byte{MsgDelete}, u32b(16400)...), 'O')
	m, err := d.Decode(append(msg, tuple(text("1"), text("abc"), text("gone"))...))
	if err != nil {
		t.Fatal(err)
	}
	if m.Old == nil || m.New != nil {
		t.Fatalf("Old=%v New=%v", m.Old, m.New)
	}
	if m.OldIsKey {
		t.Error("'O' means a full old tuple, not a key")
	}
}

func TestDecodeMessage(t *testing.T) {
	d := NewDecoder()
	content := []byte(`{"event":"paid"}`)
	w := (&builder{}).u8(MsgMessage).u8(1).u64(0x1A2B3C4D).str("sluice:orders:1").u32(uint32(len(content)))
	w.b.Write(content)

	m, err := d.Decode(w.bytes())
	if err != nil {
		t.Fatal(err)
	}
	if !m.MessageTransactional {
		t.Error("flag bit 1 means transactional")
	}
	if m.MessagePrefix != "sluice:orders:1" {
		t.Errorf("prefix = %q", m.MessagePrefix)
	}
	if string(m.MessageContent) != string(content) {
		t.Errorf("content = %q", m.MessageContent)
	}
}

func TestDecodeBeginCommit(t *testing.T) {
	d := NewDecoder()
	ts := uint64(800000000000000) // microseconds since 2000-01-01

	m, err := d.Decode((&builder{}).u8(MsgBegin).u64(0xAABB).u64(ts).u32(4242).bytes())
	if err != nil {
		t.Fatal(err)
	}
	if m.FinalLSN != 0xAABB || m.XID != 4242 {
		t.Errorf("lsn=%x xid=%d", m.FinalLSN, m.XID)
	}
	want := pgEpoch.Add(time.Duration(ts) * time.Microsecond)
	if !m.CommitTime.Equal(want) {
		t.Errorf("commit time = %v, want %v", m.CommitTime, want)
	}

	m, err = d.Decode((&builder{}).u8(MsgCommit).u8(0).u64(0xAABB).u64(0xAAFF).u64(ts).bytes())
	if err != nil {
		t.Fatal(err)
	}
	if m.CommitLSN != 0xAABB || m.EndLSN != 0xAAFF {
		t.Errorf("commit=%x end=%x", m.CommitLSN, m.EndLSN)
	}
}

func TestDecodeStreamingSetsInStream(t *testing.T) {
	d := NewDecoder()
	if d.InStream() {
		t.Fatal("should not start in a stream")
	}
	if _, err := d.Decode((&builder{}).u8(MsgStreamStart).u32(99).u8(1).bytes()); err != nil {
		t.Fatal(err)
	}
	if !d.InStream() {
		t.Fatal("StreamStart must set inStream")
	}
	if _, err := d.Decode([]byte{MsgStreamStop}); err != nil {
		t.Fatal(err)
	}
	if d.InStream() {
		t.Fatal("StreamStop must clear inStream")
	}
}

// Protocol 4 with streaming=parallel appends an abort LSN and timestamp. The
// decoder length-checks rather than tracking negotiated options, so both shapes
// must work.
func TestDecodeStreamAbortBothShapes(t *testing.T) {
	d := NewDecoder()
	short := (&builder{}).u8(MsgStreamAbort).u32(7).u32(8).bytes()
	if _, err := d.Decode(short); err != nil {
		t.Fatalf("proto<4 abort: %v", err)
	}
	long := (&builder{}).u8(MsgStreamAbort).u32(7).u32(8).u64(0x1234).u64(800000000000000).bytes()
	m, err := d.Decode(long)
	if err != nil {
		t.Fatalf("proto 4 parallel abort: %v", err)
	}
	if m.AbortLSN != 0x1234 {
		t.Errorf("AbortLSN = %x", m.AbortLSN)
	}
}

// A truncated message must produce an error, never a partially-populated message
// that downstream code would treat as real.
func TestTruncatedMessagesError(t *testing.T) {
	full := relation(16400, "public", "docs", 'd', testCols...)
	for cut := 1; cut < len(full); cut++ {
		d := NewDecoder()
		if _, err := d.Decode(full[:cut]); err == nil {
			t.Fatalf("truncating relation to %d bytes decoded without error", cut)
		}
	}

	d := decoderWithRelation(t)
	ins := append(append([]byte{MsgInsert}, u32b(16400)...), 'N')
	ins = append(ins, tuple(text("1"), text("abc"), text("hello"))...)
	for cut := 1; cut < len(ins); cut++ {
		dd := decoderWithRelation(t)
		if _, err := dd.Decode(ins[:cut]); err == nil {
			t.Fatalf("truncating insert to %d bytes decoded without error", cut)
		}
	}
	_ = d
}

func TestUnknownMessageTypeErrors(t *testing.T) {
	d := NewDecoder()
	if _, err := d.Decode([]byte{'Z'}); err == nil {
		t.Fatal("an unknown message type must be an error, not silently ignored")
	}
	if _, err := d.Decode(nil); err == nil {
		t.Fatal("an empty message must be an error")
	}
}

func TestUnknownTupleKindErrors(t *testing.T) {
	d := decoderWithRelation(t)
	msg := append(append([]byte{MsgInsert}, u32b(16400)...), 'N')
	msg = append(msg, (&builder{}).u16(1).u8('X').bytes()...)
	if _, err := d.Decode(msg); err == nil {
		t.Fatal("an unknown TupleData kind must error rather than be skipped")
	}
}

func TestTypeMessageOverridesBuiltinName(t *testing.T) {
	d := NewDecoder()
	// A user-defined enum gets a Type message before the Relation message.
	if _, err := d.Decode((&builder{}).u8(MsgType).u32(99999).str("public").str("mood").bytes()); err != nil {
		t.Fatal(err)
	}
	m, err := d.Decode(relation(16401, "public", "t", 'd',
		ColumnMeta{Name: "m", TypeOID: 99999, TypeMod: -1}))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.Relation.Columns[0].TypeName; got != "mood" {
		t.Errorf("type name = %q, want mood", got)
	}
}

func u32b(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}
