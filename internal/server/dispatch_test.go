package server

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

type walColumn struct {
	key  byte
	name string
	oid  uint32
}

// walRelation decodes a Relation message the way the reader does.
func walRelation(t *testing.T, oid uint32, schema, name string, cols ...walColumn) *pgoutput.Relation {
	t.Helper()
	var b bytes.Buffer
	b.WriteByte(pgoutput.MsgRelation)
	_ = binary.Write(&b, binary.BigEndian, oid)
	b.WriteString(schema + "\x00" + name + "\x00")
	b.WriteByte('d')
	_ = binary.Write(&b, binary.BigEndian, uint16(len(cols)))
	for _, c := range cols {
		b.WriteByte(c.key)
		b.WriteString(c.name + "\x00")
		_ = binary.Write(&b, binary.BigEndian, c.oid)
		_ = binary.Write(&b, binary.BigEndian, int32(-1))
	}
	m, err := pgoutput.NewDecoder().Decode(b.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	return m.Relation
}

// walDocs decodes a Relation message for docsRel(), with id as the replica
// identity key.
func walDocs(t *testing.T) *pgoutput.Relation {
	t.Helper()
	return walRelation(t, 100, "public", "documents",
		walColumn{1, "id", 20}, walColumn{0, "project_id", 25}, walColumn{0, "title", 25})
}

func row(vals ...string) *pgoutput.Tuple {
	t := &pgoutput.Tuple{}
	for _, v := range vals {
		t.Columns = append(t.Columns, pgoutput.Column{Kind: pgoutput.ColText, Data: []byte(v)})
	}
	return t
}

func changesOf(st *hub.Stream) []event.Change {
	var out []event.Change
	for _, ev := range st.Take(nil) {
		if c, ok := ev.Data.(event.Change); ok {
			out = append(out, c)
		}
	}
	return out
}

func deliverUpdate(t *testing.T, transitions bool, columns []string) []event.Change {
	t.Helper()
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	st := s.hub.Open("n1.1", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	ops, _ := shape.ParseOps(nil)
	sub := &registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Ops: ops, Filter: f, Columns: columns,
		Transitions: transitions,
		Decision:    authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey:  f.RoutingKey(rel),
	}
	if !s.reg.Add(sub) {
		t.Fatal("add")
	}
	wal := walDocs(t)
	// DEFAULT replica identity with an unchanged key: pgoutput sends no old tuple.
	m := &pgoutput.Message{Type: pgoutput.MsgUpdate, RelationOID: 100, New: row("7", "42", "renamed")}
	if err := s.OnChange(m, wal, 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}
	return changesOf(st)
}

// An UPDATE that carries no old tuple is the normal case for DEFAULT and USING
// INDEX replica identities. It must arrive as an UPDATE, not as a row entering
// the shape.
func TestUpdateWithoutOldTupleIsAnUpdate(t *testing.T) {
	for _, transitions := range []bool{false, true} {
		got := deliverUpdate(t, transitions, []string{"id", "project_id", "title"})
		if len(got) != 1 {
			t.Fatalf("transitions=%v: got %d events, want 1", transitions, len(got))
		}
		if got[0].Op != "UPDATE" || got[0].Transition != "" {
			t.Errorf("transitions=%v: op=%q transition=%q, want a plain UPDATE", transitions, got[0].Op, got[0].Transition)
		}
	}
}

// The projection is exactly the granted columns: a key column is included only
// because the oracle put it there, and nothing else leaks in.
func TestProjectionIsExactlyTheSubscriptionColumns(t *testing.T) {
	got := deliverUpdate(t, false, []string{"id", "title"})
	if len(got) != 1 {
		t.Fatalf("got %d events", len(got))
	}
	rec := got[0].Record.Values
	if _, ok := rec["project_id"]; ok {
		t.Errorf("record = %v, carries a column outside the projection", rec)
	}
	if rec["title"] != "renamed" {
		t.Errorf("record = %v", rec)
	}
	// Values are encoded by column type, as to_jsonb would: id is an int8.
	if b, _ := json.Marshal(rec["id"]); string(b) != "7" {
		t.Errorf("id encoded as %s, want the number 7", b)
	}
}

// Subscriptions that project the same columns share one record, so a change
// is encoded once per projection rather than once per subscriber; a different
// projection gets its own.
func TestProjectionIsSharedAcrossSubscribers(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	ops, _ := shape.ParseOps(nil)
	var streams []*hub.Stream
	for i, cols := range [][]string{{"id", "title"}, {"id", "title"}, {"id"}} {
		st := s.hub.Open(fmt.Sprintf("n1.%d", i), authz.Identity{Sub: "u1", Role: "authenticated"})
		streams = append(streams, st)
		if !s.reg.Add(&registry.Subscription{
			Label: "docs", Sink: st, Relation: rel, Ops: ops, Filter: f, Columns: cols,
			Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
			RoutingKey: f.RoutingKey(rel),
		}) {
			t.Fatal("add")
		}
	}
	m := &pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row("7", "42", "hello")}
	if err := s.OnChange(m, walDocs(t), 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}

	var recs []*event.Row
	for _, st := range streams {
		got := changesOf(st)
		if len(got) != 1 {
			t.Fatalf("got %d events, want 1", len(got))
		}
		recs = append(recs, got[0].Record)
	}
	if recs[0] != recs[1] {
		t.Error("two subscriptions with the same projection got separate records")
	}
	if recs[0] == recs[2] {
		t.Fatal("a narrower projection shared the wider record")
	}
	if b, _ := json.Marshal(recs[2]); string(b) != `{"id":7}` {
		t.Errorf("narrow record encoded as %s", b)
	}
	if b, _ := json.Marshal(recs[0]); string(b) != `{"id":7,"title":"hello"}` {
		t.Errorf("shared record encoded as %s", b)
	}
}
