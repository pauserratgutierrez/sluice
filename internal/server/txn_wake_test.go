package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

func docsStream(t *testing.T, s *Server) *hub.Stream {
	t.Helper()
	st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	ops, _ := shape.ParseOps(nil)
	if !s.reg.Add(&registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Ops: ops, Filter: f, Columns: []string{"id", "project_id", "title"},
		Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}), RoutingKey: f.RoutingKey(rel),
	}) {
		t.Fatal("add")
	}
	return st
}

func woken(st *hub.Stream) bool {
	select {
	case <-st.Ready():
		return true
	default:
		return false
	}
}

// The events one transaction brings a stream go out together: its writer is
// woken at the commit, once, not by each event.
func TestATransactionWakesEachStreamOnceAtItsCommit(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	st := docsStream(t, s)
	wal := walDocs(t)
	s.OnBegin(0x10, time.Now(), 1)
	for i := range 3 {
		m := &pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row(fmt.Sprint(i), "42", "t")}
		if err := s.OnChange(m, wal, 0x10, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	if woken(st) {
		t.Fatal("the writer was woken before the commit")
	}
	s.OnCommit(0x10, time.Now())
	if !woken(st) {
		t.Fatal("the writer was not woken at the commit")
	}
	if n := len(changesOf(st)); n != 3 {
		t.Fatalf("%d changes queued, want 3", n)
	}
}

// A transaction that has been dispatching for longer than txnWakeDelay wakes
// writers as its events arrive, so its first rows are not held for the rest.
func TestALongTransactionWakesWritersAtOnce(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	st := docsStream(t, s)
	s.OnBegin(0x10, time.Now(), 1)
	s.txnBegan = time.Now().Add(-2 * txnWakeDelay)
	m := &pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row("1", "42", "t")}
	if err := s.OnChange(m, walDocs(t), 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !woken(st) {
		t.Fatal("past txnWakeDelay the writer must be woken at once")
	}
}

// Events queued by a transaction are woken at the next row dispatched once
// txnWakeDelay has passed, even when that row reaches no stream.
func TestQueuedEventsAreWokenAtTheNextRowAfterTheDelay(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	st := docsStream(t, s)
	wal := walDocs(t)
	s.OnBegin(0x10, time.Now(), 1)
	if err := s.OnChange(&pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row("1", "42", "t")}, wal, 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}
	if woken(st) {
		t.Fatal("woken before the delay")
	}
	s.txnBegan = time.Now().Add(-2 * txnWakeDelay)
	// A row of another project: no stream wants it.
	if err := s.OnChange(&pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row("2", "99", "t")}, wal, 0x10, time.Now()); err != nil {
		t.Fatal(err)
	}
	if !woken(st) {
		t.Fatal("past txnWakeDelay, the next row must wake the queued stream")
	}
}
