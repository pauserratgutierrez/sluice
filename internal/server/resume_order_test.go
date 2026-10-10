package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

// resumeServer is a server whose buffer holds n inserts into documents, ids 1
// to n in project 42, committed at 0x201 to 0x200+n.
func resumeServer(t *testing.T, n int) (*Server, *pgoutput.Relation) {
	t.Helper()
	s := testServer(t, &slowOracle{stubOracle: stubOracle{name: oracle.NameRLS}})
	s.cat.PutForTest(docsRel())
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10
	s.hub.Rings().SetStart(0x100)
	wal := walDocs(t)
	for id := 1; id <= n; id++ {
		insertDoc(t, s, wal, id)
	}
	return s, wal
}

func insertDoc(t *testing.T, s *Server, wal *pgoutput.Relation, id int) {
	t.Helper()
	m := &pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row(fmt.Sprint(id), "42", "t")}
	if err := s.OnChange(m, wal, uint64(0x200+id), time.Now()); err != nil {
		t.Fatal(err)
	}
}

func resumeDocs(t *testing.T, s *Server, st *hub.Stream, ops []string) {
	t.Helper()
	id := st.Identity()
	res := s.applySubscriptions(context.Background(), st, id,
		[]subSpec{{Sub: "docs", Shape: &shapeSpec{Table: "documents", Filter: "project_id=eq.42", Ops: ops}}},
		map[string]string{"public.documents": "0/201"})[0]
	if !res.OK || res.Resumed == nil || !*res.Resumed {
		t.Fatalf("resume: ok %v, resumed %v, error %+v", res.OK, deref(res.Resumed), res.Error)
	}
}

// collect drains a stream until it has received n changes or wait has passed.
func collect(st *hub.Stream, n int, wait time.Duration) []event.Change {
	var got []event.Change
	for deadline := time.Now().Add(wait); len(got) < n && time.Now().Before(deadline); time.Sleep(time.Millisecond) {
		got = append(got, changesOf(st)...)
	}
	return got
}

// A resume replays the buffer off the request. A change the reader dispatches
// meanwhile reaches the subscription after the replay, not ahead of it: a
// client that upserts by key would otherwise apply an older row over a newer
// one. The replay waits for room in the queue, which makes it last here.
func TestResumedChangesReachTheStreamInCommitOrder(t *testing.T) {
	s, wal := resumeServer(t, 8) // testServer's buffer holds 8 changes a table
	st := s.hub.Open("n1.order", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	for range 8 { // half of the queue of 16: the replay waits from its first change
		hub.SendWarning(st, event.Warning{Code: "filler"})
	}
	resumeDocs(t, s, st, nil)
	insertDoc(t, s, wal, 9) // dispatched while the replay waits
	got := collect(st, 9, time.Second)
	insertDoc(t, s, wal, 10)
	got = append(got, collect(st, 1, time.Second)...)

	var lsns []string
	for _, c := range got {
		lsns = append(lsns, c.CommitLSN)
	}
	want := "[0/201 0/202 0/203 0/204 0/205 0/206 0/207 0/208 0/209 0/20A]"
	if fmt.Sprint(lsns) != want {
		t.Fatalf("changes arrived as %v, want %s", lsns, want)
	}
}

// A replay keeps to the shape's operations, as the reader does: a shape that
// asked for inserts gets no delete from the buffer.
func TestReplayKeepsToTheShapesOps(t *testing.T) {
	s, wal := resumeServer(t, 1)
	del := &pgoutput.Message{Type: pgoutput.MsgDelete, RelationOID: 100, Old: row("1", "42", "t")}
	if err := s.OnChange(del, wal, 0x202, time.Now()); err != nil {
		t.Fatal(err)
	}
	st := s.hub.Open("n1.ops", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	resumeDocs(t, s, st, []string{"INSERT"})
	got := collect(st, 2, 200*time.Millisecond)
	if len(got) != 1 || got[0].Op != "INSERT" {
		var ops []string
		for _, c := range got {
			ops = append(ops, c.Op)
		}
		t.Fatalf("replayed %v to a shape of inserts, want [INSERT]", ops)
	}
}
