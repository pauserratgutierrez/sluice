package registry

import (
	"fmt"
	"sync"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

type fakeSink struct {
	id   string
	mu   sync.Mutex
	sent []event.Event
}

func (f *fakeSink) StreamID() string { return f.id }
func (f *fakeSink) Send(e event.Event) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, e)
	return true
}
func (f *fakeSink) Identity() authz.Identity { return authz.Identity{Role: "authenticated"} }

func testRel() *catalog.Relation {
	return &catalog.Relation{
		Schema: "public", Name: "docs", OID: 16400,
		ReplicaIdentityColumns: []string{"owner_id", "id"},
		Columns: []catalog.Column{
			{Name: "id", TypeName: "bigint"},
			{Name: "owner_id", TypeName: "uuid"},
			{Name: "title", TypeName: "text"},
		},
		IndexedColumns: map[string]bool{"id": true, "owner_id": true},
	}
}

func sub(t *testing.T, sink Sink, label, filter string) *Subscription {
	t.Helper()
	r := testRel()
	f, err := shape.Parse(filter, r)
	if err != nil {
		t.Fatalf("parse filter %q: %v", filter, err)
	}
	ops, _ := shape.ParseOps(nil)
	return &Subscription{
		Label: label, Sink: sink, Relation: r, Ops: ops, Filter: f,
		Decision:   &authz.Decision{Tier: authz.TierA, Granted: true},
		RoutingKey: f.RoutingKey(r),
	}
}

func lookup(vals map[string]string) func(string) (expr.Value, bool) {
	return func(col string) (expr.Value, bool) {
		v, ok := vals[col]
		if !ok {
			return expr.Null, false
		}
		return expr.Text(v), true
	}
}

// The routing index is the mechanism that makes dispatch O(1) in the number of
// subscribers instead of O(N). This is the test that proves it.
func TestCandidatesUsesConstantIndex(t *testing.T) {
	r := New()
	sinks := make([]*fakeSink, 100)
	for i := range sinks {
		sinks[i] = &fakeSink{id: fmt.Sprintf("s%d", i)}
		// Each subscriber watches a different owner.
		if !r.Add(sub(t, sinks[i], "docs", fmt.Sprintf("owner_id=eq.owner-%d", i))) {
			t.Fatalf("add %d failed", i)
		}
	}

	got := r.Candidates(16400, lookup(map[string]string{"owner_id": "owner-42", "id": "1"}))
	if len(got) != 1 {
		t.Fatalf("candidates = %d, want exactly 1 out of 100 subscribers", len(got))
	}
	if got[0].Sink.StreamID() != "s42" {
		t.Errorf("matched %s, want s42", got[0].Sink.StreamID())
	}

	// A value nobody subscribed to matches nobody.
	if got := r.Candidates(16400, lookup(map[string]string{"owner_id": "nobody"})); len(got) != 0 {
		t.Errorf("candidates for an unwatched value = %d, want 0", len(got))
	}
}

func TestUnindexedSubscriptionsAlwaysConsidered(t *testing.T) {
	r := New()
	indexed := &fakeSink{id: "indexed"}
	scanned := &fakeSink{id: "scanned"}
	r.Add(sub(t, indexed, "a", "owner_id=eq.x"))
	// No equality at all, so there is nothing to index by.
	r.Add(sub(t, scanned, "b", "title=like.foo*"))

	got := r.Candidates(16400, lookup(map[string]string{"owner_id": "someone-else"}))
	if len(got) != 1 || got[0].Sink.StreamID() != "scanned" {
		t.Fatalf("unindexed subscription must always be a candidate, got %v", ids(got))
	}
	if r.Stats().Unindexed != 1 {
		t.Errorf("unindexed count = %d, want 1", r.Stats().Unindexed)
	}
}

// When the routing column's value is unavailable -- an unchanged TOASTed value,
// or a column outside a narrow replica identity -- the index cannot be consulted.
// Correctness requires falling back to considering everything indexed on that
// column, because the residual filter and the authorizer still run afterwards.
func TestUnknownRoutingValueFallsBackConservatively(t *testing.T) {
	r := New()
	a := &fakeSink{id: "a"}
	b := &fakeSink{id: "b"}
	r.Add(sub(t, a, "a", "owner_id=eq.x"))
	r.Add(sub(t, b, "b", "owner_id=eq.y"))

	// owner_id not available at all.
	got := r.Candidates(16400, lookup(map[string]string{"id": "1"}))
	if len(got) != 2 {
		t.Fatalf("candidates = %v, want both (conservative fallback)", ids(got))
	}
}

func TestDuplicateLabelRejected(t *testing.T) {
	r := New()
	s := &fakeSink{id: "s"}
	if !r.Add(sub(t, s, "dup", "owner_id=eq.x")) {
		t.Fatal("first add should succeed")
	}
	if r.Add(sub(t, s, "dup", "owner_id=eq.y")) {
		t.Fatal("a duplicate label on the same stream must be rejected")
	}
	// The same label on a DIFFERENT stream is fine.
	if !r.Add(sub(t, &fakeSink{id: "other"}, "dup", "owner_id=eq.y")) {
		t.Fatal("same label on another stream should be allowed")
	}
}

func TestRemoveCleansUpIndex(t *testing.T) {
	r := New()
	s := &fakeSink{id: "s"}
	r.Add(sub(t, s, "a", "owner_id=eq.x"))
	r.Add(sub(t, s, "b", "title=like.z*"))

	if got := r.Stats(); got.Subscriptions != 2 || got.Unindexed != 1 {
		t.Fatalf("stats before = %+v", got)
	}
	if r.Remove("s", "a") == nil {
		t.Fatal("remove should return the subscription")
	}
	if r.Remove("s", "nope") != nil {
		t.Error("removing an unknown label should return nil")
	}
	r.Remove("s", "b")

	got := r.Stats()
	if got.Subscriptions != 0 || got.Unindexed != 0 || got.Relations != 0 || got.Streams != 0 {
		t.Errorf("stats after full removal = %+v, want all zero", got)
	}
	if c := r.Candidates(16400, lookup(map[string]string{"owner_id": "x"})); len(c) != 0 {
		t.Errorf("index still returns %v after removal", ids(c))
	}
}

// A disconnect must remove everything the stream held. Leaking subscriptions
// here would leak memory and keep dispatching to a dead connection.
func TestRemoveStream(t *testing.T) {
	r := New()
	s := &fakeSink{id: "s"}
	for i := 0; i < 10; i++ {
		r.Add(sub(t, s, fmt.Sprintf("sub%d", i), fmt.Sprintf("owner_id=eq.o%d", i)))
	}
	r.Add(sub(t, &fakeSink{id: "keep"}, "x", "owner_id=eq.keep"))

	removed := r.RemoveStream("s")
	if len(removed) != 10 {
		t.Fatalf("removed %d, want 10", len(removed))
	}
	st := r.Stats()
	if st.Subscriptions != 1 || st.Streams != 1 {
		t.Errorf("stats = %+v, want 1 subscription on 1 stream", st)
	}
	if len(r.RemoveStream("s")) != 0 {
		t.Error("removing an already-removed stream must be a no-op")
	}
}

func TestConcurrentAddRemoveAndDispatch(t *testing.T) {
	r := New()
	const streams = 32
	const perStream = 8

	var wg sync.WaitGroup
	// Writers churn subscriptions while readers dispatch, which is exactly the
	// shape of live traffic: subscribes and disconnects during a change storm.
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := &fakeSink{id: fmt.Sprintf("s%d", i)}
			for round := 0; round < 20; round++ {
				for j := 0; j < perStream; j++ {
					r.Add(sub(t, s, fmt.Sprintf("l%d", j), fmt.Sprintf("owner_id=eq.o%d", j)))
				}
				r.RemoveStream(s.id)
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for round := 0; round < 500; round++ {
				r.Candidates(16400, lookup(map[string]string{"owner_id": "o3"}))
				r.Stats()
				r.All()
			}
		}()
	}
	wg.Wait()

	// Every stream removed itself last, so the registry must be empty.
	if st := r.Stats(); st.Subscriptions != 0 || st.Streams != 0 {
		t.Errorf("after churn, stats = %+v, want empty", st)
	}
}

func ids(subs []*Subscription) []string {
	out := make([]string, 0, len(subs))
	for _, s := range subs {
		out = append(out, s.Sink.StreamID()+"/"+s.Label)
	}
	return out
}
