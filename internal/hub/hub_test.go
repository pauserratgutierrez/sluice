package hub

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

func newHub(queue int) *Hub { return New(queue, 8, time.Minute, 10*time.Millisecond) }

func drain(s *Stream) []event.Event {
	return s.Take(nil)
}

// The overflow policy differs by kind on purpose, and each choice is a
// correctness statement rather than a tuning knob.
func TestBackpressurePolicyPerKind(t *testing.T) {
	t.Run("change closes the stream", func(t *testing.T) {
		// A silently truncated change stream is worse than a closed one: the
		// client would believe it has a complete view when it does not.
		h := newHub(2)
		s := h.Open("s", authz.Identity{})
		for i := 0; i < 2; i++ {
			if !s.Send(event.Event{Kind: event.KindChange}) {
				t.Fatalf("send %d should fit in the queue", i)
			}
		}
		if s.Send(event.Event{Kind: event.KindChange}) {
			t.Fatal("overflowing change event should not be accepted")
		}
		if s.CloseCode() != "stream_lagging" {
			t.Errorf("close code = %q, want stream_lagging", s.CloseCode())
		}
	})

	t.Run("broadcast drops without closing", func(t *testing.T) {
		// Broadcast is explicitly best-effort, so losing one must not take the
		// change stream down with it.
		h := newHub(1)
		s := h.Open("s", authz.Identity{})
		s.Send(event.Event{Kind: event.KindBroadcast})
		if s.Send(event.Event{Kind: event.KindBroadcast}) {
			t.Fatal("overflowing broadcast should be dropped")
		}
		if s.CloseCode() != "" {
			t.Errorf("stream closed on a dropped broadcast: %q", s.CloseCode())
		}
	})

	t.Run("presence is dropped and never reorders the queue", func(t *testing.T) {
		// Making room by taking something off the queue would reorder it: a
		// change put back at the tail would arrive after later changes.
		h := newHub(2)
		s := h.Open("s", authz.Identity{})
		s.Send(event.Event{Kind: event.KindChange, Data: "first"})
		s.Send(event.Event{Kind: event.KindChange, Data: "second"})
		if s.Send(event.Event{Kind: event.KindPresence, Data: "presence"}) {
			t.Fatal("presence should be dropped when the queue is full")
		}
		got := drain(s)
		if len(got) != 2 || got[0].Data != "first" || got[1].Data != "second" {
			t.Errorf("queue = %v, want the changes untouched and in order", got)
		}
		if s.CloseCode() != "" {
			t.Errorf("stream closed on a dropped presence event: %q", s.CloseCode())
		}
	})
}

func TestQueueHoldsNothingUntilUsed(t *testing.T) {
	h := newHub(256)
	s := h.Open("s", authz.Identity{})
	if s.queue != nil {
		t.Fatal("an idle stream must not hold a queue buffer")
	}
	for i := 0; i < 3; i++ {
		s.Send(event.Event{Kind: event.KindChange, Data: i})
	}
	select {
	case <-s.Ready():
	default:
		t.Fatal("queuing an event must signal Ready")
	}
	got := s.Take(nil)
	if len(got) != 3 || got[0].Data != 0 || got[2].Data != 2 {
		t.Fatalf("Take = %v, want the three events in order", got)
	}
	if s.queue != nil {
		t.Fatal("a drained stream given no spare must hold no buffer")
	}
}

// Backfill waits for room instead of closing the stream, and leaves half the
// queue to live changes, which never wait.
func TestBackfillWaitsForRoom(t *testing.T) {
	h := newHub(4)
	s := h.Open("s", authz.Identity{})
	ctx := t.Context()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 10; i++ {
			if !s.SendBackfill(ctx, event.Event{Kind: event.KindChange, Data: i}) {
				t.Errorf("backfill %d refused", i)
				return
			}
		}
	}()

	var got []event.Event
	for len(got) < 10 {
		<-s.Ready()
		batch := s.Take(nil)
		if len(batch) > 2 {
			t.Fatalf("backfill queued %d events, past half of the queue", len(batch))
		}
		got = append(got, batch...)
	}
	<-done
	for i, e := range got {
		if e.Data != i {
			t.Fatalf("event %d = %v, backfill reordered the queue", i, e.Data)
		}
	}

	s.Send(event.Event{Kind: event.KindChange})
	s.Send(event.Event{Kind: event.KindChange})
	if !s.Send(event.Event{Kind: event.KindChange}) || s.CloseCode() != "" {
		t.Fatal("live changes must still fit while backfill holds half the queue")
	}
}

func TestBackfillStopsWithTheStream(t *testing.T) {
	h := newHub(2)
	s := h.Open("s", authz.Identity{})
	if !s.SendBackfill(t.Context(), event.Event{Kind: event.KindChange}) {
		t.Fatal("the first backfill must fit")
	}
	result := make(chan bool)
	go func() { result <- s.SendBackfill(t.Context(), event.Event{Kind: event.KindChange}) }()
	time.Sleep(20 * time.Millisecond)
	s.CloseWith("client_closed")
	select {
	case ok := <-result:
		if ok {
			t.Fatal("backfill into a closed stream reported success")
		}
	case <-time.After(time.Second):
		t.Fatal("backfill kept waiting after the stream closed")
	}
}

// A join that races a disconnect must not put a closed stream back into a
// channel's fan-out set, where it would stay forever.
func TestJoinAfterCloseIsRefused(t *testing.T) {
	h := newHub(4)
	s := h.Open("s", authz.Identity{})
	h.Close("s", "client_closed")
	if h.JoinChannel("room:1", s, "r", time.Time{}) {
		t.Fatal("joining a channel on a closed stream must fail")
	}
	if len(h.ChannelMembers("room:1")) != 0 {
		t.Error("a closed stream was added to a channel")
	}
}

func TestSendAfterCloseIsRejected(t *testing.T) {
	h := newHub(4)
	s := h.Open("s", authz.Identity{})
	s.CloseWith("test")
	if s.Send(event.Event{Kind: event.KindChange}) {
		t.Fatal("a closed stream must not accept events")
	}
	// Closing twice must be safe: several paths can race to close a stream.
	s.CloseWith("again")
	if s.CloseCode() != "test" {
		t.Errorf("close code = %q, want the first one to win", s.CloseCode())
	}
}

func TestChannelFanoutAndSelf(t *testing.T) {
	h := newHub(8)
	a := h.Open("a", authz.Identity{Sub: "ua"})
	b := h.Open("b", authz.Identity{Sub: "ub"})
	h.JoinChannel("room:1", a, "sub-a", time.Time{})
	h.JoinChannel("room:1", b, "sub-b", time.Time{})

	n := h.PublishBroadcast("room:1", "ping", "ua", "client", "", json.RawMessage(`{}`), false, "a")
	if n != 1 {
		t.Errorf("delivered = %d, want 1 (sender excluded)", n)
	}
	if len(drain(a)) != 0 {
		t.Error("sender should not receive its own broadcast when self=false")
	}
	if len(drain(b)) != 1 {
		t.Error("other member should receive it")
	}

	n = h.PublishBroadcast("room:1", "ping", "ua", "client", "", json.RawMessage(`{}`), true, "a")
	if n != 2 {
		t.Errorf("delivered with self=true = %d, want 2", n)
	}

	h.LeaveChannel("room:1", b)
	if n := h.PublishBroadcast("room:1", "ping", "ua", "client", "", nil, true, "a"); n != 1 {
		t.Errorf("after leave, delivered = %d, want 1", n)
	}
}

// A join's expiry is tracked per label, and removing one join by label leaves a
// newer join of the same channel alone.
func TestChannelRecheckIsPerJoin(t *testing.T) {
	h := newHub(8)
	s := h.Open("a", authz.Identity{Sub: "ua"})
	t0 := time.Now()
	h.JoinChannel("chat:1", s, "old", t0)
	if due := s.DueChannels(t0); len(due) != 1 || due[0].Label != "old" {
		t.Fatalf("due = %+v, want the old join", due)
	}
	h.JoinChannel("chat:1", s, "new", t0.Add(time.Minute))
	s.SetChannelRecheck("chat:1", "old", t0)
	if due := s.DueChannels(t0); len(due) != 0 {
		t.Fatalf("a stale label moved the newer join's expiry: due = %+v", due)
	}
	if h.LeaveChannelLabel("chat:1", s, "old") {
		t.Fatal("a stale label removed the newer join")
	}
	if !h.LeaveChannelLabel("chat:1", s, "new") {
		t.Fatal("the current join was not removed")
	}
	if _, ok := s.ChannelLabel("chat:1"); ok {
		t.Fatal("still joined")
	}
}

// A disconnect is the only leave signal Sluice needs, so closing a stream must
// clean up channel membership and presence with no client cooperation.
func TestCloseRemovesChannelAndPresence(t *testing.T) {
	h := newHub(8)
	go h.Presence().Run()
	defer h.Presence().Stop()

	s := h.Open("s", authz.Identity{Sub: "u1"})
	h.JoinChannel("room:1", s, "r", time.Time{})
	h.Presence().Track("room:1", "u1", "s", json.RawMessage(`{"n":1}`), 0)

	if h.Presence().Count("room:1") != 1 {
		t.Fatal("presence should have one member")
	}
	h.Close("s", "client_closed")

	if h.Presence().Count("room:1") != 0 {
		t.Error("closing the stream must remove its presence entries")
	}
	if len(h.ChannelMembers("room:1")) != 0 {
		t.Error("closing the stream must remove it from channels")
	}
	if _, ok := h.Get("s"); ok {
		t.Error("closed stream should not be retrievable")
	}
}

func TestPresenceStateAndDiff(t *testing.T) {
	h := newHub(16)
	go h.Presence().Run()
	defer h.Presence().Stop()

	s := h.Open("s", authz.Identity{Sub: "u1"})
	h.JoinChannel("room:1", s, "r", time.Time{})

	h.Presence().Track("room:1", "u1", "s", json.RawMessage(`{"name":"a"}`), 0)
	state := h.Presence().State("room:1")
	if len(state) != 1 || state["u1"].Ref == "" {
		t.Fatalf("state = %v", state)
	}

	// Diffs are coalesced onto a tick rather than emitted per call, which is what
	// stops track()-per-mousemove from amplifying into N^2 messages.
	deadline := time.After(2 * time.Second)
	var sawDiff bool
	for !sawDiff {
		select {
		case <-s.Ready():
			for _, e := range s.Take(nil) {
				if e.Kind == event.KindPresence {
					p := e.Data.(event.Presence)
					if p.Type == "diff" && len(p.Joins) == 1 {
						sawDiff = true
					}
				}
			}
		case <-deadline:
			t.Fatal("timed out waiting for a presence diff")
		}
	}

	h.Presence().UntrackKey("room:1", "u1", "s")
	if h.Presence().Count("room:1") != 0 {
		t.Error("untrack should remove the member")
	}
	// A stream may not untrack a key it does not own.
	h.Presence().Track("room:1", "u1", "s", nil, 0)
	if h.Presence().UntrackKey("room:1", "u1", "other-stream") {
		t.Error("a stream must not be able to untrack another stream's key")
	}
}

// A key over the limit is refused before it is stored, so the channel never
// broadcasts a join or a leave for a member that was never admitted.
func TestPresenceKeyLimit(t *testing.T) {
	p := NewPresence(newHub(4), time.Hour)
	if _, ok := p.Track("room:1", "u1", "s1", nil, 1); !ok {
		t.Fatal("the first key must fit")
	}
	if n, ok := p.Track("room:1", "u2", "s2", nil, 1); ok || n != 1 {
		t.Fatalf("a second key over the limit: n=%d ok=%v, want refused", n, ok)
	}
	if _, ok := p.Track("room:1", "u1", "s1", json.RawMessage(`{"x":1}`), 1); !ok {
		t.Fatal("updating an existing key must not count against the limit")
	}
	p.mu.Lock()
	_, leaked := p.channels["room:1"].leaves["u2"]
	p.mu.Unlock()
	if leaked {
		t.Error("the refused key produced a leave")
	}
}

// ---------------------------------------------------------------------------
// Ring buffer
// ---------------------------------------------------------------------------

func ringEntry(lsn uint64) RingEntry {
	return RingEntry{
		LSN:      lsn,
		Op:       pgoutput.MsgInsert,
		Relation: &pgoutput.Relation{OID: 1, Namespace: "public", Name: "t"},
	}
}

func TestRingReplay(t *testing.T) {
	r := NewRings(4, time.Minute)
	r.SetStart(5)
	for i := uint64(1); i <= 3; i++ {
		r.Append(ringEntry(i * 10))
	}

	// From a commit LSN is inclusive: a client cut off part-way through that
	// transaction must get the rest of it.
	got, ok := r.Replay(1, 20)
	if !ok {
		t.Fatal("replay from a covered position should succeed")
	}
	if len(got) != 2 || got[0].LSN != 20 || got[1].LSN != 30 {
		t.Errorf("replay = %v, want LSNs 20 and 30", lsns(got))
	}

	// Past the newest entry is nothing, not a gap.
	if got, ok := r.Replay(1, 31); !ok || len(got) != 0 {
		t.Errorf("replay past head = %v ok=%v", lsns(got), ok)
	}
}

// The buffer is bounded, and being honest about that boundary is the whole point:
// a client asking for a position that has been evicted must be told to
// resnapshot rather than silently handed a gap.
func TestRingReportsGapWhenOverwritten(t *testing.T) {
	r := NewRings(3, time.Minute)
	r.SetStart(5)
	for i := uint64(1); i <= 10; i++ {
		r.Append(ringEntry(i * 10))
	}
	for _, from := range []uint64{10, 70} {
		if _, ok := r.Replay(1, from); ok {
			t.Errorf("replay from %d reaches evicted entries and must report a gap", from)
		}
	}
	if got, ok := r.Replay(1, 80); !ok || len(got) != 3 {
		t.Errorf("a covered position should replay: %v ok=%v", lsns(got), ok)
	}
}

// A ring covers only what this process replicated. Before replication starts
// nothing is covered; after, a quiet table with no buffered change is covered
// from the start LSN on.
func TestRingCoverageStartsWithReplication(t *testing.T) {
	r := NewRings(4, time.Minute)
	if _, ok := r.Replay(99, 500); ok {
		t.Fatal("nothing is covered before replication starts")
	}
	r.SetStart(100)
	r.SetStart(900) // a reconnect must not move the start forward
	if _, ok := r.Replay(99, 50); ok {
		t.Error("a position before the start of replication must report a gap")
	}
	if got, ok := r.Replay(99, 500); !ok || len(got) != 0 {
		t.Errorf("a quiet relation after the start must replay nothing, cleanly: %v ok=%v", lsns(got), ok)
	}
}

// Age is a memory bound, not a coverage rule: a table that went quiet keeps
// its newest transaction, so a client that saw it can still resume.
func TestRingSweepKeepsNewestTransaction(t *testing.T) {
	r := NewRings(8, time.Millisecond)
	r.SetStart(5)
	r.Append(ringEntry(10))
	r.Append(ringEntry(20))
	r.Append(ringEntry(20))
	time.Sleep(5 * time.Millisecond)
	r.Sweep()

	if got, ok := r.Replay(1, 20); !ok || len(got) != 2 {
		t.Errorf("the newest transaction must survive the sweep: %v ok=%v", lsns(got), ok)
	}
	if _, ok := r.Replay(1, 10); ok {
		t.Error("a swept transaction must report a gap")
	}
}

func TestConcurrentStreamsAndBroadcast(t *testing.T) {
	h := newHub(64)
	const n = 64
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		s := h.Open(fmt.Sprintf("s%d", i), authz.Identity{Sub: fmt.Sprintf("u%d", i)})
		h.JoinChannel("room:1", s, "r", time.Time{})
		wg.Add(1)
		go func(s *Stream) {
			defer wg.Done()
			for {
				select {
				case <-s.Done():
					return
				case <-s.Ready():
					s.Take(nil)
				case <-time.After(300 * time.Millisecond):
					return
				}
			}
		}(s)
	}

	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				h.PublishBroadcast("room:1", "e", "u", "client", "", json.RawMessage(`{}`), true, "")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i += 2 {
			h.Close(fmt.Sprintf("s%d", i), "test")
		}
	}()

	wg.Wait()
	if h.Count() > n {
		t.Errorf("stream count = %d, want at most %d", h.Count(), n)
	}
}

func lsns(entries []RingEntry) []uint64 {
	out := make([]uint64, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.LSN)
	}
	return out
}
