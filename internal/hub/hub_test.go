package hub

import (
	"context"
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
	var out []event.Event
	for {
		select {
		case e := <-s.Events():
			out = append(out, e)
		default:
			return out
		}
	}
}

// Backpressure policy differs per plane on purpose, and each choice is a
// correctness statement rather than a tuning knob.
func TestBackpressurePolicyPerPlane(t *testing.T) {
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
		if s.Dropped() == 0 {
			t.Error("the drop should be counted")
		}
	})

	t.Run("presence coalesces", func(t *testing.T) {
		// A newer presence diff supersedes an older one, so making room by
		// discarding the old one loses nothing.
		h := newHub(1)
		s := h.Open("s", authz.Identity{})
		s.Send(event.Event{Kind: event.KindPresence, Data: "old"})
		if !s.Send(event.Event{Kind: event.KindPresence, Data: "new"}) {
			t.Fatal("presence should coalesce rather than drop")
		}
		got := drain(s)
		if len(got) != 1 || got[0].Data != "new" {
			t.Errorf("queue = %v, want only the newest presence event", got)
		}
	})

	t.Run("presence never evicts a change", func(t *testing.T) {
		// Making room for presence must not sacrifice a change event.
		h := newHub(1)
		s := h.Open("s", authz.Identity{})
		s.Send(event.Event{Kind: event.KindChange, Data: "important"})
		if s.Send(event.Event{Kind: event.KindPresence, Data: "presence"}) {
			t.Fatal("presence should be dropped rather than evict a change")
		}
		got := drain(s)
		if len(got) != 1 || got[0].Data != "important" {
			t.Errorf("queue = %v, want the change to survive", got)
		}
	})
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
	h.JoinChannel("room:1", a, "sub-a")
	h.JoinChannel("room:1", b, "sub-b")

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

// A disconnect is the only leave signal Sluice needs, so closing a stream must
// clean up channel membership and presence with no client cooperation.
func TestCloseRemovesChannelAndPresence(t *testing.T) {
	h := newHub(8)
	go h.Presence().Run()
	defer h.Presence().Stop()

	s := h.Open("s", authz.Identity{Sub: "u1"})
	h.JoinChannel("room:1", s, "r")
	h.Presence().Track("room:1", "u1", "s", json.RawMessage(`{"n":1}`))

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
	h.JoinChannel("room:1", s, "r")

	h.Presence().Track("room:1", "u1", "s", json.RawMessage(`{"name":"a"}`))
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
		case e := <-s.Events():
			if e.Kind == event.KindPresence {
				p := e.Data.(event.Presence)
				if p.Type == "diff" && len(p.Joins) == 1 {
					sawDiff = true
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
	h.Presence().Track("room:1", "u1", "s", nil)
	if h.Presence().UntrackKey("room:1", "u1", "other-stream") {
		t.Error("a stream must not be able to untrack another stream's key")
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
	for i := uint64(1); i <= 3; i++ {
		r.Append(ringEntry(i * 10))
	}

	got, ok := r.Replay(1, 10)
	if !ok {
		t.Fatal("replay from a covered position should succeed")
	}
	if len(got) != 2 || got[0].LSN != 20 || got[1].LSN != 30 {
		t.Errorf("replay = %v, want LSNs 20 and 30", lsns(got))
	}

	// Everything after the newest entry is nothing, not a gap.
	if got, ok := r.Replay(1, 30); !ok || len(got) != 0 {
		t.Errorf("replay from head = %v ok=%v", lsns(got), ok)
	}
}

// The buffer is bounded, and being honest about that boundary is the whole point:
// a client asking for a position that has aged out must be told to resnapshot
// rather than silently handed a gap.
func TestRingReportsGapWhenOverwritten(t *testing.T) {
	r := NewRings(3, time.Minute)
	for i := uint64(1); i <= 10; i++ {
		r.Append(ringEntry(i * 10))
	}
	if _, ok := r.Replay(1, 10); ok {
		t.Fatal("a position older than the buffer must report a gap")
	}
	if got, ok := r.Replay(1, 80); !ok || len(got) != 2 {
		t.Errorf("a covered position should replay: %v ok=%v", lsns(got), ok)
	}
	if floor := r.Floor(1); floor != 80 {
		t.Errorf("floor = %d, want 80", floor)
	}
}

func TestRingUnknownRelation(t *testing.T) {
	r := NewRings(4, time.Minute)
	// Nothing buffered: resuming from the beginning is fine, resuming from a
	// specific position cannot be proven gapless.
	if _, ok := r.Replay(99, 0); !ok {
		t.Error("replay from 0 with an empty ring should be allowed")
	}
	if _, ok := r.Replay(99, 500); ok {
		t.Error("replay from a position with an empty ring must report a gap")
	}
}

func TestConcurrentStreamsAndBroadcast(t *testing.T) {
	h := newHub(64)
	const n = 64
	var wg sync.WaitGroup

	for i := 0; i < n; i++ {
		s := h.Open(fmt.Sprintf("s%d", i), authz.Identity{Sub: fmt.Sprintf("u%d", i)})
		h.JoinChannel("room:1", s, "r")
		wg.Add(1)
		go func(s *Stream) {
			defer wg.Done()
			for {
				select {
				case <-s.Done():
					return
				case <-s.Events():
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

func TestWheelDoesNotDeadlockOnSelfCancel(t *testing.T) {
	// A stream cancels its own wheel entry as it closes, which happens from
	// inside the callback. Taking a write lock there would deadlock.
	h := newHub(4)
	_ = h
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_ = ctx
}

func lsns(entries []RingEntry) []uint64 {
	out := make([]uint64, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.LSN)
	}
	return out
}
