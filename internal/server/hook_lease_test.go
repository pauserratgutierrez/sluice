package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
)

// fakeHook is a channel hook whose answer the test flips: a block between two
// users denies both of them the chat's channel.
type fakeHook struct {
	blocked atomic.Bool
	status  atomic.Int32 // non-zero: answer with this status instead
	calls   atomic.Int32
	ttl     *int // omitted when nil
	srv     *httptest.Server
}

func newFakeHook(t *testing.T, ttl *int) *fakeHook {
	h := &fakeHook{ttl: ttl}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.calls.Add(1)
		if st := h.status.Load(); st != 0 {
			w.WriteHeader(int(st))
			return
		}
		body := map[string]any{"allow": !h.blocked.Load()}
		if h.ttl != nil {
			body["ttl"] = *h.ttl
		}
		_ = json.NewEncoder(w).Encode(body)
	}))
	t.Cleanup(h.srv.Close)
	return h
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

const chatChannel = "chat:1"

var (
	alice = authz.Identity{Role: "authenticated", Sub: "alice", SessionID: "s-alice", ClaimsRaw: `{"sub":"alice"}`}
	bob   = authz.Identity{Role: "authenticated", Sub: "bob", SessionID: "s-bob", ClaimsRaw: `{"sub":"bob"}`}
)

func hookTestServer(t *testing.T, h *fakeHook) (*Server, *fakeClock) {
	t.Helper()
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.cfg.Channels = []config.Channel{{Namespace: "chat", Mode: config.ChannelHook, HookURL: h.srv.URL}}
	clock := &fakeClock{t: time.Now()}
	s.hooks = newHookCache(time.Minute, time.Second, "")
	s.hooks.now = clock.Now
	return s, clock
}

var streamSeq atomic.Int32

func joinChat(t *testing.T, s *Server, id authz.Identity) (*hub.Stream, subResult) {
	t.Helper()
	st := s.hub.Open("n1.hook-"+string(rune('a'+streamSeq.Add(1))), id)
	return st, s.subscribeChannel(st, id, subSpec{Sub: "typing", Channel: chatChannel})
}

// events drains what a stream has been sent so far.
func events(st *hub.Stream) []event.Event {
	var out []event.Event
	for {
		select {
		case ev := <-st.Events():
			out = append(out, ev)
		default:
			return out
		}
	}
}

func receivesTyping(s *Server, from, to *hub.Stream) bool {
	events(to)
	s.hub.PublishBroadcast(chatChannel, "typing", from.Identity().Sub, "client", "", json.RawMessage(`{}`), false, from.StreamID())
	for _, ev := range events(to) {
		if ev.Kind == event.KindBroadcast {
			return true
		}
	}
	return false
}

func revokedWith(st *hub.Stream) string {
	for _, ev := range events(st) {
		if e, ok := ev.Data.(event.Error); ok && ev.Kind == event.KindError && e.Sub == "typing" {
			return e.Code
		}
	}
	return ""
}

// The report, step by step, with the endpoint's default TTL (60s): a verdict
// answers for its TTL, and when it expires a stream that joined is asked about
// again and removed if it is now denied.
func TestHookBlockTimeline(t *testing.T) {
	h := newFakeHook(t, nil)
	s, clock := hookTestServer(t, h)
	ctx := context.Background()

	// Before the block: both may join and typing flows.
	a1, res := joinChat(t, s, alice)
	if !res.OK {
		t.Fatalf("join before block refused: %+v", res.Error)
	}
	b1, res := joinChat(t, s, bob)
	if !res.OK {
		t.Fatalf("join before block refused: %+v", res.Error)
	}
	if !receivesTyping(s, a1, b1) {
		t.Fatal("typing does not flow before the block")
	}

	// Right after the block: the verdicts are still valid, so a new join by
	// the same session is answered from the cache and typing still flows.
	h.blocked.Store(true)
	a2, res := joinChat(t, s, alice)
	if !res.OK {
		t.Fatal("a join within the verdict's TTL was not answered from the cache")
	}
	s.recheckHookChannels(ctx, clock.Now())
	if !receivesTyping(s, a1, b1) {
		t.Fatal("a joined stream was removed before its verdict expired")
	}

	// Block + 65s: every expired join is asked about again and removed, once
	// per session however many of its streams joined.
	clock.Advance(65 * time.Second)
	before := h.calls.Load()
	s.recheckHookChannels(ctx, clock.Now())
	if got := h.calls.Load() - before; got != 2 {
		t.Errorf("re-check made %d hook calls, want 2 (one per session: alice's two streams share a verdict)", got)
	}
	for name, st := range map[string]*hub.Stream{"a1": a1, "a2": a2, "b1": b1} {
		if code := revokedWith(st); code != "channel_not_authorized" {
			t.Errorf("%s: revoked with %q, want channel_not_authorized", name, code)
		}
		if _, joined := st.ChannelLabel(chatChannel); joined {
			t.Errorf("%s is still joined after the block", name)
		}
	}
	if receivesTyping(s, a1, b1) {
		t.Fatal("a stream that joined before the block still receives typing")
	}
	if _, res := joinChat(t, s, alice); res.OK || res.Error.Code != "channel_not_authorized" {
		t.Fatalf("a new join after the block: %+v", res)
	}

	// Right after the unblock: the denial is still valid for its TTL.
	h.blocked.Store(false)
	if _, res := joinChat(t, s, alice); res.OK {
		t.Fatal("a denial was not cached for its TTL")
	}

	// Unblock + 65s: the join is allowed again.
	clock.Advance(65 * time.Second)
	if _, res := joinChat(t, s, alice); !res.OK {
		t.Fatalf("a join after the denial expired was refused: %+v", res.Error)
	}
}

// ttl: 0 makes the endpoint authoritative at every join and on every tick.
func TestHookTTLZeroIsNotCached(t *testing.T) {
	zero := 0
	h := newFakeHook(t, &zero)
	s, clock := hookTestServer(t, h)
	ctx := context.Background()

	a1, _ := joinChat(t, s, alice)
	b1, res := joinChat(t, s, bob)
	if !res.OK {
		t.Fatalf("join refused: %+v", res.Error)
	}

	h.blocked.Store(true)
	if _, res := joinChat(t, s, alice); res.OK {
		t.Fatal("a join right after the block was answered from a cache")
	}
	s.recheckHookChannels(ctx, clock.Now())
	if code := revokedWith(b1); code != "channel_not_authorized" {
		t.Fatalf("joined stream revoked with %q on the next tick, want channel_not_authorized", code)
	}
	if receivesTyping(s, a1, b1) {
		t.Fatal("a revoked stream still receives typing")
	}

	h.blocked.Store(false)
	if _, res := joinChat(t, s, alice); !res.OK {
		t.Fatal("a join right after the unblock was answered from a cached denial")
	}
}

// A re-check that gets no verdict keeps the join and asks again on the next
// tick; only an explicit denial removes it.
func TestHookRecheckWithoutVerdictKeepsTheJoin(t *testing.T) {
	zero := 0
	h := newFakeHook(t, &zero)
	s, clock := hookTestServer(t, h)
	ctx := context.Background()

	a1, _ := joinChat(t, s, alice)
	b1, _ := joinChat(t, s, bob)

	h.status.Store(http.StatusServiceUnavailable)
	s.recheckHookChannels(ctx, clock.Now())
	if _, joined := b1.ChannelLabel(chatChannel); !joined || revokedWith(b1) != "" {
		t.Fatal("an endpoint outage removed a joined stream")
	}
	if _, res := joinChat(t, s, alice); res.OK {
		t.Fatal("a join during an endpoint outage was admitted")
	}

	h.status.Store(0)
	h.blocked.Store(true)
	clock.Advance(hookRetryTTL + time.Second)
	s.recheckHookChannels(ctx, clock.Now())
	if code := revokedWith(b1); code != "channel_not_authorized" {
		t.Fatalf("after the outage, revoked with %q, want channel_not_authorized", code)
	}
	if receivesTyping(s, a1, b1) {
		t.Fatal("a revoked stream still receives typing")
	}
}

// The periodic tick runs the re-checks.
func TestRefreshLeasesRechecksHookChannels(t *testing.T) {
	zero := 0
	h := newFakeHook(t, &zero)
	s, _ := hookTestServer(t, h)

	b1, _ := joinChat(t, s, bob)
	h.blocked.Store(true)
	s.RefreshLeases(context.Background())

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, joined := b1.ChannelLabel(chatChannel); !joined {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("the tick did not remove a denied join")
}

// /token asks again, bypassing verdicts that are still valid, because the
// claims behind them may have changed.
func TestTokenRechecksHookChannels(t *testing.T) {
	h := newFakeHook(t, nil)
	s, _ := hookTestServer(t, h)

	b1, _ := joinChat(t, s, bob)
	h.blocked.Store(true)
	if n := s.recheckStreamHooks(context.Background(), b1, bob); n != 1 {
		t.Fatalf("revoked %d channels, want 1", n)
	}
	if code := revokedWith(b1); code != "channel_not_authorized" {
		t.Fatalf("revoked with %q, want channel_not_authorized", code)
	}
}
