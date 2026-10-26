package server

import (
	"bufio"
	"encoding/json"
	"slices"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
)

// delaysOf takes a stream's queued events and returns the lag_ms of its delay
// events.
func delaysOf(st *hub.Stream) []int64 {
	var out []int64
	for _, ev := range events(st) {
		if d, ok := ev.Data.(event.Delay); ok {
			out = append(out, d.LagMs)
		}
	}
	return out
}

// Every stream hears when changes start arriving late, at SLUICE_LAG_DEGRADED,
// and when they stop, below half of it; nothing in between.
func TestStreamsHearWhenChangesArriveLate(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameRLS})
	s.cfg.LagDegraded = 5 * time.Second
	a := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	b := s.hub.Open("n1.2", "", authz.Identity{Sub: "u2", Role: "authenticated"})

	for _, lag := range []time.Duration{0, 4 * time.Second, 6 * time.Second, 9 * time.Second, 3 * time.Second, 2 * time.Second, 4 * time.Second} {
		s.checkDelay(lag)
	}
	for _, st := range []*hub.Stream{a, b} {
		if got := delaysOf(st); !slices.Equal(got, []int64{6000, 0}) {
			t.Fatalf("%s heard %v, want [6000 0]", st.StreamID(), got)
		}
	}
}

// A stream opened while changes arrive late reads it in its ready.
func TestReadyCarriesTheDelay(t *testing.T) {
	s, srv, token := streamServer(t)
	s.cfg.LagDegraded = 5 * time.Second
	readyLag := func() int64 {
		t.Helper()
		res := postStream(t, srv, token)
		ev, err := readEvent(bufio.NewReader(res.Body))
		if err != nil || ev.name != "ready" {
			t.Fatalf("first event = %+v, %v; want ready", ev, err)
		}
		var ready struct {
			LagMs int64 `json:"lag_ms"`
		}
		if err := json.Unmarshal([]byte(ev.data), &ready); err != nil {
			t.Fatal(err)
		}
		return ready.LagMs
	}
	if got := readyLag(); got != 0 {
		t.Fatalf("lag_ms %d in the ready of a stream that is not delayed", got)
	}
	s.checkDelay(7 * time.Second)
	if got := readyLag(); got != 7000 {
		t.Fatalf("lag_ms %d in the ready of a delayed stream, want 7000", got)
	}
}
