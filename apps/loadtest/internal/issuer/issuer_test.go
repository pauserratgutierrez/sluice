package issuer

import (
	"testing"
	"time"
)

func TestParseLatency(t *testing.T) {
	for _, c := range []struct {
		in       string
		min, max time.Duration
	}{
		{"", 0, 0},
		{"0", 0, 0},
		{"10ms", 8 * time.Millisecond, 12 * time.Millisecond},
		{"5ms-20ms", 5 * time.Millisecond, 20 * time.Millisecond},
	} {
		l, err := ParseLatency(c.in, 0.2)
		if err != nil || l.Min != c.min || l.Max != c.max {
			t.Errorf("%q: %+v, %v", c.in, l, err)
		}
	}
	if _, err := ParseLatency("20ms-5ms", 0.2); err == nil {
		t.Error("an inverted range must fail")
	}
}

func TestDecideRooms(t *testing.T) {
	s := newServer("b", Latency{}, 0, 0, 0)
	s.rooms[3] = map[string]bool{"u1": true}
	s.userRm["u1"] = 3

	req := func(table, filter, sub string) issuerRequest {
		var r issuerRequest
		r.Identity.Sub = sub
		r.Requested.Table = table
		r.Requested.Filter = filter
		return r
	}
	allow := func(m map[string]any) bool { return m["allow"] == true }

	if g := s.decide(req("room_messages", "room_id=eq.3", "u1")); !allow(g) {
		t.Fatalf("member denied: %v", g)
	} else if h := g["holds"].([]map[string]any)[0]; h["filter"] != "user_id=eq.u1,room_id=eq.3" {
		t.Fatalf("hold %v", h)
	}
	s.roomFirst = true
	if g := s.decide(req("room_messages", "room_id=eq.3", "u1")); g["holds"].([]map[string]any)[0]["filter"] != "room_id=eq.3,user_id=eq.u1" {
		t.Fatalf("room-first hold %v", g["holds"])
	}
	s.roomFirst = false
	if allow(s.decide(req("room_messages", "room_id=eq.4", "u1"))) {
		t.Fatal("non-member allowed")
	}
	if !allow(s.decide(req("room_members", "user_id=eq.u1", "u1"))) {
		t.Fatal("own memberships denied")
	}
	if allow(s.decide(req("room_members", "user_id=eq.u2", "u1"))) {
		t.Fatal("someone else's memberships allowed")
	}
	if !allow(s.decide(req("notifications", "user_id=eq.u1", "U1"))) {
		t.Fatal("own notifications denied (subjects compare lowercased)")
	}
}

func TestStatsSeconds(t *testing.T) {
	st := newStats()
	st.count(kindSubscribe)
	st.count(kindHook)
	snap := st.snapshot(time.Now().Unix() - 5)
	if snap.Totals["subscribe"] != 1 || snap.Totals["hook"] != 1 || len(snap.Seconds) != 1 {
		t.Fatalf("snapshot %+v", snap)
	}
}
