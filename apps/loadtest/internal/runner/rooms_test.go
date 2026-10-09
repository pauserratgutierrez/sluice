package runner

import (
	"math"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/issuer"
)

func TestHistQuantiles(t *testing.T) {
	var h Hist
	for i := 1; i <= 1000; i++ {
		h.Record(time.Duration(i) * time.Millisecond)
	}
	for _, c := range []struct {
		q    float64
		want time.Duration
	}{{0.5, 500 * time.Millisecond}, {0.95, 950 * time.Millisecond}, {0.99, 990 * time.Millisecond}} {
		got := h.Quantile(c.q)
		if math.Abs(float64(got-c.want))/float64(c.want) > 0.02 {
			t.Errorf("q%.2f = %v, want %v ±2%%", c.q, got, c.want)
		}
	}
	if s := h.Summary(); s.Count != 1000 || s.Max != 1000 {
		t.Errorf("summary = %+v", s)
	}
}

func TestHistIndexIsMonotonic(t *testing.T) {
	prev := -1
	for us := uint64(0); us < 1<<22; us += 1 + us/97 {
		i := histIndex(us)
		if i < prev {
			t.Fatalf("index(%d) = %d < %d", us, i, prev)
		}
		if v := histValue(i); us >= 64 && math.Abs(float64(v)-float64(us))/float64(us) > 0.016 {
			t.Fatalf("value(index(%d)) = %d, more than 1.6%% off", us, v)
		}
		prev = i
	}
}

func TestParseRoomClasses(t *testing.T) {
	got, err := parseRoomClasses("40:80:20:0.5, 3:250:60")
	if err != nil {
		t.Fatal(err)
	}
	want := []roomClass{{40, 80, 20, 0.5}, {3, 250, 60, 0}}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v", got)
	}
	if _, err := parseRoomClasses("1:2"); err == nil {
		t.Fatal("want an error for a short entry")
	}
}

func TestExpectedRates(t *testing.T) {
	rc := roomsConfig{
		Classes:     []roomClass{{Count: 5, Members: 80, Streams: 20, Typing: 0.5}},
		Shapes:      7,
		MsgEvery:    15 * time.Second,
		TypingEvery: 2 * time.Second, TypingEnabled: true,
	}
	ch, msg, mem, typ := rc.expectedRates()
	near := func(a, b float64) bool { return math.Abs(a-b) < 1e-9 }
	if !near(ch, 5*81/15.0) || !near(msg, 5*20/15.0) || !near(mem, 5*20/15.0) || !near(typ, 2*19/2.0) {
		t.Fatalf("rates = %v %v %v %v", ch, msg, mem, typ)
	}
}

func TestParseLSN(t *testing.T) {
	n, err := parseLSN("16/B374D848")
	if err != nil || n != 0x16B374D848 {
		t.Fatalf("got %x, %v", n, err)
	}
	if _, err := parseLSN("nope"); err == nil {
		t.Fatal("want an error")
	}
}

func TestIssuerPeak(t *testing.T) {
	secs := []issuer.SecondCounts{
		{At: 10, N: map[string]int64{"subscribe": 7, "hook": 1}},
		{At: 11, N: map[string]int64{"subscribe": 70, "hook": 10, "refresh": 3}},
		{At: 30, N: map[string]int64{"subscribe": 500}},
	}
	peak, by, total := issuerPeak(secs, 10, 20)
	if peak != 83 || total != 91 || by["subscribe"] != 70 || by["hook"] != 10 {
		t.Fatalf("peak %d by %v total %d", peak, by, total)
	}
}

func TestRoomSubs(t *testing.T) {
	subs := roomSubs(7, "u", 7)
	if len(subs) != 8 {
		t.Fatalf("%d subscriptions, want 7 shapes and a channel", len(subs))
	}
	if subs[0].Label != "msgs" || subs[1].Label != "me" || subs[2].Label != "nt" || subs[7].Label != "chat" {
		t.Fatalf("labels %v %v %v %v", subs[0].Label, subs[1].Label, subs[2].Label, subs[7].Label)
	}
	if subs[7].Table != "" || subs[0].Table != "public.room_messages" {
		t.Fatalf("tables %q %q", subs[0].Table, subs[7].Table)
	}
	if got := len(roomSubs(7, "u", 1)); got != 2 {
		t.Fatalf("one shape: %d subscriptions", got)
	}
}

func TestUUIDFromIsDeterministic(t *testing.T) {
	a := uuidFrom(rand.New(rand.NewPCG(1, 2)))
	b := uuidFrom(rand.New(rand.NewPCG(1, 2)))
	if a != b || len(a) != 36 || a[14] != '4' {
		t.Fatalf("%s %s", a, b)
	}
}

func TestLSNTrail(t *testing.T) {
	var tr lsnTrail
	t0 := time.Now().Add(-10 * time.Second)
	tr.note("t", "0/10", t0)
	tr.note("t", "0/20", t0.Add(2*time.Second))
	tr.note("t", "0/30", t0.Add(4*time.Second))
	if _, ok := tr.at("t", t0.Add(-time.Second)); ok {
		t.Fatal("a time before the trail must not resolve")
	}
	if p, ok := tr.at("t", t0.Add(3*time.Second)); !ok || p.lsn != "0/20" {
		t.Fatalf("at +3s = %+v, %v; want 0/20", p, ok)
	}
	if p, ok := tr.at("t", t0.Add(4*time.Second)); !ok || p.lsn != "0/30" {
		t.Fatalf("at +4s = %+v, %v; want 0/30", p, ok)
	}
}

func TestTableChanges(t *testing.T) {
	m := parseProm(`sluice_changes_total{op="UPDATE",schema="public",table="room_members"} 100
sluice_changes_total{op="INSERT",schema="public",table="room_members"} 5
sluice_changes_total{op="INSERT",schema="public",table="room_messages"} 7
`)
	got := tableChanges(m)
	if got["public.room_members"] != 105 || got["public.room_messages"] != 7 {
		t.Fatalf("got %v", got)
	}
}

func TestSummarizeProbes(t *testing.T) {
	rounds := []ProbeRound{
		{Phase: "steady", Tables: map[string]*ProbeTable{"a": {CoverS: 2}}, Derived: map[string]DerivedCoverage{"a": {ChangesPerSec: 2000, CoverS: 2.05}}},
		{Phase: "steady", Tables: map[string]*ProbeTable{"a": {CoverS: 3}}, Derived: map[string]DerivedCoverage{"a": {ChangesPerSec: 1000, CoverS: 4.1}}},
		{Phase: "steady", Tables: map[string]*ProbeTable{"a": {CoverS: 5}}},
	}
	s := summarizeProbes(rounds, "")
	if s.Rounds != 3 || s.CoverMedianS["a"] != 3 || s.CoverMinS["a"] != 2 || s.RateMedian["a"] != 1500 {
		t.Fatalf("%+v", s)
	}
}

func TestTypistsUpTo(t *testing.T) {
	// 0.3 of 50 rooms is 15 typists, despite 0.3*50 being 15.000000000000002
	// and 0.24*380 being 91.19999999999999.
	if n := typistsUpTo(0.3, 50); n != 15 {
		t.Fatalf("0.3 × 50 = %d", n)
	}
	if n := typistsUpTo(0.24, 380); n != 91 {
		t.Fatalf("0.24 × 380 = %d", n)
	}
	total := 0
	for j := range 14 {
		total += typistsUpTo(0.3, j+1) - typistsUpTo(0.3, j)
	}
	if total != 4 {
		t.Fatalf("spread over 14 rooms: %d typists, want 4", total)
	}
}

func TestLagSeconds(t *testing.T) {
	pts := []LSNPoint{{T: 0, Current: 100}, {T: 1, Current: 200}, {T: 2, Current: 300}}
	if s := lagSeconds(pts, 2, 150); s != 1.5 {
		t.Fatalf("lag of 150 at t=2: %v, want 1.5", s)
	}
	if s := lagSeconds(pts, 2, 300); s != 0 {
		t.Fatalf("caught up: %v", s)
	}
	if s := lagSeconds(pts, 2, 50); s != -1 {
		t.Fatalf("before the readings: %v", s)
	}
}
