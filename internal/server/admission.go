package server

import (
	"math"
	"net/http"
	"runtime"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/metrics"
)

// admission decides whether a new stream may open from the memory it would
// leave in use: the memory the last collection could not give back (the live
// heap and the runtime's memory outside it, goroutine stacks above all), plus
// what the streams opened since are expected to hold, against the Go memory limit
// (GOMEMLIMIT, which Sluice sets from the container's limit). Past the
// threshold it refuses.
//
// That floor only changes at a collection, and a reconnect storm opens
// thousands of streams between two. Projecting them at the measured floor per
// stream is what keeps a storm from overshooting.
//
// A reconnect also lifts the floor it is measured by: streams still being
// set up, and the garbage of setting them up, counted live because it was
// allocated during the collection. Up to the most streams admitted below the
// threshold in the last fitWindow, the projection may therefore go a share of
// the way from the threshold to the limit (reconnectHeadroom), so the streams
// that fit come back; past that ceiling a flood is still refused.
type admission struct {
	threshold float64      // share of the memory limit; 0 turns admission off
	limit     func() int64 // the Go memory limit, math.MaxInt64 when none
	read      func() (live, gcs uint64)

	mu          sync.Mutex
	lastGC      uint64
	liveAtGC    uint64
	streamsAtGC int
	fit         int       // the most streams admitted below the threshold
	fitAt       time.Time // the last time that many were open; fit lapses fitWindow after

	now      func() time.Time
	collect  func()
	forcedGC time.Time
}

// reconnectHeadroom is the share of the way from the threshold to the limit
// that the projection may go while fewer streams are open than fit: 0.6 of
// the limit at the default threshold of 0.5. Measured at that threshold, a
// reconnect of every stream read 0.51-0.56 at 2,700-4,900 of the 5,770 that
// had fit, and the refill stopped short at 96%. A stream admitted below the
// threshold counts toward fit when admitted: the node fills to the threshold,
// so the collections after the last streams of a fill read at or past it.
const reconnectHeadroom = 0.2

// fitWindow is how long the count that fit below the threshold is remembered
// after the node last held that many streams. It is seen only when a stream
// opens or is refused, so a node no client opened a stream on for that long
// before a reconnect refills with the threshold alone.
const fitWindow = 5 * time.Minute

// minFloorShare is the least of the measured floor a projection keeps after
// streams close: some of it is the process, not the streams.
const minFloorShare = 0.2

// staleGCInterval is how often admission may force a collection when its
// floor is stale.
const staleGCInterval = 5 * time.Second

// defaultStreamBytes is the memory a stream is assumed to hold until enough
// streams are open to measure it. Measured: 44-49 KiB of live heap with seven
// shapes and a channel, plus two goroutine stacks; the assumption errs high.
const (
	defaultStreamBytes  = 128 << 10
	minStreamsToMeasure = 100
)

func newAdmission(threshold float64) *admission {
	return &admission{
		threshold: threshold,
		limit:     func() int64 { return debug.SetMemoryLimit(-1) },
		read:      readLiveHeap,
		now:       time.Now,
		collect:   runtime.GC,
	}
}

// readLiveHeap returns the memory a collection cannot give back, as of the last
// one: the live heap, the stack space goroutines are using (two per stream),
// and the runtime's own structures. GOMEMLIMIT bounds all of it; the live heap
// alone left out the stacks of thousands of streams.
//
// It leaves out what the runtime holds but can reuse: free and unused heap
// (fragmentation, filled again by later allocations) and stack space reserved
// but idle. Counting those made the floor jump by a fifth from one sample to
// the next and, after a mass disconnect, kept refusing streams that fit.
func readLiveHeap() (floor, gcs uint64) {
	s := []rtmetrics.Sample{
		{Name: "/gc/heap/live:bytes"},
		{Name: "/gc/scan/stack:bytes"},
		{Name: "/memory/classes/metadata/mcache/inuse:bytes"},
		{Name: "/memory/classes/metadata/mspan/inuse:bytes"},
		{Name: "/memory/classes/metadata/other:bytes"},
		{Name: "/memory/classes/os-stacks:bytes"},
		{Name: "/memory/classes/other:bytes"},
		{Name: "/memory/classes/profiling/buckets:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	rtmetrics.Read(s)
	for i := range s[:len(s)-1] {
		if s[i].Value.Kind() == rtmetrics.KindUint64 {
			floor += s[i].Value.Uint64()
		}
	}
	if last := s[len(s)-1]; last.Value.Kind() == rtmetrics.KindUint64 {
		gcs = last.Value.Uint64()
	}
	return floor, gcs
}

// enabled reports whether there is a limit to measure against.
func (a *admission) enabled() bool {
	if a == nil || a.threshold <= 0 {
		return false
	}
	l := a.limit()
	return l > 0 && l != math.MaxInt64
}

// admit reports whether one more stream may open, with open streams in the hub
// and pending opens admitted but not yet in it, and the projected share of the
// memory limit in use.
//
// The projection moves from the floor the last collection found by the
// streams opened or closed since, at the floor per stream then: opens add to
// it, and so do the opens in progress, so that a storm's simultaneous opens do
// not all see the same count; closes take from it, so that after a mass
// disconnect the floor that held the departed streams does not refuse their
// reconnections until the next collection. It never goes below
// minFloorShare of the measured floor, since part of it is not the streams'.
func (a *admission) admit(open, pending int) (bool, float64) {
	if !a.enabled() {
		return true, 0
	}
	limit := a.limit()
	live, gcs := a.read()

	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	if gcs != a.lastGC {
		a.lastGC, a.liveAtGC, a.streamsAtGC = gcs, live, open
	}
	perStream := float64(defaultStreamBytes)
	if a.streamsAtGC >= minStreamsToMeasure {
		perStream = float64(a.liveAtGC) / float64(a.streamsAtGC)
	}
	streams := open + pending
	projected := float64(a.liveAtGC) + float64(streams-a.streamsAtGC)*perStream
	projected = max(projected, minFloorShare*float64(a.liveAtGC))
	share := projected / float64(limit)
	// fit is remembered for fitWindow after the node last held that many.
	if streams >= a.fit {
		a.fitAt = now
		if share < a.threshold {
			a.fit = streams + 1
		}
	} else if now.Sub(a.fitAt) > fitWindow {
		a.fit = 0
	}
	ceiling := a.threshold
	if streams < a.fit {
		ceiling += reconnectHeadroom * (1 - a.threshold)
	}
	refuse := share >= ceiling
	// Refusing with far fewer streams than the last collection saw: the floor
	// is stale. Ask for a collection, at most one every staleGCInterval.
	if refuse && streams < a.streamsAtGC*9/10 {
		if now.Sub(a.forcedGC) >= staleGCInterval {
			a.forcedGC = now
			go a.collect()
		}
	}
	return !refuse, share
}

// withIndexGauges sets the gauges of the hold and subscription indexes and
// of memory just before each scrape: counting them walks the indexes, which
// is cheap once per scrape but not per change.
func (s *Server) withIndexGauges(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := s.holds.Size()
		for kind, v := range map[string]int{"watches": h.Watches, "constants": h.Constants, "routed": h.Routed,
			"routed_cap": h.RoutedCap, "unindexed": h.Unindexed, "counted": h.Counted} {
			metrics.IndexSize.WithLabelValues("holds", kind).Set(float64(v))
		}
		g := s.reg.IndexSize()
		for kind, v := range map[string]int{"subscriptions": g.Subscriptions, "constants": g.Constants, "routed": g.Routed,
			"routed_cap": g.RoutedCap, "unindexed": g.Unindexed} {
			metrics.IndexSize.WithLabelValues("subscriptions", kind).Set(float64(v))
		}
		floor, _ := readLiveHeap()
		metrics.MemoryFloorBytes.Set(float64(floor))
		s := []rtmetrics.Sample{{Name: "/gc/heap/live:bytes"}}
		rtmetrics.Read(s)
		if s[0].Value.Kind() == rtmetrics.KindUint64 {
			metrics.HeapLiveBytes.Set(float64(s[0].Value.Uint64()))
		}
		next.ServeHTTP(w, r)
	})
}
