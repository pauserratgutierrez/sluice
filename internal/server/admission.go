package server

import (
	"math"
	"net/http"
	"runtime/debug"
	rtmetrics "runtime/metrics"
	"sync"

	"github.com/pauserratgutierrez/sluice/internal/metrics"
)

// admission decides whether a new stream may open from the memory it would
// leave in use: the memory the last collection could not give back (the live
// heap and the runtime's memory outside it, goroutine stacks above all), plus
// what the streams opened since are expected to hold, against the Go memory limit
// (GOMEMLIMIT, which Sluice sets from the container's limit). Past the
// threshold it refuses until the projection falls admissionHysteresis below
// it, so admission does not flap with every collection.
//
// That floor only changes at a collection, and a reconnect storm opens
// thousands of streams between two. Projecting them at the measured floor per
// stream is what keeps a storm from overshooting.
type admission struct {
	threshold float64      // share of the memory limit; 0 turns admission off
	limit     func() int64 // the Go memory limit, math.MaxInt64 when none
	read      func() (live, gcs uint64)

	mu          sync.Mutex
	lastGC      uint64
	liveAtGC    uint64
	streamsAtGC int
	refusing    bool
}

// admissionHysteresis is how far below the threshold the projection must fall
// before streams are admitted again.
const admissionHysteresis = 0.05

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
	}
}

// readLiveHeap returns the memory a collection cannot give back, as of the last
// one: the live heap plus everything the runtime holds outside the heap
// (goroutine stacks -- two per stream -- buffers, metadata). GOMEMLIMIT bounds
// all of it, so this is the floor the limit is measured against; the live heap
// alone left out the stacks of thousands of streams.
func readLiveHeap() (floor, gcs uint64) {
	s := []rtmetrics.Sample{
		{Name: "/gc/heap/live:bytes"},
		{Name: "/memory/classes/total:bytes"},
		{Name: "/memory/classes/heap/released:bytes"},
		{Name: "/memory/classes/heap/free:bytes"},
		{Name: "/memory/classes/heap/objects:bytes"},
		{Name: "/gc/cycles/total:gc-cycles"},
	}
	rtmetrics.Read(s)
	v := make([]uint64, len(s))
	for i := range s {
		if s[i].Value.Kind() == rtmetrics.KindUint64 {
			v[i] = s[i].Value.Uint64()
		}
	}
	live, total, released, free, objects := v[0], v[1], v[2], v[3], v[4]
	// total less what is free or returned to the OS, less the garbage among
	// the heap's objects (objects minus live).
	reclaimable := released + free + objects - min(live, objects)
	if total > reclaimable {
		floor = total - reclaimable
	}
	return floor, v[5]
}

// enabled reports whether there is a limit to measure against.
func (a *admission) enabled() bool {
	if a == nil || a.threshold <= 0 {
		return false
	}
	l := a.limit()
	return l > 0 && l != math.MaxInt64
}

// admit reports whether one more stream may open with streams open now, and
// the projected share of the memory limit in use.
func (a *admission) admit(streams int) (bool, float64) {
	if !a.enabled() {
		return true, 0
	}
	limit := a.limit()
	live, gcs := a.read()

	a.mu.Lock()
	defer a.mu.Unlock()
	if gcs != a.lastGC {
		a.lastGC, a.liveAtGC, a.streamsAtGC = gcs, live, streams
	}
	perStream := float64(defaultStreamBytes)
	if a.streamsAtGC >= minStreamsToMeasure {
		perStream = float64(a.liveAtGC) / float64(a.streamsAtGC)
	}
	projected := float64(a.liveAtGC)
	if opened := streams - a.streamsAtGC; opened > 0 {
		projected += float64(opened) * perStream
	}
	share := projected / float64(limit)
	switch {
	case a.refusing && share <= a.threshold-admissionHysteresis:
		a.refusing = false
	case !a.refusing && share >= a.threshold:
		a.refusing = true
	}
	return !a.refusing, share
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
