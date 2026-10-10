package server

import (
	"math"
	"runtime/debug"
	"runtime/metrics"
	"sync"
)

// admission decides whether a new stream may open from the memory it would
// leave in use: the live heap the last collection found, plus what the streams
// opened since are expected to hold, against the Go memory limit
// (GOMEMLIMIT, which Sluice sets from the container's limit). Past the
// threshold it refuses until the projection falls admissionHysteresis below
// it, so admission does not flap with every collection.
//
// The live heap only changes at a collection, and a reconnect storm opens
// thousands of streams between two. Projecting them at the measured live heap
// per stream is what keeps a storm from overshooting.
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

// defaultStreamBytes is the live heap a stream is assumed to hold until enough
// streams are open to measure it. Measured: 44-49 KiB with seven shapes and a
// channel; the assumption errs high.
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

func readLiveHeap() (live, gcs uint64) {
	s := []metrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/gc/cycles/total:gc-cycles"}}
	metrics.Read(s)
	if s[0].Value.Kind() == metrics.KindUint64 {
		live = s[0].Value.Uint64()
	}
	if s[1].Value.Kind() == metrics.KindUint64 {
		gcs = s[1].Value.Uint64()
	}
	return live, gcs
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
