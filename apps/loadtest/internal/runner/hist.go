package runner

import (
	"math/bits"
	"sync/atomic"
	"time"
)

// Hist is a log-linear latency histogram in microseconds: exact below 64 µs,
// then 64 buckets per power of two (about 1.5% resolution) up to ~35 min. It
// is lock-free, so every reader goroutine records into it directly, and it
// keeps no samples, so an hour-long run costs the same as a minute.
type Hist struct {
	counts [histBuckets]atomic.Uint64
	n      atomic.Uint64
	sumUs  atomic.Uint64
	maxUs  atomic.Uint64
}

const (
	histSub     = 64
	histBuckets = 27 * histSub
)

func histIndex(us uint64) int {
	if us < histSub {
		return int(us)
	}
	exp := bits.Len64(us) - 7 // us>>exp is in [64, 128)
	i := (exp+1)*histSub + int(us>>exp) - histSub
	if i >= histBuckets {
		return histBuckets - 1
	}
	return i
}

// histValue is the midpoint of bucket i, in microseconds.
func histValue(i int) uint64 {
	if i < histSub {
		return uint64(i)
	}
	exp := i/histSub - 1
	lo := uint64(i%histSub+histSub) << exp
	return lo + (uint64(1)<<exp)/2
}

func (h *Hist) Record(d time.Duration) {
	if d < 0 {
		d = 0
	}
	us := uint64(d / time.Microsecond)
	h.counts[histIndex(us)].Add(1)
	h.n.Add(1)
	h.sumUs.Add(us)
	for {
		m := h.maxUs.Load()
		if us <= m || h.maxUs.CompareAndSwap(m, us) {
			return
		}
	}
}

func (h *Hist) Count() uint64 { return h.n.Load() }

// Quantile returns the q-quantile (0..1).
func (h *Hist) Quantile(q float64) time.Duration {
	n := h.n.Load()
	if n == 0 {
		return 0
	}
	rank := uint64(q*float64(n-1)) + 1
	var seen uint64
	for i := range h.counts {
		seen += h.counts[i].Load()
		if seen >= rank {
			v := histValue(i)
			if m := h.maxUs.Load(); v > m {
				v = m
			}
			return time.Duration(v) * time.Microsecond
		}
	}
	return time.Duration(h.maxUs.Load()) * time.Microsecond
}

// Summary is a histogram as reported.
type Summary struct {
	Count uint64  `json:"count"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	P99   float64 `json:"p99_ms"`
	Max   float64 `json:"max_ms"`
	Mean  float64 `json:"mean_ms"`
}

func (h *Hist) Summary() Summary {
	n := h.n.Load()
	if n == 0 {
		return Summary{}
	}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	return Summary{
		Count: n,
		P50:   ms(h.Quantile(0.50)),
		P95:   ms(h.Quantile(0.95)),
		P99:   ms(h.Quantile(0.99)),
		Max:   float64(h.maxUs.Load()) / 1000,
		Mean:  float64(h.sumUs.Load()) / float64(n) / 1000,
	}
}

// Latency kinds the rooms family records.
const (
	latMessage      = iota // a room message reaching a member's stream
	latMember              // a member's own last_message_at UPDATE
	latTyping              // a typing broadcast
	latNotification        // a bigtxn notification
	latOpen                // POST /stream to its ready event
	latBack                // a stream dropped to ready again
	latToken               // POST /token round trip
	latPublish             // POST /publish round trip
	latTxn                 // a message transaction in PostgreSQL
	latReplayed            // a change replayed by a resume, from its commit
	nLat
)

var latNames = [nLat]string{"message", "member", "typing", "notification", "open", "back", "token", "publish", "txn", "replayed"}

// deliveryKinds are the latencies of events Sluice delivers, which the pass
// criteria apply to.
var deliveryKinds = []int{latMessage, latMember, latTyping, latNotification}

// LatSet is one histogram per latency kind.
type LatSet struct{ h [nLat]Hist }

func (s *LatSet) Record(kind int, d time.Duration) { s.h[kind].Record(d) }

func (s *LatSet) Summaries() map[string]Summary {
	out := map[string]Summary{}
	for k := range nLat {
		if s.h[k].Count() > 0 {
			out[latNames[k]] = s.h[k].Summary()
		}
	}
	return out
}

// recorders fans a latency out to the whole run, the current phase, the
// current timeline window, and an optional measurement window.
type recorders struct {
	total  LatSet
	phase  atomic.Pointer[LatSet]
	window atomic.Pointer[LatSet]
	extra  atomic.Pointer[LatSet]
}

func (r *recorders) Record(kind int, d time.Duration) {
	r.total.Record(kind, d)
	if s := r.phase.Load(); s != nil {
		s.Record(kind, d)
	}
	if s := r.window.Load(); s != nil {
		s.Record(kind, d)
	}
	if s := r.extra.Load(); s != nil {
		s.Record(kind, d)
	}
}
