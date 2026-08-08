package hub

import (
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

// RingEntry is one buffered change, kept in decoded-but-unprojected form so that
// a replay can be re-filtered and RE-AUTHORIZED against the current
// subscription. Replay is never trusted to have been authorized before.
type RingEntry struct {
	LSN        uint64
	Seq        int
	CommitTime time.Time
	Op         byte
	Relation   *pgoutput.Relation
	New        *pgoutput.Tuple
	Old        *pgoutput.Tuple
	OldIsKey   bool
	At         time.Time
}

// Rings holds one bounded ring buffer per relation.
//
// Per relation, not per shape: the number of distinct shapes can be one per user,
// so a per-shape buffer would be O(shapes x ring) memory. Per-relation is
// O(relations x ring) and replay simply re-filters, which is affordable because
// reconnections are rare relative to changes.
//
// This is what makes `resume` possible at all. It is bounded, and when a client
// asks for an LSN older than the floor it is told to resnapshot rather than
// silently given a gap.
type Rings struct {
	capacity int
	maxAge   time.Duration

	mu    sync.RWMutex
	rings map[uint32]*ring
}

type ring struct {
	buf   []RingEntry
	start int
	size  int
}

func NewRings(capacity int, maxAge time.Duration) *Rings {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Rings{capacity: capacity, maxAge: maxAge, rings: map[uint32]*ring{}}
}

// Append records a change.
func (r *Rings) Append(e RingEntry) {
	if r.capacity == 0 {
		return
	}
	e.At = time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rg := r.rings[e.Relation.OID]
	if rg == nil {
		rg = &ring{buf: make([]RingEntry, r.capacity)}
		r.rings[e.Relation.OID] = rg
	}
	idx := (rg.start + rg.size) % r.capacity
	rg.buf[idx] = e
	if rg.size < r.capacity {
		rg.size++
	} else {
		rg.start = (rg.start + 1) % r.capacity
	}
}

// Replay returns entries strictly after `afterLSN`, and reports whether the
// requested position is still covered.
//
// ok=false means the caller must resnapshot. Saying so explicitly is the honest
// alternative to pretending a bounded buffer is unbounded.
func (r *Rings) Replay(oid uint32, afterLSN uint64) (entries []RingEntry, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rg := r.rings[oid]
	if rg == nil || rg.size == 0 {
		// Nothing buffered. If the client is asking to resume from anything at
		// all we cannot prove there was no gap.
		return nil, afterLSN == 0
	}
	cutoff := time.Time{}
	if r.maxAge > 0 {
		cutoff = time.Now().Add(-r.maxAge)
	}
	oldest := rg.buf[rg.start]
	if afterLSN != 0 && oldest.LSN > afterLSN {
		return nil, false
	}
	if !cutoff.IsZero() && oldest.At.Before(cutoff) && afterLSN != 0 {
		// The buffer has aged past the retention window; treat as a gap.
		return nil, false
	}
	for i := 0; i < rg.size; i++ {
		e := rg.buf[(rg.start+i)%r.capacity]
		if !cutoff.IsZero() && e.At.Before(cutoff) {
			continue
		}
		if e.LSN > afterLSN {
			entries = append(entries, e)
		}
	}
	return entries, true
}

// Floor returns the oldest LSN still available for a relation.
func (r *Rings) Floor(oid uint32) uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rg := r.rings[oid]
	if rg == nil || rg.size == 0 {
		return 0
	}
	return rg.buf[rg.start].LSN
}
