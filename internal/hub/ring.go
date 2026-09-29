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
	LSN        uint64 // commit LSN of the enclosing transaction
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
// A ring holds every change the reader dispatched for its relation since the
// process started replicating, minus what capacity (SLUICE_RING_EVENTS) or age
// (SLUICE_RING_MAX_AGE, applied by Sweep) removed. The newest transaction of a
// relation is never aged out, so a client that was up to date on a quiet table
// can always resume. A request that reaches into a removed range is told to
// resnapshot rather than silently given a partial replay.
type Rings struct {
	capacity int
	maxAge   time.Duration

	mu    sync.RWMutex
	start uint64 // replication start LSN; 0 until the reader has started
	rings map[uint32]*ring
}

type ring struct {
	buf  []RingEntry
	head int
	size int
	// evicted is the highest commit LSN removed from this ring.
	evicted uint64
}

func (rg *ring) at(i int) *RingEntry { return &rg.buf[(rg.head+i)%len(rg.buf)] }

func (rg *ring) dropOldest() {
	e := rg.at(0)
	if e.LSN > rg.evicted {
		rg.evicted = e.LSN
	}
	*e = RingEntry{}
	rg.head = (rg.head + 1) % len(rg.buf)
	rg.size--
}

func NewRings(capacity int, maxAge time.Duration) *Rings {
	if capacity <= 0 {
		capacity = 1024
	}
	return &Rings{capacity: capacity, maxAge: maxAge, rings: map[uint32]*ring{}}
}

// SetStart records the LSN replication started from. Only the first call
// counts: a reader reconnect resumes from the slot, at or before what was
// already dispatched, so coverage does not shrink.
func (r *Rings) SetStart(lsn uint64) {
	r.mu.Lock()
	if r.start == 0 {
		r.start = lsn
	}
	r.mu.Unlock()
}

// Append records a change.
func (r *Rings) Append(e RingEntry) {
	e.At = time.Now()
	r.mu.Lock()
	defer r.mu.Unlock()
	rg := r.rings[e.Relation.OID]
	if rg == nil {
		rg = &ring{buf: make([]RingEntry, r.capacity)}
		r.rings[e.Relation.OID] = rg
	}
	if rg.size == r.capacity {
		rg.dropOldest()
	}
	*rg.at(rg.size) = e
	rg.size++
}

// Sweep removes entries older than the maximum age, keeping each relation's
// newest transaction. It is what bounds the memory held for tables that have
// gone quiet.
func (r *Rings) Sweep() {
	if r.maxAge <= 0 {
		return
	}
	cutoff := time.Now().Add(-r.maxAge)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, rg := range r.rings {
		if rg.size == 0 {
			continue
		}
		newest := rg.at(rg.size - 1).LSN
		for rg.size > 0 {
			e := rg.at(0)
			if e.LSN == newest || !e.At.Before(cutoff) {
				break
			}
			rg.dropOldest()
		}
	}
}

// Replay returns every buffered change of a relation whose commit LSN is at or
// after `from`, and reports whether that is ALL of them.
//
// "At or after" rather than "after": a client that lost its connection part-way
// through a transaction resumes from that transaction's commit LSN and must get
// the rest of it. Re-delivering the part it had is the at-least-once contract;
// clients upsert by primary key.
//
// ok is false when a change the caller needs may be missing: `from` predates
// the start of replication, or an entry with a commit LSN >= from was removed.
func (r *Rings) Replay(oid uint32, from uint64) (entries []RingEntry, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.start == 0 || from < r.start {
		return nil, false
	}
	rg := r.rings[oid]
	if rg == nil {
		return nil, true
	}
	if rg.evicted >= from {
		return nil, false
	}
	for i := 0; i < rg.size; i++ {
		if e := rg.at(i); e.LSN >= from {
			entries = append(entries, *e)
		}
	}
	return entries, true
}
