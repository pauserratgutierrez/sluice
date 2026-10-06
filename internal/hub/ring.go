package hub

import (
	"sync"
	"time"
	"unsafe"

	"github.com/pauserratgutierrez/sluice/internal/metrics"
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

	size int    // bytes held, counted against SLUICE_RING_MAX_BYTES
	seq  uint64 // append order across every ring
}

const (
	entryOverhead  = int(unsafe.Sizeof(RingEntry{}))
	tupleOverhead  = int(unsafe.Sizeof(pgoutput.Tuple{}))
	columnOverhead = int(unsafe.Sizeof(pgoutput.Column{}))
)

func tupleSize(t *pgoutput.Tuple) int {
	if t == nil {
		return 0
	}
	n := tupleOverhead + len(t.Columns)*columnOverhead
	for _, c := range t.Columns {
		n += len(c.Data)
	}
	return n
}

// Rings holds one bounded ring buffer per relation.
//
// Per relation, not per shape: the number of distinct shapes can be one per user,
// so a per-shape buffer would be O(shapes x ring) memory. Per-relation is
// O(relations x ring) and replay simply re-filters, which is affordable because
// reconnections are rare relative to changes.
//
// A ring holds every change the reader dispatched for its relation since the
// process started replicating, minus what was removed: the oldest entries past
// SLUICE_RING_EVENTS per relation or SLUICE_RING_MAX_BYTES across all of them,
// and those older than SLUICE_RING_MAX_AGE (applied by Sweep). Age never removes
// a relation's newest transaction, so a client that was up to date on a quiet
// table can always resume. A request that reaches into a removed range is told
// it was not resumed rather than silently given a partial replay.
//
// A capacity of 0 disables the buffer: nothing is kept and no position is
// covered.
type Rings struct {
	capacity int
	maxBytes int
	maxAge   time.Duration

	mu    sync.RWMutex
	start uint64 // replication start LSN; 0 until the reader has started
	bytes int    // held by every ring together
	seq   uint64 // the last append's order
	rings map[uint32]*ring
}

// minRing is the slot count a ring starts with. A ring grows towards the
// capacity as its relation gets busy, so a quiet table holds a few slots rather
// than thousands.
const minRing = 16

type ring struct {
	buf  []RingEntry
	head int
	size int
	// evicted is the highest commit LSN removed from this ring.
	evicted uint64
}

func (rg *ring) at(i int) *RingEntry { return &rg.buf[(rg.head+i)%len(rg.buf)] }

// dropOldest removes the oldest entry and returns the bytes it held.
func (rg *ring) dropOldest() int {
	e := rg.at(0)
	if e.LSN > rg.evicted {
		rg.evicted = e.LSN
	}
	freed := e.size
	*e = RingEntry{}
	rg.head = (rg.head + 1) % len(rg.buf)
	rg.size--
	return freed
}

// resize moves the entries, oldest first, into a buffer of n slots.
func (rg *ring) resize(n int) {
	buf := make([]RingEntry, n)
	for i := range rg.size {
		buf[i] = *rg.at(i)
	}
	rg.buf, rg.head = buf, 0
}

func NewRings(capacity, maxBytes int, maxAge time.Duration) *Rings {
	return &Rings{capacity: max(capacity, 0), maxBytes: maxBytes, maxAge: maxAge, rings: map[uint32]*ring{}}
}

// SetStart records the LSN replication started from. Only the first call
// counts: a reader reconnect resumes from the slot, at or before what was
// already dispatched, so coverage does not shrink. A disabled buffer covers
// nothing, so it never starts.
func (r *Rings) SetStart(lsn uint64) {
	if r.capacity == 0 {
		return
	}
	r.mu.Lock()
	if r.start == 0 {
		r.start = lsn
	}
	r.mu.Unlock()
}

// Append records a change.
func (r *Rings) Append(e RingEntry) {
	if r.capacity == 0 {
		return
	}
	e.At = time.Now()
	e.size = entryOverhead + tupleSize(e.New) + tupleSize(e.Old)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seq++
	e.seq = r.seq
	rg := r.rings[e.Relation.OID]
	if rg == nil {
		rg = &ring{}
		r.rings[e.Relation.OID] = rg
	}
	if rg.size == r.capacity {
		r.bytes -= rg.dropOldest()
	}
	if rg.size == len(rg.buf) {
		rg.resize(min(r.capacity, max(minRing, 2*len(rg.buf))))
	}
	*rg.at(rg.size) = e
	rg.size++
	r.bytes += e.size
	for r.bytes > r.maxBytes {
		r.dropOldestOverall()
	}
}

// dropOldestOverall removes the entry appended longest ago, whichever relation
// holds it. Only called while some ring holds bytes.
func (r *Rings) dropOldestOverall() {
	var oldest *ring
	for _, rg := range r.rings {
		if rg.size > 0 && (oldest == nil || rg.at(0).seq < oldest.at(0).seq) {
			oldest = rg
		}
	}
	r.bytes -= oldest.dropOldest()
}

// Sweep removes entries older than the maximum age, keeping each relation's
// newest transaction, and shrinks rings that hold far fewer entries than they
// have room for. It is what bounds the memory held for tables that have gone
// quiet.
func (r *Rings) Sweep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-r.maxAge)
	for _, rg := range r.rings {
		if r.maxAge > 0 && rg.size > 0 {
			newest := rg.at(rg.size - 1).LSN
			for rg.size > 0 {
				e := rg.at(0)
				if e.LSN == newest || !e.At.Before(cutoff) {
					break
				}
				r.bytes -= rg.dropOldest()
			}
		}
		if n := max(minRing, 2*rg.size); n < len(rg.buf)/2 {
			rg.resize(n)
		}
	}
	metrics.ResumeBufferBytes.Set(float64(r.bytes))
}

// Replay returns every buffered change of a relation whose commit LSN is at or
// after `from`, and reports whether that is ALL of them.
//
// "At or after" rather than "after": a client that lost its connection part-way
// through a transaction resumes from that transaction's commit LSN and must get
// the rest of it. Re-delivering the part it had is the at-least-once contract;
// clients upsert by primary key.
//
// ok is false when a change the caller needs may be missing: the buffer is
// disabled, `from` predates the start of replication, or an entry with a commit
// LSN >= from was removed.
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
