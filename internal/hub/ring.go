package hub

import (
	"cmp"
	"sort"
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

	size int    // bytes held, counted against the budget (SLUICE_RING_MEMORY)
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

// Rings holds one ring buffer per relation.
//
// Per relation, not per shape: the number of distinct shapes can be one per user,
// so a per-shape buffer would be O(shapes x ring) memory. Per-relation is
// O(relations x ring) and replay simply re-filters, which is affordable because
// reconnections are rare relative to changes.
//
// A ring holds every change the reader dispatched for its relation since the
// process started replicating, minus what was removed: changes older than
// SLUICE_RING_MAX_AGE (applied by Sweep), and, while every ring together holds
// more than the budget (maxBytes), the oldest change of any relation. So every
// busy relation reaches back as far as the others, by time rather than by a
// number of its changes. Neither removes a relation's newest transaction while
// anything else is left, so a client that was up to date on a quiet table can
// resume. A request that reaches into a removed range is told it was not
// resumed rather than silently given a partial replay.
//
// A budget of 0 disables the buffer: nothing is kept and no position is
// covered.
type Rings struct {
	maxBytes int
	maxAge   time.Duration

	mu    sync.RWMutex
	start uint64 // replication start LSN; 0 until the reader has started
	// startedAt is when replication started, and removedAt when the newest
	// change removed so far was appended: every position since the later of the
	// two is covered on every relation.
	startedAt, removedAt time.Time
	bytes                int    // held by every ring together
	seq                  uint64 // the last append's order
	rings                map[uint32]*ring
	list                 []*ring // the same rings, for dropOldestOverall to scan
}

// minRing is the slot count a ring starts with. A ring grows as its relation
// gets busy, so a quiet table holds a few slots rather than thousands.
const minRing = 16

type ring struct {
	buf  []RingEntry
	head int
	size int
	// evicted is the highest commit LSN removed from this ring, and
	// evictedSeq the append order of the last entry removed.
	evicted    uint64
	evictedSeq uint64
}

func (rg *ring) at(i int) *RingEntry { return &rg.buf[(rg.head+i)%len(rg.buf)] }

// dropOldest removes the oldest entry and returns the bytes it held and when
// it was appended.
func (rg *ring) dropOldest() (freed int, at time.Time) {
	e := rg.at(0)
	if e.LSN > rg.evicted {
		rg.evicted = e.LSN
	}
	rg.evictedSeq = e.seq
	freed, at = e.size, e.At
	*e = RingEntry{}
	rg.head = (rg.head + 1) % len(rg.buf)
	rg.size--
	return freed, at
}

// newestOnly reports whether the ring holds nothing but its newest transaction.
func (rg *ring) newestOnly() bool { return rg.at(0).LSN == rg.at(rg.size-1).LSN }

// resize moves the entries, oldest first, into a buffer of n slots.
func (rg *ring) resize(n int) {
	buf := make([]RingEntry, n)
	for i := range rg.size {
		buf[i] = *rg.at(i)
	}
	rg.buf, rg.head = buf, 0
}

// NewRings returns a buffer that keeps changes for maxAge (0: until the budget
// removes them) in at most maxBytes (0: disabled).
func NewRings(maxBytes int, maxAge time.Duration) *Rings {
	return &Rings{maxBytes: max(maxBytes, 0), maxAge: maxAge, rings: map[uint32]*ring{}}
}

// SetStart records the LSN replication started from. Only the first call
// counts: a reader reconnect resumes from the slot, at or before what was
// already dispatched, so coverage does not shrink. A disabled buffer covers
// nothing, so it never starts.
func (r *Rings) SetStart(lsn uint64) {
	if r.maxBytes == 0 {
		return
	}
	r.mu.Lock()
	if r.start == 0 {
		r.start, r.startedAt = lsn, time.Now()
	}
	r.mu.Unlock()
}

// Enabled reports whether the buffer keeps anything.
func (r *Rings) Enabled() bool { return r.maxBytes > 0 }

// Seq returns the append order the buffer has reached.
func (r *Rings) Seq() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.seq
}

// Append records a change and returns its place in the append order, 0 when
// the buffer is disabled.
func (r *Rings) Append(e RingEntry) uint64 {
	if r.maxBytes == 0 {
		return 0
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
		r.list = append(r.list, rg)
	}
	if rg.size == len(rg.buf) {
		rg.resize(max(minRing, 2*len(rg.buf)))
	}
	*rg.at(rg.size) = e
	rg.size++
	r.bytes += e.size
	for r.bytes > r.maxBytes {
		r.dropOldestOverall()
	}
	return e.seq
}

// drop removes a ring's oldest entry.
func (r *Rings) drop(rg *ring) {
	freed, at := rg.dropOldest()
	r.bytes -= freed
	if at.After(r.removedAt) {
		r.removedAt = at
	}
}

// dropOldestOverall removes the entry appended longest ago, whichever relation
// holds it, sparing each relation's newest transaction while anything else is
// left: past that, the newest transactions alone exceed the budget. Only
// called while some ring holds bytes.
func (r *Rings) dropOldestOverall() {
	var oldest, spared *ring
	for _, rg := range r.list {
		switch {
		case rg.size == 0:
		case rg.newestOnly():
			if spared == nil || rg.at(0).seq < spared.at(0).seq {
				spared = rg
			}
		case oldest == nil || rg.at(0).seq < oldest.at(0).seq:
			oldest = rg
		}
	}
	r.drop(cmp.Or(oldest, spared))
}

// Sweep removes entries older than the maximum age, keeping each relation's
// newest transaction, and shrinks rings that hold far fewer entries than they
// have room for. It is what bounds the memory held for tables that have gone
// quiet. It also reports the buffer's size and how far back it covers every
// relation.
func (r *Rings) Sweep() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-r.maxAge)
	for _, rg := range r.list {
		for r.maxAge > 0 && rg.size > 0 && !rg.newestOnly() && rg.at(0).At.Before(cutoff) {
			r.drop(rg)
		}
		if n := max(minRing, 2*rg.size); n < len(rg.buf)/2 {
			rg.resize(n)
		}
	}
	metrics.ResumeBufferBytes.Set(float64(r.bytes))
	if since := r.startedAt; !since.IsZero() {
		if r.removedAt.After(since) {
			since = r.removedAt
		}
		metrics.ResumeBufferSeconds.Set(now.Sub(since).Seconds())
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
// ok is false when a change the caller needs may be missing: the buffer is
// disabled, `from` predates the start of replication, or an entry with a commit
// LSN >= from was removed.
func (r *Rings) Replay(oid uint32, from uint64) (entries []RingEntry, ok bool) {
	entries, _, ok = r.ReplayThrough(oid, from, nil)
	return entries, ok
}

// ReplayThrough is Replay that also returns the append order the buffer had
// reached: every change appended up to it is accounted for, and Since takes
// over from there.
//
// match, when not nil, keeps only the changes it accepts. It runs under the
// buffer's lock, so it must be cheap: it saves copying a whole table's changes
// for each subscription that resumes, which thousands do at once after a cut.
func (r *Rings) ReplayThrough(oid uint32, from uint64, match func(*RingEntry) bool) (entries []RingEntry, through uint64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if !r.coversLocked(oid, from) {
		return nil, r.seq, false
	}
	if rg := r.rings[oid]; rg != nil {
		// Entries are in commit order, so the scan starts at the first one at
		// or after from instead of reading the whole buffer of a busy table. A
		// reader reconnect appends again, in order, the transactions after the
		// slot's position: whichever copy the search lands on, the rest of the
		// buffer holds every change at or after from.
		first := sort.Search(rg.size, func(i int) bool { return rg.at(i).LSN >= from })
		for i := first; i < rg.size; i++ {
			if e := rg.at(i); e.LSN >= from && (match == nil || match(e)) {
				entries = append(entries, *e)
			}
		}
	}
	return entries, r.seq, true
}

// Covers reports whether Replay from `from` would return every change.
func (r *Rings) Covers(oid uint32, from uint64) bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.coversLocked(oid, from)
}

func (r *Rings) coversLocked(oid uint32, from uint64) bool {
	if r.start == 0 || from < r.start {
		return false
	}
	rg := r.rings[oid]
	return rg == nil || rg.evicted < from
}

// Since returns a relation's changes appended after `after` that match keeps
// (all of them when it is nil, as for ReplayThrough), in append order, and the
// append order the buffer has reached. ok is false when one of them was
// already removed.
func (r *Rings) Since(oid uint32, after uint64, match func(*RingEntry) bool) (entries []RingEntry, through uint64, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	rg := r.rings[oid]
	if rg == nil {
		return nil, r.seq, true
	}
	if rg.evictedSeq > after {
		return nil, r.seq, false
	}
	first := rg.size
	for first > 0 && rg.at(first-1).seq > after {
		first--
	}
	for i := first; i < rg.size; i++ {
		if e := rg.at(i); match == nil || match(e) {
			entries = append(entries, *e)
		}
	}
	return entries, r.seq, true
}
