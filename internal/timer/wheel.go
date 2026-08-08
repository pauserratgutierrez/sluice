// Package timer provides a shared, jittered timer wheel.
//
// A `time.Ticker` per connection is the documented way to make a hundred thousand
// connections expensive: every ticker is a runtime timer that must be inserted,
// heap-managed and woken individually, and they all fire in lockstep. The wheel
// replaces N timers with one, and spreads the work across the period so that a
// hundred thousand heartbeats become a steady trickle rather than a spike every
// twenty seconds.
//
// Go's stdlib hit the same class of problem with SO_KEEPALIVE on mobile
// (golang/go#48622): it is not the work that hurts, it is waking everything at
// once.
package timer

import (
	"context"
	"sync"
	"time"
)

// Wheel fires registered callbacks approximately once per period.
type Wheel struct {
	period time.Duration
	tick   time.Duration

	mu      sync.RWMutex
	buckets []map[uint64]func()
	next    uint64
}

// New creates a wheel with the given period and bucket count.
//
// More buckets means smoother spreading and a shorter tick. 64 buckets over a 20
// second period is a tick every 312ms, which is cheap and spreads a hundred
// thousand callbacks into batches of about 1,500.
func New(period time.Duration, buckets int) *Wheel {
	if buckets < 1 {
		buckets = 1
	}
	if period <= 0 {
		period = time.Second
	}
	w := &Wheel{
		period:  period,
		tick:    period / time.Duration(buckets),
		buckets: make([]map[uint64]func(), buckets),
	}
	if w.tick <= 0 {
		w.tick = time.Millisecond
	}
	for i := range w.buckets {
		w.buckets[i] = map[uint64]func(){}
	}
	return w
}

// Add registers a callback and returns a function that removes it.
//
// Placement is round-robin rather than hashed, which spreads registrations
// evenly no matter how they arrive -- a reconnect storm after a deploy would
// otherwise pile every new stream into the same bucket if the key hashed poorly.
func (w *Wheel) Add(fn func()) (cancel func()) {
	w.mu.Lock()
	id := w.next
	w.next++
	slot := int(id % uint64(len(w.buckets)))
	w.buckets[slot][id] = fn
	w.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			w.mu.Lock()
			delete(w.buckets[slot], id)
			w.mu.Unlock()
		})
	}
}

// Len reports how many callbacks are registered.
func (w *Wheel) Len() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	n := 0
	for _, b := range w.buckets {
		n += len(b)
	}
	return n
}

// Run advances the wheel until the context is cancelled.
func (w *Wheel) Run(ctx context.Context) {
	t := time.NewTicker(w.tick)
	defer t.Stop()
	slot := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		// Snapshot under a read lock so a callback that cancels itself -- which is
		// exactly what a closing stream does -- cannot deadlock on the write lock.
		w.mu.RLock()
		fns := make([]func(), 0, len(w.buckets[slot]))
		for _, fn := range w.buckets[slot] {
			fns = append(fns, fn)
		}
		w.mu.RUnlock()

		for _, fn := range fns {
			fn()
		}
		slot = (slot + 1) % len(w.buckets)
	}
}
