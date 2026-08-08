package timer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestFiresEveryCallbackOncePerPeriod(t *testing.T) {
	w := New(100*time.Millisecond, 10)
	var counts [50]atomic.Int32
	for i := range counts {
		c := &counts[i]
		w.Add(func() { c.Add(1) })
	}

	ctx, cancel := context.WithTimeout(context.Background(), 320*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	for i := range counts {
		// Three periods elapsed; allow one either side for scheduling slop.
		if n := counts[i].Load(); n < 2 || n > 4 {
			t.Fatalf("callback %d fired %d times, want ~3", i, n)
		}
	}
}

// The point of the wheel is that work is spread across the period rather than
// every connection waking at once.
func TestWorkIsSpreadAcrossBuckets(t *testing.T) {
	const buckets = 10
	w := New(100*time.Millisecond, buckets)

	var mu sync.Mutex
	byTick := map[int64]int{}
	start := time.Now()
	for i := 0; i < 100; i++ {
		w.Add(func() {
			mu.Lock()
			byTick[time.Since(start).Milliseconds()/10]++
			mu.Unlock()
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 110*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	mu.Lock()
	defer mu.Unlock()
	if len(byTick) < buckets/2 {
		t.Fatalf("callbacks fired in only %d distinct ticks: %v", len(byTick), byTick)
	}
	for tick, n := range byTick {
		if n > 100/buckets*3 {
			t.Errorf("tick %d fired %d callbacks: work is not evenly spread", tick, n)
		}
	}
}

func TestCancelRemovesCallback(t *testing.T) {
	w := New(20*time.Millisecond, 4)
	var fired atomic.Int32
	cancelFn := w.Add(func() { fired.Add(1) })

	if w.Len() != 1 {
		t.Fatalf("Len = %d, want 1", w.Len())
	}
	cancelFn()
	if w.Len() != 0 {
		t.Fatalf("Len after cancel = %d, want 0", w.Len())
	}
	// Cancelling twice must be safe: a stream can be closed from several paths.
	cancelFn()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	w.Run(ctx)

	if n := fired.Load(); n != 0 {
		t.Errorf("cancelled callback fired %d times", n)
	}
}

// A callback that cancels itself is the normal case: a stream closing tears down
// its own heartbeat. If Run held a write lock while invoking callbacks this
// would deadlock.
func TestSelfCancellingCallbackDoesNotDeadlock(t *testing.T) {
	w := New(10*time.Millisecond, 2)
	done := make(chan struct{})
	var cancelFn func()
	var once sync.Once
	cancelFn = w.Add(func() {
		once.Do(func() {
			cancelFn()
			close(done)
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	go w.Run(ctx)

	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("self-cancelling callback deadlocked")
	}
}

func TestConcurrentAddCancel(t *testing.T) {
	w := New(10*time.Millisecond, 16)
	ctx, cancel := context.WithCancel(context.Background())
	go w.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				c := w.Add(func() {})
				c()
			}
		}()
	}
	wg.Wait()
	cancel()

	if w.Len() != 0 {
		t.Errorf("Len after churn = %d, want 0", w.Len())
	}
}

func TestDegenerateConfiguration(t *testing.T) {
	// Must not divide by zero or spin.
	w := New(0, 0)
	if w.tick <= 0 {
		t.Fatal("tick must be positive")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	w.Run(ctx)
}
