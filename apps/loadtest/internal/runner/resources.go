package runner

import (
	"context"
	"sync"
	"time"
)

const mib = 1 << 20

// liveBytes is the memory Sluice holds: heap in use plus goroutine stacks, from
// the Go collector its /metrics exposes. Unlike RSS it falls once garbage is
// collected, so a step's baseline does not carry the previous step's garbage,
// and the runtime does not hand freed pages back to the OS promptly enough for
// RSS to be read per stream.
func liveBytes(m map[string]float64) float64 {
	return m["go_memstats_heap_inuse_bytes"] + m["go_memstats_stack_inuse_bytes"]
}

// resources keeps the peaks of Sluice's resource use over one step.
type resources struct {
	mu         sync.Mutex
	rss        float64
	live       float64
	goroutines float64
	gcPause    float64
}

func (r *resources) observe(m map[string]float64) {
	if m == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.rss = max(r.rss, m["process_resident_memory_bytes"])
	r.live = max(r.live, liveBytes(m))
	r.goroutines = max(r.goroutines, m["go_goroutines"])
	// The summary's top quantile is the longest of the recent pauses.
	r.gcPause = max(r.gcPause, m[`go_gc_duration_seconds{quantile="1"}`])
}

// sampleResources scrapes the metrics every interval into r until the returned
// stop function is called. stop waits for the sampler to exit and may be called
// more than once.
func (a *app) sampleResources(ctx context.Context, r *resources) (stop func()) {
	quit := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		t := time.NewTicker(a.cfg.SampleEvery)
		defer t.Stop()
		for {
			select {
			case <-quit:
				return
			case <-ctx.Done():
				return
			case <-t.C:
				if m, err := fetchMetrics(ctx, a.http, a.cfg.MetricsURL); err == nil {
					r.observe(m)
				}
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(quit) })
		<-done
	}
}

// fillResources reports a step's resource use. base is scraped once, when the
// run starts on a fresh process; before when the step starts, opened once every
// stream is open and before any traffic, and end before the streams are closed.
//
// Per-stream figures are measured from base, not from before: when a step
// starts, the previous step's streams are closed but their memory is usually
// not collected yet, and subtracting it would understate what a stream costs.
func fillResources(st *Step, r *resources, base, before, opened, end map[string]float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if base == nil || before == nil || end == nil {
		return
	}
	st.LiveBaselineMiB = liveBytes(base) / mib
	st.LivePeakMiB = r.live / mib
	st.RSSPeakMiB = r.rss / mib
	st.GoroutinesPeak = r.goroutines
	st.GCPauseMax = time.Duration(r.gcPause * float64(time.Second)).Round(time.Microsecond).String()
	st.CPUSeconds = end["process_cpu_seconds_total"] - before["process_cpu_seconds_total"]
	if st.Received > 0 {
		st.CPUMsPer1kEvents = st.CPUSeconds * 1e6 / float64(st.Received)
	}
	if opened != nil && st.Opened > 0 {
		n := float64(st.Opened)
		st.LiveOpenMiB = liveBytes(opened) / mib
		st.KiBPerStream = (liveBytes(opened) - liveBytes(base)) / 1024 / n
		st.GoroutinesPerStream = (opened["go_goroutines"] - base["go_goroutines"]) / n
	}
}
