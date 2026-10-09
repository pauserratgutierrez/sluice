package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/dockerapi"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/issuer"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/procstat"
)

// Point is one sample of the run's timeline. Counters are cumulative over
// the run and survive a Sluice restart (each process's counter is added
// piecewise); rates are over the interval since the previous point.
type Point struct {
	T     float64 `json:"t"` // seconds since the run started
	Phase string  `json:"phase"`

	Streams     float64 `json:"sluice_streams"`
	LiveClients int64   `json:"live_clients"`
	SluiceCPU   float64 `json:"sluice_cpu_s"`
	SluiceCores float64 `json:"sluice_cores"`
	RSSMiB      float64 `json:"rss_mib"`
	GoMemMiB    float64 `json:"go_mem_mib"` // what GOMEMLIMIT bounds: sys - released
	HeapLiveMiB float64 `json:"heap_live_mib"`
	// NextGCMiB is the heap goal set at the last GC. With GOGC=100 and
	// GOMEMLIMIT far away it is about twice the heap the GC found live, so
	// half of it estimates live memory, garbage excluded.
	NextGCMiB     float64 `json:"next_gc_mib,omitempty"`
	HeapAllocMiB  float64 `json:"heap_alloc_mib,omitempty"`
	GoMemLimitMiB float64 `json:"gomemlimit_mib,omitempty"`
	GoMaxProcs    float64 `json:"gomaxprocs,omitempty"`
	Goroutines    float64 `json:"goroutines"`
	GCCount       float64 `json:"gc_count"`
	GCPauseSum    float64 `json:"gc_pause_s"`
	GCPauseMaxMs  float64 `json:"gc_pause_max_ms"`
	Lagging       float64 `json:"stream_lagging"`
	Dropped       float64 `json:"dropped_events"`
	Changes       float64 `json:"changes"`
	ChangesPerSec float64 `json:"changes_per_s"`
	// TableRates is changes decoded per second, per table.
	TableRates     map[string]float64 `json:"table_changes_per_s,omitempty"`
	DispatchSum    float64            `json:"dispatch_s"`
	DispatchCount  float64            `json:"dispatch_count"`
	ResumeBufMiB   float64            `json:"resume_buffer_mib"`
	HookRechecks   float64            `json:"hook_rechecks"`
	SluiceRestarts int                `json:"sluice_restarts,omitempty"`

	// Replication, in bytes behind pg_current_wal_lsn(): what Sluice has not
	// dispatched yet (backlog, from its live confirmed position), what the
	// slot has not acknowledged (slot_lag, confirmed_flush_lsn, which moves
	// every SLUICE_STATUS_INTERVAL), what the slot retains (restart_lsn), and
	// what the walsender has not sent yet.
	BacklogBytes int64 `json:"backlog_bytes"`
	// WALLagBytes is Sluice's own sluice_wal_lag_bytes: received minus
	// confirmed, as of its last commit.
	WALLagBytes   int64 `json:"wal_lag_bytes"`
	SlotLagBytes  int64 `json:"slot_lag_bytes"`
	RetainedBytes int64 `json:"retained_bytes"`
	SendLagBytes  int64 `json:"send_lag_bytes"`

	Events       int64   `json:"events"`
	EventsPerSec float64 `json:"events_per_s"`

	IssuerCallsPerSec float64 `json:"issuer_calls_per_s"`
	IssuerInflight    int64   `json:"issuer_inflight"`
	IssuerQueued      int64   `json:"issuer_queued"`
	IssuerCores       float64 `json:"issuer_cores"`

	RunnerCores      float64 `json:"runner_cores"`
	RunnerRSSMiB     float64 `json:"runner_rss_mib"`
	RunnerGoroutines int     `json:"runner_goroutines"`

	Containers map[string]ContainerPoint `json:"containers,omitempty"`
}

// ContainerPoint is a container's CPU (cores over the interval) and memory.
type ContainerPoint struct {
	Cores  float64 `json:"cores"`
	MemMiB float64 `json:"mem_mib"`
}

type sampler struct {
	e *engine

	mu       sync.Mutex
	start    time.Time
	prevAt   time.Time
	prev     map[string]float64 // the previous scrape's raw counters
	acc      map[string]float64 // piecewise sums of counters
	procT0   float64
	restarts int
	points   []Point

	prevEvents  int64
	prevTables  map[string]float64
	prevChanges float64
	prevRunner  float64
	prevIssuer  float64
	issuerLast  int64
	issuerSecs  []issuer.SecondCounts

	docker    *dockerapi.Client
	project   string
	conts     []dockerapi.Container
	contsAt   time.Time
	contsPrev map[string]dockerapi.Usage

	http *http.Client
}

func newSampler(e *engine) *sampler {
	return &sampler{
		e:         e,
		start:     time.Now(),
		prev:      map[string]float64{},
		acc:       map[string]float64{},
		contsPrev: map[string]dockerapi.Usage{},
		docker:    e.docker,
		http:      &http.Client{Timeout: 5 * time.Second},
	}
}

func (s *sampler) run(ctx context.Context, every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_, _ = s.sample(ctx, "")
		}
	}
}

// counters added piecewise across process restarts.
var pieceCounters = []string{
	"process_cpu_seconds_total",
	"go_gc_duration_seconds_count",
	"go_gc_duration_seconds_sum",
	"lagging",
	"dropped",
	"changes",
	"dispatch_sum",
	"dispatch_count",
	"hook_rechecks",
}

// sample takes one point now and appends it to the timeline.
func (s *sampler) sample(ctx context.Context, phase string) (Point, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.e
	now := time.Now()
	dt := now.Sub(s.prevAt).Seconds()
	if s.prevAt.IsZero() {
		dt = 0
	}
	p := Point{T: now.Sub(s.start).Seconds(), Phase: phase}
	if p.Phase == "" {
		p.Phase = e.phaseName()
	}

	// Sluice: /readyz first, so the backlog is never measured against a
	// position newer than the WAL end read after it.
	confirmed := s.readyzLSN(ctx)
	m, _ := fetchMetrics(ctx, s.http, e.cfg.MetricsURL)
	if m != nil {
		raw := map[string]float64{
			"process_cpu_seconds_total":    m["process_cpu_seconds_total"],
			"go_gc_duration_seconds_count": m["go_gc_duration_seconds_count"],
			"go_gc_duration_seconds_sum":   m["go_gc_duration_seconds_sum"],
			"lagging":                      m[`sluice_stream_closed_total{reason="stream_lagging"}`],
			"dropped":                      promSum(m, "sluice_stream_dropped_events_total"),
			"changes":                      promSum(m, "sluice_changes_total"),
			"dispatch_sum":                 promSum(m, "sluice_change_dispatch_seconds_sum"),
			"dispatch_count":               promSum(m, "sluice_change_dispatch_seconds_count"),
			"hook_rechecks":                promSum(m, "sluice_channel_hook_rechecks_total"),
		}
		restarted := false
		if t0 := m["process_start_time_seconds"]; t0 != 0 {
			// The start time is derived from the boot time, which drifts under
			// some hypervisors (WSL2): only a jump of seconds is a new process.
			if s.procT0 != 0 && math.Abs(t0-s.procT0) > 2 {
				restarted = true
				s.restarts++
			}
			s.procT0 = t0
		}
		for _, k := range pieceCounters {
			cur := raw[k]
			switch prev, seen := s.prev[k]; {
			case !seen:
				// The first scrape is the baseline.
			case restarted || cur < prev:
				s.acc[k] += cur
			default:
				s.acc[k] += cur - prev
			}
			s.prev[k] = cur
		}
		p.Streams = m["sluice_streams"]
		p.RSSMiB = m["process_resident_memory_bytes"] / mib
		p.GoMemMiB = (m["go_memstats_sys_bytes"] - m["go_memstats_heap_released_bytes"]) / mib
		p.HeapLiveMiB = liveBytes(m) / mib
		p.NextGCMiB = m["go_memstats_next_gc_bytes"] / mib
		p.HeapAllocMiB = m["go_memstats_heap_alloc_bytes"] / mib
		p.GoMemLimitMiB = m["go_gc_gomemlimit_bytes"] / mib
		p.GoMaxProcs = m["go_sched_gomaxprocs_threads"]
		p.Goroutines = m["go_goroutines"]
		p.GCPauseMaxMs = m[`go_gc_duration_seconds{quantile="1"}`] * 1000
		p.ResumeBufMiB = m["sluice_resume_buffer_bytes"] / mib
		p.WALLagBytes = int64(m["sluice_wal_lag_bytes"])
		tables := tableChanges(m)
		if dt > 0 && s.prevTables != nil && !restarted {
			p.TableRates = map[string]float64{}
			for tb, n := range tables {
				if d := n - s.prevTables[tb]; d >= 0 {
					p.TableRates[tb] = d / dt
				}
			}
		}
		s.prevTables = tables
	}
	p.SluiceRestarts = s.restarts
	p.SluiceCPU = s.acc["process_cpu_seconds_total"]
	p.GCCount = s.acc["go_gc_duration_seconds_count"]
	p.GCPauseSum = s.acc["go_gc_duration_seconds_sum"]
	p.Lagging = s.acc["lagging"]
	p.Dropped = s.acc["dropped"]
	p.Changes = s.acc["changes"]
	p.DispatchSum = s.acc["dispatch_sum"]
	p.DispatchCount = s.acc["dispatch_count"]
	p.HookRechecks = s.acc["hook_rechecks"]
	p.LiveClients = e.liveClients.Load()
	p.Events = e.delivered()

	// Replication.
	if row, err := e.slotRow(ctx); err == nil {
		p.SlotLagBytes, p.RetainedBytes, p.SendLagBytes = row.slotLag, row.retained, row.sendLag
		if confirmed > 0 && row.current >= confirmed {
			p.BacklogBytes = int64(row.current - confirmed)
		}
	}

	// Issuer.
	if snap, err := s.issuerStats(ctx); err == nil {
		p.IssuerInflight, p.IssuerQueued = snap.Inflight, snap.Queued
		if dt > 0 && s.prevIssuer > 0 {
			p.IssuerCores = (snap.CPUSeconds - s.prevIssuer) / dt
		}
		s.prevIssuer = snap.CPUSeconds
		var calls int64
		for _, sc := range snap.Seconds {
			if sc.At >= snap.Now {
				continue // still filling
			}
			s.issuerSecs = append(s.issuerSecs, sc)
			calls += sc.N["subscribe"] + sc.N["refresh"] + sc.N["hook"]
			s.issuerLast = sc.At
		}
		if dt > 0 {
			p.IssuerCallsPerSec = float64(calls) / dt
		}
		if s.issuerLast < snap.Now-1 {
			s.issuerLast = snap.Now - 1
		}
	}

	// Runner.
	cpu, rss := procstat.Self()
	if dt > 0 {
		p.RunnerCores = (cpu - s.prevRunner) / dt
		p.SluiceCores = (p.SluiceCPU - s.lastCPU()) / dt
		p.EventsPerSec = float64(p.Events-s.prevEvents) / dt
		p.ChangesPerSec = (p.Changes - s.prevChanges) / dt
	}
	s.prevRunner = cpu
	s.prevEvents = p.Events
	s.prevChanges = p.Changes
	p.RunnerRSSMiB = float64(rss) / mib
	p.RunnerGoroutines = runtime.NumGoroutine()

	// Containers.
	if s.docker != nil {
		p.Containers = s.containerUsage(ctx, dt)
	}

	s.prevAt = now
	s.points = append(s.points, p)
	return p, len(s.points) - 1
}

func (s *sampler) lastCPU() float64 {
	if len(s.points) == 0 {
		return 0
	}
	return s.points[len(s.points)-1].SluiceCPU
}

func (s *sampler) readyzLSN(ctx context.Context) uint64 {
	base := strings.TrimSuffix(s.e.cfg.MetricsURL, "/metrics")
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/readyz", nil)
	if err != nil {
		return 0
	}
	resp, err := s.http.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	var body struct {
		Confirmed string `json:"confirmed_lsn"`
	}
	if json.NewDecoder(resp.Body).Decode(&body) != nil {
		return 0
	}
	lsn, _ := parseLSN(body.Confirmed)
	return lsn
}

func (s *sampler) issuerStats(ctx context.Context) (issuer.Snapshot, error) {
	return fetchIssuerStats(ctx, s.http, s.e.cfg.IssuerURL, s.issuerLast+1)
}

func fetchIssuerStats(ctx context.Context, c *http.Client, base string, since int64) (issuer.Snapshot, error) {
	var snap issuer.Snapshot
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("%s/stats?since=%d", base, since), nil)
	if err != nil {
		return snap, err
	}
	resp, err := c.Do(req)
	if err != nil {
		return snap, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&snap)
	return snap, err
}

func (s *sampler) containerUsage(ctx context.Context, dt float64) map[string]ContainerPoint {
	if s.project == "" {
		p, err := s.docker.Project(ctx)
		if err != nil {
			s.docker = nil
			return nil
		}
		s.project = p
	}
	if s.conts == nil || time.Since(s.contsAt) > 30*time.Second {
		if cs, err := s.docker.ProjectContainers(ctx, s.project); err == nil {
			s.conts, s.contsAt = cs, time.Now()
		}
	}
	type res struct {
		name string
		u    dockerapi.Usage
		err  error
	}
	ch := make(chan res, len(s.conts))
	for _, c := range s.conts {
		go func(c dockerapi.Container) {
			u, err := s.docker.Stats(ctx, c.ID)
			ch <- res{c.Service(), u, err}
		}(c)
	}
	out := map[string]ContainerPoint{}
	for range s.conts {
		r := <-ch
		if r.err != nil || r.name == "" {
			continue
		}
		cp := ContainerPoint{MemMiB: float64(r.u.MemBytes) / mib}
		if prev, ok := s.contsPrev[r.name]; ok && dt > 0 && r.u.CPUSeconds >= prev.CPUSeconds {
			cp.Cores = (r.u.CPUSeconds - prev.CPUSeconds) / r.u.At.Sub(prev.At).Seconds()
		}
		s.contsPrev[r.name] = r.u
		out[r.name] = cp
	}
	return out
}

// snapshotPoints returns the timeline so far.
func (s *sampler) snapshotPoints() []Point {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]Point(nil), s.points...)
}

func (s *sampler) issuerSeconds() []issuer.SecondCounts {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]issuer.SecondCounts(nil), s.issuerSecs...)
}

// parseLSN reads PostgreSQL's "X/Y" notation.
func parseLSN(s string) (uint64, error) {
	hi, lo, ok := strings.Cut(strings.TrimSpace(s), "/")
	if !ok {
		return 0, fmt.Errorf("bad lsn %q", s)
	}
	h, err := strconv.ParseUint(hi, 16, 32)
	if err != nil {
		return 0, err
	}
	l, err := strconv.ParseUint(lo, 16, 32)
	if err != nil {
		return 0, err
	}
	return h<<32 | l, nil
}

// tableChanges sums sluice_changes_total by schema.table.
func tableChanges(m map[string]float64) map[string]float64 {
	out := map[string]float64{}
	for k, v := range m {
		labels, ok := strings.CutPrefix(k, "sluice_changes_total{")
		if !ok {
			continue
		}
		schema, table := labelValue(labels, "schema"), labelValue(labels, "table")
		if table != "" {
			out[schema+"."+table] += v
		}
	}
	return out
}

// labelValue reads one label of a Prometheus series' label set.
func labelValue(labels, name string) string {
	_, rest, ok := strings.Cut(labels, name+`="`)
	if !ok {
		return ""
	}
	v, _, _ := strings.Cut(rest, `"`)
	return v
}
