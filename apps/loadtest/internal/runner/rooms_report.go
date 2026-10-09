package runner

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/issuer"
	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/procstat"
)

// RoomsReport is the result of one rooms-family run.
type RoomsReport struct {
	Scenario   string            `json:"scenario"`
	Label      string            `json:"label,omitempty"`
	Target     string            `json:"target"`
	StartedAt  time.Time         `json:"started_at"`
	FinishedAt time.Time         `json:"finished_at"`
	Seed       uint64            `json:"seed"`
	Config     roomsConfig       `json:"config"`
	Env        map[string]string `json:"env"`
	Host       HostInfo          `json:"host"`
	Expected   Expected          `json:"expected"`

	ArrivalSeconds  float64 `json:"arrival_seconds"`
	ArrivalComplete bool    `json:"arrival_complete"`
	KiBPerStream    float64 `json:"kib_per_stream"`

	Phases      []PhaseSummary     `json:"phases"`
	BigTxns     []BigTxnResult     `json:"bigtxns,omitempty"`
	Reconnects  []WaveResult       `json:"reconnects,omitempty"`
	Calibration *CalibrationResult `json:"calibration,omitempty"`
	Verdict     Verdict            `json:"verdict"`
	Counters    map[string]int64   `json:"counters"`

	Windows         []window              `json:"windows"`
	EventsPerSecond []int32               `json:"events_per_second"`
	IssuerSeconds   []issuer.SecondCounts `json:"issuer_seconds,omitempty"`
	Timeline        []Point               `json:"timeline"`
}

type HostInfo struct {
	CPUModel      string            `json:"cpu_model"`
	Kernel        string            `json:"kernel"`
	RunnerCPUs    int               `json:"runner_cpus"`
	RunnerMemMiB  float64           `json:"runner_mem_mib,omitempty"`
	SluiceProcs   float64           `json:"sluice_gomaxprocs,omitempty"`
	Postgres      map[string]string `json:"postgres,omitempty"`
	SluiceLimitMB float64           `json:"sluice_gomemlimit_mib,omitempty"`
}

// Expected is what the configuration produces with every stream open.
type Expected struct {
	Rooms          int     `json:"rooms"`
	Members        int     `json:"members"`
	Streams        int     `json:"streams"`
	ChangesPerSec  float64 `json:"changes_per_s"`
	MessagesPerSec float64 `json:"message_events_per_s"`
	MembersPerSec  float64 `json:"member_events_per_s"`
	TypingPerSec   float64 `json:"typing_events_per_s"`
	EventsPerSec   float64 `json:"events_per_s"`
}

func expectedOf(rc roomsConfig) Expected {
	ch, msg, mem, typ := rc.expectedRates()
	return Expected{
		Rooms: rc.rooms(), Members: rc.members(), Streams: rc.streams(),
		ChangesPerSec: ch, MessagesPerSec: msg, MembersPerSec: mem, TypingPerSec: typ,
		EventsPerSec: msg + mem + typ,
	}
}

type Delivery struct {
	Expected int64   `json:"expected"`
	Received int64   `json:"received"`
	Pct      float64 `json:"pct"`
}

// PhaseSummary is everything measured between two marks.
type PhaseSummary struct {
	Name     string  `json:"name"`
	From     float64 `json:"from_s"`
	To       float64 `json:"to_s"`
	Duration float64 `json:"duration_s"`

	Events           int64               `json:"events"`
	EventsPerSec     float64             `json:"events_per_s"`
	EventsPerSecPeak float64             `json:"events_per_s_peak"`
	ChangesPerSec    float64             `json:"changes_per_s"`
	Delivery         map[string]Delivery `json:"delivery"`
	Latency          map[string]Summary  `json:"latency"`

	SluiceCPUSeconds   float64 `json:"sluice_cpu_s"`
	CPUMsPer1kEvents   float64 `json:"cpu_ms_per_1k_events,omitempty"`
	CPUMsPer1kChanges  float64 `json:"cpu_ms_per_1k_changes,omitempty"`
	CPUMsPerOpen       float64 `json:"cpu_ms_per_open,omitempty"`
	SluiceCoresAvg     float64 `json:"sluice_cores_avg"`
	SluiceCoresPeak    float64 `json:"sluice_cores_peak"`
	RSSPeakMiB         float64 `json:"rss_peak_mib"`
	GoMemPeakMiB       float64 `json:"go_mem_peak_mib"`
	HeapLivePeakMiB    float64 `json:"heap_live_peak_mib"`
	GoMemLimitMiB      float64 `json:"gomemlimit_mib"`
	GoroutinesPeak     float64 `json:"goroutines_peak"`
	GCCount            float64 `json:"gc_count"`
	GCPauseMaxMs       float64 `json:"gc_pause_max_ms"`
	GCPauseTotalMs     float64 `json:"gc_pause_total_ms"`
	DispatchAvgUs      float64 `json:"dispatch_avg_us"`
	BacklogMaxBytes    int64   `json:"backlog_max_bytes"`
	WALLagMaxBytes     int64   `json:"wal_lag_max_bytes"`
	BacklogFirstQBytes float64 `json:"backlog_first_quarter_bytes"`
	BacklogLastQBytes  float64 `json:"backlog_last_quarter_bytes"`
	SlotLagMaxBytes    int64   `json:"slot_lag_max_bytes"`
	RetainedMaxBytes   int64   `json:"retained_max_bytes"`
	Lagging            float64 `json:"stream_lagging"`
	Dropped            float64 `json:"dropped_events"`
	SluiceRestarts     int     `json:"sluice_restarts"`

	Opens        int64 `json:"open_attempts"`
	OpenFailures int64 `json:"open_failures"`

	IssuerCallsPerSec  float64          `json:"issuer_calls_per_s"`
	IssuerPeakPerSec   int64            `json:"issuer_peak_per_s"`
	IssuerPeakByKind   map[string]int64 `json:"issuer_peak_by_kind,omitempty"`
	IssuerQueuedPeak   int64            `json:"issuer_queued_peak"`
	IssuerInflightPeak int64            `json:"issuer_inflight_peak"`
	IssuerCoresPeak    float64          `json:"issuer_cores_peak"`

	RunnerCoresAvg  float64 `json:"runner_cores_avg"`
	RunnerCoresPeak float64 `json:"runner_cores_peak"`
	RunnerRSSPeak   float64 `json:"runner_rss_peak_mib"`

	Containers map[string]ContainerSummary `json:"containers,omitempty"`
}

type ContainerSummary struct {
	CoresAvg   float64 `json:"cores_avg"`
	CoresPeak  float64 `json:"cores_peak"`
	MemPeakMiB float64 `json:"mem_peak_mib"`
}

type BigTxnResult struct {
	N                int                `json:"n"`
	Rows             int                `json:"rows"`
	Streamed         int                `json:"streamed_recipients"`
	Expected         int64              `json:"expected"`
	Delivered        int64              `json:"delivered"`
	TxnMs            float64            `json:"txn_ms"`
	SentMs           float64            `json:"walsender_sent_ms"`
	DispatchedMs     float64            `json:"sluice_dispatched_ms"`
	LastDeliveryMs   float64            `json:"last_delivery_ms"`
	BacklogMaxBytes  int64              `json:"backlog_max_bytes"`
	SluiceCPUSeconds float64            `json:"sluice_cpu_s"`
	DispatchAvgUs    float64            `json:"dispatch_avg_us"`
	RSSPeakMiB       float64            `json:"rss_peak_mib"`
	GCPauseMaxMs     float64            `json:"gc_pause_max_ms"`
	Lagging          float64            `json:"stream_lagging"`
	Baseline         map[string]Summary `json:"baseline_latency"`
	During           map[string]Summary `json:"during_latency"`
	Error            string             `json:"error,omitempty"`
}

type WaveResult struct {
	N                 int                `json:"n"`
	Mode              string             `json:"mode"`
	LiveBefore        int64              `json:"live_before"`
	Down              int64              `json:"down"`
	Back              int64              `json:"back"`
	Attempts          int64              `json:"open_attempts"`
	P50BackS          float64            `json:"p50_back_s"`
	P90BackS          float64            `json:"p90_back_s"`
	P99BackS          float64            `json:"p99_back_s"`
	AllBackS          float64            `json:"all_back_s"`
	DowntimeP50S      float64            `json:"downtime_p50_s"`
	DowntimeP95S      float64            `json:"downtime_p95_s"`
	DowntimeP99S      float64            `json:"downtime_p99_s"`
	IssuerPeakPerSec  int64              `json:"issuer_peak_per_s"`
	IssuerPeakByKind  map[string]int64   `json:"issuer_peak_by_kind,omitempty"`
	IssuerCalls       int64              `json:"issuer_calls"`
	IssuerUnavailable int64              `json:"issuer_unavailable"`
	FullyResumed      int64              `json:"fully_resumed"`
	NotFullyResumed   int64              `json:"not_fully_resumed"`
	GapsAfterResume   int64              `json:"msg_gaps_after_resume"`
	GapsUnresumed     int64              `json:"msg_gaps_unresumed"`
	SluiceCPUSeconds  float64            `json:"sluice_cpu_s"`
	RSSPeakMiB        float64            `json:"rss_peak_mib"`
	GCPauseMaxMs      float64            `json:"gc_pause_max_ms"`
	RunnerCoresPeak   float64            `json:"runner_cores_peak"`
	Lagging           float64            `json:"stream_lagging"`
	ProxyClosed       int                `json:"proxy_closed,omitempty"`
	RestartMs         float64            `json:"restart_ms,omitempty"`
	Latency           map[string]Summary `json:"latency"`
	Counters          map[string]int64   `json:"counters"`
	Error             string             `json:"error,omitempty"`
}

type CalWindow struct {
	Events            int64   `json:"events"`
	EventsPerSec      float64 `json:"events_per_s"`
	Changes           float64 `json:"changes"`
	CPUSeconds        float64 `json:"cpu_s"`
	CPUMsPer1kEvents  float64 `json:"cpu_ms_per_1k_events"`
	CPUMsPer1kChanges float64 `json:"cpu_ms_per_1k_changes"`
}

type CalibrationResult struct {
	CPUMsPer1kEvents  float64     `json:"cpu_ms_per_1k_events"`
	CPUMsPer1kChanges float64     `json:"cpu_ms_per_1k_changes"`
	CPUMsPerOpen      float64     `json:"cpu_ms_per_open"`
	Windows           []CalWindow `json:"windows"`
}

// Verdict applies the pass criteria to the steady phase.
type Verdict struct {
	Pass    bool     `json:"pass"`
	Reasons []string `json:"reasons,omitempty"`
}

func (e *engine) summarize(name string, a, b mark, lat *LatSet) PhaseSummary {
	pts := e.sampler.snapshotPoints()
	if b.idx >= len(pts) {
		b.idx = len(pts) - 1
	}
	in := pts[a.idx : b.idx+1]
	d := b.at.Sub(a.at).Seconds()
	s := PhaseSummary{
		Name: name, From: a.point.T, To: b.point.T, Duration: d,
		Delivery: map[string]Delivery{},
	}
	if lat != nil {
		s.Latency = lat.Summaries()
	}
	for _, k := range deliveryKinds {
		ex, rx := b.expect[k]-a.expect[k], b.recv[k]-a.recv[k]
		if ex == 0 && rx == 0 {
			continue
		}
		dl := Delivery{Expected: ex, Received: rx}
		if ex > 0 {
			dl.Pct = 100 * float64(rx) / float64(ex)
		}
		s.Delivery[latNames[k]] = dl
	}
	s.Events = b.point.Events - a.point.Events
	if d > 0 {
		s.EventsPerSec = float64(s.Events) / d
		s.ChangesPerSec = (b.point.Changes - a.point.Changes) / d
	}
	e.secMu.Lock()
	from, to := int(a.point.T), min(int(b.point.T), len(e.secs))
	for i := max(from, 0); i < to; i++ {
		s.EventsPerSecPeak = max(s.EventsPerSecPeak, float64(e.secs[i]))
	}
	e.secMu.Unlock()

	s.SluiceCPUSeconds = b.point.SluiceCPU - a.point.SluiceCPU
	if s.Events > 0 {
		s.CPUMsPer1kEvents = s.SluiceCPUSeconds * 1e6 / float64(s.Events)
	}
	if ch := b.point.Changes - a.point.Changes; ch > 0 {
		s.CPUMsPer1kChanges = s.SluiceCPUSeconds * 1e6 / ch
	}
	if d > 0 {
		s.SluiceCoresAvg = s.SluiceCPUSeconds / d
	}
	if dc := b.point.DispatchCount - a.point.DispatchCount; dc > 0 {
		s.DispatchAvgUs = (b.point.DispatchSum - a.point.DispatchSum) / dc * 1e6
	}
	s.GCCount = b.point.GCCount - a.point.GCCount
	s.GCPauseTotalMs = (b.point.GCPauseSum - a.point.GCPauseSum) * 1000
	s.Lagging = b.point.Lagging - a.point.Lagging
	s.Dropped = b.point.Dropped - a.point.Dropped
	s.SluiceRestarts = b.point.SluiceRestarts - a.point.SluiceRestarts
	s.Opens = b.opens - a.opens
	s.OpenFailures = b.fails - a.fails

	var runnerSum float64
	var backlog []float64
	conts := map[string]*ContainerSummary{}
	contN := map[string]int{}
	for i, p := range in {
		if i > 0 {
			s.SluiceCoresPeak = max(s.SluiceCoresPeak, p.SluiceCores)
			s.RunnerCoresPeak = max(s.RunnerCoresPeak, p.RunnerCores)
			s.IssuerCoresPeak = max(s.IssuerCoresPeak, p.IssuerCores)
			runnerSum += p.RunnerCores
		}
		s.RSSPeakMiB = max(s.RSSPeakMiB, p.RSSMiB)
		s.GoMemPeakMiB = max(s.GoMemPeakMiB, p.GoMemMiB)
		s.HeapLivePeakMiB = max(s.HeapLivePeakMiB, p.HeapLiveMiB)
		s.GoMemLimitMiB = max(s.GoMemLimitMiB, p.GoMemLimitMiB)
		s.GoroutinesPeak = max(s.GoroutinesPeak, p.Goroutines)
		s.GCPauseMaxMs = max(s.GCPauseMaxMs, p.GCPauseMaxMs)
		s.BacklogMaxBytes = max(s.BacklogMaxBytes, p.BacklogBytes)
		s.WALLagMaxBytes = max(s.WALLagMaxBytes, p.WALLagBytes)
		s.SlotLagMaxBytes = max(s.SlotLagMaxBytes, p.SlotLagBytes)
		s.RetainedMaxBytes = max(s.RetainedMaxBytes, p.RetainedBytes)
		s.IssuerQueuedPeak = max(s.IssuerQueuedPeak, p.IssuerQueued)
		s.IssuerInflightPeak = max(s.IssuerInflightPeak, p.IssuerInflight)
		s.RunnerRSSPeak = max(s.RunnerRSSPeak, p.RunnerRSSMiB)
		backlog = append(backlog, float64(p.BacklogBytes))
		for name, c := range p.Containers {
			cs := conts[name]
			if cs == nil {
				cs = &ContainerSummary{}
				conts[name] = cs
			}
			if i > 0 {
				cs.CoresPeak = max(cs.CoresPeak, c.Cores)
				cs.CoresAvg += c.Cores
				contN[name]++
			}
			cs.MemPeakMiB = max(cs.MemPeakMiB, c.MemMiB)
		}
	}
	if len(in) > 1 {
		s.RunnerCoresAvg = runnerSum / float64(len(in)-1)
	}
	if q := len(backlog) / 4; q > 0 {
		s.BacklogFirstQBytes = mean(backlog[:q])
		s.BacklogLastQBytes = mean(backlog[len(backlog)-q:])
	}
	if len(conts) > 0 {
		s.Containers = map[string]ContainerSummary{}
		for name, cs := range conts {
			if n := contN[name]; n > 0 {
				cs.CoresAvg /= float64(n)
			}
			s.Containers[name] = *cs
		}
	}

	// The second a phase starts in mostly belongs to the one before, unless
	// the phase is that short.
	secs := e.sampler.issuerSeconds()
	lo, hi := a.at.Unix()+1, b.at.Unix()
	if hi < lo {
		lo = a.at.Unix()
	}
	var calls int64
	s.IssuerPeakPerSec, s.IssuerPeakByKind, calls = issuerPeak(secs, lo, hi)
	if d > 0 {
		s.IssuerCallsPerSec = float64(calls) / d
	}
	return s
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var t float64
	for _, x := range v {
		t += x
	}
	return t / float64(len(v))
}

// issuerPeak is the busiest second of issuer calls in [from, to], with the
// busiest second of each kind, and the calls in the range.
func issuerPeak(secs []issuer.SecondCounts, from, to int64) (peak int64, byKind map[string]int64, total int64) {
	byKind = map[string]int64{}
	for _, sc := range secs {
		if sc.At < from || sc.At > to {
			continue
		}
		n := sc.N["subscribe"] + sc.N["refresh"] + sc.N["hook"]
		total += n
		peak = max(peak, n)
		for k, v := range sc.N {
			byKind[k] = max(byKind[k], v)
		}
	}
	return peak, byKind, total
}

func (e *engine) report(mStart, mSteady, mEnd mark, arrival, steady *LatSet, arrivalOK bool) RoomsReport {
	rc := e.rc
	r := RoomsReport{
		Scenario: e.cfg.Scenario, Label: rc.Label, Target: e.cfg.TargetImage,
		StartedAt: e.started.UTC(), FinishedAt: time.Now().UTC(), Seed: e.seed,
		Config: rc, Env: reportEnv(), Expected: expectedOf(rc),
		ArrivalSeconds: math.Round(e.arrivalT.Seconds()*1000) / 1000, ArrivalComplete: arrivalOK,
		BigTxns: e.bigs, Reconnects: e.waves, Calibration: e.cal,
		Counters: e.ctr.snapshot(),
	}
	r.Host = HostInfo{
		CPUModel: procstat.CPUModel(), Kernel: procstat.Kernel(), RunnerCPUs: runtime.NumCPU(),
		SluiceProcs: mSteady.point.GoMaxProcs, SluiceLimitMB: mSteady.point.GoMemLimitMiB,
	}
	r.Host.Postgres = e.pgInfo
	if m := cgroupMemoryLimit(); m > 0 {
		r.Host.RunnerMemMiB = float64(m) / mib
	}
	if live := mSteady.point.LiveClients; live > 0 {
		r.KiBPerStream = (mSteady.point.HeapLiveMiB - mStart.point.HeapLiveMiB) * 1024 / float64(live)
	}
	pa := e.summarize("arrival", mStart, mSteady, arrival)
	if opened := e.firstReady.Load(); opened > 0 {
		pa.CPUMsPerOpen = pa.SluiceCPUSeconds * 1000 / float64(opened)
	}
	ps := e.summarize("steady", mSteady, mEnd, steady)
	whole := e.summarize("run", mStart, mEnd, &e.lat.total)
	r.Phases = []PhaseSummary{pa, ps, whole}
	if r.Calibration != nil {
		r.Calibration.CPUMsPerOpen = pa.CPUMsPerOpen
	}
	r.Verdict = e.verdict(ps, whole)
	windowsMu.Lock()
	r.Windows = append([]window(nil), e.windows...)
	windowsMu.Unlock()
	e.secMu.Lock()
	r.EventsPerSecond = append([]int32(nil), e.secs...)
	e.secMu.Unlock()
	r.IssuerSeconds = e.sampler.issuerSeconds()
	r.Timeline = e.sampler.snapshotPoints()
	return r
}

// verdict applies the criteria: no stream_lagging; p95 under LOAD_P95_MAX for
// every kind of event Sluice delivered; Go's memory under GOMEMLIMIT and no
// restart nobody asked for; the backlog bounded and not growing; nothing lost.
func (e *engine) verdict(steady, whole PhaseSummary) Verdict {
	var v Verdict
	fail := func(f string, a ...any) { v.Reasons = append(v.Reasons, fmt.Sprintf(f, a...)) }
	if whole.Lagging > 0 {
		fail("%.0f streams closed with stream_lagging", whole.Lagging)
	}
	for _, k := range deliveryKinds {
		if s, ok := steady.Latency[latNames[k]]; ok && s.Count > 0 && s.P95 > float64(e.rc.P95Max)/float64(time.Millisecond) {
			fail("%s p95 %.0f ms > %s", latNames[k], s.P95, e.rc.P95Max)
		}
	}
	if whole.GoMemLimitMiB > 0 && whole.GoMemPeakMiB >= whole.GoMemLimitMiB {
		fail("Go memory peaked at %.0f MiB, GOMEMLIMIT is %.0f MiB", whole.GoMemPeakMiB, whole.GoMemLimitMiB)
	}
	if whole.SluiceRestarts > 0 && !(e.cfg.Scenario == "reconnect" && e.rc.ReconnectMode == "restart") {
		fail("Sluice restarted %d times (OOM kill?)", whole.SluiceRestarts)
	}
	if steady.BacklogMaxBytes > e.rc.SlotLagMax {
		fail("backlog peaked at %.1f MiB > %d MiB", float64(steady.BacklogMaxBytes)/mib, e.rc.SlotLagMax>>20)
	}
	if steady.BacklogLastQBytes > 2*steady.BacklogFirstQBytes+4*mib {
		fail("backlog grew from %.1f to %.1f MiB over the steady phase", steady.BacklogFirstQBytes/mib, steady.BacklogLastQBytes/mib)
	}
	if e.cfg.Scenario != "reconnect" {
		for name, d := range whole.Delivery {
			if d.Expected > 0 && d.Pct < 100*e.rc.DeliveryMin {
				fail("%s delivered %.2f%% < %.0f%%", name, d.Pct, 100*e.rc.DeliveryMin)
			}
		}
	}
	if steady.OpenFailures > 0 && e.cfg.Scenario != "reconnect" {
		fail("%d stream opens failed in the steady phase", steady.OpenFailures)
	}
	v.Pass = len(v.Reasons) == 0
	return v
}

// reportEnv records the knobs a run was made with: the load's, Sluice's, and
// the container limits the compose file passed through.
func reportEnv() map[string]string {
	out := map[string]string{}
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		switch {
		case strings.HasPrefix(k, "LOAD_PRIVATE"), strings.Contains(k, "PASSWORD"), strings.Contains(k, "BEARER"),
			k == "LOAD_DB_URL":
			continue
		case strings.HasPrefix(k, "LOAD_"), strings.HasPrefix(k, "SLUICE_"), strings.HasPrefix(k, "POSTGRES_IMAGE"),
			strings.HasSuffix(k, "_CPUSET"), strings.HasSuffix(k, "_CPUS"):
			out[k] = v
		}
	}
	return out
}

func cgroupMemoryLimit() int64 {
	b, err := os.ReadFile("/sys/fs/cgroup/memory.max")
	if err != nil {
		return 0
	}
	var n int64
	if _, err := fmt.Sscan(strings.TrimSpace(string(b)), &n); err != nil {
		return 0
	}
	return n
}

func writeRoomsReport(dir string, r RoomsReport) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	name := r.Scenario
	if r.Label != "" {
		name += "-" + sanitize(r.Label)
	}
	base := fmt.Sprintf("%s-%s", name, r.FinishedAt.Format("20060102T150405Z"))
	raw, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return "", err
	}
	jsonPath := filepath.Join(dir, base+".json")
	if err := os.WriteFile(jsonPath, append(raw, '\n'), 0o644); err != nil {
		return "", err
	}
	if err := os.WriteFile(filepath.Join(dir, base+".md"), []byte(roomsMarkdown(r)), 0o644); err != nil {
		return "", err
	}
	_ = os.WriteFile(filepath.Join(dir, r.Scenario+"-latest.json"), append(raw, '\n'), 0o644)
	return jsonPath, nil
}

func sanitize(s string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			return r
		}
		return '-'
	}, s)
}

func roomsMarkdown(r RoomsReport) string {
	var b strings.Builder
	title := r.Scenario
	if r.Label != "" {
		title += " — " + r.Label
	}
	fmt.Fprintf(&b, "# Sluice load test — %s\n\n", title)
	fmt.Fprintf(&b, "- Target: `%s`\n", r.Target)
	fmt.Fprintf(&b, "- Host: %s, kernel %s; runner sees %d CPUs\n", r.Host.CPUModel, r.Host.Kernel, r.Host.RunnerCPUs)
	fmt.Fprintf(&b, "- Sluice: GOMAXPROCS %.0f, GOMEMLIMIT %.0f MiB, cpus %s, memory %s, cpuset %s\n",
		r.Host.SluiceProcs, r.Host.SluiceLimitMB, orDash(r.Env["SLUICE_CPUS"]), orDash(r.Env["SLUICE_MEM_LIMIT"]), orDash(r.Env["SLUICE_CPUSET"]))
	pgVersion, _, _ := strings.Cut(r.Host.Postgres["version"], " on ")
	fmt.Fprintf(&b, "- PostgreSQL: %s, synchronous_commit %s\n", orDash(pgVersion), orDash(r.Host.Postgres["synchronous_commit"]))
	fmt.Fprintf(&b, "- Started %s, finished %s, seed %d\n", r.StartedAt.Format(time.RFC3339), r.FinishedAt.Format(time.RFC3339), r.Seed)
	verdict := "PASS"
	if !r.Verdict.Pass {
		verdict = "FAIL: " + strings.Join(r.Verdict.Reasons, "; ")
	}
	fmt.Fprintf(&b, "- **Verdict: %s**\n\n", verdict)

	fmt.Fprintf(&b, "## Load\n\n")
	fmt.Fprintf(&b, "| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |\n| --- | ---: | ---: | --- | --- | ---: | ---: |\n")
	var classes []string
	for _, k := range r.Config.Classes {
		classes = append(classes, fmt.Sprintf("%d × %d / %d / %g", k.Count, k.Members, k.Streams, k.Typing))
	}
	typing := "off"
	if r.Config.TypingEnabled {
		typing = "every " + r.Config.TypingEvery.String()
	}
	fmt.Fprintf(&b, "| %s | %d | %s | %s | %s | %s | %s |\n\n", strings.Join(classes, ", "), r.Config.Shapes,
		r.Config.MsgEvery, typing, r.Config.ReplicaIdentity, r.Config.Arrival, r.Config.Duration)
	x := r.Expected
	fmt.Fprintf(&b, "Expected with every stream open: %d streams, %.0f changes/s, %.0f events/s (%.0f messages, %.0f member updates, %.0f typing).\n\n",
		x.Streams, x.ChangesPerSec, x.EventsPerSec, x.MessagesPerSec, x.MembersPerSec, x.TypingPerSec)

	fmt.Fprintf(&b, "## Phases\n\n")
	fmt.Fprintf(&b, "| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |\n")
	fmt.Fprintf(&b, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, p := range r.Phases {
		fmt.Fprintf(&b, "| %s | %.0f | %.0f (%.0f) | %.0f | %.2f | %.1f | %.1f | %.2f / %.2f | %.0f | %.0f / %.0f | %.2f | %.2f | %.2f | %.0f | %.1f (%d) | %.2f |\n",
			p.Name, p.Duration, p.EventsPerSec, p.EventsPerSecPeak, p.ChangesPerSec, p.SluiceCPUSeconds, p.CPUMsPer1kEvents, p.CPUMsPer1kChanges,
			p.SluiceCoresAvg, p.SluiceCoresPeak, p.RSSPeakMiB, p.GoMemPeakMiB, p.GoMemLimitMiB, p.GCPauseMaxMs,
			float64(p.BacklogMaxBytes)/mib, float64(p.SlotLagMaxBytes)/mib, p.Lagging, p.IssuerCallsPerSec, p.IssuerPeakPerSec, p.RunnerCoresPeak)
	}
	fmt.Fprintf(&b, "\nArrival: %.1f s to open every stream (complete: %t); %.2f ms of Sluice CPU per open; %.1f KiB of live heap per stream after arrival.\n\n",
		r.ArrivalSeconds, r.ArrivalComplete, r.Phases[0].CPUMsPerOpen, r.KiBPerStream)

	fmt.Fprintf(&b, "## Latency (ms)\n\n| Phase | Kind | Count | p50 | p95 | p99 | max |\n| --- | --- | ---: | ---: | ---: | ---: | ---: |\n")
	for _, p := range r.Phases {
		kinds := make([]string, 0, len(p.Latency))
		for k := range p.Latency {
			kinds = append(kinds, k)
		}
		slices.Sort(kinds)
		for _, k := range kinds {
			s := p.Latency[k]
			fmt.Fprintf(&b, "| %s | %s | %d | %.1f | %.1f | %.1f | %.1f |\n", p.Name, k, s.Count, s.P50, s.P95, s.P99, s.Max)
		}
	}
	b.WriteByte('\n')

	if len(r.Phases) > 0 {
		fmt.Fprintf(&b, "## Delivery (whole run)\n\n| Kind | Expected | Received | %% |\n| --- | ---: | ---: | ---: |\n")
		whole := r.Phases[len(r.Phases)-1]
		for _, k := range deliveryKinds {
			if d, ok := whole.Delivery[latNames[k]]; ok {
				fmt.Fprintf(&b, "| %s | %d | %d | %.2f |\n", latNames[k], d.Expected, d.Received, d.Pct)
			}
		}
		b.WriteByte('\n')
	}

	if len(r.BigTxns) > 0 {
		fmt.Fprintf(&b, "## Big transactions\n\n| # | Rows | Streamed | Delivered | Txn ms | Sent ms | Dispatched ms | Last delivery ms | Backlog max MiB | Sluice CPU s | Others' message p95 before → during |\n")
		fmt.Fprintf(&b, "| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |\n")
		for _, t := range r.BigTxns {
			fmt.Fprintf(&b, "| %d | %d | %d | %d/%d | %.0f | %.0f | %.0f | %.0f | %.1f | %.2f | %.0f → %.0f |\n",
				t.N, t.Rows, t.Streamed, t.Delivered, t.Expected, t.TxnMs, t.SentMs, t.DispatchedMs, t.LastDeliveryMs,
				float64(t.BacklogMaxBytes)/mib, t.SluiceCPUSeconds, t.Baseline["message"].P95, t.During["message"].P95)
		}
		b.WriteByte('\n')
	}

	if len(r.Reconnects) > 0 {
		fmt.Fprintf(&b, "## Reconnect waves\n\n| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |\n")
		fmt.Fprintf(&b, "| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |\n")
		for _, w := range r.Reconnects {
			fmt.Fprintf(&b, "| %d | %s | %d / %d | %.2f / %.2f / %.2f / %.2f | %.2f / %.2f | %d | %d | %d | %d of %d | %d | %.2f |\n",
				w.N, w.Mode, w.Down, w.Back, w.P50BackS, w.P90BackS, w.P99BackS, w.AllBackS, w.DowntimeP50S, w.DowntimeP99S,
				w.Attempts, w.IssuerPeakPerSec, w.IssuerUnavailable, w.FullyResumed, w.FullyResumed+w.NotFullyResumed,
				w.GapsAfterResume, w.SluiceCPUSeconds)
		}
		b.WriteByte('\n')
	}

	if r.Calibration != nil {
		fmt.Fprintf(&b, "## Calibration\n\n**%.1f ms of Sluice CPU per 1000 events** (median of %d windows), %.2f ms per stream open.\n\n",
			r.Calibration.CPUMsPer1kEvents, len(r.Calibration.Windows), r.Calibration.CPUMsPerOpen)
		fmt.Fprintf(&b, "| Window | Events | Events/s | CPU s | ms / 1k events |\n| ---: | ---: | ---: | ---: | ---: |\n")
		for i, w := range r.Calibration.Windows {
			fmt.Fprintf(&b, "| %d | %d | %.0f | %.2f | %.1f |\n", i+1, w.Events, w.EventsPerSec, w.CPUSeconds, w.CPUMsPer1kEvents)
		}
		b.WriteByte('\n')
	}

	keys := make([]string, 0, len(r.Counters))
	for k := range r.Counters {
		keys = append(keys, k)
	}
	slices.Sort(keys)
	fmt.Fprintf(&b, "## Counters\n\n| Counter | Value |\n| --- | ---: |\n")
	for _, k := range keys {
		fmt.Fprintf(&b, "| %s | %d |\n", k, r.Counters[k])
	}
	return b.String()
}

func orDash(s string) string {
	if s == "" {
		return "—"
	}
	return s
}

func printRoomsSummary(r RoomsReport) {
	fmt.Println()
	fmt.Printf("== %s %s against %s ==\n", r.Scenario, r.Label, r.Target)
	for _, p := range r.Phases {
		fmt.Printf("  %-8s %6.0fs  events/s %7.0f (peak %6.0f)  cpu ms/1k %6.1f  rss %5.0f MiB  backlog %6.2f MiB  lagging %.0f\n",
			p.Name, p.Duration, p.EventsPerSec, p.EventsPerSecPeak, p.CPUMsPer1kEvents, p.RSSPeakMiB, float64(p.BacklogMaxBytes)/mib, p.Lagging)
	}
	if r.Calibration != nil {
		fmt.Printf("  calibration: %.1f ms CPU per 1000 events, %.2f ms per open\n", r.Calibration.CPUMsPer1kEvents, r.Calibration.CPUMsPerOpen)
	}
	if r.Verdict.Pass {
		fmt.Println("  verdict: PASS")
	} else {
		fmt.Println("  verdict: FAIL — " + strings.Join(r.Verdict.Reasons, "; "))
	}
}
