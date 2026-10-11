package runner

import (
	"context"
	"fmt"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// The ramp profile finds a ceiling. Rooms join in steps of about
// LOAD_RAMP_STEP streams, each with its writer and typists, so streams and
// traffic grow together at the configured mix (with LOAD_MSG_EVERY=0 and
// typing off it is a ramp of idle streams). After each step settles, a
// window of LOAD_RAMP_HOLD is judged by the same criteria as a rooms run; the
// ramp stops at the first step that fails (LOAD_RAMP_AFTER_FAIL more to
// confirm), and names what was near its limit.

// RampStep is one step of a ramp.
type RampStep struct {
	N            int      `json:"n"`
	Rooms        int      `json:"rooms"`
	Streams      int      `json:"streams"`
	Opened       int64    `json:"opened"`
	Expected     Expected `json:"expected"`
	KiBPerStream float64  `json:"kib_per_stream"`
	OpenFailures int64    `json:"open_failures"`
	ArrivalS     float64  `json:"arrival_s"`
	// CPUMsPerOpen is Sluice's CPU from the step's start to its window, per
	// stream the step added: the opens, plus whatever else ran meanwhile.
	CPUMsPerOpen float64      `json:"cpu_ms_per_open"`
	Window       PhaseSummary `json:"window"`
	Pass         bool         `json:"pass"`
	Reasons      []string     `json:"reasons,omitempty"`
	Limits       []string     `json:"limits,omitempty"`
}

func (e *engine) runRamp(ctx context.Context, mStart mark) []RampStep {
	rng := rand.New(rand.NewPCG(e.seed^0x5bd1e995, e.seed))
	order := append([]*room(nil), e.rooms...)
	rng.Shuffle(len(order), func(i, j int) { order[i], order[j] = order[j], order[i] })
	var groups [][]*room
	var cur []*room
	n := 0
	for _, r := range order {
		cur = append(cur, r)
		n += len(r.clients)
		if n >= e.rc.RampStep {
			groups = append(groups, cur)
			cur, n = nil, 0
		}
	}
	if len(cur) > 0 {
		groups = append(groups, cur)
	}

	close(e.trafficOn)
	var steps []RampStep
	var joined []*room
	streams, fails := 0, 0
	for i, g := range groups {
		if ctx.Err() != nil {
			break
		}
		mStep := e.mark(ctx, fmt.Sprintf("ramp-%d", i+1))
		for _, r := range g {
			e.startRoom(r)
			streams += len(r.clients)
		}
		joined = append(joined, g...)
		deadline := time.Now().Add(e.rc.RampArrival + e.rc.OpenTimeout)
		for int(e.firstReady.Load()) < streams && time.Now().Before(deadline) && ctx.Err() == nil {
			time.Sleep(100 * time.Millisecond)
		}
		arrival := time.Since(mStep.at)
		sleepCtx(ctx, e.rc.RampSettle)

		set := &LatSet{}
		e.lat.extra.Store(set)
		mA := e.mark(ctx, fmt.Sprintf("ramp-%d-window", i+1))
		sleepCtx(ctx, e.rc.RampHold)
		mB := e.mark(ctx, fmt.Sprintf("ramp-%d-end", i+1))
		e.lat.extra.Store(nil)

		st := RampStep{
			N: i + 1, Rooms: len(joined), Streams: streams, Opened: e.firstReady.Load(),
			Expected:     expectedOfRooms(e.rc, joined),
			OpenFailures: mB.fails - mStep.fails,
			ArrivalS:     arrival.Seconds(),
			Window:       e.summarize(fmt.Sprintf("ramp-%d", i+1), mA, mB, set),
		}
		if added := streamsOf(g); added > 0 {
			st.CPUMsPerOpen = (mA.point.SluiceCPU - mStep.point.SluiceCPU) * 1000 / float64(added)
		}
		if live := mA.point.LiveClients; live > 0 {
			st.KiBPerStream = (mA.point.HeapLiveMiB - mStart.point.HeapLiveMiB) * 1024 / float64(live)
		}
		st.Reasons = e.windowReasons(st.Window, st.OpenFailures)
		if st.Opened < int64(streams) {
			st.Reasons = append(st.Reasons, fmt.Sprintf("only %d of %d streams opened", st.Opened, streams))
		}
		st.Pass = len(st.Reasons) == 0
		st.Limits = e.limits(st.Window, mA, mB)
		steps = append(steps, st)
		e.log.Info("ramp step", "n", st.N, "streams", streams, "opened", st.Opened, "pass", st.Pass,
			"events_per_s", fmt.Sprintf("%.0f", st.Window.EventsPerSec), "changes_per_s", fmt.Sprintf("%.0f", st.Window.ChangesPerSec),
			"cores", fmt.Sprintf("%.2f", st.Window.SluiceCoresAvg), "go_mem_mib", fmt.Sprintf("%.0f", st.Window.GoMemPeakMiB),
			"message_p95", fmtMs(st.Window.Latency["message"].P95), "kib_per_stream", fmt.Sprintf("%.1f", st.KiBPerStream),
			"reasons", strings.Join(st.Reasons, "; "), "limits", strings.Join(st.Limits, "; "))
		if !st.Pass {
			fails++
			if fails > e.rc.RampAfterFail {
				break
			}
		}
	}
	return steps
}

// startRoom opens a room's streams, spread over LOAD_RAMP_ARRIVAL, and
// starts its writer.
func (e *engine) startRoom(r *room) {
	for _, c := range r.clients {
		delay := time.Duration(0)
		if e.rc.RampArrival > 0 {
			delay = time.Duration(rand.Int64N(int64(e.rc.RampArrival)))
		}
		time.AfterFunc(delay, func() {
			if e.clientsCtx.Err() != nil {
				return
			}
			e.scheduleToken(c, time.Duration(rand.Int64N(int64(max(e.rc.TokenEvery, time.Second)))))
			if c.typist {
				go e.typist(c)
			}
			c.run(e.clientsCtx)
		})
	}
	if e.rc.MsgEvery > 0 {
		go e.roomWriter(r)
	}
}

// windowReasons applies the pass criteria to one window.
func (e *engine) windowReasons(s PhaseSummary, openFailures int64) []string {
	var out []string
	add := func(f string, a ...any) { out = append(out, fmt.Sprintf(f, a...)) }
	if s.Lagging > 0 {
		add("%.0f streams closed with stream_lagging", s.Lagging)
	}
	for _, k := range deliveryKinds {
		if l, ok := s.Latency[latNames[k]]; ok && l.Count > 0 && l.P95 > float64(e.rc.P95Max)/float64(time.Millisecond) {
			add("%s p95 %.0f ms > %s", latNames[k], l.P95, e.rc.P95Max)
		}
	}
	if s.GoMemLimitMiB > 0 && s.GoMemPeakMiB >= s.GoMemLimitMiB {
		add("Go memory %.0f MiB reached GOMEMLIMIT %.0f MiB", s.GoMemPeakMiB, s.GoMemLimitMiB)
	}
	if s.SluiceRestarts > 0 {
		add("Sluice restarted (OOM kill?)")
	}
	if s.BacklogMaxBytes > e.rc.SlotLagMax {
		add("backlog peaked at %.1f MiB", float64(s.BacklogMaxBytes)/mib)
	}
	if s.BacklogLastQBytes > 2*s.BacklogFirstQBytes+4*mib {
		add("backlog grew from %.1f to %.1f MiB", s.BacklogFirstQBytes/mib, s.BacklogLastQBytes/mib)
	}
	for name, d := range s.Delivery {
		if d.Expected >= 100 && d.Pct < 100*e.rc.DeliveryMin {
			add("%s delivered %.2f%%", name, d.Pct)
		}
	}
	if openFailures > 0 {
		add("%d stream opens failed", openFailures)
	}
	return out
}

// limits names what was close to its limit in a window: Sluice's CPU quota,
// its GOMEMLIMIT, the runner's cores, or the issuer's queue.
func (e *engine) limits(s PhaseSummary, a, b mark) []string {
	var out []string
	quota, _ := strconv.ParseFloat(envOr("SLUICE_CPUS", "1"), 64)
	if quota > 0 && s.SluiceCoresAvg >= 0.85*quota {
		out = append(out, fmt.Sprintf("cpu: %.2f of %.0f cores on average", s.SluiceCoresAvg, quota))
	}
	if s.BacklogLastQBytes > 2*s.BacklogFirstQBytes+4*mib {
		out = append(out, "cpu: the backlog grows (Sluice dispatches slower than PostgreSQL sends)")
	}
	if s.GoMemLimitMiB > 0 && s.GoMemPeakMiB >= 0.85*s.GoMemLimitMiB {
		out = append(out, fmt.Sprintf("memory: %.0f of %.0f MiB GOMEMLIMIT", s.GoMemPeakMiB, s.GoMemLimitMiB))
	}
	if cpus := float64(runtime.NumCPU()); cpus > 0 && s.RunnerCoresPeak >= 0.8*cpus {
		out = append(out, fmt.Sprintf("client: the runner used %.1f of its %.0f cores", s.RunnerCoresPeak, cpus))
	}
	if n := b.ctr["open_failed:no_local_port"] - a.ctr["open_failed:no_local_port"]; n > 0 {
		out = append(out, fmt.Sprintf("client: %d opens found no local port", n))
	}
	unavailable := int64(0)
	for k, v := range b.ctr {
		if strings.HasSuffix(k, ":issuer_unavailable") {
			unavailable += v - a.ctr[k]
		}
	}
	if s.IssuerQueuedPeak > 0 || unavailable > 0 {
		out = append(out, fmt.Sprintf("issuer: %d calls queued at peak, %d issuer_unavailable", s.IssuerQueuedPeak, unavailable))
	}
	return out
}

// expectedOfRooms is expectedOf for a subset of the rooms.
func expectedOfRooms(rc roomsConfig, rooms []*room) Expected {
	counts := map[int]int{}
	for _, r := range rooms {
		counts[r.class]++
	}
	sub := rc
	sub.Classes = nil
	for ci, k := range rc.Classes {
		if counts[ci] > 0 {
			k.Count = counts[ci]
			sub.Classes = append(sub.Classes, k)
		}
	}
	return expectedOf(sub)
}

// ramp runs the ramp profile and writes its report.
func (e *engine) ramp(ctx context.Context) error {
	set := &LatSet{}
	e.setPhase("ramp", set)
	mStart := e.mark(ctx, "start")
	go e.probeLoop(e.clientsCtx)
	steps := e.runRamp(ctx, mStart)
	e.trafficCancel()
	sleepCtx(ctx, e.rc.Drain)
	mEnd := e.mark(ctx, "end")
	e.setPhase("end", nil)
	e.clientsCancel()

	rep := e.report(mStart, mStart, mEnd, &LatSet{}, set, true)
	rep.Phases = rep.Phases[len(rep.Phases)-1:]
	rep.Ramp = steps
	rep.Verdict = Verdict{Pass: len(steps) > 0 && steps[0].Pass}
	for _, st := range steps {
		if st.Pass {
			rep.RampMaxStreams = st.Streams
			continue
		}
		rep.RampFailedAt = st.Streams
		rep.Verdict.Reasons = append([]string{fmt.Sprintf("step %d (%d streams) failed", st.N, st.Streams)}, st.Reasons...)
		rep.RampLimits = st.Limits
		break
	}
	path, err := writeRoomsReport(e.cfg.ResultsDir, rep)
	if err != nil {
		return err
	}
	e.log.Info("wrote report", "path", path, "max_passing_streams", rep.RampMaxStreams, "failed_at", rep.RampFailedAt,
		"limits", strings.Join(rep.RampLimits, "; "))
	printRoomsSummary(rep)
	return nil
}

func streamsOf(rooms []*room) int {
	n := 0
	for _, r := range rooms {
		n += len(r.clients)
	}
	return n
}
