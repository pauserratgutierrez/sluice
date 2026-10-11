package runner

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
)

// Summarize reads the reports of the production-shaped profiles in dirs and
// writes Markdown tables of them to w: one row per label, with the median of
// its repetitions (labels that differ only in a trailing -rN).
//
//	loadtest summarize results/phase2/raw
func Summarize(w io.Writer, dirs []string) error {
	var reps []RoomsReport
	for _, d := range dirs {
		// Reports kept in git are compressed (.json.gz).
		files, err := filepath.Glob(filepath.Join(d, "*.json"))
		if err != nil {
			return err
		}
		gz, err := filepath.Glob(filepath.Join(d, "*.json.gz"))
		if err != nil {
			return err
		}
		for _, f := range append(files, gz...) {
			if strings.HasSuffix(f, "-latest.json") {
				continue
			}
			b, err := readReport(f)
			if err != nil {
				return err
			}
			var r RoomsReport
			if json.Unmarshal(b, &r) != nil || len(r.Phases) == 0 || r.Config.Classes == nil {
				continue
			}
			reps = append(reps, r)
		}
	}
	groups := map[string][]RoomsReport{}
	var order []string
	for _, r := range reps {
		g := r.Scenario + " " + repSuffix.ReplaceAllString(r.Label, "")
		if _, ok := groups[g]; !ok {
			order = append(order, g)
		}
		groups[g] = append(groups[g], r)
	}
	slices.Sort(order)

	fmt.Fprintf(w, "## Steady phase (median of the repetitions)\n\n")
	fmt.Fprintf(w, "| Run | n | Pass | Streams | Changes/s | Events/s (peak) | msg p50/p95/p99 ms | member p95/p99 ms | typing p95/p99 ms | CPU ms / 1k events | CPU ms / 1k changes | Cores avg | CPU ms / open | RSS peak MiB | Go mem / limit MiB | KiB/stream | GC max ms | GC ms/min | Backlog max MiB | Slot lag max MiB | Issuer calls/s (peak) | Runner cores peak | Delivery min %% |\n")
	fmt.Fprintf(w, "| --- | ---: | ---: | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n")
	for _, g := range order {
		rs := groups[g]
		if rs[0].Scenario == "ramp" {
			continue
		}
		pass := 0
		var col = func(f func(r RoomsReport) float64) float64 {
			v := make([]float64, 0, len(rs))
			for _, r := range rs {
				v = append(v, f(r))
			}
			return median(v)
		}
		for _, r := range rs {
			if r.Verdict.Pass {
				pass++
			}
		}
		st := func(r RoomsReport) PhaseSummary { return phase(r, "steady") }
		lat := func(kind string, q func(Summary) float64) float64 {
			return col(func(r RoomsReport) float64 { return q(st(r).Latency[kind]) })
		}
		p50 := func(s Summary) float64 { return s.P50 }
		p95 := func(s Summary) float64 { return s.P95 }
		p99 := func(s Summary) float64 { return s.P99 }
		fmt.Fprintf(w, "| %s | %d | %d | %d | %.0f | %.0f (%.0f) | %.1f / %.1f / %.1f | %.1f / %.1f | %.1f / %.1f | %.1f | %.1f | %.3f | %.2f | %.0f | %.0f / %.0f | %.1f | %.1f | %.1f | %.2f | %.2f | %.1f (%.0f) | %.2f | %.2f |\n",
			g, len(rs), pass, rs[0].Expected.Streams,
			col(func(r RoomsReport) float64 { return st(r).ChangesPerSec }),
			col(func(r RoomsReport) float64 { return st(r).EventsPerSec }),
			col(func(r RoomsReport) float64 { return st(r).EventsPerSecPeak }),
			lat("message", p50), lat("message", p95), lat("message", p99),
			lat("member", p95), lat("member", p99), lat("typing", p95), lat("typing", p99),
			col(func(r RoomsReport) float64 { return st(r).CPUMsPer1kEvents }),
			col(func(r RoomsReport) float64 { return st(r).CPUMsPer1kChanges }),
			col(func(r RoomsReport) float64 { return st(r).SluiceCoresAvg }),
			col(func(r RoomsReport) float64 { return phase(r, "arrival").CPUMsPerOpen }),
			col(func(r RoomsReport) float64 { return phase(r, "run").RSSPeakMiB }),
			col(func(r RoomsReport) float64 { return phase(r, "run").GoMemPeakMiB }),
			col(func(r RoomsReport) float64 { return phase(r, "run").GoMemLimitMiB }),
			col(func(r RoomsReport) float64 { return r.KiBPerStream }),
			col(func(r RoomsReport) float64 { return st(r).GCPauseMaxMs }),
			col(func(r RoomsReport) float64 {
				s := st(r)
				if s.Duration <= 0 {
					return 0
				}
				return s.GCPauseTotalMs / s.Duration * 60
			}),
			col(func(r RoomsReport) float64 { return float64(st(r).BacklogMaxBytes) / mib }),
			col(func(r RoomsReport) float64 { return float64(st(r).SlotLagMaxBytes) / mib }),
			col(func(r RoomsReport) float64 { return st(r).IssuerCallsPerSec }),
			col(func(r RoomsReport) float64 { return float64(st(r).IssuerPeakPerSec) }),
			col(func(r RoomsReport) float64 { return st(r).RunnerCoresPeak }),
			col(func(r RoomsReport) float64 {
				low := 100.0
				for _, d := range phase(r, "run").Delivery {
					if d.Expected > 0 {
						low = min(low, d.Pct)
					}
				}
				return low
			}),
		)
	}
	fmt.Fprintln(w)

	// Resume buffer coverage.
	fmt.Fprintf(w, "## Resume buffer coverage (median over the runs' probe rounds)\n\n| Run | Table | Changes/s | Probed s | Probed min s | Derived s |\n| --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, g := range order {
		var rounds []ProbeRound
		for _, r := range groups[g] {
			rounds = append(rounds, r.Probes...)
		}
		s := summarizeProbes(rounds, "")
		if s == nil {
			continue
		}
		for _, tb := range probeTables {
			if _, ok := s.DerivedMedianS[tb]; !ok {
				continue
			}
			fmt.Fprintf(w, "| %s | %s | %.0f | %s | %s | %.1f |\n", g, tb, s.RateMedian[tb], optS(s.CoverMedianS, tb), optS(s.CoverMinS, tb), s.DerivedMedianS[tb])
		}
	}
	fmt.Fprintln(w)

	// Reconnect waves.
	header := false
	for _, g := range order {
		var waves []WaveResult
		for _, r := range groups[g] {
			waves = append(waves, r.Reconnects...)
		}
		if len(waves) == 0 {
			continue
		}
		if !header {
			header = true
			fmt.Fprintf(w, "## Reconnect waves (median over the waves)\n\n| Run | Waves | Down | p50 / p99 / all back s | Downtime p50 / p99 s | Opens per stream | Issuer peak/s | issuer_unavailable | Fully resumed %% | messages / members / notifications resumed %% | Gaps after resume | Sluice CPU s |\n")
			fmt.Fprintf(w, "| --- | ---: | ---: | --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: |\n")
		}
		wc := func(f func(WaveResult) float64) float64 {
			v := make([]float64, 0, len(waves))
			for _, x := range waves {
				v = append(v, f(x))
			}
			return median(v)
		}
		pct := func(tb string) float64 {
			return wc(func(x WaveResult) float64 { return x.ResumeByTable[tb].PctResumed })
		}
		fmt.Fprintf(w, "| %s | %d | %.0f | %.2f / %.2f / %.2f | %.2f / %.2f | %.2f | %.0f | %.0f | %.1f | %.1f / %.1f / %.1f | %.0f | %.2f |\n",
			g, len(waves), wc(func(x WaveResult) float64 { return float64(x.Down) }),
			wc(func(x WaveResult) float64 { return x.P50BackS }), wc(func(x WaveResult) float64 { return x.P99BackS }),
			wc(func(x WaveResult) float64 { return x.AllBackS }),
			wc(func(x WaveResult) float64 { return x.DowntimeP50S }), wc(func(x WaveResult) float64 { return x.DowntimeP99S }),
			wc(func(x WaveResult) float64 {
				if x.Down == 0 {
					return 0
				}
				return float64(x.Attempts) / float64(x.Down)
			}),
			wc(func(x WaveResult) float64 { return float64(x.IssuerPeakPerSec) }),
			wc(func(x WaveResult) float64 { return float64(x.IssuerUnavailable) }),
			wc(func(x WaveResult) float64 {
				n := x.FullyResumed + x.NotFullyResumed
				if n == 0 {
					return 0
				}
				return 100 * float64(x.FullyResumed) / float64(n)
			}),
			pct("public.room_messages"), pct("public.room_members"), pct("public.notifications"),
			wc(func(x WaveResult) float64 { return float64(x.GapsAfterResume) }),
			wc(func(x WaveResult) float64 { return x.SluiceCPUSeconds }))
	}
	if header {
		fmt.Fprintln(w)
	}

	// Big transactions, by size.
	type bigKey struct {
		group string
		rows  int
	}
	bigs := map[bigKey][]BigTxnResult{}
	var bigOrder []bigKey
	for _, g := range order {
		for _, r := range groups[g] {
			for _, b := range r.BigTxns {
				k := bigKey{g, b.Rows}
				if _, ok := bigs[k]; !ok {
					bigOrder = append(bigOrder, k)
				}
				bigs[k] = append(bigs[k], b)
			}
		}
	}
	if len(bigOrder) > 0 {
		fmt.Fprintf(w, "## Big transactions (median per size)\n\n| Run | Rows | n | Streamed | Txn ms | Walsender sent ms | Sluice dispatched ms | Last delivery ms | Backlog max MiB | Sluice CPU s | Others' message p95 / max before | during |\n")
		fmt.Fprintf(w, "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
		for _, k := range bigOrder {
			bs := bigs[k]
			bc := func(f func(BigTxnResult) float64) float64 {
				v := make([]float64, 0, len(bs))
				for _, x := range bs {
					v = append(v, f(x))
				}
				return median(v)
			}
			fmt.Fprintf(w, "| %s | %d | %d | %.0f | %.0f | %.0f | %.0f | %.0f | %.1f | %.2f | %.0f / %.0f | %.0f / %.0f |\n",
				k.group, k.rows, len(bs), bc(func(x BigTxnResult) float64 { return float64(x.Streamed) }),
				bc(func(x BigTxnResult) float64 { return x.TxnMs }), bc(func(x BigTxnResult) float64 { return x.SentMs }),
				bc(func(x BigTxnResult) float64 { return x.DispatchedMs }), bc(func(x BigTxnResult) float64 { return x.LastDeliveryMs }),
				bc(func(x BigTxnResult) float64 { return float64(x.BacklogMaxBytes) / mib }),
				bc(func(x BigTxnResult) float64 { return x.SluiceCPUSeconds }),
				bc(func(x BigTxnResult) float64 { return x.Baseline["message"].P95 }), bc(func(x BigTxnResult) float64 { return x.Baseline["message"].Max }),
				bc(func(x BigTxnResult) float64 { return x.During["message"].P95 }), bc(func(x BigTxnResult) float64 { return x.During["message"].Max }))
		}
		fmt.Fprintln(w)
	}

	// Ramps.
	for _, g := range order {
		for _, r := range groups[g] {
			if len(r.Ramp) == 0 {
				continue
			}
			fmt.Fprintf(w, "## Ramp %s (memory %s, cpus %s)\n\nLargest passing step: **%d streams**", g, orDash(r.Env["SLUICE_MEM_LIMIT"]), orDash(r.Env["SLUICE_CPUS"]), r.RampMaxStreams)
			if r.RampFailedAt > 0 {
				fmt.Fprintf(w, "; failed at %d (%s). Near the limit: %s", r.RampFailedAt, strings.Join(r.Verdict.Reasons, "; "), orDash(strings.Join(r.RampLimits, "; ")))
			}
			fmt.Fprintf(w, ".\n\n| Streams | Changes/s | Events/s | Cores avg | Go mem / limit MiB | RSS MiB | KiB/stream | GC max ms | msg p95 ms | Pass |\n| ---: | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | --- |\n")
			for _, s := range r.Ramp {
				fmt.Fprintf(w, "| %d | %.0f | %.0f | %.2f | %.0f / %.0f | %.0f | %.1f | %.1f | %.0f | %t |\n", s.Streams, s.Window.ChangesPerSec, s.Window.EventsPerSec,
					s.Window.SluiceCoresAvg, s.Window.GoMemPeakMiB, s.Window.GoMemLimitMiB, s.Window.RSSPeakMiB, s.KiBPerStream,
					s.Window.GCPauseMaxMs, s.Window.Latency["message"].P95, s.Pass)
			}
			fmt.Fprintln(w)
		}
	}
	return nil
}

var repSuffix = regexp.MustCompile(`-r\d+$`)

func phase(r RoomsReport, name string) PhaseSummary {
	for _, p := range r.Phases {
		if p.Name == name {
			return p
		}
	}
	return PhaseSummary{}
}

// readReport reads a JSON report, gunzipping one that ends in .gz.
func readReport(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var r io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		z, err := gzip.NewReader(f)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		defer z.Close()
		r = z
	}
	return io.ReadAll(r)
}
