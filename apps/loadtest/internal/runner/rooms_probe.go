package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/sse"
)

// How far back can a stream resume each table? Sluice keeps every table's
// changes for SLUICE_RING_MAX_AGE within its budget (SLUICE_RING_MEMORY),
// delivered to someone or not, and reports how far back that covers every
// table (sluice_resume_buffer_seconds). The probe asks Sluice itself: it opens
// short streams that resume each table from positions of known age and reads
// the `resumed` of every subscription.

// lsnTrail remembers, per table, which commit LSN the run saw at what time.
type lsnTrail struct {
	mu sync.Mutex
	m  map[string][]lsnAt
}

type lsnAt struct {
	at  time.Time
	lsn string
}

const trailKeep = 150 * time.Second

func (t *lsnTrail) note(table, lsn string, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string][]lsnAt{}
	}
	s := t.m[table]
	if n := len(s); n > 0 && (s[n-1].lsn == lsn || now.Sub(s[n-1].at) < 100*time.Millisecond) {
		return
	}
	s = append(s, lsnAt{now, lsn})
	i := 0
	for i < len(s) && now.Sub(s[i].at) > trailKeep {
		i++
	}
	t.m[table] = s[i:]
}

// at returns the newest position seen at or before `when`, and when it was
// seen. ok is false when the trail does not go back that far.
func (t *lsnTrail) at(table string, when time.Time) (lsnAt, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	s := t.m[table]
	i, _ := slices.BinarySearchFunc(s, when, func(e lsnAt, w time.Time) int { return e.at.Compare(w) })
	// s[i] is the first entry after when (or equal).
	if i < len(s) && s[i].at.Equal(when) {
		return s[i], true
	}
	if i == 0 {
		return lsnAt{}, false
	}
	return s[i-1], true
}

// ProbeRound is one round of the probe: per table, the ages Sluice resumed
// and the ones it could not.
type ProbeRound struct {
	T       float64                    `json:"t"`
	Phase   string                     `json:"phase"`
	Tables  map[string]*ProbeTable     `json:"tables"`
	Derived map[string]DerivedCoverage `json:"derived,omitempty"`
}

type ProbeTable struct {
	Resumed    []float64 `json:"resumed_s"`
	NotResumed []float64 `json:"not_resumed_s"`
	// CoverS is the oldest age resumed with every younger age resumed too.
	CoverS float64 `json:"cover_s"`
}

// DerivedCoverage is a table's change rate, and how far back Sluice reports
// its buffer covers every table (sluice_resume_buffer_seconds).
type DerivedCoverage struct {
	ChangesPerSec float64 `json:"changes_per_s"`
	CoverS        float64 `json:"cover_s"`
}

var probeTables = []string{"public.room_messages", "public.room_members"}

func (e *engine) probeLoop(ctx context.Context) {
	if e.rc.ProbeEvery <= 0 {
		return
	}
	t := time.NewTicker(e.rc.ProbeEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if r, ok := e.probe(ctx); ok {
				e.probeMu.Lock()
				e.probes = append(e.probes, r)
				e.probeMu.Unlock()
				parts := []string{}
				for _, tb := range probeTables {
					if pt := r.Tables[tb]; pt != nil {
						parts = append(parts, strings.TrimPrefix(tb, "public.")+"="+fmtS(pt.CoverS))
					}
				}
				e.log.Info("resume buffer", "covers", strings.Join(parts, " "))
			}
		}
	}
}

func fmtS(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) + "s" }

// probe runs one round with the identity of a live client, whose grants it
// shares; its streams are not counted as the run's.
func (e *engine) probe(ctx context.Context) (ProbeRound, bool) {
	var live []*rclient
	for _, c := range e.clients {
		if c.live.Load() {
			live = append(live, c)
		}
	}
	if len(live) == 0 {
		return ProbeRound{}, false
	}
	c := live[rand.IntN(len(live))]
	subs := []json.RawMessage{}
	labels := map[string]string{}
	for _, s := range c.subs[:min(2, len(c.subs))] {
		if s.Table == "" {
			continue
		}
		var spec map[string]any
		_ = json.Unmarshal(s.JSON, &spec)
		spec["sub"] = "probe-" + s.Label
		b, _ := json.Marshal(spec)
		subs = append(subs, b)
		labels["probe-"+s.Label] = s.Table
	}
	now := time.Now()
	r := ProbeRound{T: now.Sub(e.started).Seconds(), Phase: e.phaseName(), Tables: map[string]*ProbeTable{}}
	for _, age := range e.rc.ProbeAges {
		resume := map[string]string{}
		for _, tb := range probeTables {
			if p, ok := e.trail.at(tb, now.Add(-age)); ok {
				resume[tb] = p.lsn
			}
		}
		if len(resume) == 0 {
			continue
		}
		results, err := e.probeOnce(ctx, c, subs, resume)
		if err != nil {
			e.count("probe_failed")
			continue
		}
		for _, res := range results {
			tb := labels[res.Sub]
			if tb == "" || res.Resumed == nil || resume[tb] == "" {
				continue
			}
			pt := r.Tables[tb]
			if pt == nil {
				pt = &ProbeTable{}
				r.Tables[tb] = pt
			}
			if *res.Resumed {
				pt.Resumed = append(pt.Resumed, age.Seconds())
			} else {
				pt.NotResumed = append(pt.NotResumed, age.Seconds())
			}
		}
	}
	for _, pt := range r.Tables {
		minNo := 1e9
		for _, a := range pt.NotResumed {
			minNo = min(minNo, a)
		}
		for _, a := range pt.Resumed {
			if a < minNo {
				pt.CoverS = max(pt.CoverS, a)
			}
		}
	}
	r.Derived = e.derivedCoverage()
	return r, true
}

func (e *engine) probeOnce(ctx context.Context, c *rclient, subs []json.RawMessage, resume map[string]string) ([]wireResult, error) {
	body, _ := json.Marshal(map[string]any{"subscriptions": subs, "resume": resume})
	pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(pctx, http.MethodPost, e.cfg.SluiceURL+"/stream", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.stamp())
	req.Header.Set("Content-Type", "application/json")
	resp, err := e.streamHTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	var out []wireResult
	_ = sse.Read(resp.Body, func(ev sse.Event) bool {
		if ev.Name != "ready" {
			return true
		}
		var r struct {
			Subscriptions []wireResult `json:"subscriptions"`
		}
		_ = json.Unmarshal(ev.Data, &r)
		out = r.Subscriptions
		return false
	})
	return out, nil
}

// derivedCoverage is each table's change rate over the last
// ProbeDerivedWindow, with the coverage Sluice last reported.
func (e *engine) derivedCoverage() map[string]DerivedCoverage {
	pts := e.sampler.snapshotPoints()
	if len(pts) < 2 {
		return nil
	}
	// The mean rate over the last minute: messages come in bursts, so one
	// sampling interval says little.
	sum := map[string]float64{}
	n := 0
	for i := len(pts) - 1; i > 0 && pts[len(pts)-1].T-pts[i].T < e.rc.ProbeDerivedWindow.Seconds(); i-- {
		for tb, r := range pts[i].TableRates {
			sum[tb] += r
		}
		n++
	}
	out := map[string]DerivedCoverage{}
	for tb, total := range sum {
		out[tb] = DerivedCoverage{ChangesPerSec: total / float64(n), CoverS: pts[len(pts)-1].ResumeBufferS}
	}
	return out
}

// ProbeSummary is the median coverage per table over the rounds of a phase.
type ProbeSummary struct {
	Rounds         int                `json:"rounds"`
	CoverMedianS   map[string]float64 `json:"cover_median_s"`
	CoverMinS      map[string]float64 `json:"cover_min_s"`
	DerivedMedianS map[string]float64 `json:"derived_median_s"`
	RateMedian     map[string]float64 `json:"changes_per_s_median"`
}

func summarizeProbes(rounds []ProbeRound, phase string) *ProbeSummary {
	cover := map[string][]float64{}
	derived := map[string][]float64{}
	rates := map[string][]float64{}
	n := 0
	for _, r := range rounds {
		if phase != "" && r.Phase != phase {
			continue
		}
		n++
		for tb, pt := range r.Tables {
			cover[tb] = append(cover[tb], pt.CoverS)
		}
		for tb, d := range r.Derived {
			derived[tb] = append(derived[tb], d.CoverS)
			rates[tb] = append(rates[tb], d.ChangesPerSec)
		}
	}
	if n == 0 {
		return nil
	}
	s := &ProbeSummary{Rounds: n, CoverMedianS: map[string]float64{}, CoverMinS: map[string]float64{},
		DerivedMedianS: map[string]float64{}, RateMedian: map[string]float64{}}
	for tb, v := range cover {
		s.CoverMedianS[tb] = median(v)
		s.CoverMinS[tb] = slices.Min(v)
	}
	for tb, v := range derived {
		s.DerivedMedianS[tb] = median(v)
		s.RateMedian[tb] = median(rates[tb])
	}
	return s
}
