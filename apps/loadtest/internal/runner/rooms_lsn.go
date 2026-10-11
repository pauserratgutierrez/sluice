package runner

import (
	"context"
	"slices"
	"time"
)

// Where a change waits between its commit and its stream. Every LOAD_LSN_EVERY
// the runner reads, in one query, PostgreSQL's WAL end and how far its
// walsender has sent (and what Sluice last acknowledged, written and flushed),
// and from /readyz how far Sluice has dispatched. Each position is turned into
// seconds by finding when the WAL end passed it: that is how long ago the
// newest change it covers was committed.

// LSNPoint is one reading of the replication positions.
type LSNPoint struct {
	T         float64 `json:"t"`
	Current   uint64  `json:"current"`
	Sent      uint64  `json:"sent"`
	Write     uint64  `json:"write"`
	Confirmed uint64  `json:"sluice_confirmed"`
}

func (e *engine) lsnLoop(ctx context.Context) {
	every := envDur("LOAD_LSN_EVERY", time.Second)
	if every <= 0 {
		return
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		conf := e.sampler.readyzLSN(ctx)
		var cur, sent, write *string
		err := e.pool.QueryRow(ctx, `
			select pg_current_wal_lsn()::text, r.sent_lsn::text, r.write_lsn::text
			  from pg_replication_slots s
			  left join pg_stat_replication r on r.pid = s.active_pid
			 where s.slot_name = $1`, e.cfg.SlotName).Scan(&cur, &sent, &write)
		if err != nil || cur == nil {
			continue
		}
		p := LSNPoint{T: time.Since(e.started).Seconds(), Confirmed: conf}
		p.Current, _ = parseLSN(*cur)
		if sent != nil {
			p.Sent, _ = parseLSN(*sent)
		}
		if write != nil {
			p.Write, _ = parseLSN(*write)
		}
		e.lsnMu.Lock()
		e.lsns = append(e.lsns, p)
		e.lsnMu.Unlock()
	}
}

// StageLag is, for one phase, how many seconds behind PostgreSQL's WAL end the
// walsender's sent position and Sluice's dispatched position were.
type StageLag struct {
	Phase           string  `json:"phase"`
	Samples         int     `json:"samples"`
	SentLagP50S     float64 `json:"sent_lag_p50_s"`
	SentLagP95S     float64 `json:"sent_lag_p95_s"`
	SentLagMaxS     float64 `json:"sent_lag_max_s"`
	DispatchLagP50S float64 `json:"dispatched_lag_p50_s"`
	DispatchLagP95S float64 `json:"dispatched_lag_p95_s"`
	DispatchLagMaxS float64 `json:"dispatched_lag_max_s"`
}

// lagSeconds is how long before pts[i] the WAL end reached lsn: 0 when it is
// at or past the WAL end at pts[i], and -1 when the readings do not go back
// far enough.
func lagSeconds(pts []LSNPoint, i int, lsn uint64) float64 {
	if lsn == 0 {
		return -1
	}
	if lsn >= pts[i].Current {
		return 0
	}
	for j := i; j > 0; j-- {
		a, b := pts[j-1], pts[j]
		if a.Current <= lsn && lsn <= b.Current {
			// Interpolate when, between the two readings, the WAL end passed lsn.
			at := b.T
			if b.Current > a.Current {
				at = a.T + (b.T-a.T)*float64(lsn-a.Current)/float64(b.Current-a.Current)
			}
			return pts[i].T - at
		}
	}
	return -1
}

func stageLags(pts []LSNPoint, phases []PhaseSummary) []StageLag {
	var out []StageLag
	for _, ph := range phases {
		var sent, disp []float64
		for i, p := range pts {
			if p.T < ph.From || p.T > ph.To {
				continue
			}
			if s := lagSeconds(pts, i, p.Sent); s >= 0 {
				sent = append(sent, s)
			}
			if s := lagSeconds(pts, i, p.Confirmed); s >= 0 {
				disp = append(disp, s)
			}
		}
		if len(disp) == 0 {
			continue
		}
		q := func(v []float64, f float64) float64 {
			if len(v) == 0 {
				return 0
			}
			s := slices.Clone(v)
			slices.Sort(s)
			return s[int(float64(len(s)-1)*f)]
		}
		out = append(out, StageLag{
			Phase: ph.Name, Samples: len(disp),
			SentLagP50S: q(sent, .5), SentLagP95S: q(sent, .95), SentLagMaxS: q(sent, 1),
			DispatchLagP50S: q(disp, .5), DispatchLagP95S: q(disp, .95), DispatchLagMaxS: q(disp, 1),
		})
	}
	return out
}
