package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type drive struct {
	st          *Step
	users       []User
	lives       []*live
	changesN    *atomic.Int64
	clientBcast *atomic.Int64
	dbBcast     *atomic.Int64
	kicks       *atomic.Int64
	writeAt     *sync.Map
	latMu       *sync.Mutex
	lats        *[]time.Duration
	started     time.Time
}

func (a *app) driveChange(ctx context.Context, d drive) {
	if a.scenario.Write == nil {
		d.st.FailReason = "scenario has no write function"
		return
	}
	writes, errs := a.writeChanges(ctx, d)
	d.st.Writes = writes
	d.st.WriteErrors = errs
	d.st.Expected = int64(d.st.Opened) * int64(d.st.Changes)
	waitCount(ctx, a.cfg.DeliveryTimeout, d.st.Expected, d.changesN.Load)
	d.st.Received = d.changesN.Load()
	d.st.ChangesRecv = d.st.Received
	finishFanout(d)
}

func (a *app) driveBroadcast(ctx context.Context, d drive) {
	ch := a.channel()
	n, errs := a.publishAll(ctx, d, ch, "client")
	d.st.Writes = n
	d.st.WriteErrors = errs
	want := int64(d.st.Opened) * int64(d.st.Changes)
	waitCount(ctx, a.cfg.DeliveryTimeout, want, d.clientBcast.Load)
	clientGot := d.clientBcast.Load()

	dbN, dbErrs := a.emitAll(ctx, d, ch)
	d.st.Writes += dbN
	d.st.WriteErrors += dbErrs
	waitCount(ctx, a.cfg.DeliveryTimeout, want, d.dbBcast.Load)
	dbGot := d.dbBcast.Load()

	d.st.Broadcasts = clientGot
	d.st.DBBroadcasts = dbGot
	d.st.Expected = want * 2
	d.st.Received = clientGot + dbGot
	finishFanout(d)
	switch {
	case want > 0 && float64(clientGot)/float64(want) < a.cfg.DeliveryMin:
		d.st.FailReason = fmt.Sprintf("client broadcast delivered %.1f%% < %.0f%%", 100*float64(clientGot)/float64(want), 100*a.cfg.DeliveryMin)
	case want > 0 && float64(dbGot)/float64(want) < a.cfg.DeliveryMin:
		d.st.FailReason = fmt.Sprintf("database broadcast delivered %.1f%% < %.0f%%", 100*float64(dbGot)/float64(want), 100*a.cfg.DeliveryMin)
	}
}

func (a *app) driveMixed(ctx context.Context, d drive) {
	ch := a.channel()
	writes, errs := a.writeChanges(ctx, d)
	d.st.Writes = writes
	d.st.WriteErrors = errs
	wantChange := int64(d.st.Opened) * int64(d.st.Changes)
	waitCount(ctx, a.cfg.DeliveryTimeout, wantChange, d.changesN.Load)

	pubN, pubErrs := a.publishAll(ctx, d, ch, "mix")
	d.st.Writes += pubN
	d.st.WriteErrors += pubErrs
	waitCount(ctx, a.cfg.DeliveryTimeout, wantChange, d.clientBcast.Load)

	gotChange := d.changesN.Load()
	gotBcast := d.clientBcast.Load()
	d.st.ChangesRecv = gotChange
	d.st.Broadcasts = gotBcast
	d.st.Expected = wantChange * 2
	d.st.Received = gotChange + gotBcast
	finishFanout(d)
	switch {
	case wantChange > 0 && float64(gotChange)/float64(wantChange) < a.cfg.DeliveryMin:
		d.st.FailReason = fmt.Sprintf("changes delivered %.1f%% < %.0f%%", 100*float64(gotChange)/float64(wantChange), 100*a.cfg.DeliveryMin)
	case wantChange > 0 && float64(gotBcast)/float64(wantChange) < a.cfg.DeliveryMin:
		d.st.FailReason = fmt.Sprintf("broadcast delivered %.1f%% < %.0f%%", 100*float64(gotBcast)/float64(wantChange), 100*a.cfg.DeliveryMin)
	}
}

func (a *app) drivePresence(ctx context.Context, d drive) {
	ch := a.channel()
	var errs atomic.Int64
	sem := make(chan struct{}, max(1, a.cfg.Wave))
	var wg sync.WaitGroup
	for _, l := range d.lives {
		wg.Add(1)
		go func(l *live) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			body := fmt.Sprintf(`{"stream_id":%q,"channel":%q,"action":"track","key":%q,"meta":{"n":1}}`,
				l.streamID, ch, l.user.ID)
			if err := postJSON(ctx, a.http, a.cfg.SluiceURL+"/presence", l.user.Token, body); err != nil {
				errs.Add(1)
			}
		}(l)
	}
	wg.Wait()
	d.st.Writes = len(d.lives)
	d.st.WriteErrors = int(errs.Load())

	wantMembers := len(d.lives)
	need := floorMin(d.st.Opened, a.cfg.DeliveryMin)
	minMembers := floorMin(wantMembers, a.cfg.DeliveryMin)
	waitUntil(ctx, a.cfg.PresenceWait, func() bool {
		return countConverged(d.lives, minMembers) >= need
	})
	joined := countConverged(d.lives, minMembers)
	d.st.PresenceReady = joined

	leaveN := len(d.lives) / 2
	for i := 0; i < leaveN; i++ {
		d.lives[i].sse.Close()
	}
	remain := d.lives[leaveN:]
	remainNeed := floorMin(len(remain), a.cfg.DeliveryMin)
	minRemain := floorMin(wantMembers-leaveN, a.cfg.DeliveryMin)
	waitUntil(ctx, a.cfg.PresenceWait, func() bool {
		return countConverged(remain, minRemain) >= remainNeed
	})
	after := countConverged(remain, minRemain)
	d.st.PresenceLeave = after

	d.st.Expected = int64(d.st.Opened)
	d.st.Received = int64(joined)
	finishFanout(d)
	switch {
	case joined < need:
		d.st.FailReason = fmt.Sprintf("presence converged on %d/%d streams (need %d with ≥%d members)", joined, d.st.Opened, need, minMembers)
	case len(remain) > 0 && after < remainNeed:
		d.st.FailReason = fmt.Sprintf("after leave, %d/%d remaining streams still see ≥%d members", after, len(remain), minRemain)
	}
}

func (a *app) driveKick(ctx context.Context, d drive) {
	if a.scenario.Write == nil {
		d.st.FailReason = "scenario has no write function"
		return
	}
	d.st.Changes = 1
	writes, errs := a.writeChanges(ctx, d)
	d.st.Writes = writes
	d.st.WriteErrors = errs
	wantChange := int64(d.st.Opened)
	waitCount(ctx, a.cfg.DeliveryTimeout, wantChange, d.changesN.Load)

	var delErrs atomic.Int64
	sem := make(chan struct{}, 16)
	var wg sync.WaitGroup
	for _, u := range d.users {
		wg.Add(1)
		go func(u User) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			if err := deleteHold(ctx, a.pool, u.ID); err != nil {
				delErrs.Add(1)
			}
		}(u)
	}
	wg.Wait()
	d.st.WriteErrors += int(delErrs.Load())

	waitCount(ctx, a.cfg.DeliveryTimeout, wantChange, d.kicks.Load)
	gotKick := d.kicks.Load()
	d.st.ChangesRecv = d.changesN.Load()
	d.st.Kicked = gotKick
	d.st.Expected = wantChange
	d.st.Received = gotKick
	finishFanout(d)
	if wantChange > 0 && float64(d.changesN.Load())/float64(wantChange) < a.cfg.DeliveryMin {
		d.st.FailReason = fmt.Sprintf("pre-kick changes delivered %.1f%%", 100*float64(d.changesN.Load())/float64(wantChange))
		return
	}
	if wantChange > 0 && float64(gotKick)/float64(wantChange) < a.cfg.DeliveryMin {
		d.st.FailReason = fmt.Sprintf("hold kicks delivered %.1f%% < %.0f%%", 100*float64(gotKick)/float64(wantChange), 100*a.cfg.DeliveryMin)
	}
}

func (a *app) writeChanges(ctx context.Context, d drive) (int, int) {
	seq := atomic.Int64{}
	var writeErrs atomic.Int64
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)
	for _, u := range d.users {
		u := u
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < d.st.Changes; i++ {
				sem <- struct{}{}
				n := seq.Add(1)
				title := fmt.Sprintf("m-%d", n)
				d.writeAt.Store(n, time.Now())
				err := a.scenario.Write(ctx, a.pool, u, title)
				<-sem
				if err != nil {
					writeErrs.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return int(seq.Load()), int(writeErrs.Load())
}

func (a *app) publishAll(ctx context.Context, d drive, channel, ev string) (int, int) {
	if len(d.lives) == 0 {
		return 0, 0
	}
	pub := d.lives[0]
	var errs int
	for i := 0; i < d.st.Changes; i++ {
		n := int64(i + 1)
		d.writeAt.Store(n, time.Now())
		body := fmt.Sprintf(`{"stream_id":%q,"channel":%q,"event":%q,"payload":{"n":%d},"self":true}`,
			pub.streamID, channel, ev, n)
		if err := postJSON(ctx, a.http, a.cfg.SluiceURL+"/publish", pub.user.Token, body); err != nil {
			errs++
		}
	}
	return d.st.Changes, errs
}

func (a *app) emitAll(ctx context.Context, d drive, channel string) (int, int) {
	var errs int
	for i := 0; i < d.st.Changes; i++ {
		n := int64(100000 + i + 1)
		d.writeAt.Store(n, time.Now())
		payload := fmt.Sprintf(`{"event":"db","payload":{"n":%d}}`, n)
		if err := emitBroadcast(ctx, a.pool, channel, payload); err != nil {
			errs++
		}
	}
	return d.st.Changes, errs
}

func (a *app) channel() string {
	if a.scenario.Channel != "" {
		return a.scenario.Channel
	}
	return a.cfg.Channel
}

func finishFanout(d drive) {
	fanout := time.Since(d.started)
	d.st.FanoutElapsed = fanout.Round(time.Millisecond).String()
	if d.st.Expected > 0 {
		d.st.DeliveryPct = 100 * float64(d.st.Received) / float64(d.st.Expected)
	}
	if fanout.Seconds() > 0 {
		d.st.EventsPerSec = float64(d.st.Received) / fanout.Seconds()
	}
	var snap []time.Duration
	if d.latMu != nil && d.lats != nil {
		d.latMu.Lock()
		snap = append([]time.Duration(nil), *d.lats...)
		d.latMu.Unlock()
	}
	d.st.LatencyP50, d.st.LatencyP95, d.st.LatencyP99 = percentiles(snap)
}

func floorMin(n int, frac float64) int {
	if n <= 0 {
		return 0
	}
	v := int(float64(n) * frac)
	if v < 1 {
		return n
	}
	return v
}

func waitCount(ctx context.Context, timeout time.Duration, want int64, got func() int64) {
	if want <= 0 {
		return
	}
	quiet := 0
	var last int64
	deadline := time.After(timeout)
	for {
		cur := got()
		if cur >= want {
			return
		}
		if cur == last {
			if cur > 0 {
				quiet++
				if quiet >= 25 {
					return
				}
			}
		} else {
			quiet = 0
			last = cur
		}
		select {
		case <-deadline:
			return
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func waitUntil(ctx context.Context, timeout time.Duration, ok func() bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func countConverged(lives []*live, minMembers int) int {
	n := 0
	for _, l := range lives {
		l.mu.Lock()
		ok := len(l.roster) >= minMembers
		l.mu.Unlock()
		if ok {
			n++
		}
	}
	return n
}

func applyPresence(roster map[string]struct{}, data []byte) {
	var p struct {
		Type    string                     `json:"type"`
		Members map[string]json.RawMessage `json:"members"`
		Joins   map[string]json.RawMessage `json:"joins"`
		Leaves  map[string]json.RawMessage `json:"leaves"`
	}
	if json.Unmarshal(data, &p) != nil {
		return
	}
	if p.Type == "state" {
		for k := range roster {
			delete(roster, k)
		}
		for k := range p.Members {
			roster[k] = struct{}{}
		}
		return
	}
	for k := range p.Joins {
		roster[k] = struct{}{}
	}
	for k := range p.Leaves {
		delete(roster, k)
	}
}

func broadcastMeta(data []byte) (origin string, seq int64, ok bool) {
	var ev struct {
		Origin  string `json:"origin"`
		Payload struct {
			N *int64 `json:"n"`
		} `json:"payload"`
	}
	if json.Unmarshal(data, &ev) != nil {
		return "", 0, false
	}
	if ev.Payload.N != nil {
		return ev.Origin, *ev.Payload.N, true
	}
	return ev.Origin, 0, false
}

func errorCode(data []byte) string {
	var e struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(data, &e)
	return e.Code
}
