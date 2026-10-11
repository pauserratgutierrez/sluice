package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/pauserratgutierrez/sluice/apps/loadtest/internal/sse"
)

// subDef is one subscription a client registers, as the SDK keeps it: resent
// on every reconnect, asked for again after a retryable refusal.
type subDef struct {
	Label string
	Table string // schema.table of a shape; "" for a channel
	Kind  int    // the latency kind its events are recorded as; -1 for none
	JSON  json.RawMessage
}

// roomSubs is a member's stream in a room: the room's messages, the member's
// own membership rows (their last_message_at), their notifications, filler
// shapes up to `shapes`, and the room's hook channel.
//
// The fillers stand for a product's other shapes: they cost what a shape
// costs (an issuer call, a hold, a registry entry, a /token refresh) but ask
// for DELETE on the member's own row only, which never happens, so they carry
// no traffic.
func roomSubs(room int64, user string, shapes int) []subDef {
	shape := func(label, table, filter, ops string) subDef {
		s := fmt.Sprintf(`{"sub":%q,"shape":{"schema":"public","table":%q,"filter":%q%s}}`, label, table, filter, ops)
		return subDef{Label: label, Table: "public." + table, Kind: -1, JSON: json.RawMessage(s)}
	}
	var out []subDef
	msgs := shape("msgs", "room_messages", fmt.Sprintf("room_id=eq.%d", room), "")
	msgs.Kind = latMessage
	out = append(out, msgs)
	if shapes >= 2 {
		me := shape("me", "room_members", "user_id=eq."+user, "")
		me.Kind = latMember
		out = append(out, me)
	}
	if shapes >= 3 {
		nt := shape("nt", "notifications", "user_id=eq."+user, `,"ops":["INSERT"]`)
		nt.Kind = latNotification
		out = append(out, nt)
	}
	for i := 3; i < shapes; i++ {
		out = append(out, shape(fmt.Sprintf("f%d", i), "room_members", "user_id=eq."+user, `,"ops":["DELETE"]`))
	}
	out = append(out, subDef{Label: "chat", Kind: latTyping,
		JSON: json.RawMessage(fmt.Sprintf(`{"sub":"chat","channel":"room:%d"}`, room))})
	return out
}

// rclient is one member's stream, driven the way packages/sluice-js drives
// it: one POST /stream carrying every registration and a resume map, the
// same backoff after it ends, the same handling of retryable refusals, and
// /token on a timer.
type rclient struct {
	e      *engine
	room   *room
	idx    int
	userID string
	sessID string
	typist bool

	token atomic.Pointer[string]
	sid   atomic.Pointer[string] // the stream id while a stream is open

	mu      sync.Mutex
	subs    []subDef
	resume  map[string]string
	seen    map[string]time.Time // when each table's resume position was current
	retries map[string]*time.Timer

	// posAge is how old each table's position was when the last stream was
	// requested (reader goroutine).
	posAge map[string]time.Duration

	// Reader-goroutine state.
	attempt    int
	retryAfter time.Duration // < 0: none
	lastSeq    atomic.Int64
	msgResumed atomic.Bool

	live    atomic.Bool
	readyN  atomic.Int64 // ready events received
	readyAt atomic.Int64 // the last ready, unix ns
	downAt  atomic.Int64 // when a reconnect wave saw it drop, unix ns
	// stop ends the stream open now, for LOAD_CHURN; churned marks that it was
	// ended on purpose, so its end is counted as churn, not as a failure.
	stop    atomic.Pointer[context.CancelFunc]
	churned atomic.Bool
}

func (c *rclient) stamp() string { return *c.token.Load() }

// run keeps the stream open until ctx ends, as SluiceClient.runStream does.
func (c *rclient) run(ctx context.Context) {
	c.retryAfter = -1
	for ctx.Err() == nil {
		c.openOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		schedule := c.e.rc.Backoff
		wait := schedule[min(c.attempt, len(schedule)-1)]
		c.attempt++
		// Full jitter, and as the SDK does, the longer of that and a delay the
		// server asked for, so a refusal that lasts backs off too.
		delay := time.Duration(rand.Float64() * float64(wait))
		if c.retryAfter >= 0 {
			delay = max(delay, c.retryAfter)
		}
		c.retryAfter = -1
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (c *rclient) openOnce(ctx context.Context) {
	e := c.e
	c.mu.Lock()
	specs := make([]json.RawMessage, len(c.subs))
	for i, s := range c.subs {
		specs[i] = s.JSON
	}
	resume := make(map[string]string, len(c.resume))
	posAge := make(map[string]time.Duration, len(c.resume))
	now := time.Now()
	for k, v := range c.resume {
		resume[k] = v
		posAge[k] = now.Sub(c.seen[k])
	}
	c.mu.Unlock()
	c.posAge = posAge
	body, _ := json.Marshal(map[string]any{"subscriptions": specs, "resume": resume})

	first := c.readyN.Load() == 0
	if first && e.waveSem != nil {
		select {
		case e.waveSem <- struct{}{}:
		case <-ctx.Done():
			return
		}
	}
	released := false
	release := func() {
		if first && e.waveSem != nil && !released {
			released = true
			<-e.waveSem
		}
	}
	defer release()

	e.openAttempts.Add(1)
	if w := e.wave.Load(); w != nil {
		w.attempts.Add(1)
	}
	t0 := time.Now()
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	c.stop.Store(&stop)
	req, err := http.NewRequestWithContext(sctx, http.MethodPost, e.cfg.SluiceURL+"/stream", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Authorization", "Bearer "+c.stamp())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")
	resp, err := e.streamHTTP.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			e.openFailed(classifyNetErr(err))
		}
		return
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		var eb struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(b, &eb)
		code := fmt.Sprintf("http_%d", resp.StatusCode)
		if eb.Error != "" {
			code += ":" + eb.Error
		}
		e.openFailed(code)
		// As the SDK does: a refusal for capacity says when to come back.
		if d, ok := retryAfterHeader(resp); ok {
			c.retryAfter = d
		} else if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusTooManyRequests {
			e.count("refused_without_retry_after")
		}
		if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
			// The SDK stops here; for the runner it is a broken setup.
			e.count("fatal_" + code)
		}
		return
	}
	c.attempt = 0
	readErr := sse.Read(resp.Body, func(ev sse.Event) bool {
		if ev.Name == "ready" {
			release()
		}
		c.handle(ev, t0)
		return true
	})
	resp.Body.Close()
	c.wentDown(ctx, readErr)
}

// wentDown is called when a stream that answered 200 ends.
func (c *rclient) wentDown(ctx context.Context, readErr error) {
	c.sid.Store(nil)
	if c.live.CompareAndSwap(true, false) {
		c.room.open.Add(-1)
		c.e.liveClients.Add(-1)
	}
	if ctx.Err() != nil {
		return
	}
	if c.churned.Swap(false) {
		c.e.count("churned")
		return
	}
	reason := "eof"
	if readErr != nil {
		reason = classifyNetErr(readErr)
	}
	c.e.count("stream_end:" + reason)
	if w := c.e.wave.Load(); w != nil && c.downAt.Load() == 0 {
		c.downAt.Store(time.Now().UnixNano())
		w.down.Add(1)
	}
}

type wireError struct {
	Sub          string `json:"sub"`
	Code         string `json:"code"`
	Retryable    bool   `json:"retryable"`
	RetryAfterMs *int64 `json:"retry_after_ms"`
}

type wireResult struct {
	Sub     string     `json:"sub"`
	OK      bool       `json:"ok"`
	Resumed *bool      `json:"resumed"`
	Error   *wireError `json:"error"`
}

type wireChange struct {
	Sub       string `json:"sub"`
	Schema    string `json:"schema"`
	Table     string `json:"table"`
	CommitLSN string `json:"commit_lsn"`
	Record    struct {
		Seq           int64  `json:"seq"`
		CreatedAt     string `json:"created_at"`
		LastMessageAt string `json:"last_message_at"`
	} `json:"record"`
	CommitTime string `json:"commit_time"`
}

func (c *rclient) handle(ev sse.Event, t0 time.Time) {
	e := c.e
	now := time.Now()
	switch ev.Name {
	case "ready":
		var r struct {
			StreamID      string       `json:"stream_id"`
			WalLSN        string       `json:"wal_lsn"`
			Subscriptions []wireResult `json:"subscriptions"`
		}
		if json.Unmarshal(ev.Data, &r) != nil {
			e.count("bad_ready")
			return
		}
		sid := r.StreamID
		c.sid.Store(&sid)
		if c.live.CompareAndSwap(false, true) {
			c.room.open.Add(1)
			e.liveClients.Add(1)
		}
		n := c.readyN.Add(1)
		c.readyAt.Store(now.UnixNano())
		e.lat.Record(latOpen, now.Sub(t0))
		// The stream request carried every registration, retries included.
		c.mu.Lock()
		for label, t := range c.retries {
			t.Stop()
			delete(c.retries, label)
		}
		c.mu.Unlock()
		w := e.wave.Load()
		full := true
		agesSeen := map[string]bool{}
		for _, res := range r.Subscriptions {
			c.applyResult(res, r.WalLSN, w)
			if w != nil && n > 1 && res.Resumed != nil {
				if def, ok := c.findLocked(res.Sub); ok && def.Table != "" && !agesSeen[def.Table] {
					agesSeen[def.Table] = true
					if age, ok := c.posAge[def.Table]; ok {
						w.addAge(def.Table, *res.Resumed, age)
					}
				}
			}
			if res.Resumed != nil && !*res.Resumed {
				full = false
			}
			if !res.OK {
				full = false
			}
		}
		if w != nil && n > 1 {
			if full {
				w.counters.add("fully_resumed")
			} else {
				w.counters.add("not_fully_resumed")
			}
			if at := c.downAt.Swap(0); at != 0 {
				w.back(now, time.Unix(0, at))
			}
		}
		e.onReady(c, n == 1)
	case "change":
		var ch wireChange
		if json.Unmarshal(ev.Data, &ch) != nil {
			e.count("bad_change")
			return
		}
		if ch.CommitLSN != "" {
			table := ch.Schema + "." + ch.Table
			c.mu.Lock()
			c.resume[table] = ch.CommitLSN
			c.seen[table] = now
			c.mu.Unlock()
			e.trail.note(table, ch.CommitLSN, now)
		}
		switch ch.Sub {
		case "msgs":
			e.recv[latMessage].Add(1)
			if t, err := time.Parse(time.RFC3339Nano, ch.Record.CreatedAt); err == nil {
				c.recordChange(latMessage, now, t)
				if ct, err := time.Parse(time.RFC3339Nano, ch.CommitTime); err == nil && !(c.readyN.Load() > 1 && t.UnixNano() < c.readyAt.Load()) {
					e.lat.Record(latWriteCommit, ct.Sub(t))
					e.lat.Record(latCommitClient, now.Sub(ct))
				}
			}
			c.checkSeq(ch.Record.Seq)
		case "me":
			e.recv[latMember].Add(1)
			if t, err := time.Parse(time.RFC3339Nano, ch.Record.LastMessageAt); err == nil {
				c.recordChange(latMember, now, t)
			}
		case "nt":
			e.recv[latNotification].Add(1)
			if t, err := time.Parse(time.RFC3339Nano, ch.Record.CreatedAt); err == nil {
				c.recordChange(latNotification, now, t)
			}
			if b := e.big.Load(); b != nil {
				b.got.Add(1)
				b.last.Store(now.UnixNano())
			}
		default:
			e.count("change_other:" + ch.Sub)
		}
		e.meter.Add(1)
	case "broadcast":
		var b struct {
			Payload struct {
				T int64 `json:"t"`
			} `json:"payload"`
		}
		if json.Unmarshal(ev.Data, &b) == nil && b.Payload.T > 0 {
			e.recv[latTyping].Add(1)
			e.lat.Record(latTyping, now.Sub(time.Unix(0, b.Payload.T)))
		}
		e.meter.Add(1)
	case "error":
		var er wireError
		if json.Unmarshal(ev.Data, &er) != nil {
			return
		}
		if er.Sub == "" {
			e.count("stream_error:" + er.Code)
			if er.RetryAfterMs != nil && *er.RetryAfterMs >= 0 {
				c.retryAfter = time.Duration(*er.RetryAfterMs) * time.Millisecond
			}
			return
		}
		e.count("sub_error:" + er.Code)
		if w := e.wave.Load(); w != nil {
			w.counters.add("sub_error:" + er.Code)
		}
		if !er.Retryable {
			c.forget(er.Sub)
		} else if er.RetryAfterMs != nil {
			c.scheduleRetry(er.Sub, time.Duration(*er.RetryAfterMs)*time.Millisecond)
		}
	case "warning":
		var wn struct {
			Code string `json:"code"`
		}
		_ = json.Unmarshal(ev.Data, &wn)
		e.count("warning:" + wn.Code)
	}
}

// checkSeq follows the room's message sequence on this stream. A gap after a
// resumed reconnect is a change the resume should have replayed.
func (c *rclient) checkSeq(seq int64) {
	if seq <= 0 {
		return
	}
	last := c.lastSeq.Load()
	switch {
	case last == 0:
	case seq == last+1:
	case seq <= last:
		c.e.count("msg_duplicate")
		return
	default:
		if c.msgResumed.Load() {
			c.e.count("msg_gap_after_resume")
		} else {
			c.e.count("msg_gap_unresumed")
		}
	}
	c.lastSeq.Store(seq)
}

// applyResult is SluiceClient.handleResult and live().
func (c *rclient) applyResult(res wireResult, walLSN string, w *waveStats) {
	e := c.e
	if res.OK {
		e.count("sub_ok")
	} else if res.Error != nil {
		e.count("sub_refused:" + res.Error.Code)
	}
	if w != nil && !res.OK && res.Error != nil {
		w.counters.add("sub_refused:" + res.Error.Code)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	def, ok := c.find(res.Sub)
	if !ok {
		return
	}
	if !res.OK {
		if res.Error == nil || !res.Error.Retryable {
			c.forgetLocked(res.Sub)
			return
		}
		delay := c.retryDelay()
		if res.Error.RetryAfterMs != nil {
			delay = time.Duration(*res.Error.RetryAfterMs) * time.Millisecond
		}
		c.scheduleRetryLocked(res.Sub, delay)
		return
	}
	if def.Table == "" {
		return
	}
	if res.Resumed != nil {
		state := "false"
		if *res.Resumed {
			state = "true"
		}
		e.count("resumed_" + state + ":" + def.Table)
		if w != nil {
			w.counters.add("resumed_" + state + ":" + def.Table)
		}
		if def.Label == "msgs" {
			c.msgResumed.Store(*res.Resumed)
			if !*res.Resumed {
				// The client reads the room's state again; follow the
				// sequence from the next message.
				c.lastSeq.Store(0)
			}
		}
	}
	if walLSN != "" {
		if _, has := c.resume[def.Table]; !has || (res.Resumed != nil && !*res.Resumed) {
			c.resume[def.Table] = walLSN
			c.seen[def.Table] = time.Now()
		}
	}
}

func (c *rclient) find(label string) (subDef, bool) {
	for _, s := range c.subs {
		if s.Label == label {
			return s, true
		}
	}
	return subDef{}, false
}

func (c *rclient) forget(label string) {
	c.mu.Lock()
	c.forgetLocked(label)
	c.mu.Unlock()
}

func (c *rclient) forgetLocked(label string) {
	for i, s := range c.subs {
		if s.Label == label {
			c.subs = append(c.subs[:i], c.subs[i+1:]...)
			c.e.count("sub_forgotten")
			break
		}
	}
	if t := c.retries[label]; t != nil {
		t.Stop()
		delete(c.retries, label)
	}
}

// retryDelay is the SDK's full jitter over the last backoff step.
func (c *rclient) retryDelay() time.Duration {
	b := c.e.rc.Backoff
	return time.Duration(rand.Float64() * float64(b[len(b)-1]))
}

func (c *rclient) scheduleRetry(label string, d time.Duration) {
	c.mu.Lock()
	c.scheduleRetryLocked(label, d)
	c.mu.Unlock()
}

func (c *rclient) scheduleRetryLocked(label string, d time.Duration) {
	if t := c.retries[label]; t != nil {
		t.Stop()
	}
	c.retries[label] = time.AfterFunc(d, func() {
		c.mu.Lock()
		delete(c.retries, label)
		c.mu.Unlock()
		c.resubscribe(label)
	})
}

// resubscribe asks for one registration again on the open stream. Without a
// stream there is nothing to do: the reconnect resends every registration.
func (c *rclient) resubscribe(label string) {
	e := c.e
	sid := c.sid.Load()
	c.mu.Lock()
	def, ok := c.find(label)
	c.mu.Unlock()
	if sid == nil || !ok || e.clientsCtx.Err() != nil {
		return
	}
	body, _ := json.Marshal(map[string]any{"stream_id": *sid, "subscriptions": []json.RawMessage{def.JSON}})
	var out struct {
		Results []wireResult `json:"results"`
		WalLSN  string       `json:"wal_lsn"`
	}
	e.count("resubscribe")
	if err := c.post(e.clientsCtx, "/subscribe", *sid, body, &out); err != nil {
		e.count("resubscribe_failed")
		c.scheduleRetry(label, c.retryDelay())
		return
	}
	for _, res := range out.Results {
		c.applyResult(res, out.WalLSN, e.wave.Load())
	}
}

// refreshToken is setAuth: a new token for future opens, and POST /token on
// the open stream.
func (c *rclient) refreshToken(ctx context.Context) {
	e := c.e
	tok, err := e.signer.User(c.userID, c.sessID, e.rc.TokenTTL)
	if err != nil {
		e.count("token_mint_failed")
		return
	}
	c.token.Store(&tok)
	sid := c.sid.Load()
	if sid == nil {
		return
	}
	body, _ := json.Marshal(map[string]string{"stream_id": *sid, "access_token": tok})
	var out struct {
		Revoked int `json:"revoked_subscriptions"`
	}
	t0 := time.Now()
	err = c.post(ctx, "/token", *sid, body, &out)
	e.lat.Record(latToken, time.Since(t0))
	if err != nil {
		e.count("token_failed:" + err.Error())
		return
	}
	e.count("token_ok")
	if out.Revoked > 0 {
		e.countN("token_revoked_subscriptions", int64(out.Revoked))
	}
}

// typing publishes one typing broadcast to the room.
func (c *rclient) typing(ctx context.Context) {
	e := c.e
	sid := c.sid.Load()
	if sid == nil {
		return
	}
	body := fmt.Appendf(nil, `{"stream_id":%q,"channel":"room:%d","event":"typing","payload":{"t":%d},"self":false}`,
		*sid, c.room.id, time.Now().UnixNano())
	others := c.room.open.Load() - 1
	t0 := time.Now()
	err := c.post(ctx, "/publish", *sid, body, nil)
	e.lat.Record(latPublish, time.Since(t0))
	if err != nil {
		e.count("publish_failed:" + err.Error())
		return
	}
	e.publishes.Add(1)
	if others > 0 {
		e.expect[latTyping].Add(others)
	}
}

// post sends a control request and decodes a 200 into out. Any other status
// is an error named by its status and Sluice's error code.
func (c *rclient) post(ctx context.Context, path, sid string, body []byte, out any) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.e.cfg.SluiceURL+path, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.stamp())
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sluice-Stream-Id", sid)
	resp, err := c.e.ctlHTTP.Do(req)
	if err != nil {
		return errors.New(classifyNetErr(err))
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		var eb struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(b, &eb)
		return fmt.Errorf("http_%d:%s", resp.StatusCode, eb.Error)
	}
	if out != nil {
		return json.Unmarshal(b, out)
	}
	return nil
}

// classifyNetErr names a transport error by what a client would see.
func classifyNetErr(err error) string {
	var ne net.Error
	switch {
	case err == nil:
		return "eof"
	case errors.Is(err, syscall.ECONNREFUSED):
		return "connection_refused"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	case errors.Is(err, syscall.EADDRNOTAVAIL):
		return "no_local_port"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, io.ErrUnexpectedEOF), errors.Is(err, io.EOF):
		return "unexpected_eof"
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.As(err, &ne) && ne.Timeout():
		return "timeout"
	}
	s := err.Error()
	if i := strings.LastIndex(s, ": "); i >= 0 {
		s = s[i+2:]
	}
	return strings.ReplaceAll(s, " ", "_")
}

// recordChange records a change's latency from when it was written. One
// written before this stream opened, on a stream that reopened, reached it
// through the resume replay: its latency includes the outage, so it is kept
// apart from the live ones.
func (c *rclient) recordChange(kind int, now, written time.Time) {
	if c.readyN.Load() > 1 && written.UnixNano() < c.readyAt.Load() {
		c.e.lat.Record(latReplayed, now.Sub(written))
		return
	}
	c.e.lat.Record(kind, now.Sub(written))
}

// findLocked is find for a caller that does not hold c.mu.
func (c *rclient) findLocked(label string) (subDef, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.find(label)
}

// churn ends the client's stream on purpose; run opens it again.
func (c *rclient) churn() bool {
	stop := c.stop.Load()
	if stop == nil || !c.live.Load() {
		return false
	}
	c.churned.Store(true)
	(*stop)()
	return true
}

// retryAfterHeader reads Retry-After (seconds or an HTTP date) on a 429 or
// 503, at most a minute, plus up to a tenth at random, like the SDK.
func retryAfterHeader(resp *http.Response) (time.Duration, bool) {
	if resp.StatusCode != http.StatusTooManyRequests && resp.StatusCode != http.StatusServiceUnavailable {
		return 0, false
	}
	h := strings.TrimSpace(resp.Header.Get("Retry-After"))
	if h == "" {
		return 0, false
	}
	var d time.Duration
	if n, err := strconv.Atoi(h); err == nil {
		d = time.Duration(n) * time.Second
	} else if t, err := http.ParseTime(h); err == nil {
		d = time.Until(t)
	} else {
		return 0, false
	}
	d = min(max(d, 0), time.Minute)
	return d + time.Duration(rand.Float64()*float64(d)/10), true
}
