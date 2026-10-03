package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
)

// Hook authorization is the escape hatch for business rules Sluice cannot know.
//
// A channel namespace in `hook` mode delegates the join decision to an HTTP
// endpoint the application owns. A verdict is valid for its TTL: it answers
// later joins by the same caller until then, and a stream that joined is asked
// about again once it expires (on the next lease tick, and on /token). It is
// never asked per message: publishing and presence only require that the
// stream is joined. The endpoint sits on the join path, so it should be close
// to Sluice.
type hookRequest struct {
	Action    string          `json:"action"` // "subscribe"
	Channel   string          `json:"channel"`
	Namespace string          `json:"namespace"`
	Role      string          `json:"role"`
	Subject   string          `json:"sub,omitempty"`
	SessionID string          `json:"session_id,omitempty"`
	Claims    json.RawMessage `json:"claims,omitempty"`
}

type hookResponse struct {
	Allow  bool   `json:"allow"`
	Reason string `json:"reason,omitempty"`
	// TTL overrides the configured lifetime of this verdict, in seconds. 0
	// means it is not cached: every join asks, and a joined stream is asked
	// again on every lease tick.
	TTL *int `json:"ttl,omitempty"`
}

type hookOutcome int

const (
	hookAllow hookOutcome = iota
	// hookDeny is a verdict about the caller: a 2xx with allow:false, or 403.
	hookDeny
	// hookUnavailable says nothing about the caller: a transport failure, an
	// unexpected status, an unreadable body, or Sluice's credential rejected.
	// A join is refused (fail closed); a stream already joined keeps the
	// channel and is asked again on the next tick.
	hookUnavailable
)

type hookVerdict struct {
	outcome hookOutcome
	reason  string
	// expires is when the verdict stops answering for the caller. A verdict
	// that is not cached expires immediately.
	expires time.Time
}

func (v hookVerdict) allowed() bool { return v.outcome == hookAllow }

type hookCache struct {
	ttl     time.Duration
	timeout time.Duration
	bearer  string
	client  *http.Client
	now     func() time.Time

	mu sync.RWMutex
	m  map[string]hookVerdict
}

// hookRetryTTL is how long a verdict that says nothing about the caller -- a
// transport failure, an unexpected status, an unreadable body, a rejected
// Sluice credential -- is cached, so a blip does not become a minute-long
// outage.
const hookRetryTTL = 2 * time.Second

func newHookCache(ttl, timeout time.Duration, bearer string) *hookCache {
	if ttl <= 0 {
		ttl = time.Minute
	}
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	return &hookCache{
		ttl:     ttl,
		timeout: timeout,
		bearer:  bearer,
		client:  &http.Client{Timeout: timeout, CheckRedirect: noHookRedirect, Transport: hookTransport()},
		now:     time.Now,
		m:       map[string]hookVerdict{},
	}
}

func hookKey(ch config.Channel, id authz.Identity, channel string) string {
	return ch.HookURL + "\x00" + channel + "\x00" + id.Role + "\x00" + id.Sub + "\x00" + id.SessionID
}

// hookJitter shortens a verdict's lifetime by up to a fifth, so joins made
// together (a reconnect wave after a restart) do not all expire on the same
// tick. It only ever shortens: a verdict is never trusted past its TTL.
func hookJitter(ttl time.Duration) time.Duration {
	if ttl < 5 {
		return ttl
	}
	return ttl - rand.N(ttl/5)
}

// hookTransport keeps enough idle connections to a hook for the joins of a
// reconnect wave and for a round of re-checks. The default transport keeps two
// per host, so the rest of a burst each opened a connection and closed it.
func hookTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConnsPerHost = 64
	return t
}

// noHookRedirect stops the client from following a redirect. Following would
// accept a verdict from a URL nobody configured: a 302 turns the POST into a
// GET without the body, so an allow:true there says nothing about the caller,
// and a 307 re-sends the identity (and, on the same host, the bearer) to the
// new location. The 3xx is then denied like any other non-2xx.
func noHookRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// Authorize returns the verdict on whether this caller may join a channel: the
// cached one while it is valid, else the endpoint's. fresh skips the cache,
// for when the caller's claims may have changed.
//
// Only hookAllow admits a join, so the join path fails CLOSED: a timeout, a
// non-2xx, or an unparseable body all refuse. An application whose
// authorization service is down should stop granting access, not start
// granting it to everyone.
func (h *hookCache) Authorize(ctx context.Context, ch config.Channel, id authz.Identity, channel string, fresh bool) hookVerdict {
	key := hookKey(ch, id, channel)

	if !fresh {
		h.mu.RLock()
		v, ok := h.m[key]
		h.mu.RUnlock()
		if ok && h.now().Before(v.expires) {
			return v
		}
	}

	outcome, reason, ttl := h.ask(ctx, ch, id, channel)
	v := hookVerdict{outcome: outcome, reason: reason, expires: h.now().Add(hookJitter(ttl))}

	h.mu.Lock()
	if ttl <= 0 {
		delete(h.m, key)
	} else {
		// Bound the cache so a channel-name-spraying client cannot grow it
		// without limit. Dropping everything is crude but correct, and only
		// costs a burst of re-asks.
		if len(h.m) > 10000 {
			h.m = make(map[string]hookVerdict, 1024)
		}
		h.m[key] = v
	}
	h.mu.Unlock()

	return v
}

// hookRecheckConcurrency bounds how many endpoint calls one round of re-checks
// makes at once.
const hookRecheckConcurrency = 16

type hookJoin struct {
	st             *hub.Stream
	ch             config.Channel
	channel, label string
}

// startHookRechecks runs a round of re-checks in the background, unless the
// previous round is still running: the tick must not wait on the endpoint.
func (s *Server) startHookRechecks(ctx context.Context, now time.Time) {
	if !s.hookRecheck.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer s.hookRecheck.Store(false)
		s.recheckHookChannels(ctx, now)
	}()
}

// recheckHookChannels asks again about every joined hook channel whose verdict
// has expired, and removes the joins that are now denied.
//
// Joins are grouped by verdict (channel and caller), so a caller's streams on
// the same channel cost one call, and a verdict another join refreshed in the
// meantime is used without asking.
func (s *Server) recheckHookChannels(ctx context.Context, now time.Time) {
	type group struct {
		id    authz.Identity
		joins []hookJoin
	}
	groups := map[string]*group{}
	for _, st := range s.hub.Streams() {
		id := st.Identity()
		for _, d := range st.DueChannels(now) {
			ch, ok := s.cfg.Channel(d.Channel)
			if !ok || ch.Mode != config.ChannelHook {
				continue
			}
			key := hookKey(ch, id, d.Channel)
			g := groups[key]
			if g == nil {
				g = &group{id: id}
				groups[key] = g
			}
			g.joins = append(g.joins, hookJoin{st: st, ch: ch, channel: d.Channel, label: d.Label})
		}
	}
	list := slices.Collect(maps.Values(groups))
	boundedEach(len(list), hookRecheckConcurrency, func(i int) {
		g := list[i]
		first := g.joins[0]
		v := s.hooks.Authorize(ctx, first.ch, g.id, first.channel, false)
		if v.outcome == hookUnavailable {
			s.log.Warn("channel hook re-check got no verdict; joined streams keep the channel until the next tick",
				"channel", first.channel, "streams", len(g.joins), "reason", v.reason)
		}
		for _, j := range g.joins {
			s.applyHookVerdict(j, v, now)
		}
	})
}

// recheckStreamHooks asks again, bypassing the cache, about every hook channel
// a stream joined, under its new identity. It returns how many were revoked.
func (s *Server) recheckStreamHooks(ctx context.Context, st *hub.Stream, id authz.Identity) int {
	var joins []hookJoin
	for _, name := range st.Channels() {
		ch, ok := s.cfg.Channel(name)
		if !ok || ch.Mode != config.ChannelHook {
			continue
		}
		if label, ok := st.ChannelLabel(name); ok {
			joins = append(joins, hookJoin{st: st, ch: ch, channel: name, label: label})
		}
	}
	var revoked atomic.Int32
	now := time.Now()
	boundedEach(len(joins), hookRecheckConcurrency, func(i int) {
		j := joins[i]
		if s.applyHookVerdict(j, s.hooks.Authorize(ctx, j.ch, id, j.channel, true), now) {
			revoked.Add(1)
		}
	})
	return int(revoked.Load())
}

// applyHookVerdict acts on a re-check. An allow extends the join to the new
// verdict's expiry. A denial removes the join and tells the stream. No verdict
// keeps the join and asks again on the next tick: an endpoint outage must not
// eject every stream from every channel, and joins meanwhile fail closed.
func (s *Server) applyHookVerdict(j hookJoin, v hookVerdict, now time.Time) (revoked bool) {
	switch v.outcome {
	case hookAllow:
		metrics.HookRechecks.WithLabelValues("held").Inc()
		j.st.SetChannelRecheck(j.channel, j.label, v.expires)
	case hookDeny:
		metrics.HookRechecks.WithLabelValues("revoked").Inc()
		if s.hub.LeaveChannelLabel(j.channel, j.st, j.label) {
			hub.SendError(j.st, event.Error{Sub: j.label, Code: "channel_not_authorized", Message: v.reason})
			return true
		}
	default:
		metrics.HookRechecks.WithLabelValues("unavailable").Inc()
		j.st.SetChannelRecheck(j.channel, j.label, now)
	}
	return false
}

// boundedEach calls fn(0..n-1) with at most limit calls running at once, and
// returns when all have.
func boundedEach(n, limit int, fn func(i int)) {
	sem := make(chan struct{}, limit)
	var wg sync.WaitGroup
	for i := range n {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			fn(i)
		})
	}
	wg.Wait()
}

// ask calls the endpoint and returns its verdict with how long it is valid.
func (h *hookCache) ask(ctx context.Context, ch config.Channel, id authz.Identity, channel string) (hookOutcome, string, time.Duration) {
	body, err := json.Marshal(hookRequest{
		Action:    "subscribe",
		Channel:   channel,
		Namespace: ch.Namespace,
		Role:      id.Role,
		Subject:   id.Sub,
		SessionID: id.SessionID,
		Claims:    json.RawMessage(id.ClaimsRaw),
	})
	if err != nil {
		return hookUnavailable, "could not encode the authorization request", hookRetryTTL
	}

	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.HookURL, bytes.NewReader(body))
	if err != nil {
		return hookUnavailable, "invalid hook URL", hookRetryTTL
	}
	req.Header.Set("Content-Type", "application/json")
	if h.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.bearer)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return hookUnavailable, fmt.Sprintf("authorization endpoint unreachable: %v", err), hookRetryTTL
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusForbidden:
		return hookDeny, "the authorization endpoint denied this channel", h.ttl
	case resp.StatusCode == http.StatusUnauthorized:
		// The endpoint rejected Sluice's own credential, which says nothing
		// about this caller and is fixed by configuration, not by waiting.
		return hookUnavailable, "the authorization endpoint rejected Sluice's credential (HTTP 401); check SLUICE_CHANNEL_HOOK_BEARER", hookRetryTTL
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return hookUnavailable, fmt.Sprintf("authorization endpoint returned %d", resp.StatusCode), hookRetryTTL
	}

	var out hookResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return hookUnavailable, "authorization endpoint returned an unreadable body", hookRetryTTL
	}
	ttl := h.ttl
	if out.TTL != nil {
		ttl = time.Duration(max(*out.TTL, 0)) * time.Second
	}
	if !out.Allow {
		if out.Reason == "" {
			out.Reason = "the authorization endpoint denied this channel"
		}
		return hookDeny, out.Reason, ttl
	}
	return hookAllow, out.Reason, ttl
}
