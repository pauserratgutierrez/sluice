package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/config"
)

// Hook authorization is the escape hatch for business rules Sluice cannot know.
//
// A channel namespace in `hook` mode delegates the subscribe decision to an HTTP
// endpoint the application owns. It is asked once per join, never per message:
// publishing and presence only require that the stream already joined. The
// endpoint sits on the join path, so it should be close to Sluice.
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
	// TTL overrides the configured cache lifetime for this decision, in seconds.
	// An application that knows a grant is short-lived can say so.
	TTL int `json:"ttl,omitempty"`
}

type hookVerdict struct {
	allow   bool
	reason  string
	expires time.Time
}

type hookCache struct {
	ttl     time.Duration
	timeout time.Duration
	bearer  string
	client  *http.Client

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
		client:  &http.Client{Timeout: timeout, CheckRedirect: noHookRedirect},
		m:       map[string]hookVerdict{},
	}
}

// noHookRedirect stops the client from following a redirect. Following would
// accept a verdict from a URL nobody configured: a 302 turns the POST into a
// GET without the body, so an allow:true there says nothing about the caller,
// and a 307 re-sends the identity (and, on the same host, the bearer) to the
// new location. The 3xx is then denied like any other non-2xx.
func noHookRedirect(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

// Authorize asks the configured endpoint whether this caller may join a channel,
// caching the verdict.
//
// Fails CLOSED: a timeout, a non-2xx, or an unparseable body all deny. An
// application whose authorization service is down should stop granting access,
// not start granting it to everyone.
func (h *hookCache) Authorize(ctx context.Context, ch config.Channel, id authz.Identity, channel string) (bool, string) {
	key := ch.HookURL + "\x00" + channel + "\x00" + id.Role + "\x00" + id.Sub + "\x00" + id.SessionID

	h.mu.RLock()
	v, ok := h.m[key]
	h.mu.RUnlock()
	if ok && time.Now().Before(v.expires) {
		return v.allow, v.reason
	}

	allow, reason, ttl := h.ask(ctx, ch, id, channel)

	h.mu.Lock()
	// Bound the cache so a channel-name-spraying client cannot grow it without
	// limit. Dropping everything is crude but correct, and only costs a burst of
	// re-asks.
	if len(h.m) > 10000 {
		h.m = make(map[string]hookVerdict, 1024)
	}
	h.m[key] = hookVerdict{allow: allow, reason: reason, expires: time.Now().Add(ttl)}
	h.mu.Unlock()

	return allow, reason
}

func (h *hookCache) ask(ctx context.Context, ch config.Channel, id authz.Identity, channel string) (bool, string, time.Duration) {
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
		return false, "could not encode the authorization request", h.ttl
	}

	ctx, cancel := context.WithTimeout(ctx, h.timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, ch.HookURL, bytes.NewReader(body))
	if err != nil {
		return false, "invalid hook URL", h.ttl
	}
	req.Header.Set("Content-Type", "application/json")
	if h.bearer != "" {
		req.Header.Set("Authorization", "Bearer "+h.bearer)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return false, fmt.Sprintf("authorization endpoint unreachable: %v", err), hookRetryTTL
	}
	defer resp.Body.Close()

	switch {
	case resp.StatusCode == http.StatusForbidden:
		return false, "the authorization endpoint denied this channel", h.ttl
	case resp.StatusCode == http.StatusUnauthorized:
		// The endpoint rejected Sluice's own credential, which says nothing
		// about this caller and is fixed by configuration, not by waiting.
		return false, "the authorization endpoint rejected Sluice's credential (HTTP 401); check SLUICE_CHANNEL_HOOK_BEARER", hookRetryTTL
	case resp.StatusCode < 200 || resp.StatusCode >= 300:
		return false, fmt.Sprintf("authorization endpoint returned %d", resp.StatusCode), hookRetryTTL
	}

	var out hookResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&out); err != nil {
		return false, "authorization endpoint returned an unreadable body", hookRetryTTL
	}
	ttl := h.ttl
	if out.TTL > 0 {
		ttl = time.Duration(out.TTL) * time.Second
	}
	if !out.Allow && out.Reason == "" {
		out.Reason = "the authorization endpoint denied this channel"
	}
	return out.Allow, out.Reason, ttl
}
