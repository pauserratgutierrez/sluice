package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/config"
)

func hookChannel(url string) config.Channel {
	return config.Channel{Namespace: "billing", Mode: config.ChannelHook, HookURL: url}
}

var hookCaller = authz.Identity{Role: "authenticated", Sub: "u1", ClaimsRaw: `{"sub":"u1"}`}

func TestHookSendsBearerOnlyWhenConfigured(t *testing.T) {
	for _, bearer := range []string{"", "hook-secret"} {
		var got []string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = r.Header.Values("Authorization")
			_, _ = w.Write([]byte(`{"allow":true}`))
		}))
		h := newHookCache(time.Minute, time.Second, bearer)
		if v := h.Authorize(context.Background(), hookChannel(srv.URL), hookCaller, "billing:1", false); !v.allowed() {
			t.Fatalf("bearer=%q: denied: %s", bearer, v.reason)
		}
		srv.Close()
		switch {
		case bearer == "" && len(got) != 0:
			t.Errorf("no bearer configured, but Authorization was sent: %v", got)
		case bearer != "" && (len(got) != 1 || got[0] != "Bearer "+bearer):
			t.Errorf("Authorization = %v, want Bearer %s", got, bearer)
		}
	}
}

// A redirect must deny rather than accept a verdict from a URL nobody
// configured, and the redirect target must never be contacted.
func TestHookDoesNotFollowRedirects(t *testing.T) {
	for _, code := range []int{http.StatusFound, http.StatusTemporaryRedirect} {
		var allowHits atomic.Int32
		mux := http.NewServeMux()
		mux.HandleFunc("/allow", func(w http.ResponseWriter, r *http.Request) {
			allowHits.Add(1)
			_, _ = w.Write([]byte(`{"allow":true}`))
		})
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/allow", code)
		})
		srv := httptest.NewServer(mux)
		h := newHookCache(time.Minute, time.Second, "hook-secret")
		v := h.Authorize(context.Background(), hookChannel(srv.URL+"/authz"), hookCaller, "billing:1", false)
		srv.Close()
		if v.allowed() {
			t.Errorf("%d: a redirect was followed to an allow", code)
		}
		if allowHits.Load() != 0 {
			t.Errorf("%d: the redirect target was contacted", code)
		}
	}
}

// 403 is a verdict about the caller and is cached for the TTL. 401 is the
// endpoint rejecting Sluice's credential, fixed by configuration, so it says
// nothing about the caller and is retried soon.
func TestHook401IsRetriedSoonAnd403IsCached(t *testing.T) {
	for _, tc := range []struct {
		status  int
		outcome hookOutcome
		ttl     time.Duration
	}{{http.StatusForbidden, hookDeny, time.Minute}, {http.StatusUnauthorized, hookUnavailable, hookRetryTTL}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		h := newHookCache(time.Minute, time.Second, "hook-secret")
		outcome, reason, ttl := h.ask(context.Background(), hookChannel(srv.URL), hookCaller, "billing:1")
		srv.Close()
		if outcome != tc.outcome {
			t.Fatalf("%d: outcome %d, want %d (%s)", tc.status, outcome, tc.outcome, reason)
		}
		if ttl != tc.ttl {
			t.Errorf("%d cached for %s, want %s (%s)", tc.status, ttl, tc.ttl, reason)
		}
	}
}

// A verdict is trusted for at most its TTL, and verdicts issued together
// expire spread over the last fifth of it.
func TestHookVerdictExpiryIsJitteredWithinTTL(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"allow":true}`))
	}))
	defer srv.Close()
	h := newHookCache(time.Minute, time.Second, "")
	now := time.Now()
	h.now = func() time.Time { return now }
	seen := map[time.Time]bool{}
	for i := range 20 {
		v := h.Authorize(context.Background(), hookChannel(srv.URL), hookCaller, fmt.Sprintf("billing:%d", i), false)
		if left := v.expires.Sub(now); left > time.Minute || left < 48*time.Second {
			t.Fatalf("verdict valid for %s, want within [48s, 60s]", left)
		}
		seen[v.expires] = true
	}
	if len(seen) < 2 {
		t.Error("20 verdicts issued together all expire at the same instant")
	}
}
