package server

import (
	"context"
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
		if ok, reason := h.Authorize(context.Background(), hookChannel(srv.URL), hookCaller, "billing:1"); !ok {
			t.Fatalf("bearer=%q: denied: %s", bearer, reason)
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
		ok, _ := h.Authorize(context.Background(), hookChannel(srv.URL+"/authz"), hookCaller, "billing:1")
		srv.Close()
		if ok {
			t.Errorf("%d: a redirect was followed to an allow", code)
		}
		if allowHits.Load() != 0 {
			t.Errorf("%d: the redirect target was contacted", code)
		}
	}
}

// 403 is a verdict about the caller and is cached for the TTL. 401 is the
// endpoint rejecting Sluice's credential, fixed by configuration, so it is
// retried soon and reported differently.
func TestHook401IsRetriedSoonAnd403IsCached(t *testing.T) {
	for _, tc := range []struct {
		status int
		ttl    time.Duration
	}{{http.StatusForbidden, time.Minute}, {http.StatusUnauthorized, hookRetryTTL}} {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(tc.status)
		}))
		h := newHookCache(time.Minute, time.Second, "hook-secret")
		ok, reason, ttl := h.ask(context.Background(), hookChannel(srv.URL), hookCaller, "billing:1")
		srv.Close()
		if ok {
			t.Fatalf("%d allowed", tc.status)
		}
		if ttl != tc.ttl {
			t.Errorf("%d cached for %s, want %s (%s)", tc.status, ttl, tc.ttl, reason)
		}
	}
}
