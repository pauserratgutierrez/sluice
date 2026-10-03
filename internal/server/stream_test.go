package server

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
)

// streamServer serves the whole HTTP surface with a real token verifier, and
// returns a token it accepts.
func streamServer(t *testing.T) (*Server, *httptest.Server, string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	b64 := base64.RawURLEncoding.EncodeToString
	jwks, _ := json.Marshal(map[string]any{"keys": []map[string]string{{
		"kty": "EC", "kid": "k", "crv": "P-256",
		"x": b64(key.X.FillBytes(make([]byte, 32))), "y": b64(key.Y.FillBytes(make([]byte, 32))),
	}}})
	keys := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(jwks) }))
	t.Cleanup(keys.Close)

	s := testServer(t, stubOracle{name: oracle.NameRLS})
	s.cfg.MaxStreams = 10
	s.cfg.WriteTimeout = 5 * time.Second
	s.cfg.ReconnectSpread = time.Second
	s.verify = auth.NewVerifier(keys.URL, "ES256", "", "authenticated", time.Second, time.Minute, []string{"authenticated"})
	if err := s.verify.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	tok := jwt.NewWithClaims(jwt.SigningMethodES256, jwt.MapClaims{
		"role": "authenticated", "sub": "u1", "aud": "authenticated",
		"exp": time.Now().Add(time.Hour).Unix(),
	})
	tok.Header["kid"] = "k"
	signed, err := tok.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}
	return s, srv, signed
}

func postStream(t *testing.T, srv *httptest.Server, token string) *http.Response {
	t.Helper()
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, srv.URL+"/sluice/v1/stream", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

type sseEvent struct{ name, data string }

// readEvent returns the next event, skipping comments, or io.EOF.
func readEvent(r *bufio.Reader) (sseEvent, error) {
	var ev sseEvent
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return ev, err
		}
		line = strings.TrimSuffix(line, "\n")
		switch {
		case line == "":
			if ev.name != "" {
				return ev, nil
			}
		case strings.HasPrefix(line, "event: "):
			ev.name = strings.TrimPrefix(line, "event: ")
		case strings.HasPrefix(line, "data: "):
			ev.data = strings.TrimPrefix(line, "data: ")
		}
	}
}

func openReady(t *testing.T, s *Server, srv *httptest.Server, token string) (*bufio.Reader, string) {
	t.Helper()
	res := postStream(t, srv, token)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST /stream = %d", res.StatusCode)
	}
	r := bufio.NewReader(res.Body)
	ev, err := readEvent(r)
	if err != nil || ev.name != "ready" {
		t.Fatalf("first event = %+v, %v; want ready", ev, err)
	}
	var ready struct {
		StreamID string `json:"stream_id"`
	}
	_ = json.Unmarshal([]byte(ev.data), &ready)
	return r, ready.StreamID
}

// A burst queued faster than the writer drains it arrives whole and in order.
func TestStreamWritesBurstInOrder(t *testing.T) {
	s, srv, token := streamServer(t)
	r, id := openReady(t, s, srv, token)
	st, ok := s.hub.Get(id)
	if !ok {
		t.Fatal("stream not registered")
	}
	const n = 16 // the test hub's queue size
	for i := 0; i < n; i++ {
		if !st.Send(event.Event{Kind: event.KindBroadcast, Data: event.Broadcast{Sub: "b", Event: fmt.Sprint(i)}}) {
			t.Fatalf("send %d refused", i)
		}
	}
	for i := 0; i < n; i++ {
		ev, err := readEvent(r)
		if err != nil {
			t.Fatal(err)
		}
		var b event.Broadcast
		if err := json.Unmarshal([]byte(ev.data), &b); err != nil || ev.name != "broadcast" || b.Event != fmt.Sprint(i) {
			t.Fatalf("event %d = %+v, want broadcast %d", i, ev, i)
		}
	}
}

// Shutdown ends every stream with a reason and a spread-out retry delay, and
// refuses new streams, instead of leaving them open until the process is killed.
func TestDrainEndsStreams(t *testing.T) {
	s, srv, token := streamServer(t)
	r, _ := openReady(t, s, srv, token)

	s.Drain()

	ev, err := readEvent(r)
	if err != nil {
		t.Fatal(err)
	}
	var e event.Error
	if err := json.Unmarshal([]byte(ev.data), &e); err != nil || ev.name != "error" || e.Code != "server_shutdown" {
		t.Fatalf("last event = %+v, want error server_shutdown", ev)
	}
	if !e.Retryable || e.RetryAfterMs < 0 || e.RetryAfterMs >= 1000 {
		t.Errorf("error = %+v, want retryable with retry_after_ms in [0, 1000)", e)
	}
	if _, err := readEvent(r); err != io.EOF {
		t.Fatalf("stream still open after the shutdown error: %v", err)
	}

	res := postStream(t, srv, token)
	if res.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("POST /stream while draining = %d, want 503", res.StatusCode)
	}
}
