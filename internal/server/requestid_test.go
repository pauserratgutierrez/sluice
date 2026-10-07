package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/requestid"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

// logSink collects a server's JSON log lines.
type logSink struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func logTo(s *Server) *logSink {
	l := &logSink{}
	s.log = slog.New(slog.NewJSONHandler(l, &slog.HandlerOptions{Level: slog.LevelDebug}))
	return l
}

func (l *logSink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logSink) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// records returns the lines logged with msg whose fields include match.
func (l *logSink) records(t *testing.T, msg string, match map[string]string) []map[string]any {
	t.Helper()
	var out []map[string]any
	for line := range strings.Lines(l.String()) {
		var rec map[string]any
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("log line %q: %v", line, err)
		}
		if rec["msg"] != msg {
			continue
		}
		matched := true
		for k, v := range match {
			if rec[k] != v {
				matched = false
			}
		}
		if matched {
			out = append(out, rec)
		}
	}
	return out
}

// one returns the single line logged with msg whose fields include match.
func (l *logSink) one(t *testing.T, msg string, match map[string]string) map[string]any {
	t.Helper()
	recs := l.records(t, msg, match)
	if len(recs) != 1 {
		t.Fatalf("%d lines %q with %v, want 1; log:\n%s", len(recs), msg, match, l.String())
	}
	return recs[0]
}

// endpoint is an issuer or hook that answers body and records each call's
// headers.
type endpoint struct {
	srv  *httptest.Server
	mu   sync.Mutex
	hdrs []http.Header
}

func newEndpoint(t *testing.T, body string) *endpoint {
	e := &endpoint{}
	e.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		e.mu.Lock()
		e.hdrs = append(e.hdrs, r.Header.Clone())
		e.mu.Unlock()
		_, _ = io.WriteString(w, body)
	}))
	t.Cleanup(e.srv.Close)
	return e
}

// got returns the value of header on each call so far, "" where it was absent.
func (e *endpoint) got(header string) []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]string, len(e.hdrs))
	for i, h := range e.hdrs {
		out[i] = h.Get(header)
	}
	return out
}

// traced is the whole HTTP surface in issuer mode, with an issuer that denies
// every shape, a hook namespace `chat` that denies every join, a public
// namespace `room`, and its log captured.
type traced struct {
	s            *Server
	srv          *httptest.Server
	token        string
	logs         *logSink
	issuer, hook *endpoint
}

func newTraced(t *testing.T) *traced {
	t.Helper()
	s, srv, token := streamServer(t)
	tr := &traced{s: s, srv: srv, token: token, logs: logTo(s),
		issuer: newEndpoint(t, `{"allow": false}`),
		hook:   newEndpoint(t, `{"allow": false, "reason": "not a member", "ttl": 0}`),
	}
	orc, err := oracle.NewIssuerWith(oracle.IssuerOptions{
		URL: tr.issuer.srv.URL, Bearer: "issuer-secret", Timeout: time.Second, Lookup: s.cat.Lookup,
	})
	if err != nil {
		t.Fatal(err)
	}
	s.oracle, s.cfg.ShapeOracle = orc, oracle.NameIssuer
	s.cat.PutForTest(docsRel())
	s.cfg.Channels = []config.Channel{
		{Namespace: "chat", Mode: config.ChannelHook, HookURL: tr.hook.srv.URL},
		{Namespace: "room", Mode: config.ChannelPublic},
	}
	s.cfg.MaxSubsPerStream, s.cfg.MaxShapesPerStream = 10, 10
	s.cfg.MaxPayloadBytes = 1 << 10
	return tr
}

// post sends a request with the caller's token, and with requestID in header
// unless it is empty.
func (tr *traced) post(t *testing.T, path, header, requestID, token string, body any) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(t.Context(), http.MethodPost, tr.srv.URL+"/sluice/v1"+path, bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	if requestID != "" {
		req.Header.Set(header, requestID)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { res.Body.Close() })
	return res
}

type readyEvent struct {
	StreamID      string          `json:"stream_id"`
	Subscriptions json.RawMessage `json:"subscriptions"`
}

func (tr *traced) open(t *testing.T, header, requestID string, body any) (*http.Response, *bufio.Reader, readyEvent, string) {
	t.Helper()
	res := tr.post(t, "/stream", header, requestID, tr.token, body)
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST /stream = %d", res.StatusCode)
	}
	r := bufio.NewReader(res.Body)
	ev, err := readEvent(r)
	if err != nil || ev.name != "ready" {
		t.Fatalf("first event = %+v, %v; want ready", ev, err)
	}
	var ready readyEvent
	if err := json.Unmarshal([]byte(ev.data), &ready); err != nil {
		t.Fatal(err)
	}
	return res, r, ready, ev.data
}

// A request's ID reaches the issuer and the hook called for it and is on the
// lines logged about it. Without one, Sluice makes one and uses it for both.
// The response and its events are the same either way.
func TestRequestIDReachesIssuerAndHookAndIsLogged(t *testing.T) {
	subs := map[string]any{"subscriptions": []map[string]any{
		{"sub": "docs", "shape": map[string]any{"table": "documents", "filter": "project_id=eq.42"}},
		{"sub": "chat", "channel": "chat:1"},
	}}
	var results []string
	var headerNames [][]string
	for _, tc := range []struct{ name, header, sent string }{
		{"forwarded", "X-Request-ID", "gw-7f3a9c"},
		{"generated", "X-Request-ID", ""},
		{"configured header", "X-Correlation-ID", "corr-42"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTraced(t)
			tr.s.cfg.RequestIDHeader = tc.header
			res, _, ready, data := tr.open(t, tc.header, tc.sent, subs)

			issuer, hook := tr.issuer.got(tc.header), tr.hook.got(tc.header)
			if len(issuer) != 1 || len(hook) != 1 {
				t.Fatalf("issuer called %d times, hook %d; want once each", len(issuer), len(hook))
			}
			id := issuer[0]
			switch {
			case tc.sent != "" && id != tc.sent:
				t.Fatalf("issuer got %s %q, want the request's %q", tc.header, id, tc.sent)
			case tc.sent == "" && !uuidV4.MatchString(id):
				t.Fatalf("issuer got %s %q, want a generated UUID", tc.header, id)
			case hook[0] != id:
				t.Fatalf("hook got %s %q, issuer got %q; want the same ID", tc.header, hook[0], id)
			}

			for sub, code := range map[string]string{"docs": "shape_not_authorized", "chat": "channel_not_authorized"} {
				rec := tr.logs.one(t, "subscription refused", map[string]string{"sub": sub})
				if rec["request_id"] != id || rec["code"] != code || rec["stream_id"] != ready.StreamID {
					t.Errorf("%s refusal logged as %v, want request_id %q, code %s, stream_id %s", sub, rec, id, code, ready.StreamID)
				}
				if _, ok := rec["stream_request_id"]; ok {
					t.Errorf("%s: the stream's own request logged a stream_request_id: %v", sub, rec)
				}
			}

			if strings.Contains(data, id) {
				t.Errorf("the ready event carries the request ID: %s", data)
			}
			for name, vals := range res.Header {
				if slices.Contains(vals, id) {
					t.Errorf("response header %s carries the request ID", name)
				}
			}
			results = append(results, string(ready.Subscriptions))
			headerNames = append(headerNames, slices.Sorted(maps.Keys(res.Header)))

			// A client going away is how streams end; it is not logged.
			res.Body.Close()
			waitFor(t, func() bool { return tr.s.hub.Count() == 0 })
			if recs := tr.logs.records(t, "stream ended", nil); len(recs) != 0 {
				t.Errorf("a client disconnect was logged: %v", recs)
			}
		})
	}
	for i := range results[1:] {
		if results[i+1] != results[0] {
			t.Errorf("results differ with the request ID:\n%s\n%s", results[0], results[i+1])
		}
		if !slices.Equal(headerNames[i+1], headerNames[0]) {
			t.Errorf("response headers differ with the request ID: %v, %v", headerNames[0], headerNames[i+1])
		}
	}
}

// A stream keeps the ID of the request that opened it: a later request on the
// stream logs both, and the line logged when the server ends the stream
// carries the one that opened it. Nothing secret is logged on the way.
func TestStreamKeepsTheRequestIDThatOpenedIt(t *testing.T) {
	tr := newTraced(t)
	tr.s.cfg.PublishRate = 1
	const h = "X-Request-ID"
	_, r, ready, _ := tr.open(t, h, "gw-open", map[string]any{})
	stream := ready.StreamID
	// with matches a line about request rid on the stream, and code if set.
	with := func(rid, code string) map[string]string {
		m := map[string]string{"request_id": rid, "stream_id": stream, "stream_request_id": "gw-open"}
		if code != "" {
			m["code"] = code
		}
		return m
	}

	res := tr.post(t, "/subscribe", h, "gw-sub", tr.token, map[string]any{"stream_id": stream,
		"subscriptions": []map[string]any{{"sub": "chat", "channel": "chat:1"}, {"sub": "room", "channel": "room:1"}}})
	if res.StatusCode != http.StatusOK {
		t.Fatalf("POST /subscribe = %d", res.StatusCode)
	}
	if got := tr.hook.got(h); len(got) != 1 || got[0] != "gw-sub" {
		t.Fatalf("hook got %s %v, want the subscribe's gw-sub", h, got)
	}
	tr.logs.one(t, "subscription refused", with("gw-sub", "channel_not_authorized"))

	publish := func(rid, token, channel string) int {
		res := tr.post(t, "/publish", h, rid, token, map[string]any{"stream_id": stream,
			"channel": channel, "payload": map[string]string{"note": "payload-marker"}})
		return res.StatusCode
	}
	if code := publish("gw-pub-1", tr.token, "chat:1"); code != http.StatusForbidden {
		t.Fatalf("publish to a refused channel = %d, want 403", code)
	}
	tr.logs.one(t, "request refused", with("gw-pub-1", "channel_not_subscribed"))
	if code := publish("gw-pub-2", tr.token, "room:1"); code != http.StatusOK {
		t.Fatalf("publish = %d", code)
	}
	if code := publish("gw-pub-3", tr.token, "room:1"); code != http.StatusTooManyRequests {
		t.Fatalf("publish over the rate = %d, want 429", code)
	}
	tr.logs.one(t, "request refused", with("gw-pub-3", "rate_limited"))
	const badToken = "eyJhbGciOiJFUzI1NiJ9.eyJzdWIiOiJ1MSJ9.c2lnbmF0dXJl"
	if code := publish("gw-bad", badToken, "room:1"); code != http.StatusUnauthorized {
		t.Fatalf("publish with a bad token = %d, want 401", code)
	}
	tr.logs.one(t, "request refused", map[string]string{"request_id": "gw-bad", "code": "unauthorized"})

	tr.s.DrainFor("replication_stopped")
	for {
		if _, err := readEvent(r); err != nil {
			break
		}
	}
	rec := tr.logs.one(t, "stream ended", map[string]string{"stream_id": stream})
	if rec["request_id"] != "gw-open" || rec["code"] != "server_shutdown" || rec["cause"] != "replication_stopped" {
		t.Errorf("stream end logged as %v, want request_id gw-open, code server_shutdown, cause replication_stopped", rec)
	}

	for _, secret := range []string{tr.token, badToken, "issuer-secret", "payload-marker"} {
		if strings.Contains(tr.logs.String(), secret) {
			t.Errorf("the log contains %q:\n%s", secret, tr.logs.String())
		}
	}
}

// A hook re-check on the tick serves no request, so it sends no request ID.
func TestTickHookRecheckSendsNoRequestID(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	hook := newEndpoint(t, `{"allow": true, "ttl": 0}`)
	s.cfg.Channels = []config.Channel{{Namespace: "chat", Mode: config.ChannelHook, HookURL: hook.srv.URL}}
	st := s.hub.Open("n1.tick", "gw-open", alice)

	ctx := requestid.With(context.Background(), requestid.ID{Header: "X-Request-ID", Value: "gw-join"})
	if res := s.subscribeChannel(ctx, st, alice, subSpec{Sub: "typing", Channel: chatChannel}); !res.OK {
		t.Fatalf("join refused: %+v", res.Error)
	}
	s.recheckHookChannels(context.Background(), time.Now().Add(time.Second))

	if got := hook.got("X-Request-ID"); !slices.Equal(got, []string{"gw-join", ""}) {
		t.Fatalf("hook got X-Request-ID %q, want the join's then none", got)
	}
}

// One request logs a bounded number of refused subscriptions.
func TestRefusalLinesAreBounded(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	logs := logTo(s)
	s.cfg.MaxSubsPerStream = 100
	st := s.hub.Open("n1.many", "gw-open", alice)
	var specs []subSpec
	for i := range maxRefusalLines + 5 {
		specs = append(specs, subSpec{Sub: fmt.Sprint("s", i), Channel: fmt.Sprintf("nowhere:%d", i)})
	}
	s.applySubscriptions(context.Background(), st, alice, specs, nil)

	if n := len(logs.records(t, "subscription refused", nil)); n != maxRefusalLines {
		t.Errorf("%d refusals logged, want %d", n, maxRefusalLines)
	}
	if rec := logs.one(t, "more subscriptions refused", nil); rec["count"] != float64(5) {
		t.Errorf("summary = %v, want count 5", rec)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("timed out")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
