// Command smoke-issuer is the issuer-oracle overlay for the Sluice harness.
//
// It does not replace cmd/smoke (the 36 RLS assertions). Same compose up, same
// private network; talk to the issuer process, not the RLS one.
//
//	docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 \
//	  golang:1.26-alpine go build -o .bin/smoke-issuer ./cmd/smoke-issuer
//
//	docker run --rm --network deploy_private_net -v "$PWD/.bin:/b:ro" \
//	  -e POSTGRES_PASSWORD=… alpine:3.22 /b/smoke-issuer
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

var (
	authURL   = envOr("SMOKE_AUTH_URL", "http://auth:9999")
	sluiceURL = envOr("SMOKE_SLUICE_URL", "http://sluice-issuer:4000/sluice-issuer/v1")
	stubURL   = envOr("SMOKE_STUB_URL", "http://issuer-stub:8080")

	failures int
	checks   int
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	fmt.Println("== Sluice issuer overlay smoke ==")

	aliceTok, aliceID, err := signUp(ctx, fmt.Sprintf("issuer-alice-%d@example.test", time.Now().UnixNano()),
		"sluice-issuer-smoke-1")
	must(err, "sign up Alice through GoTrue")
	bobTok, bobID, err := signUp(ctx, fmt.Sprintf("issuer-bob-%d@example.test", time.Now().UnixNano()),
		"sluice-issuer-smoke-2")
	must(err, "sign up Bob through GoTrue")
	fmt.Printf("\nAlice  %s\nBob    %s\n", aliceID, bobID)

	mustExec(ctx, fmt.Sprintf(
		`insert into iss_project_members (project_id, user_id) values ('alpha', '%s')`, aliceID))

	stream, err := openStream(ctx, aliceTok, aliceStreamBody())
	must(err, "open Alice's SSE stream")
	defer stream.Close()

	ready, err := stream.next(15 * time.Second)
	must(err, "receive the ready event")
	if ready.Name != "ready" {
		fatal("first event was %q, expected \"ready\"", ready.Name)
	}
	assertAliceReady(ready)

	fmt.Println("\n-- INSERT visibility --")
	mustExec(ctx, `insert into iss_documents (project_id, title, body) values ('alpha', 'in-alpha', 'x')`)
	mustExec(ctx, `insert into iss_documents (project_id, title, body) values ('beta', 'other-project', 'y')`)
	got := stream.collect(4 * time.Second)
	titles := changeTitles(got, "docs")
	check(len(titles) == 1 && titles[0] == "in-alpha",
		"Alice receives the alpha INSERT and not the other project",
		fmt.Sprintf("got %v", titles))

	fmt.Println("\n-- Bob is fail-closed on alpha --")
	bobStream, err := openStream(ctx, bobTok, aliceStreamBody())
	must(err, "open Bob's SSE stream")
	bobReady, err := bobStream.next(15 * time.Second)
	must(err, "receive Bob's ready event")
	denied := subscriptionDenied(bobReady, "docs")
	bobStream.Close()
	_ = stream.collect(500 * time.Millisecond)
	check(denied,
		"Bob's subscribe to alpha is denied (or not ok)",
		truncate(string(bobReady.Data), 240))

	fmt.Println("\n-- hold DELETE cuts the shape, not the stream --")
	mustExec(ctx, fmt.Sprintf(
		`delete from iss_project_members where project_id = 'alpha' and user_id = '%s'`, aliceID))
	cut, ok := stream.waitFor(8*time.Second, func(e sseEvent) bool {
		if e.Name != "error" {
			return false
		}
		return strings.Contains(string(e.Data), "shape_not_authorized")
	})
	check(ok, "DELETE of Alice's hold emits shape_not_authorized",
		"timed out waiting for the error event")
	if ok {
		check(strings.Contains(string(cut.Data), "shape_not_authorized"),
			"the cut error is shape_not_authorized", string(cut.Data))
	}

	fmt.Println("\n-- recreating the hold does not resurrect the label --")
	mustExec(ctx, fmt.Sprintf(
		`insert into iss_project_members (project_id, user_id) values ('alpha', '%s')`, aliceID))
	mustExec(ctx, `insert into iss_documents (project_id, title, body) values ('alpha', 'after-recreate', 'z')`)
	got = stream.collect(3 * time.Second)
	check(!containsTitle(got, "docs", "after-recreate"),
		"recreating the hold does not resurrect the cut label",
		fmt.Sprintf("got titles %v", changeTitles(got, "docs")))

	fmt.Println("\n-- resubscribe, then /token on a live shape --")
	subRes, err := subscribe(ctx, aliceTok, stream.id, compactJSON(`{
		"sub":"docs","shape":{"schema":"public","table":"iss_documents",
		  "filter":"project_id=eq.alpha"}}`))
	must(err, "resubscribe Alice after the cut")
	check(strings.Contains(subRes, `"ok":true`) && strings.Contains(subRes, `"oracle":"issuer"`),
		"a new subscribe on the still-open stream is accepted",
		truncate(subRes, 240))

	tokRes, err := postToken(ctx, aliceTok, stream.id)
	must(err, "POST /token with the same JWT")
	var tokenOut struct {
		OK      bool `json:"ok"`
		Revoked int  `json:"revoked_subscriptions"`
	}
	if json.Unmarshal([]byte(tokRes), &tokenOut) != nil {
		fatal("parse /token response: %s", tokRes)
	}
	check(tokenOut.OK && tokenOut.Revoked == 0,
		"POST /token on a live issuer shape returns revoked_subscriptions=0",
		tokRes)

	mustExec(ctx, `insert into iss_documents (project_id, title, body) values ('alpha', 'after-token', 'w')`)
	got = stream.collect(4 * time.Second)
	check(containsTitle(got, "docs", "after-token"),
		"an INSERT after /token is delivered on the live shape",
		fmt.Sprintf("got %v", changeTitles(got, "docs")))

	fmt.Println("\n-- stub does not receive access_token --")
	saw, n, err := stubSawAccessToken(ctx)
	must(err, "read issuer-stub /debug")
	check(n > 0, "the stub received at least one issuer POST", fmt.Sprintf("post_count=%d", n))
	check(!saw, "the stub never saw an access_token field", "")

	fmt.Println("\n-- channel on the issuer process --")
	mustExec(ctx, `
		select pg_logical_emit_message(true, 'sluice:room:issuer-smoke',
		  '{"event":"ping","payload":{"n":1}}');`)
	got = stream.collect(4 * time.Second)
	var sawBroadcast bool
	for _, e := range got {
		if e.Name != "broadcast" {
			continue
		}
		var b struct {
			Channel string `json:"channel"`
			Event   string `json:"event"`
			Origin  string `json:"origin"`
		}
		if json.Unmarshal(e.Data, &b) == nil &&
			b.Channel == "room:issuer-smoke" && b.Event == "ping" && b.Origin == "database" {
			sawBroadcast = true
		}
	}
	check(sawBroadcast, "a room channel on the issuer process still delivers", "")

	fmt.Printf("\n== %d checks, %d failures ==\n", checks, failures)
	if failures > 0 {
		os.Exit(1)
	}
}

func aliceStreamBody() string {
	return compactJSON(`{
	  "subscriptions": [
	    {"sub":"docs","shape":{"schema":"public","table":"iss_documents",
	      "filter":"project_id=eq.alpha"}},
	    {"sub":"room","channel":"room:issuer-smoke"}
	  ]
	}`)
}

func assertAliceReady(ready sseEvent) {
	var r struct {
		Subscriptions []struct {
			Sub    string `json:"sub"`
			OK     bool   `json:"ok"`
			Oracle string `json:"oracle"`
			Tier   string `json:"tier"`
			Filter string `json:"filter"`
			Error  *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(ready.Data, &r); err != nil {
		fatal("parse ready event: %v", err)
	}
	var docs *struct {
		Sub    string `json:"sub"`
		OK     bool   `json:"ok"`
		Oracle string `json:"oracle"`
		Tier   string `json:"tier"`
		Filter string `json:"filter"`
		Error  *struct {
			Code string `json:"code"`
		} `json:"error"`
	}
	for i := range r.Subscriptions {
		if r.Subscriptions[i].Sub == "docs" {
			docs = &r.Subscriptions[i]
			break
		}
	}
	if docs == nil {
		fatal("ready event had no docs subscription: %s", ready.Data)
	}
	check(docs.OK, "Alice's iss_documents subscription is ok", string(ready.Data))
	check(docs.Oracle == "issuer", `ready oracle is "issuer"`, docs.Oracle)
	check(docs.Tier == "", "issuer ready does not send tier", fmt.Sprintf("tier=%q", docs.Tier))
	check(strings.Contains(docs.Filter, "project_id"),
		"effective filter contains project_id", docs.Filter)
}

func subscriptionDenied(ready sseEvent, sub string) bool {
	var r struct {
		Subscriptions []struct {
			Sub   string `json:"sub"`
			OK    bool   `json:"ok"`
			Error *struct {
				Code string `json:"code"`
			} `json:"error"`
		} `json:"subscriptions"`
	}
	if json.Unmarshal(ready.Data, &r) != nil {
		return false
	}
	for _, s := range r.Subscriptions {
		if s.Sub != sub {
			continue
		}
		if !s.OK {
			return true
		}
		if s.Error != nil && (s.Error.Code == "shape_not_authorized" || s.Error.Code != "") {
			return true
		}
	}
	return strings.Contains(string(ready.Data), "shape_not_authorized")
}

type sseEvent struct {
	Name string
	ID   string
	Data json.RawMessage
}

type changeEvent struct {
	Sub    string         `json:"sub"`
	Op     string         `json:"op"`
	Record map[string]any `json:"record"`
}

type stream struct {
	id     string
	body   io.ReadCloser
	events chan sseEvent
	errs   chan error
}

func openStream(ctx context.Context, token, body string) (*stream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/stream",
		strings.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "text/event-stream")

	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("stream returned %d: %s", resp.StatusCode, b)
	}
	s := &stream{
		id:     resp.Header.Get("Sluice-Stream-Id"),
		body:   resp.Body,
		events: make(chan sseEvent, 256),
		errs:   make(chan error, 1),
	}
	go s.read()
	return s, nil
}

func (s *stream) read() {
	sc := bufio.NewScanner(s.body)
	sc.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	var cur sseEvent
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.Name != "" || len(cur.Data) > 0 {
				s.events <- cur
			}
			cur = sseEvent{}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			cur.Name = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "id:"):
			cur.ID = strings.TrimSpace(line[len("id:"):])
		case strings.HasPrefix(line, "data:"):
			cur.Data = append(cur.Data, []byte(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))...)
		}
	}
	if err := sc.Err(); err != nil {
		s.errs <- err
	}
	close(s.events)
}

func (s *stream) next(timeout time.Duration) (sseEvent, error) {
	select {
	case ev, ok := <-s.events:
		if !ok {
			return sseEvent{}, fmt.Errorf("stream closed")
		}
		return ev, nil
	case err := <-s.errs:
		return sseEvent{}, err
	case <-time.After(timeout):
		return sseEvent{}, fmt.Errorf("timed out after %s waiting for an event", timeout)
	}
}

func (s *stream) waitFor(timeout time.Duration, pred func(sseEvent) bool) (sseEvent, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return sseEvent{}, false
			}
			if pred(ev) {
				return ev, true
			}
		case <-deadline:
			return sseEvent{}, false
		}
	}
}

func (s *stream) collect(window time.Duration) []sseEvent {
	var out []sseEvent
	deadline := time.After(window)
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				return out
			}
			if ev.Name == "error" {
				fmt.Printf("  note  error event: %s\n", truncate(string(ev.Data), 160))
			}
			out = append(out, ev)
		case <-deadline:
			return out
		}
	}
}

func (s *stream) Close() { _ = s.body.Close() }

func signUp(ctx context.Context, email, password string) (token, userID string, err error) {
	body := fmt.Sprintf(`{"email":%q,"password":%q}`, email, password)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, authURL+"/signup",
		strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("signup returned %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	var out struct {
		AccessToken string `json:"access_token"`
		User        struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", "", err
	}
	if out.AccessToken == "" || out.User.ID == "" {
		return "", "", fmt.Errorf("signup response lacked a token or user id: %s", truncate(string(raw), 300))
	}
	return out.AccessToken, out.User.ID, nil
}

func subscribe(ctx context.Context, token, streamID, sub string) (string, error) {
	body := fmt.Sprintf(`{"stream_id":%q,"subscriptions":[%s]}`, streamID, sub)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/subscribe",
		strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Sluice-Stream-Id", streamID)
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw), nil
}

func postToken(ctx context.Context, token, streamID string) (string, error) {
	body, _ := json.Marshal(map[string]string{
		"stream_id":    streamID,
		"access_token": token,
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/token", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 20 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", fmt.Errorf("/token returned %d: %s", resp.StatusCode, truncate(string(raw), 300))
	}
	return string(raw), nil
}

func stubSawAccessToken(ctx context.Context) (saw bool, n int, err error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, stubURL+"/debug", nil)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return false, 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return false, 0, fmt.Errorf("stub /debug returned %d: %s", resp.StatusCode, raw)
	}
	var out struct {
		PostCount      int  `json:"post_count"`
		SawAccessToken bool `json:"saw_access_token"`
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return false, 0, err
	}
	return out.SawAccessToken, out.PostCount, nil
}

func mustExec(ctx context.Context, sql string) {
	if err := execViaPgx(ctx, sql); err != nil {
		fatal("exec %q: %v", truncate(sql, 80), err)
	}
	time.Sleep(250 * time.Millisecond)
}

func changeTitles(events []sseEvent, sub string) []string {
	var out []string
	for _, e := range events {
		if e.Name != "change" {
			continue
		}
		var c changeEvent
		if json.Unmarshal(e.Data, &c) != nil || c.Sub != sub {
			continue
		}
		if t, ok := c.Record["title"].(string); ok {
			out = append(out, t)
		}
	}
	return out
}

func containsTitle(events []sseEvent, sub, title string) bool {
	for _, t := range changeTitles(events, sub) {
		if t == title {
			return true
		}
	}
	return false
}

func check(ok bool, what, detail string) {
	checks++
	if ok {
		fmt.Printf("  PASS  %s\n", what)
		return
	}
	failures++
	fmt.Printf("  FAIL  %s\n", what)
	if detail != "" {
		fmt.Printf("        %s\n", detail)
	}
}

func must(err error, what string) {
	if err != nil {
		fatal("%s: %v", what, err)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "\nFATAL: "+format+"\n", args...)
	os.Exit(1)
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func compactJSON(s string) string {
	var b bytes.Buffer
	if err := json.Compact(&b, []byte(s)); err != nil {
		return s
	}
	return b.String()
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
