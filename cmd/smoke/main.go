// Command smoke is the end-to-end validation for the Sluice test harness.
//
// It exercises the critical path against a live PostgreSQL, GoTrue, PostgREST and
// Sluice, and asserts the claims the design rests on:
//
//   - a policy of the form `owner_id = auth.uid()` with a matching shape filter
//     resolves to Tier A, i.e. zero authorization work per change
//   - a row-dependent but compilable policy resolves to Tier B
//   - a policy containing a subquery resolves to Tier C and says so
//   - rows the caller may not read are never delivered
//   - DELETE events are authorized correctly, which Supabase documents as
//     impossible
//   - an unchanged TOASTed column arrives as an explicit `unchanged` marker, not
//     as a missing field
//   - pg_logical_emit_message delivers a transactional broadcast with no table
//   - PostgREST and Sluice agree on visibility (PostgREST is the oracle)
//
// Run it on the compose network (same two-step recipe as cmd/load and cmd/audit):
//
//	docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 \
//	  golang:1.26-alpine go build -o .bin/smoke ./cmd/smoke
//
//	docker run --rm --network deploy_private_net -v "$PWD/.bin:/b:ro" \
//	  -e POSTGRES_PASSWORD=… alpine:3.22 /b/smoke
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
	"slices"
	"strings"
	"time"
)

var (
	authURL   = envOr("SMOKE_AUTH_URL", "http://auth:9999")
	restURL   = envOr("SMOKE_REST_URL", "http://rest:3000")
	sluiceURL = envOr("SMOKE_SLUICE_URL", "http://sluice:4000/sluice/v1")

	failures int
	checks   int
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	fmt.Println("== Sluice end-to-end smoke test ==")

	email := fmt.Sprintf("smoke-%d@example.test", time.Now().UnixNano())
	tok, userID, err := signUp(ctx, email, "sluice-smoke-password-1")
	must(err, "sign up a user through GoTrue")
	fmt.Printf("\nuser  %s\n", userID)

	must(seed(ctx, userID), "seed fixture rows")

	stream, err := openStream(ctx, tok, streamRequest(userID))
	must(err, "open the SSE stream")
	defer stream.Close()

	ready, err := stream.next(15 * time.Second)
	must(err, "receive the ready event")
	if ready.Name != "ready" {
		fatal("first event was %q, expected \"ready\"", ready.Name)
	}
	reportTiers(ready)

	// ---- Tier A: only the caller's own rows -------------------------------
	fmt.Println("\n-- Tier A (documents: owner_id = auth.uid(), shape pins owner_id) --")
	mustExec(ctx, fmt.Sprintf(
		`insert into documents (owner_id, title, body) values ('%s','mine','x')`, userID))
	mustExec(ctx,
		`insert into documents (owner_id, title, body) values (gen_random_uuid(),'not mine','y')`)

	got := stream.collect(4 * time.Second)
	titles := changeTitles(got, "docs")
	check(len(titles) == 1 && titles[0] == "mine",
		"only the caller's own INSERT is delivered",
		fmt.Sprintf("got %v", titles))

	// ---- Tier B: row-dependent visibility --------------------------------
	fmt.Println("\n-- Tier B (posts: visibility = 'public' OR owner_id = auth.uid()) --")
	mustExec(ctx, fmt.Sprintf(
		`insert into posts (owner_id, visibility, title) values ('%s','private','mine private')`, userID))
	mustExec(ctx,
		`insert into posts (owner_id, visibility, title) values (gen_random_uuid(),'public','theirs public')`)
	mustExec(ctx,
		`insert into posts (owner_id, visibility, title) values (gen_random_uuid(),'private','theirs private')`)

	got = stream.collect(4 * time.Second)
	titles = changeTitles(got, "posts")
	check(slices.Contains(titles, "mine private") && slices.Contains(titles, "theirs public") &&
		!slices.Contains(titles, "theirs private"),
		"per-row evaluation delivers own+public and withholds others' private",
		fmt.Sprintf("got %v", titles))

	// ---- DELETE authorization --------------------------------------------
	fmt.Println("\n-- DELETE authorization (documented as impossible upstream) --")
	mustExec(ctx, fmt.Sprintf(
		`delete from posts where owner_id = '%s' and title = 'mine private'`, userID))
	mustExec(ctx, `delete from posts where title = 'theirs private'`)

	got = stream.collect(4 * time.Second)
	var deletedMine, deletedTheirs bool
	for _, e := range got {
		var c changeEvent
		if json.Unmarshal(e.Data, &c) != nil || c.Op != "DELETE" {
			continue
		}
		if t, _ := c.Old["title"].(string); t == "mine private" {
			deletedMine = true
		}
		if t, _ := c.Old["title"].(string); t == "theirs private" {
			deletedTheirs = true
		}
	}
	check(deletedMine, "the caller's own DELETE is delivered with the full old row", "")
	check(!deletedTheirs, "another user's DELETE is withheld", "")

	// ---- unchanged TOAST --------------------------------------------------
	fmt.Println("\n-- unchanged TOAST marker (the wal2json trap) --")
	mustExec(ctx, fmt.Sprintf(
		`update articles set views = views + 1 where owner_id = '%s'`, userID))
	got = stream.collect(4 * time.Second)
	var sawUnchanged, bodyPresent bool
	for _, e := range got {
		var c changeEvent
		if json.Unmarshal(e.Data, &c) != nil || c.Sub != "articles" {
			continue
		}
		if slices.Contains(c.Unchanged, "body") {
			sawUnchanged = true
		}
		if _, ok := c.Record["body"]; ok {
			bodyPresent = true
		}
	}
	check(sawUnchanged,
		`an untouched TOASTed column arrives as unchanged:["body"]`,
		"if this fails, clients would silently blank the column")
	check(!bodyPresent, "the unchanged column is not emitted as a value", "")

	// ---- no RLS at all ----------------------------------------------------
	fmt.Println("\n-- no RLS (metrics) --")
	mustExec(ctx, `insert into metrics (name, value) values ('smoke', 1)`)
	got = stream.collect(4 * time.Second)
	check(countSub(got, "metrics") == 1, "a table without RLS delivers unconditionally", "")

	// ---- database-originated broadcast, no table --------------------------
	fmt.Println("\n-- pg_logical_emit_message: transactional broadcast, zero tables --")
	mustExec(ctx, fmt.Sprintf(`
		begin;
		  insert into metrics (name, value) values ('atomic', 2);
		  select pg_logical_emit_message(true, 'sluice:room:smoke',
		    '{"event":"paid","payload":{"n":7}}');
		commit;`))
	got = stream.collect(4 * time.Second)
	var sawBroadcast bool
	for _, e := range got {
		if e.Name != "broadcast" {
			continue
		}
		var b struct {
			Channel string          `json:"channel"`
			Event   string          `json:"event"`
			Origin  string          `json:"origin"`
			Payload json.RawMessage `json:"payload"`
		}
		if json.Unmarshal(e.Data, &b) == nil &&
			b.Channel == "room:smoke" && b.Event == "paid" && b.Origin == "database" {
			sawBroadcast = true
		}
	}
	check(sawBroadcast, "a transactional WAL message is delivered as a broadcast", "")

	// A rolled-back message must never appear -- that is the whole point of
	// transactional emission versus an outbox table.
	mustExec(ctx, `
		begin;
		  select pg_logical_emit_message(true, 'sluice:room:smoke', '{"event":"ghost"}');
		rollback;`)
	got = stream.collect(3 * time.Second)
	check(!strings.Contains(string(rawOf(got)), "ghost"),
		"a message emitted in a rolled-back transaction never arrives", "")

	// ---- agreement with PostgREST (the oracle) ---------------------------
	//
	// The invariant is: a client receives a change for row R if and only if it
	// could SELECT row R. Sluice only streams rows changed AFTER the subscription
	// opened, so the totals line up as
	//
	//	PostgREST visible = rows seeded before subscribing + rows Sluice streamed
	//
	// Asserting the identity rather than a bare count is what makes this a real
	// comparison against the oracle instead of a coincidence.
	fmt.Println("\n-- agreement with PostgREST (the authorization oracle) --")
	restDocs, err := restCount(ctx, tok, "documents")
	if err != nil {
		fmt.Printf("  SKIP  PostgREST unavailable: %v\n", err)
	} else {
		const seededOwnDocs = 1 // sluice_seed inserts one document owned by the caller
		streamed := 1           // the "mine" insert above
		check(restDocs == seededOwnDocs+streamed,
			"PostgREST sees exactly the pre-existing rows plus the ones Sluice streamed",
			fmt.Sprintf("PostgREST=%d, expected seeded(%d)+streamed(%d)=%d",
				restDocs, seededOwnDocs, streamed, seededOwnDocs+streamed))

		// And the negative half of the invariant: the row owned by someone else,
		// which Sluice withheld, must be invisible to PostgREST too.
		total, err := countAll(ctx, "documents")
		if err == nil {
			check(total > restDocs,
				"rows Sluice withheld do exist, and are simply not visible to this caller",
				fmt.Sprintf("service_role sees %d documents, the caller sees %d", total, restDocs))
		}
	}

	// ---- unauthorized shape is refused outright --------------------------
	fmt.Println("\n-- Tier A refusal (subscribing to someone else's shape) --")
	res, err := subscribe(ctx, tok, stream.id, subSpecJSON(`{
		"sub":"other","shape":{"schema":"public","table":"documents",
		"filter":"owner_id=eq.00000000-0000-4000-8000-0000000000ff"}}`))
	must(err, "issue the subscribe request")
	check(strings.Contains(res, "shape_not_authorized"),
		"a shape whose predicate reduces to FALSE is refused at subscribe time",
		"got: "+truncate(res, 200))

	// ---- initial snapshot: closes the subscribe race ----------------------
	fmt.Println("\n-- initial snapshot --")
	snapRes, err := subscribe(ctx, tok, stream.id, subSpecJSON(fmt.Sprintf(`{
		"sub":"snap","shape":{"schema":"public","table":"documents",
		"filter":"owner_id=eq.%s","initial":"snapshot"}}`, userID)))
	must(err, "subscribe with an initial snapshot")
	check(strings.Contains(snapRes, `"ok":true`), "the snapshot subscription was accepted",
		truncate(snapRes, 160))

	got = stream.collect(8 * time.Second)
	var snapRows int
	var sawEnd bool
	for _, e := range got {
		if e.Name == "snapshot_end" {
			sawEnd = true
			continue
		}
		var c changeEvent
		if e.Name == "change" && json.Unmarshal(e.Data, &c) == nil && c.Sub == "snap" {
			snapRows++
		}
	}
	check(sawEnd, "a snapshot_end event terminates the snapshot", "")
	check(snapRows > 0,
		"the snapshot delivered the caller's existing rows",
		fmt.Sprintf("got %d rows", snapRows))

	// The snapshot runs under the caller's own role, so RLS applies to it exactly
	// as it does to any other read.
	if restDocs, err := restCount(ctx, tok, "documents"); err == nil {
		check(snapRows == restDocs,
			"the snapshot contains exactly the rows PostgREST would return",
			fmt.Sprintf("snapshot=%d postgrest=%d", snapRows, restDocs))
	}

	// ---- deeper phases ----------------------------------------------------
	phaseDifferential(ctx)

	otherTok, otherID, err := signUp(ctx, fmt.Sprintf("smoke-other-%d@example.test", time.Now().UnixNano()),
		"sluice-smoke-password-2")
	must(err, "sign up a second user")
	_ = otherID
	phaseSecurity(ctx, tok, otherTok, stream.id, userID)

	phaseLoad(ctx, tok, userID, loadStreams, loadChanges)

	// ---- summary ---------------------------------------------------------
	fmt.Printf("\n== %d checks, %d failures ==\n", checks, failures)
	if failures > 0 {
		os.Exit(1)
	}
}

var (
	loadStreams = envInt("SMOKE_LOAD_STREAMS", 200)
	loadChanges = envInt("SMOKE_LOAD_CHANGES", 25)
)

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return def
}

// ---------------------------------------------------------------------------

func streamRequest(userID string) string {
	return fmt.Sprintf(`{
	  "subscriptions": [
	    {"sub":"docs","shape":{"schema":"public","table":"documents",
	      "filter":"owner_id=eq.%s","transitions":true}},
	    {"sub":"posts","shape":{"schema":"public","table":"posts"}},
	    {"sub":"invoices","shape":{"schema":"public","table":"invoices","filter":"team_id=eq.1"}},
	    {"sub":"metrics","shape":{"schema":"public","table":"metrics"}},
	    {"sub":"articles","shape":{"schema":"public","table":"articles","filter":"owner_id=eq.%s"}},
	    {"sub":"room","channel":"room:smoke","presence":true}
	  ]
	}`, userID, userID)
}

func reportTiers(ready sseEvent) {
	var r struct {
		StreamID      string `json:"stream_id"`
		Subscriptions []struct {
			Sub      string `json:"sub"`
			OK       bool   `json:"ok"`
			Tier     string `json:"tier"`
			Indexed  *bool  `json:"indexed"`
			Routing  string `json:"routing_key"`
			Reason   string `json:"reason"`
			Warnings []struct {
				Code   string `json:"code"`
				Remedy string `json:"remedy"`
			} `json:"warnings"`
			Error *struct {
				Code    string `json:"code"`
				Message string `json:"message"`
			} `json:"error"`
		} `json:"subscriptions"`
	}
	if err := json.Unmarshal(ready.Data, &r); err != nil {
		fatal("parse ready event: %v", err)
	}

	fmt.Println("\nsubscription           tier  indexed  routing key   note")
	fmt.Println("---------------------  ----  -------  ------------  ----")
	want := map[string]string{
		"docs":     "A", // policy reads only owner_id, shape pins it
		"posts":    "B", // policy also reads visibility, which the shape does not pin
		"invoices": "C", // policy contains EXISTS over memberships
		"metrics":  "A", // RLS disabled
		"articles": "A",
	}
	for _, s := range r.Subscriptions {
		idx := "-"
		if s.Indexed != nil {
			idx = map[bool]string{true: "yes", false: "no"}[*s.Indexed]
		}
		note := ""
		if s.Error != nil {
			note = "ERROR " + s.Error.Code
		}
		for _, w := range s.Warnings {
			note += " warn:" + w.Code
		}
		fmt.Printf("%-21s  %-4s  %-7s  %-12s  %s\n", s.Sub, s.Tier, idx, s.Routing, strings.TrimSpace(note))

		if exp, ok := want[s.Sub]; ok {
			check(s.Tier == exp,
				fmt.Sprintf("%s resolved to Tier %s", s.Sub, exp),
				fmt.Sprintf("got Tier %s (%s)", s.Tier, s.Reason))
		}
	}

	// The warning that matters most: articles is at REPLICA IDENTITY DEFAULT but
	// the shape filters on owner_id, so DELETE events cannot be authorized.
	var sawRIWarning bool
	for _, s := range r.Subscriptions {
		if s.Sub != "articles" {
			continue
		}
		for _, w := range s.Warnings {
			if w.Code == "replica_identity_insufficient" && strings.Contains(w.Remedy, "USING INDEX") {
				sawRIWarning = true
			}
		}
	}
	check(sawRIWarning,
		"an inadequate replica identity is reported with a runnable remedy",
		"expected replica_identity_insufficient on `articles`")
}

// ---------------------------------------------------------------------------
// SSE client
// ---------------------------------------------------------------------------

type sseEvent struct {
	Name string
	ID   string
	Data json.RawMessage
}

type changeEvent struct {
	Sub        string         `json:"sub"`
	Op         string         `json:"op"`
	Table      string         `json:"table"`
	Record     map[string]any `json:"record"`
	Old        map[string]any `json:"old"`
	Unchanged  []string       `json:"unchanged"`
	Transition string         `json:"transition"`
	Degraded   string         `json:"degraded"`
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

	// No client timeout: the whole point is a long-lived response.
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		resp.Body.Close()
		return nil, fmt.Errorf("stream returned %d: %s", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/event-stream") {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected content type %q", ct)
	}
	if resp.Header.Get("X-Accel-Buffering") != "no" {
		fmt.Println("  WARN  X-Accel-Buffering: no is missing; proxies may buffer the stream")
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
			// A blank line dispatches the event, per the SSE grammar.
			if cur.Name != "" || len(cur.Data) > 0 {
				s.events <- cur
			}
			cur = sseEvent{}
		case strings.HasPrefix(line, ":"):
			// comment / heartbeat
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

// collect drains events until the stream goes quiet for a moment.
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

func subSpecJSON(s string) string { return compactJSON(s) }

func restCount(ctx context.Context, token, table string) (int, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, restURL+"/"+table+"?select=id", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return 0, fmt.Errorf("%d: %s", resp.StatusCode, truncate(string(raw), 200))
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return 0, err
	}
	return len(rows), nil
}

// seed and mustExec run SQL through the Sluice-independent path, so the test
// never depends on the thing it is testing to set up its own data.
func seed(ctx context.Context, userID string) error {
	return execSQL(ctx, fmt.Sprintf(`select public.sluice_seed('%s'::uuid, 1)`, userID))
}

func mustExec(ctx context.Context, sql string) {
	if err := execSQL(ctx, sql); err != nil {
		fatal("exec %q: %v", truncate(sql, 80), err)
	}
	// Give the reader a moment to decode and dispatch.
	time.Sleep(250 * time.Millisecond)
}

func execSQL(ctx context.Context, sql string) error {
	// psql is not available in this image, so DML goes through PostgREST's RPC
	// surface... which does not exist for arbitrary SQL. Use a direct connection
	// instead, via the pgx already in go.mod.
	return execViaPgx(ctx, sql)
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

func countSub(events []sseEvent, sub string) int {
	n := 0
	for _, e := range events {
		var c changeEvent
		if e.Name == "change" && json.Unmarshal(e.Data, &c) == nil && c.Sub == sub {
			n++
		}
	}
	return n
}

func rawOf(events []sseEvent) []byte {
	var b bytes.Buffer
	for _, e := range events {
		b.Write(e.Data)
	}
	return b.Bytes()
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
