package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// ===========================================================================
// Differential testing: Sluice's compiled evaluator vs PostgreSQL itself
// ===========================================================================

// Sluice parses RLS predicates with a pure-Go port of PostgreSQL's grammar, so
// the parse tree matches. What it does not get for free is PostgreSQL's
// evaluation semantics, which Tier B reimplements in Go. The residual risk is an
// expression Sluice parses correctly and then evaluates differently.
//
// This phase attacks that directly: the same expression is evaluated against the
// same row by both engines and the verdicts compared. Anything that disagrees is
// a compiler bug, and anything Sluice declines is fine because it fails closed
// to an impersonated probe.
func phaseDifferential(ctx context.Context) {
	fmt.Println("\n-- differential: compiled evaluator vs PostgreSQL --")

	type row struct {
		id       int64
		ownerID  string
		title    string
		score    int64
		archived bool
		tags     string
	}
	rows := []row{
		{1, "11111111-1111-4111-8111-111111111111", "draft one", 5, false, `{"a","b"}`},
		{2, "22222222-2222-4222-8222-222222222222", "FINAL", 0, true, `{"b"}`},
		{3, "11111111-1111-4111-8111-111111111111", "", -3, false, `{}`},
	}

	// Every expression here is one a real policy could plausibly contain.
	exprs := []string{
		`owner_id = '11111111-1111-4111-8111-111111111111'::uuid`,
		`score > 0`,
		`score >= 0 AND score <= 5`,
		`score > 0 OR archived`,
		`NOT archived`,
		`NOT archived AND score > 0`,
		`score > 0 OR archived AND score < 0`,
		`title = 'FINAL'`,
		`title <> 'FINAL'`,
		`title LIKE 'draft%'`,
		`title ILIKE 'DRAFT%'`,
		`title NOT LIKE 'draft%'`,
		`title = ''`,
		`title IS NULL`,
		`title IS NOT NULL`,
		`score IN (0, 5)`,
		`score NOT IN (0, 5)`,
		`score BETWEEN 0 AND 5`,
		`score NOT BETWEEN 0 AND 5`,
		`score + 1 > 1`,
		`score * 2 = 10`,
		`score - 10 < 0`,
		`(score + 1) * 2 = 12`,
		`score + 1 * 2 = 7`,
		`lower(title) = 'final'`,
		`upper(title) = 'FINAL'`,
		`length(title) = 5`,
		`coalesce(nullif(title, ''), 'empty') = 'empty'`,
		`archived IS TRUE`,
		`archived IS FALSE`,
		`archived IS NOT TRUE`,
		`title = 'draft one' AND NOT archived AND score >= 5`,
		`(owner_id = '11111111-1111-4111-8111-111111111111'::uuid AND score > 0) OR archived`,
		`score::text = '5'`,
		`title::text || 'x' = 'FINALx'`,
	}

	mismatches := 0
	skipped := 0
	compared := 0

	for _, e := range exprs {
		for i, r := range rows {
			rowJSON, _ := json.Marshal(map[string]any{
				"id": r.id, "owner_id": r.ownerID, "title": r.title,
				"score": r.score, "archived": r.archived, "tags": r.tags,
			})

			pgSaid, pgNull, err := evalInPostgres(ctx, e, string(rowJSON))
			if err != nil {
				fmt.Printf("  note  PostgreSQL rejected %q: %v\n", e, err)
				skipped++
				break
			}

			node, perr := expr.Parse(e)
			if perr != nil {
				skipped++
				continue
			}
			if info := expr.Analyze(node, "diff"); !info.Compilable() {
				skipped++
				continue
			}

			goSaid, unknown := expr.Visible(node, &expr.Context{
				Row:        jsonRow{raw: rowJSON},
				Claims:     expr.Claims{},
				ClaimsJSON: "{}",
			})
			if unknown {
				skipped++
				continue
			}

			// PostgreSQL NULL and Sluice "not visible" agree for authorization
			// purposes: neither grants access.
			want := pgSaid && !pgNull
			compared++
			if goSaid != want {
				mismatches++
				fmt.Printf("  MISMATCH  %q row %d: sluice=%v postgres=%v(null=%v)\n",
					e, i, goSaid, pgSaid, pgNull)
			}
		}
	}

	check(mismatches == 0,
		fmt.Sprintf("compiled evaluator agrees with PostgreSQL on %d evaluations", compared),
		fmt.Sprintf("%d mismatches", mismatches))
	fmt.Printf("        (%d expression/row pairs compared, %d skipped as non-compilable)\n", compared, skipped)
}

// jsonRow adapts a JSON object to the expression evaluator.
type jsonRow struct {
	raw    []byte
	parsed map[string]any
}

func (j jsonRow) Column(name string) (expr.Value, bool) {
	if j.parsed == nil {
		_ = json.Unmarshal(j.raw, &j.parsed)
	}
	v, ok := j.parsed[name]
	if !ok {
		return expr.Null, false
	}
	switch x := v.(type) {
	case nil:
		return expr.Null, true
	case bool:
		return expr.Bool(x), true
	case float64:
		if x == float64(int64(x)) {
			return expr.Int(int64(x)), true
		}
		return expr.Float(x), true
	case string:
		return expr.Text(x), true
	}
	return expr.Null, true
}

// ===========================================================================
// Security probes
// ===========================================================================

func phaseSecurity(ctx context.Context, tok, otherTok, streamID, userID string) {
	fmt.Println("\n-- security probes --")

	post := func(path, body, bearer string, extraHeaders map[string]string) (int, string) {
		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+path, strings.NewReader(body))
		if bearer != "" {
			req.Header.Set("Authorization", "Bearer "+bearer)
		}
		req.Header.Set("Content-Type", "application/json")
		for k, v := range extraHeaders {
			req.Header.Set(k, v)
		}
		resp, err := (&http.Client{Timeout: 15 * time.Second}).Do(req)
		if err != nil {
			return 0, err.Error()
		}
		defer resp.Body.Close()
		b := make([]byte, 2048)
		n, _ := resp.Body.Read(b)
		return resp.StatusCode, string(b[:n])
	}

	// A stream belongs to the token that opened it. Another user's valid token
	// must not be able to drive it.
	code, body := post("/subscribe",
		fmt.Sprintf(`{"stream_id":%q,"subscriptions":[{"sub":"x","shape":{"table":"documents"}}]}`, streamID),
		otherTok, nil)
	check(code == http.StatusForbidden,
		"another user's token cannot drive someone else's stream",
		fmt.Sprintf("status %d: %s", code, truncate(body, 120)))

	// No token at all.
	code, _ = post("/subscribe", `{"stream_id":"x"}`, "", nil)
	check(code == http.StatusUnauthorized, "an unauthenticated control request is rejected",
		fmt.Sprintf("status %d", code))

	// A structurally valid but unsigned/garbage token.
	code, _ = post("/subscribe", `{"stream_id":"x"}`, "eyJhbGciOiJub25lIn0.eyJyb2xlIjoic2VydmljZV9yb2xlIn0.", nil)
	check(code == http.StatusUnauthorized, "an alg=none token is rejected",
		fmt.Sprintf("status %d", code))

	// Opaque publishable keys are not JWTs and must not be silently accepted.
	code, _ = post("/subscribe", `{"stream_id":"x"}`, "sb_publishable_abc123", nil)
	check(code == http.StatusUnauthorized, "an opaque sb_* key is rejected", fmt.Sprintf("status %d", code))

	// Header/body disagreement is a real vulnerability class: an intermediary
	// routes on one value while the server acts on another.
	code, body = post("/subscribe",
		fmt.Sprintf(`{"stream_id":%q,"subscriptions":[]}`, streamID),
		tok, map[string]string{"Sluice-Stream-Id": "someone-elses-stream"})
	check(code == http.StatusBadRequest && strings.Contains(body, "stream_id_mismatch"),
		"a routing header that disagrees with the body is rejected",
		fmt.Sprintf("status %d: %s", code, truncate(body, 120)))

	// Publishing to a channel the stream never joined would bypass the
	// subscribe-time authorization check entirely.
	code, body = post("/publish",
		fmt.Sprintf(`{"stream_id":%q,"channel":"room:never-joined","event":"x","payload":{}}`, streamID),
		tok, nil)
	check(code == http.StatusForbidden,
		"publishing to a channel that was never joined is refused",
		fmt.Sprintf("status %d: %s", code, truncate(body, 120)))

	// An unknown namespace must never be implicitly public.
	code, body = post("/subscribe",
		fmt.Sprintf(`{"stream_id":%q,"subscriptions":[{"sub":"u","channel":"unknown-namespace:1"}]}`, streamID),
		tok, nil)
	check(strings.Contains(body, "unknown_namespace"),
		"an unconfigured channel namespace is refused, not implicitly public",
		fmt.Sprintf("status %d: %s", code, truncate(body, 160)))

	// An owner-scoped channel belonging to somebody else.
	code, body = post("/subscribe",
		fmt.Sprintf(`{"stream_id":%q,"subscriptions":[{"sub":"n","channel":"notify:00000000-0000-4000-8000-0000000000ff"}]}`, streamID),
		tok, nil)
	check(strings.Contains(body, "channel_not_authorized"),
		"an owner-scoped channel belonging to another user is refused",
		fmt.Sprintf("%s", truncate(body, 160)))

	// A presence key must be the caller's own subject: a roster is an identity
	// claim, and letting a client pick any key would let it impersonate.
	post("/subscribe", fmt.Sprintf(`{"stream_id":%q,"subscriptions":[{"sub":"rm","channel":"room:sec"}]}`, streamID), tok, nil)
	code, body = post("/presence",
		fmt.Sprintf(`{"stream_id":%q,"channel":"room:sec","action":"track","key":"somebody-else"}`, streamID),
		tok, nil)
	check(code == http.StatusForbidden,
		"a presence key that is not the caller's subject is refused",
		fmt.Sprintf("status %d: %s", code, truncate(body, 120)))

	// A filter must not be able to reference a column that does not exist, which
	// is the shape a SQL injection attempt takes here.
	for _, bad := range []string{
		`owner_id=eq.x') OR 1=1--`,
		`1=1;DROP TABLE documents--=eq.x`,
		`owner_id);DELETE FROM documents;--=eq.1`,
	} {
		code, body = post("/subscribe",
			fmt.Sprintf(`{"stream_id":%q,"subscriptions":[{"sub":"inj","shape":{"table":"documents","filter":%q}}]}`,
				streamID, bad),
			tok, nil)
		if !strings.Contains(body, "invalid_filter") && !strings.Contains(body, "\"ok\":false") {
			check(false, "a malformed filter is rejected", fmt.Sprintf("filter %q accepted: %s", bad, truncate(body, 160)))
			return
		}
	}
	check(true, "filters containing SQL syntax are rejected as unknown columns", "")

	// Diagnostics exposes policy text and must be service_role only.
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, sluiceURL+"/diagnostics", nil)
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err == nil {
		defer resp.Body.Close()
		check(resp.StatusCode == http.StatusForbidden,
			"/diagnostics is refused to a non-service_role caller",
			fmt.Sprintf("status %d", resp.StatusCode))
	}

	// Oversized broadcast payloads must be rejected synchronously rather than
	// silently dropped -- upstream's `ack:false` silently swallowing failures is
	// a documented footgun.
	big := strings.Repeat("x", 300*1024)
	code, _ = post("/publish",
		fmt.Sprintf(`{"stream_id":%q,"channel":"room:sec","event":"e","payload":{"d":%q}}`, streamID, big),
		tok, nil)
	check(code == http.StatusRequestEntityTooLarge || code == http.StatusBadRequest,
		"an oversized payload is rejected synchronously, not silently dropped",
		fmt.Sprintf("status %d", code))
}

// ===========================================================================
// Load and concurrency
// ===========================================================================

func phaseLoad(ctx context.Context, tok, userID string, streams, changes int) {
	fmt.Printf("\n-- load: %d concurrent streams, %d changes --\n", streams, changes)

	var (
		wg       sync.WaitGroup
		received atomic.Int64
		opened   atomic.Int64
		failed   atomic.Int64
	)

	ready := make(chan struct{}, streams)
	stop := make(chan struct{})

	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Every stream watches the SAME shape, which is the worst case for a
			// per-subscriber authorization model and the best case for a constant
			// index: one change, N interested subscribers.
			body := fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{
				"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, userID)

			st, err := openStream(ctx, tok, body)
			if err != nil {
				failed.Add(1)
				ready <- struct{}{}
				return
			}
			defer st.Close()
			opened.Add(1)

			if _, err := st.next(20 * time.Second); err != nil {
				failed.Add(1)
			}
			ready <- struct{}{}

			for {
				select {
				case <-stop:
					return
				case ev, ok := <-st.events:
					if !ok {
						return
					}
					if ev.Name == "change" {
						received.Add(1)
					}
				case <-time.After(15 * time.Second):
					return
				}
			}
		}(i)
	}

	for i := 0; i < streams; i++ {
		select {
		case <-ready:
		case <-time.After(60 * time.Second):
			fmt.Println("  note  timed out waiting for all streams to open")
			i = streams
		}
	}
	fmt.Printf("        %d streams open, %d failed\n", opened.Load(), failed.Load())

	start := time.Now()
	for i := 0; i < changes; i++ {
		if err := execViaPgx(ctx, fmt.Sprintf(
			`insert into documents (owner_id, title, body) values ('%s','load-%d','x')`, userID, i)); err != nil {
			fmt.Printf("  note  insert failed: %v\n", err)
			break
		}
	}
	writeElapsed := time.Since(start)

	// Wait for delivery to settle.
	want := int64(opened.Load()) * int64(changes)
	deadline := time.After(30 * time.Second)
	var last int64
	stable := 0
	for {
		time.Sleep(300 * time.Millisecond)
		cur := received.Load()
		if cur == last {
			if stable++; stable >= 6 || cur >= want {
				break
			}
		} else {
			stable = 0
		}
		last = cur
		select {
		case <-deadline:
			stable = 99
		default:
		}
		if stable >= 6 || cur >= want {
			break
		}
	}
	total := time.Since(start)
	close(stop)
	wg.Wait()

	got := received.Load()
	fmt.Printf("        writes: %d in %s (%.0f/s)\n", changes, writeElapsed.Round(time.Millisecond),
		float64(changes)/writeElapsed.Seconds())
	fmt.Printf("        fan-out: %d/%d events in %s (%.0f events/s)\n",
		got, want, total.Round(time.Millisecond), float64(got)/total.Seconds())

	// The point of the design is that a change costs the same regardless of how
	// many subscribers want it. Losing events here would mean the queues are too
	// small or the reader is the bottleneck.
	check(got >= want*9/10,
		fmt.Sprintf("at least 90%% of %d fan-out events were delivered", want),
		fmt.Sprintf("delivered %d of %d", got, want))
}

// evalInPostgres evaluates an expression against a synthetic row, the same way
// the Tier C path and the Tier B cross-check do.
func evalInPostgres(ctx context.Context, expression, rowJSON string) (result bool, isNull bool, err error) {
	if err := execViaPgx(ctx, `create table if not exists public.diff (
		id bigint, owner_id uuid, title text, score bigint, archived boolean, tags text[])`); err != nil {
		return false, false, err
	}
	sql := fmt.Sprintf(
		`SELECT (%s) FROM jsonb_populate_record(NULL::public.diff, $1::jsonb) AS diff`, expression)
	var v *bool
	if err := queryOne(ctx, sql, rowJSON, &v); err != nil {
		return false, false, err
	}
	if v == nil {
		return false, true, nil
	}
	return *v, false, nil
}
