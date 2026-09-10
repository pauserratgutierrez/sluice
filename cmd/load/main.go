// Command load is a realtime stress probe against a running Sluice harness.
//
// Build and run it on the compose network the same way as cmd/smoke:
//
//	docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 \
//	  golang:1.26-alpine go build -o .bin/load ./cmd/load
//
//	docker run --rm --network deploy_private_net -v "$PWD/.bin:/b:ro" \
//	  -e POSTGRES_PASSWORD=… alpine:3.22 /b/load
//
// Env knobs (defaults aim at a heavy laptop run):
//
//	LOAD_SCENARIO   all | A | B | C | none | multi  (default all)
//	LOAD_STREAMS    concurrent SSE streams per scenario
//	LOAD_CHANGES    inserts after streams are ready
//	LOAD_USERS      distinct GoTrue users for "multi"
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	authURL   = envOr("SMOKE_AUTH_URL", "http://auth:9999")
	sluiceURL = envOr("SMOKE_SLUICE_URL", "http://sluice:4000/sluice/v1")
)

func main() {
	ctx := context.Background()
	scenario := envOr("LOAD_SCENARIO", "all")
	streams := envInt("LOAD_STREAMS", 1500)
	changes := envInt("LOAD_CHANGES", 80)
	users := envInt("LOAD_USERS", 40)

	fmt.Println("== Sluice realtime load probe ==")
	fmt.Printf("target  %s\n", sluiceURL)
	fmt.Printf("knobs   scenario=%s streams=%d changes=%d users=%d\n\n", scenario, streams, changes, users)

	tok, uid, err := signUp(ctx, fmt.Sprintf("load-%d@example.test", time.Now().UnixNano()), "load-pass-1")
	must(err, "signup")
	must(seedUser(ctx, uid, 1), "seed")

	type run struct {
		name string
		fn   func()
	}
	var runs []run
	add := func(name string, fn func()) { runs = append(runs, run{name, fn}) }

	switch scenario {
	case "A":
		add("tier-A fan-out (documents)", func() {
			runFanout(ctx, "A", tok, uid, streams, changes,
				fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into documents (owner_id, title, body) values ('%s','A-%d','x')`, uid, i)
				})
		})
	case "B":
		add("tier-B eval (posts)", func() {
			runFanout(ctx, "B", tok, uid, min(streams, 800), min(changes, 60),
				fmt.Sprintf(`{"subscriptions":[{"sub":"p","shape":{"schema":"public","table":"posts","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into posts (owner_id, visibility, title) values ('%s','private','B-%d')`, uid, i)
				})
		})
	case "C":
		add("tier-C probe (invoices)", func() {
			runFanout(ctx, "C", tok, uid, min(streams, 200), min(changes, 40),
				`{"subscriptions":[{"sub":"i","shape":{"schema":"public","table":"invoices","filter":"team_id=eq.1"}}]}`,
				func(i int) string {
					return fmt.Sprintf(`insert into invoices (team_id, amount, note) values (1, %.2f, 'C-%d')`, 10.0+float64(i), i)
				})
		})
	case "none":
		add("no-RLS (metrics)", func() {
			runFanout(ctx, "none", tok, uid, streams, changes,
				`{"subscriptions":[{"sub":"m","shape":{"schema":"public","table":"metrics"}}]}`,
				func(i int) string {
					return fmt.Sprintf(`insert into metrics (name, value) values ('m-%d', %d)`, i, i)
				})
		})
	case "multi":
		add("multi-user tier-A", func() { runMultiUser(ctx, users, min(streams/users, 30), min(changes, 40)) })
	default: // all — escalating ladder
		add("warm-up A 200×20", func() {
			runFanout(ctx, "A", tok, uid, 200, 20,
				fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into documents (owner_id, title, body) values ('%s','w-%d','x')`, uid, i)
				})
		})
		add("tier-A 1500×80", func() {
			runFanout(ctx, "A", tok, uid, streams, changes,
				fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into documents (owner_id, title, body) values ('%s','A-%d','x')`, uid, i)
				})
		})
		add("no-RLS 1500×80", func() {
			runFanout(ctx, "none", tok, uid, streams, changes,
				`{"subscriptions":[{"sub":"m","shape":{"schema":"public","table":"metrics"}}]}`,
				func(i int) string {
					return fmt.Sprintf(`insert into metrics (name, value) values ('n-%d', %d)`, i, i)
				})
		})
		add("tier-B 800×60", func() {
			runFanout(ctx, "B", tok, uid, min(streams, 800), min(changes, 60),
				fmt.Sprintf(`{"subscriptions":[{"sub":"p","shape":{"schema":"public","table":"posts","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into posts (owner_id, visibility, title) values ('%s','private','B-%d')`, uid, i)
				})
		})
		add("tier-C 150×40", func() {
			runFanout(ctx, "C", tok, uid, 150, 40,
				`{"subscriptions":[{"sub":"i","shape":{"schema":"public","table":"invoices","filter":"team_id=eq.1"}}]}`,
				func(i int) string {
					return fmt.Sprintf(`insert into invoices (team_id, amount, note) values (1, %.2f, 'C-%d')`, 10.0+float64(i), i)
				})
		})
		add("multi-user A 40×25×30", func() { runMultiUser(ctx, users, 25, 30) })
		add("extreme A 4000×100", func() {
			runFanout(ctx, "A", tok, uid, 4000, 100,
				fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, uid),
				func(i int) string {
					return fmt.Sprintf(`insert into documents (owner_id, title, body) values ('%s','X-%d','x')`, uid, i)
				})
		})
	}

	for _, r := range runs {
		fmt.Printf("\n######## %s ########\n", r.name)
		r.fn()
	}
	fmt.Println("\n== load probe finished ==")
}

func runFanout(ctx context.Context, tier, tok, _ string, streams, changes int, body string, writeSQL func(i int) string) {
	fmt.Printf("-- fan-out tier=%s streams=%d changes=%d (expect %d events) --\n",
		tier, streams, changes, streams*changes)

	var (
		wg       sync.WaitGroup
		received atomic.Int64
		opened   atomic.Int64
		failed   atomic.Int64
	)
	ready := make(chan struct{}, streams)
	stop := make(chan struct{})

	openStart := time.Now()
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			st, err := openStream(ctx, tok, body)
			if err != nil {
				failed.Add(1)
				ready <- struct{}{}
				return
			}
			defer st.Close()
			if _, err := st.next(30 * time.Second); err != nil {
				failed.Add(1)
				ready <- struct{}{}
				return
			}
			opened.Add(1)
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
				}
			}
		}()
	}
	for i := 0; i < streams; i++ {
		select {
		case <-ready:
		case <-time.After(3 * time.Minute):
			fmt.Println("  note  timed out waiting for stream open")
			i = streams
		}
	}
	openElapsed := time.Since(openStart)
	fmt.Printf("  open   %d ok / %d fail in %s (%.0f streams/s)\n",
		opened.Load(), failed.Load(), openElapsed.Round(time.Millisecond),
		float64(opened.Load())/openElapsed.Seconds())

	if opened.Load() == 0 {
		fmt.Println("  FAIL   no streams opened")
		close(stop)
		wg.Wait()
		return
	}

	writeStart := time.Now()
	var writeErrs int
	for i := 0; i < changes; i++ {
		if err := execSQL(ctx, writeSQL(i)); err != nil {
			writeErrs++
			if writeErrs <= 3 {
				fmt.Printf("  note  write: %v\n", err)
			}
		}
	}
	writeElapsed := time.Since(writeStart)

	want := opened.Load() * int64(changes-writeErrs)
	deadline := time.After(2 * time.Minute)
	var last int64
	stable := 0
	for {
		time.Sleep(200 * time.Millisecond)
		cur := received.Load()
		if cur >= want {
			break
		}
		if cur == last {
			stable++
			if stable >= 25 { // ~5s quiet
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
		if stable >= 25 {
			break
		}
	}
	total := time.Since(writeStart)
	close(stop)
	wg.Wait()

	got := received.Load()
	pct := float64(0)
	if want > 0 {
		pct = 100 * float64(got) / float64(want)
	}
	fmt.Printf("  writes %d in %s (%.0f/s) errs=%d\n", changes, writeElapsed.Round(time.Millisecond),
		float64(changes)/maxFloat(writeElapsed.Seconds(), 0.001), writeErrs)
	fmt.Printf("  fanout %d/%d (%.1f%%) in %s → %.0f events/s\n",
		got, want, pct, total.Round(time.Millisecond), float64(got)/maxFloat(total.Seconds(), 0.001))
	if pct >= 90 {
		fmt.Println("  RESULT PASS (≥90% delivered)")
	} else {
		fmt.Println("  RESULT LIMIT (delivery below 90%)")
	}
}

func runMultiUser(ctx context.Context, users, streamsPerUser, changes int) {
	fmt.Printf("-- multi-user tier-A users=%d streams/user=%d changes/user=%d --\n",
		users, streamsPerUser, changes)

	type user struct {
		tok, id string
	}
	us := make([]user, 0, users)
	for i := 0; i < users; i++ {
		tok, id, err := signUp(ctx, fmt.Sprintf("mu-%d-%d@example.test", time.Now().UnixNano(), i), "load-pass-1")
		if err != nil {
			fmt.Printf("  note  signup %d: %v\n", i, err)
			continue
		}
		if err := seedUser(ctx, id, 1); err != nil {
			fmt.Printf("  note  seed %d: %v\n", i, err)
			continue
		}
		us = append(us, user{tok, id})
	}
	fmt.Printf("  users  %d ready\n", len(us))
	if len(us) == 0 {
		return
	}

	var (
		wg       sync.WaitGroup
		received atomic.Int64
		opened   atomic.Int64
		failed   atomic.Int64
	)
	totalStreams := len(us) * streamsPerUser
	ready := make(chan struct{}, totalStreams)
	stop := make(chan struct{})

	openStart := time.Now()
	for _, u := range us {
		body := fmt.Sprintf(`{"subscriptions":[{"sub":"d","shape":{"schema":"public","table":"documents","filter":"owner_id=eq.%s"}}]}`, u.id)
		for s := 0; s < streamsPerUser; s++ {
			wg.Add(1)
			go func(tok, body string) {
				defer wg.Done()
				st, err := openStream(ctx, tok, body)
				if err != nil {
					failed.Add(1)
					ready <- struct{}{}
					return
				}
				defer st.Close()
				if _, err := st.next(30 * time.Second); err != nil {
					failed.Add(1)
					ready <- struct{}{}
					return
				}
				opened.Add(1)
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
					}
				}
			}(u.tok, body)
		}
	}
	for i := 0; i < totalStreams; i++ {
		select {
		case <-ready:
		case <-time.After(3 * time.Minute):
			i = totalStreams
		}
	}
	fmt.Printf("  open   %d ok / %d fail in %s\n", opened.Load(), failed.Load(), time.Since(openStart).Round(time.Millisecond))

	writeStart := time.Now()
	var writeWG sync.WaitGroup
	for _, u := range us {
		writeWG.Add(1)
		go func(id string) {
			defer writeWG.Done()
			for i := 0; i < changes; i++ {
				_ = execSQL(ctx, fmt.Sprintf(
					`insert into documents (owner_id, title, body) values ('%s','mu-%d','x')`, id, i))
			}
		}(u.id)
	}
	writeWG.Wait()
	writeElapsed := time.Since(writeStart)

	// Each open stream belongs to one user and should receive that user's writes only.
	want := opened.Load() * int64(changes)

	deadline := time.After(2 * time.Minute)
	var last int64
	stable := 0
	for {
		time.Sleep(200 * time.Millisecond)
		cur := received.Load()
		if cur >= want || stable >= 25 {
			break
		}
		if cur == last {
			stable++
		} else {
			stable = 0
		}
		last = cur
		select {
		case <-deadline:
			stable = 99
		default:
		}
	}
	total := time.Since(writeStart)
	close(stop)
	wg.Wait()

	got := received.Load()
	pct := 100 * float64(got) / float64(max64(want, 1))
	fmt.Printf("  writes %d users × %d in %s\n", len(us), changes, writeElapsed.Round(time.Millisecond))
	fmt.Printf("  fanout %d/%d (%.1f%%) in %s → %.0f events/s\n",
		got, want, pct, total.Round(time.Millisecond), float64(got)/maxFloat(total.Seconds(), 0.001))
	if pct >= 90 {
		fmt.Println("  RESULT PASS (≥90% delivered)")
	} else {
		fmt.Println("  RESULT LIMIT (delivery below 90%)")
	}
}

// ---- plumbing ------------------------------------------------------------

type sseEvent struct {
	Name string
	Data []byte
}

type stream struct {
	body   io.ReadCloser
	events chan sseEvent
}

func openStream(ctx context.Context, token, body string) (*stream, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/stream", strings.NewReader(body))
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
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		resp.Body.Close()
		return nil, fmt.Errorf("status %d: %s", resp.StatusCode, b)
	}
	s := &stream{body: resp.Body, events: make(chan sseEvent, 4096)}
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
		case strings.HasPrefix(line, "data:"):
			cur.Data = append(cur.Data, []byte(strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))...)
		}
	}
	close(s.events)
}

func (s *stream) next(d time.Duration) (sseEvent, error) {
	select {
	case ev, ok := <-s.events:
		if !ok {
			return sseEvent{}, fmt.Errorf("closed")
		}
		return ev, nil
	case <-time.After(d):
		return sseEvent{}, fmt.Errorf("timeout")
	}
}

func (s *stream) Close() { _ = s.body.Close() }

func signUp(ctx context.Context, email, password string) (token, userID string, err error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, authURL+"/signup",
		strings.NewReader(fmt.Sprintf(`{"email":%q,"password":%q}`, email, password)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	var body struct {
		AccessToken string `json:"access_token"`
		User        struct {
			ID string `json:"id"`
		} `json:"user"`
	}
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return "", "", fmt.Errorf("signup %d: %s", resp.StatusCode, b)
	}
	if err := json.Unmarshal(b, &body); err != nil {
		return "", "", err
	}
	return body.AccessToken, body.User.ID, nil
}

func seedUser(ctx context.Context, userID string, team int) error {
	return execSQL(ctx, fmt.Sprintf(`
		insert into memberships (user_id, team_id) values ('%s', %d)
		on conflict do nothing;
		select public.sluice_seed('%s'::uuid, %d);
	`, userID, team, userID, team))
}

var (
	poolOnce sync.Once
	pool     *pgxpool.Pool
	poolErr  error
)

func execSQL(ctx context.Context, sql string) error {
	poolOnce.Do(func() {
		url := os.Getenv("SMOKE_DB_URL")
		if url == "" {
			url = "postgres://postgres:" + os.Getenv("POSTGRES_PASSWORD") + "@db:5432/postgres"
		}
		pool, poolErr = pgxpool.New(ctx, url)
	})
	if poolErr != nil {
		return poolErr
	}
	_, err := pool.Exec(ctx, sql)
	return err
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envInt(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return d
}

func must(err error, what string) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %s: %v\n", what, err)
		os.Exit(1)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func maxFloat(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
