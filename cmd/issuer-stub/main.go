// Command issuer-stub is the harness shape-issuer for cmd/smoke-issuer.
//
// Sluice POSTs here; the smoke process is not the issuer. Membership is a live
// SELECT on iss_project_members (superuser, no cache). GET /healthz does not
// touch that table.
package main

import (
	"context"
	"crypto/hmac"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "probe the local server and exit")
	flag.Parse()
	if *healthcheck {
		os.Exit(runHealthcheck())
	}
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "issuer-stub:", err)
		os.Exit(1)
	}
}

func runHealthcheck() int {
	addr := os.Getenv("ISSUER_STUB_LISTEN")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	if h, p, err := net.SplitHostPort(addr); err == nil {
		if h == "" || h == "0.0.0.0" || h == "::" {
			addr = net.JoinHostPort("127.0.0.1", p)
		}
	}
	c := &http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://" + addr + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, "healthcheck:", err)
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func run() error {
	listen := envOr("ISSUER_STUB_LISTEN", "0.0.0.0:8080")
	bearer := os.Getenv("ISSUER_STUB_BEARER")
	if bearer == "" {
		bearer = os.Getenv("SLUICE_ISSUER_BEARER")
	}
	if bearer == "" {
		return fmt.Errorf("ISSUER_STUB_BEARER (or SLUICE_ISSUER_BEARER) is required")
	}
	dbURL := os.Getenv("ISSUER_STUB_DB_URL")
	if dbURL == "" {
		pass := os.Getenv("POSTGRES_PASSWORD")
		db := envOr("POSTGRES_DB", "postgres")
		dbURL = "postgres://postgres:" + pass + "@db:5432/" + db
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		return fmt.Errorf("db pool: %w", err)
	}
	defer pool.Close()

	s := &stub{bearer: bearer, pool: pool}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /debug", s.handleDebug)
	mux.HandleFunc("POST /shapes", s.handleShapes)

	fmt.Println("issuer-stub listening on", listen)
	return http.ListenAndServe(listen, mux)
}

type stub struct {
	bearer string
	pool   *pgxpool.Pool

	mu             sync.Mutex
	postCount      int
	sawAccessToken bool
	lastBody       json.RawMessage
}

type issuerRequest struct {
	Action   string `json:"action"`
	Identity struct {
		Role      string          `json:"role"`
		Sub       string          `json:"sub"`
		SessionID string          `json:"session_id"`
		Claims    json.RawMessage `json:"claims"`
	} `json:"identity"`
	Requested struct {
		Schema  string   `json:"schema"`
		Table   string   `json:"table"`
		Filter  string   `json:"filter"`
		Columns []string `json:"columns"`
		Ops     []string `json:"ops"`
	} `json:"requested"`
}

func (s *stub) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *stub) handleDebug(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"post_count":       s.postCount,
		"saw_access_token": s.sawAccessToken,
		"last_body":        s.lastBody,
	})
}

func (s *stub) handleShapes(w http.ResponseWriter, r *http.Request) {
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !hmac.Equal([]byte(got), []byte(s.bearer)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	s.postCount++
	s.lastBody = append(json.RawMessage(nil), raw...)
	if jsonHasAccessToken(raw) {
		s.sawAccessToken = true
	}
	s.mu.Unlock()

	var req issuerRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}

	projectID := parseProjectID(req.Requested.Filter)
	table := req.Requested.Table
	if projectID == "" || table != "iss_documents" || req.Identity.Sub == "" {
		writeJSON(w, http.StatusOK, map[string]any{"allow": false})
		return
	}

	ok, err := s.member(r.Context(), projectID, req.Identity.Sub)
	if err != nil {
		http.Error(w, "membership lookup failed", http.StatusInternalServerError)
		return
	}
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"allow": false})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"allow": true,
		"shape": map[string]any{
			"schema": "public",
			"table":  "iss_documents",
			"filter": "project_id=eq." + projectID,
		},
		"holds": []map[string]any{{
			"schema": "public",
			"table":  "iss_project_members",
			"filter": "project_id=eq." + projectID + ",user_id=eq." + req.Identity.Sub,
		}},
	})
}

func (s *stub) member(ctx context.Context, projectID, sub string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var ok bool
	err := s.pool.QueryRow(ctx, `
		SELECT EXISTS (
		  SELECT 1 FROM public.iss_project_members
		   WHERE project_id = $1 AND user_id = $2::uuid
		)`, projectID, sub).Scan(&ok)
	return ok, err
}

func parseProjectID(filter string) string {
	for _, part := range strings.Split(filter, ",") {
		part = strings.TrimSpace(part)
		const prefix = "project_id=eq."
		if strings.HasPrefix(part, prefix) {
			return strings.TrimSpace(part[len(prefix):])
		}
	}
	return ""
}

func jsonHasAccessToken(raw []byte) bool {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return strings.Contains(strings.ToLower(string(raw)), "access_token")
	}
	return walkAccessToken(v)
}

func walkAccessToken(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, val := range x {
			if strings.EqualFold(k, "access_token") {
				return true
			}
			if walkAccessToken(val) {
				return true
			}
		}
	case []any:
		for _, val := range x {
			if walkAccessToken(val) {
				return true
			}
		}
	}
	return false
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
