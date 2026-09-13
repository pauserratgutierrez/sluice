package issuer

import (
	"crypto/hmac"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

func Main() error {
	bearer := os.Getenv("SLUICE_ISSUER_BEARER")
	if bearer == "" {
		bearer = os.Getenv("ISSUER_BEARER")
	}
	if bearer == "" {
		return fmt.Errorf("issuer: SLUICE_ISSUER_BEARER is required")
	}
	s := &server{bearer: bearer, members: map[string]struct{}{}}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", s.health)
	mux.HandleFunc("POST /shapes", s.shapes)
	mux.HandleFunc("POST /seed", s.seed)
	mux.HandleFunc("POST /reset", s.reset)

	addr := envOr("ISSUER_LISTEN", "0.0.0.0:8080")
	fmt.Println("issuer: listening on", addr)
	hs := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	return hs.ListenAndServe()
}

type server struct {
	bearer  string
	mu      sync.RWMutex
	members map[string]struct{}
}

type issuerRequest struct {
	Identity struct {
		Sub string `json:"sub"`
	} `json:"identity"`
	Requested struct {
		Schema string `json:"schema"`
		Table  string `json:"table"`
		Filter string `json:"filter"`
	} `json:"requested"`
}

type member struct {
	ProjectID string `json:"project_id"`
	UserID    string `json:"user_id"`
}

func (s *server) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) reset(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	s.mu.Lock()
	s.members = map[string]struct{}{}
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *server) seed(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var body struct {
		Members []member `json:"members"`
	}
	if err := readJSON(r, &body); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	s.mu.Lock()
	for _, m := range body.Members {
		s.members[key(m.ProjectID, m.UserID)] = struct{}{}
	}
	n := len(s.members)
	s.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "members": n})
}

func (s *server) shapes(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	var req issuerRequest
	if err := readJSON(r, &req); err != nil {
		http.Error(w, "unreadable body", http.StatusBadRequest)
		return
	}
	projectID := parseEq(req.Requested.Filter, "project_id")
	sub := strings.ToLower(strings.TrimSpace(req.Identity.Sub))
	if projectID == "" || sub == "" || req.Requested.Table != "project_docs" {
		writeJSON(w, http.StatusOK, map[string]any{"allow": false})
		return
	}
	s.mu.RLock()
	_, ok := s.members[key(projectID, sub)]
	s.mu.RUnlock()
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"allow": false})
		return
	}
	cols := []string{"id", "project_id", "title", "body"}
	writeJSON(w, http.StatusOK, map[string]any{
		"allow": true,
		"shape": map[string]any{
			"schema":  "public",
			"table":   "project_docs",
			"filter":  "project_id=eq." + projectID,
			"columns": cols,
		},
		"holds": []map[string]any{{
			"schema": "public",
			"table":  "project_members",
			"filter": "project_id=eq." + projectID + ",user_id=eq." + sub,
		}},
	})
}

func (s *server) auth(w http.ResponseWriter, r *http.Request) bool {
	got := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
	if !hmac.Equal([]byte(got), []byte(s.bearer)) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}

func parseEq(filter, col string) string {
	prefix := col + "=eq."
	for _, part := range strings.Split(filter, ",") {
		part = strings.TrimSpace(part)
		if strings.HasPrefix(part, prefix) {
			return strings.TrimSpace(part[len(prefix):])
		}
	}
	return ""
}

func key(projectID, userID string) string {
	return projectID + "\x00" + strings.ToLower(userID)
}

func readJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	return json.NewDecoder(io.LimitReader(r.Body, 8<<20)).Decode(v)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
