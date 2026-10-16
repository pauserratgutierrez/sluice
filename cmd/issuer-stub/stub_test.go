package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestParseProjectID(t *testing.T) {
	if got := parseProjectID("project_id=eq.alpha"); got != "alpha" {
		t.Fatalf("got %q", got)
	}
	if got := parseProjectID("project_id=eq.alpha,status=eq.open"); got != "alpha" {
		t.Fatalf("got %q", got)
	}
	if got := parseProjectID(""); got != "" {
		t.Fatalf("empty filter must yield no project, got %q", got)
	}
}

func TestJSONHasAccessToken(t *testing.T) {
	if jsonHasAccessToken([]byte(`{"action":"subscribe","identity":{"sub":"u1"}}`)) {
		t.Fatal("must not treat a normal issuer request as carrying access_token")
	}
	if !jsonHasAccessToken([]byte(`{"identity":{"access_token":"eyJ"}}`)) {
		t.Fatal("nested access_token must be detected")
	}
}

func TestOutageProjectAnswers503WithRetryAfter(t *testing.T) {
	s := &stub{bearer: "b"}
	req := httptest.NewRequest(http.MethodPost, "/shapes", strings.NewReader(
		`{"identity":{"sub":"u1"},"requested":[{"table":"iss_documents","filter":"project_id=eq.alpha"},{"table":"iss_documents","filter":"project_id=eq.outage"}]}`))
	req.Header.Set("Authorization", "Bearer b")
	w := httptest.NewRecorder()
	s.handleShapes(w, req)
	if w.Code != http.StatusServiceUnavailable || w.Header().Get("Retry-After") != "2" {
		t.Fatalf("status %d, Retry-After %q; want 503 with Retry-After: 2", w.Code, w.Header().Get("Retry-After"))
	}
}

// Each requested shape gets a result, in order; one the stub cannot grant is
// denied without failing the others.
func TestShapesAnswersEachRequestedShape(t *testing.T) {
	s := &stub{bearer: "b"}
	req := httptest.NewRequest(http.MethodPost, "/shapes", strings.NewReader(
		`{"identity":{"sub":"u1"},"requested":[{"sub":"x","table":"other"},{"sub":"y","table":"iss_documents","filter":"status=eq.open"}]}`))
	req.Header.Set("Authorization", "Bearer b")
	w := httptest.NewRecorder()
	s.handleShapes(w, req)
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != `{"results":[{"allow":false},{"allow":false}]}` {
		t.Fatalf("status %d, body %s; want two denials", w.Code, w.Body.String())
	}
}
