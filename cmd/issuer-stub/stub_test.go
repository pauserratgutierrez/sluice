package main

import "testing"

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
