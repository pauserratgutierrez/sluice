package requestid

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

var uuidV4 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestReadKeepsAnIDFitForALogLine(t *testing.T) {
	for _, v := range []string{"9c2f0f9e-3d4b-4a8e-9f1e-0d6c5b4a3f21", "Root=1-67891233-abcdef012345678912345678", "a"} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.Header.Set("X-Request-ID", v)
		if got := Read(r, "X-Request-ID"); got.Value != v || got.Header != "X-Request-ID" {
			t.Errorf("Read(%q) = %+v, want it kept", v, got)
		}
	}
}

// An ID that is missing, or could not go into a log line as is, is replaced.
func TestReadReplacesAMissingOrUnfitID(t *testing.T) {
	for _, v := range []string{"", "has space", "tab\there", "naïve", strings.Repeat("a", MaxLen+1)} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		if v != "" {
			r.Header.Set("X-Request-ID", v)
		}
		if got := Read(r, "X-Request-ID").Value; !uuidV4.MatchString(got) {
			t.Errorf("Read(%q) = %q, want a new UUID", v, got)
		}
	}
}

func TestReadUsesTheConfiguredHeader(t *testing.T) {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("X-Request-ID", "ignored")
	r.Header.Set("X-Correlation-ID", "corr-1")
	if got := Read(r, "X-Correlation-ID"); got.Value != "corr-1" {
		t.Fatalf("Read = %+v, want corr-1", got)
	}
}

func TestNewIsARandomUUIDv4(t *testing.T) {
	a, b := New(), New()
	if !uuidV4.MatchString(a) || !uuidV4.MatchString(b) {
		t.Fatalf("New() = %q, %q; want UUIDv4", a, b)
	}
	if a == b {
		t.Fatal("two IDs are equal")
	}
}

func TestSetSendsTheIDTheContextCarries(t *testing.T) {
	ctx := With(context.Background(), ID{Header: "X-Correlation-ID", Value: "corr-1"})
	h := http.Header{}
	From(ctx).Set(h)
	if got := h.Get("X-Correlation-ID"); got != "corr-1" {
		t.Fatalf("header = %q, want corr-1", got)
	}

	h = http.Header{}
	From(context.Background()).Set(h)
	if len(h) != 0 {
		t.Fatalf("work no request started sent %v", h)
	}
}
