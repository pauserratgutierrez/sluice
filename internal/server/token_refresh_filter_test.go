package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
)

// Every POST /token re-grants each shape. The issuer must be asked about what
// the client requested, as at subscribe, and the effective filter must come
// out the same each time. Asking about the effective filter instead added the
// grant's terms to it once more on every renewal: memory per shape grew with
// the number of renewals, and so did the cost of matching every change.
func TestTokenRefreshAsksAboutTheRequestedShape(t *testing.T) {
	var mu sync.Mutex
	var asked []string // the requested filter of each issuer call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Requested []struct {
				Filter string `json:"filter"`
			} `json:"requested"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		for _, req := range body.Requested {
			asked = append(asked, req.Filter)
		}
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "documents", "filter": "project_id=eq.42"},
			"holds": []map[string]any{{"schema": "public", "table": "project_members",
				"filter": "user_id=eq.u1,project_id=eq.42"}},
		}}})
	}))
	t.Cleanup(srv.Close)
	cat := catalog.New(nil)
	cat.PutForTest(docsRel(), membersRel())
	iss, err := oracle.NewIssuerWith(oracle.IssuerOptions{
		URL: srv.URL, Bearer: "issuer-secret", Timeout: 2 * time.Second, Client: srv.Client(),
		Lookup: cat.Lookup,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := testServer(t, iss)
	s.cat = cat
	s.holdCheck = holdsAnswer(nil)
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10

	ctx := context.Background()
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.1", "", id)
	spec := subSpec{Sub: "docs", Shape: &shapeSpec{Table: "documents", Filter: "title=eq.public", Columns: []string{"id", "title"}}}
	if res := s.applySubscriptions(ctx, st, id, []subSpec{spec}, nil)[0]; !res.OK {
		t.Fatalf("subscribe: %+v", res.Error)
	}
	first := s.reg.Get(st.StreamID(), "docs").Filter.Describe()

	for range 50 {
		sub := s.reg.Get(st.StreamID(), "docs")
		if got := refreshOne(s, ctx, st, sub, id); got != "held" {
			t.Fatalf("refresh: %s", got)
		}
	}
	sub := s.reg.Get(st.StreamID(), "docs")
	if got := sub.Filter.Describe(); got != first {
		t.Fatalf("the effective filter changed over 50 renewals:\n  first %s\n  now   %s", first, got)
	}
	mu.Lock()
	defer mu.Unlock()
	for i, f := range asked {
		if f != "title=eq.public" {
			t.Fatalf("issuer call %d was asked about %q, want the requested %q", i, f, "title=eq.public")
		}
	}
}

// A grant that narrows on one renewal and widens back on the next leaves the
// shape as wide as the grant: the previous grant's terms are not carried over
// as if the client had asked for them.
func TestTokenRefreshFollowsAGrantThatWidens(t *testing.T) {
	var mu sync.Mutex
	grant := "project_id=eq.42"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		f := grant
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"results": []any{map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "documents", "filter": f},
			"holds": []map[string]any{{"schema": "public", "table": "project_members",
				"filter": "user_id=eq.u1,project_id=eq.42"}},
		}}})
	}))
	t.Cleanup(srv.Close)
	cat := catalog.New(nil)
	cat.PutForTest(docsRel(), membersRel())
	iss, err := oracle.NewIssuerWith(oracle.IssuerOptions{
		URL: srv.URL, Bearer: "issuer-secret", Timeout: 2 * time.Second, Client: srv.Client(),
		Lookup: cat.Lookup,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := testServer(t, iss)
	s.cat = cat
	s.holdCheck = holdsAnswer(nil)
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10
	ctx := context.Background()
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.1", "", id)
	spec := subSpec{Sub: "docs", Shape: &shapeSpec{Table: "documents", Columns: []string{"id", "title"}}}
	if res := s.applySubscriptions(ctx, st, id, []subSpec{spec}, nil)[0]; !res.OK {
		t.Fatalf("subscribe: %+v", res.Error)
	}
	renew := func(g string) string {
		mu.Lock()
		grant = g
		mu.Unlock()
		if got := refreshOne(s, ctx, st, s.reg.Get(st.StreamID(), "docs"), id); got != "held" {
			t.Fatalf("refresh: %s", got)
		}
		return s.reg.Get(st.StreamID(), "docs").Filter.Describe()
	}
	if got := renew("project_id=eq.42,title=eq.public"); got != "project_id=eq.42,title=eq.public" {
		t.Fatalf("narrowed: %s", got)
	}
	if got := renew("project_id=eq.42"); got != "project_id=eq.42" {
		t.Fatalf("a grant that widens back must widen the shape back, got %s", got)
	}
}

// A stream that opens with several shapes asks the issuer about all of them
// in one call, and POST /token renews all of them in one more. Each time, the
// holds of every shape are checked together, in one round trip.
func TestIssuerShapesOfARequestShareOneCall(t *testing.T) {
	var mu sync.Mutex
	var calls []int // the shapes asked about in each issuer call
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Requested []json.RawMessage `json:"requested"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		calls = append(calls, len(body.Requested))
		mu.Unlock()
		results := make([]any, len(body.Requested))
		for i := range results {
			results[i] = map[string]any{
				"allow": true,
				"shape": map[string]any{"schema": "public", "table": "documents", "filter": "project_id=eq.42"},
				"holds": []map[string]any{{"schema": "public", "table": "project_members",
					"filter": "user_id=eq.u1,project_id=eq.42"}},
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"results": results})
	}))
	t.Cleanup(srv.Close)
	cat := catalog.New(nil)
	cat.PutForTest(docsRel(), membersRel())
	iss, err := oracle.NewIssuerWith(oracle.IssuerOptions{
		URL: srv.URL, Bearer: "issuer-secret", Timeout: 2 * time.Second, Client: srv.Client(),
		Lookup: cat.Lookup,
	})
	if err != nil {
		t.Fatal(err)
	}
	s := testServer(t, iss)
	s.cat = cat
	var checks []int // the holds asked about in each check
	s.holdCheck = func(_ context.Context, specs []hold.Spec) []error {
		checks = append(checks, len(specs))
		return make([]error, len(specs))
	}
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10

	ctx := context.Background()
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.1", "", id)
	specs := []subSpec{
		{Sub: "a", Shape: &shapeSpec{Table: "documents"}},
		{Sub: "b", Shape: &shapeSpec{Table: "documents", Columns: []string{"id", "title"}}},
		{Sub: "c", Shape: &shapeSpec{Table: "documents", Filter: "title=eq.x"}},
	}
	for _, r := range s.applySubscriptions(ctx, st, id, specs, nil) {
		if !r.OK {
			t.Fatalf("subscribe %s: %+v", r.Sub, r.Error)
		}
	}
	subs := s.reg.StreamSubscriptions(st.StreamID())
	for k, got := range s.refreshIssuerShapes(ctx, st, subs, id) {
		if got != "held" {
			t.Fatalf("refresh of %s: %s", subs[k].Label, got)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 || calls[0] != 3 || calls[1] != 3 {
		t.Fatalf("issuer calls asked about %v shapes, want [3 3]", calls)
	}
	if !slices.Equal(checks, []int{3, 3}) {
		t.Fatalf("hold checks asked about %v holds, want [3 3]", checks)
	}
}
