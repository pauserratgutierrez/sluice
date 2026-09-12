package oracle

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

func docsRel() *catalog.Relation {
	return &catalog.Relation{
		OID: 100, Schema: "public", Name: "documents",
		ReplicaIdentity:        'i',
		ReplicaIdentityColumns: []string{"id", "project_id"},
		Columns: []catalog.Column{
			{Name: "id", TypeName: "bigint"},
			{Name: "project_id", TypeName: "text"},
			{Name: "title", TypeName: "text"},
			{Name: "body", TypeName: "text"},
			{Name: "status", TypeName: "text"},
		},
		IndexedColumns: map[string]bool{"id": true, "project_id": true},
	}
}

func membersRel() *catalog.Relation {
	return &catalog.Relation{
		OID: 200, Schema: "public", Name: "project_members",
		ReplicaIdentity:        'i',
		ReplicaIdentityColumns: []string{"project_id", "user_id"},
		Columns: []catalog.Column{
			{Name: "project_id", TypeName: "text"},
			{Name: "user_id", TypeName: "text"},
		},
		IndexedColumns: map[string]bool{"project_id": true, "user_id": true},
	}
}

func lookup(rels ...*catalog.Relation) func(string, string) (*catalog.Relation, bool) {
	m := map[string]*catalog.Relation{}
	for _, r := range rels {
		m[r.FullName()] = r
	}
	return func(schema, name string) (*catalog.Relation, bool) {
		r, ok := m[schema+"."+name]
		return r, ok
	}
}

func grantAll(_ context.Context, _ *catalog.Relation, requested []string) ([]string, []string, error) {
	return requested, nil, nil
}

func identity() authz.Identity {
	return authz.Identity{
		Role: "authenticated", Sub: "user-1", SessionID: "sess-1",
		ClaimsRaw: `{"sub":"user-1","role":"authenticated"}`,
	}
}

func clientFilter(t *testing.T, rel *catalog.Relation, raw string) *shape.Filter {
	t.Helper()
	f, err := shape.Parse(raw, rel)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func issuerOK(w http.ResponseWriter, allowlist []string) {
	cols := any(nil)
	if allowlist != nil {
		cols = allowlist
	}
	_ = json.NewEncoder(w).Encode(map[string]any{
		"allow": true,
		"shape": map[string]any{
			"schema": "public", "table": "documents",
			"filter":  "project_id=eq.42",
			"columns": cols,
		},
		"holds": []map[string]any{{
			"schema": "public", "table": "project_members",
			"filter": "project_id=eq.42,user_id=eq.user-1",
		}},
	})
}

func newIssuer(t *testing.T, h http.HandlerFunc) *Issuer {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	iss, err := NewIssuerWith(IssuerOptions{
		URL:     srv.URL,
		Bearer:  "issuer-secret",
		Timeout: 2 * time.Second,
		Client:  srv.Client(),
		Lookup:  lookup(docsRel(), membersRel()),
		Columns: grantAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	return iss
}

func TestIssuerFailClosedOnTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		issuerOK(w, []string{"id", "title"})
	}))
	t.Cleanup(srv.Close)
	iss, err := NewIssuerWith(IssuerOptions{
		URL:     srv.URL,
		Bearer:  "issuer-secret",
		Timeout: 20 * time.Millisecond,
		Client:  &http.Client{Timeout: 20 * time.Millisecond},
		Lookup:  lookup(docsRel(), membersRel()),
		Columns: grantAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter: clientFilter(t, docsRel(), "project_id=eq.42"),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("timeout must deny, got %v", err)
	}
}

func TestIssuerFailClosedOnNon2xx(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = io.WriteString(w, "nope")
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("non-2xx must deny, got %v", err)
	}
}

func TestIssuerFailClosedOnRedirect(t *testing.T) {
	allowHits := 0
	mux := http.NewServeMux()
	mux.HandleFunc("/allow", func(w http.ResponseWriter, r *http.Request) {
		allowHits++
		issuerOK(w, []string{"id", "title"})
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/allow", http.StatusFound)
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	iss, err := NewIssuerWith(IssuerOptions{
		URL:     srv.URL,
		Bearer:  "issuer-secret",
		Timeout: 2 * time.Second,
		Client:  srv.Client(),
		Lookup:  lookup(docsRel(), membersRel()),
		Columns: grantAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter: clientFilter(t, docsRel(), "project_id=eq.42"),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("302 must deny, got %v", err)
	}
	if allowHits != 0 {
		t.Fatalf("redirect target was hit %d times; Authorization must not follow Location", allowHits)
	}
}

func TestIssuerFailClosedOnUnreadableBody(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, "not-json")
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("bad body must deny, got %v", err)
	}
}

func TestIssuerDenyAllowFalse(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"allow": false})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("allow=false must deny, got %v", err)
	}
}

func TestIssuerRejectsDifferentRelation(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "other", "filter": "id=eq.1"},
			"holds": []map[string]any{{"schema": "public", "table": "project_members", "filter": "user_id=eq.user-1"}},
		})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if err == nil || !strings.Contains(err.Error(), "not the requested") {
		t.Fatalf("wrong table must deny, got %v", err)
	}
}

func TestIssuerRejectsWholeTableFilter(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "documents", "filter": ""},
			"holds": []map[string]any{{"schema": "public", "table": "project_members", "filter": "user_id=eq.user-1"}},
		})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if err == nil || !strings.Contains(err.Error(), "whole table") {
		t.Fatalf("empty authorized filter must deny, got %v", err)
	}
}

func TestIssuerRejectsWiden(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		issuerOK(w, []string{"id", "title"})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter: clientFilter(t, docsRel(), "project_id=eq.99"),
	})
	if err == nil || !strings.Contains(err.Error(), "contradict") {
		t.Fatalf("client widening an authorized equality must deny, got %v", err)
	}
}

func TestIssuerNarrowsClientTerms(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		issuerOK(w, []string{"id", "title", "body"})
	})
	g, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter:  clientFilter(t, docsRel(), "project_id=eq.42,status=eq.open"),
		Columns: []string{"id", "title"},
		Ops:     []string{"INSERT", "UPDATE"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if g.Decision.Tier != authz.TierA || !g.Decision.Granted {
		t.Fatal("issuer grant must be an internal no-op decision, not a product tier")
	}
	if _, ok := g.Filter.Equalities["project_id"]; !ok {
		t.Fatal("authorized equality must survive")
	}
	foundStatus := false
	for _, term := range g.Filter.Terms {
		if term.Column == "status" {
			foundStatus = true
		}
	}
	if !foundStatus {
		t.Fatal("client narrowing term must be kept")
	}
	if len(g.Holds) != 1 {
		t.Fatalf("holds = %d", len(g.Holds))
	}
}

func TestIssuerEmptyShapeIsNotAnError(t *testing.T) {
	// Zero rows on the subscribed table are not checked. A grant with a live
	// hold is valid even if documents currently has no matching rows.
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		issuerOK(w, []string{"id", "title"})
	})
	g, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter: clientFilter(t, docsRel(), "project_id=eq.42"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if g.Filter.Describe() != "project_id=eq.42" && !strings.Contains(g.Filter.Describe(), "project_id=eq.42") {
		t.Fatalf("filter = %s", g.Filter.Describe())
	}
}

func TestIssuerRequiresHoldAndPublishedTable(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "documents", "filter": "project_id=eq.42"},
		})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if err == nil || !strings.Contains(err.Error(), "hold") {
		t.Fatalf("missing holds must deny, got %v", err)
	}

	iss = newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"allow": true,
			"shape": map[string]any{"schema": "public", "table": "documents", "filter": "project_id=eq.42"},
			"holds": []map[string]any{{"schema": "public", "table": "not_published", "filter": "id=eq.1"}},
		})
	})
	_, err = iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if err == nil || !strings.Contains(err.Error(), "publication") {
		t.Fatalf("unpublished hold must deny, got %v", err)
	}
}

func TestIssuerDeniesHoldWithoutReplicaIdentity(t *testing.T) {
	members := membersRel()
	members.ReplicaIdentity = 'd'
	members.ReplicaIdentityColumns = []string{"id"}
	members.Columns = append(members.Columns, catalog.Column{Name: "id", TypeName: "bigint"})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		issuerOK(w, []string{"id"})
	}))
	t.Cleanup(srv.Close)
	iss, err := NewIssuerWith(IssuerOptions{
		URL: srv.URL, Bearer: "issuer-secret", Timeout: time.Second, Client: srv.Client(),
		Lookup:  lookup(docsRel(), members),
		Columns: grantAll,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
	})
	if err == nil || !strings.Contains(err.Error(), "REPLICA IDENTITY") {
		t.Fatalf("hold columns outside replica identity must deny with an actionable reason, got %v", err)
	}
}

func TestIssuerSendsBearerNotUserToken(t *testing.T) {
	var gotAuth, gotBody string
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		issuerOK(w, []string{"id", "title"})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Action:   ActionSubscribe,
		Identity: identity(),
		Relation: docsRel(),
		Filter:   clientFilter(t, docsRel(), "project_id=eq.42"),
		Columns:  []string{"id", "title"},
		Ops:      []string{"INSERT"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth != "Bearer issuer-secret" {
		t.Fatalf("Authorization = %q, want the issuer bearer", gotAuth)
	}
	if strings.Contains(gotBody, "user-access-token") || strings.Contains(strings.ToLower(gotBody), "access_token") {
		t.Fatalf("must not forward the user access token: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"action":"subscribe"`) {
		t.Fatalf("action missing: %s", gotBody)
	}
	if !strings.Contains(gotBody, `"sub":"user-1"`) {
		t.Fatalf("identity missing: %s", gotBody)
	}
}

func TestIssuerRefreshDeny(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req["action"] != "refresh" {
			t.Errorf("action = %v, want refresh", req["action"])
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"allow": false})
	})
	_, err := iss.Refresh(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Filter: clientFilter(t, docsRel(), "project_id=eq.42"),
	})
	if _, ok := err.(*ErrDenied); !ok {
		t.Fatalf("refresh deny must drop the shape, got %v", err)
	}
}

func TestIssuerLeaseTickIsNoop(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Fatal("LeaseTick must not call the issuer")
	})
	held, err := iss.LeaseTick(context.Background(), nil, docsRel(), identity(), nil)
	if err != nil || !held {
		t.Fatalf("held=%v err=%v", held, err)
	}
}

func TestIssuerEmptyColumnIntersectionDenies(t *testing.T) {
	iss := newIssuer(t, func(w http.ResponseWriter, r *http.Request) {
		issuerOK(w, []string{"secret"})
	})
	_, err := iss.Resolve(context.Background(), Request{
		Identity: identity(), Relation: docsRel(),
		Columns: []string{"id", "title"},
	})
	if err == nil || !strings.Contains(err.Error(), "empty") {
		t.Fatalf("empty column intersection must deny, got %v", err)
	}
}

func TestIssuerDoesNotImpersonate(t *testing.T) {
	iss := newIssuer(t, issuerOKHandler)
	if iss.SnapshotImpersonate() {
		t.Fatal("issuer snapshots are privileged; they must not SET ROLE")
	}
	if iss.Name() != NameIssuer {
		t.Fatal(iss.Name())
	}
}

func issuerOKHandler(w http.ResponseWriter, r *http.Request) {
	issuerOK(w, []string{"id", "title"})
}
