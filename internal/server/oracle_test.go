package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

type stubOracle struct{ name string }

func (s stubOracle) Name() string { return s.name }
func (s stubOracle) Resolve(context.Context, oracle.Request) (*oracle.Grant, error) {
	return nil, &oracle.ErrDenied{Reason: "unused"}
}
func (s stubOracle) Refresh(context.Context, oracle.Request) (*oracle.Grant, error) {
	return nil, &oracle.ErrDenied{Reason: "unused"}
}
func (s stubOracle) LeaseTick(context.Context, *authz.Handle, *catalog.Relation, authz.Identity, map[string]expr.Value) (bool, error) {
	return true, nil
}
func (s stubOracle) SnapshotImpersonate() bool { return s.name != oracle.NameIssuer }

func testServer(t *testing.T, orc oracle.Oracle) *Server {
	t.Helper()
	cfg := &config.Config{
		PathPrefix:         "/sluice/v1",
		ShapeOracle:        orc.Name(),
		IssuerURL:          "http://issuer.test/shapes",
		IssuerBearer:       "issuer-secret",
		IssuerTimeout:      2 * time.Second,
		Heartbeat:          20 * time.Second,
		DiagnosticsEnabled: true,
		SnapshotEnabled:    true,
		UnindexedMax:       200,
		ReplicaIdentity:    "warn",
	}
	h := hub.New(16, 8, time.Minute, time.Second)
	s := New(context.Background(), Options{
		Config:  cfg,
		Logger:  slog.New(slog.DiscardHandler),
		Catalog: catalog.New(nil),
		Oracle:  orc,
		Hub:     h,
	})
	return s
}

func docsRel() *catalog.Relation {
	return &catalog.Relation{
		OID: 100, Schema: "public", Name: "documents",
		ReplicaIdentityColumns: []string{"id", "project_id"},
		Columns: []catalog.Column{
			{Name: "id", TypeName: "bigint"},
			{Name: "project_id", TypeName: "text"},
			{Name: "title", TypeName: "text"},
		},
		IndexedColumns: map[string]bool{"project_id": true, "id": true},
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

func TestIssuerReadyOmitsTier(t *testing.T) {
	res := subResult{
		Sub: "docs", OK: true,
		Oracle: oracle.NameIssuer, Filter: "project_id=eq.42",
		Reason: "authorized by the shape issuer",
	}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"tier"`) {
		t.Fatalf("issuer ready must not send tier: %s", b)
	}
	if !strings.Contains(string(b), `"oracle":"issuer"`) {
		t.Fatalf("issuer ready must send oracle: %s", b)
	}
}

func TestRLSReadyKeepsTierOmitsOracle(t *testing.T) {
	res := subResult{Sub: "docs", OK: true, Tier: "A"}
	b, err := json.Marshal(res)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), `"oracle"`) {
		t.Fatalf("rls ready may omit oracle: %s", b)
	}
	if !strings.Contains(string(b), `"tier":"A"`) {
		t.Fatalf("rls ready must send tier: %s", b)
	}
}

func TestPublicationDropRemovesShapeAndHold(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.1", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	sub := &registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Filter: f,
		Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey: f.RoutingKey(rel),
	}
	if !s.reg.Add(sub) {
		t.Fatal("add shape")
	}
	mf, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	if !s.holds.Add(st.StreamID(), "docs", []hold.Spec{{Rel: membersRel(), Filter: mf}}) {
		t.Fatal("add hold")
	}

	s.RefreshLeases(context.Background())
	if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 0 {
		t.Fatalf("unpublished shape must drop, still have %d", len(got))
	}
	if s.holds.Count() != 0 {
		t.Fatal("hold watch on an unpublished table must drop")
	}
	select {
	case ev := <-st.Events():
		errp, ok := ev.Data.(event.Error)
		if !ok || errp.Code != "relation_unpublished" {
			t.Fatalf("event = %+v, want relation_unpublished", ev.Data)
		}
	default:
		t.Fatal("expected relation_unpublished on the still-open stream")
	}
	if _, ok := s.hub.Get(st.StreamID()); !ok {
		t.Fatal("publication drop must not close the stream")
	}
}

func membersRIDefault() *catalog.Relation {
	m := membersRel()
	m.ReplicaIdentity = 'd'
	m.ReplicaIdentityColumns = []string{"id"}
	m.Columns = append(m.Columns, catalog.Column{Name: "id", TypeName: "bigint"})
	return m
}

func liveDocsHold(t *testing.T, s *Server, st *hub.Stream, holdRel *catalog.Relation) {
	t.Helper()
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	if !s.reg.Add(&registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Filter: f,
		Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey: f.RoutingKey(rel),
	}) {
		t.Fatal("add shape")
	}
	mf, err := shape.Parse("project_id=eq.42,user_id=eq.u1", holdRel)
	if err != nil {
		t.Fatal(err)
	}
	if !s.holds.Add(st.StreamID(), "docs", []hold.Spec{{Rel: holdRel, Filter: mf}}) {
		t.Fatal("add hold")
	}
}

func TestHoldReplicaIdentityWeakenedCutsShape(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.ri-weak", authz.Identity{Sub: "u1", Role: "authenticated"})
	good := membersRel()
	s.cat.PutForTest(docsRel(), good)
	liveDocsHold(t, s, st, good)

	s.cat.PutForTest(docsRel(), membersRIDefault())
	s.RefreshLeases(context.Background())

	if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 0 {
		t.Fatalf("weakened hold RI must drop the shape, still have %d", len(got))
	}
	if s.holds.Count() != 0 {
		t.Fatal("hold watch must drop with the shape")
	}
	select {
	case ev := <-st.Events():
		errp, ok := ev.Data.(event.Error)
		if !ok || errp.Code != "shape_not_authorized" {
			t.Fatalf("event = %+v, want shape_not_authorized", ev.Data)
		}
		if !strings.Contains(errp.Message, "REPLICA IDENTITY") {
			t.Fatalf("message must be the join RI remedy, got %q", errp.Message)
		}
	default:
		t.Fatal("expected shape_not_authorized on the still-open stream")
	}
	if _, ok := s.hub.Get(st.StreamID()); !ok {
		t.Fatal("RI cut must not close the stream")
	}
}

func TestHoldReplicaIdentityStillCoveringKeepsShape(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.ri-ok", authz.Identity{Sub: "u1", Role: "authenticated"})
	joined := membersRel()
	s.cat.PutForTest(docsRel(), joined)
	liveDocsHold(t, s, st, joined)

	fresh := membersRel()
	s.cat.PutForTest(docsRel(), fresh)
	s.RefreshLeases(context.Background())

	if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 1 {
		t.Fatalf("covering RI must keep the shape, have %d", len(got))
	}
	if s.holds.Count() != 1 {
		t.Fatal("hold watch must stay")
	}
	watches := s.holds.All()
	if len(watches) != 1 || len(watches[0].Holds) != 1 || watches[0].Holds[0].Rel != fresh {
		t.Fatal("hold Rel must swap to the catalog pointer so diagnostics is not stale")
	}
	select {
	case ev := <-st.Events():
		t.Fatalf("covering RI must not cut, got %+v", ev.Data)
	default:
	}
	if _, ok := s.hub.Get(st.StreamID()); !ok {
		t.Fatal("stream must stay open")
	}
}

func TestHoldCutDoesNotCloseStream(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.2", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	f, _ := shape.Parse("project_id=eq.42", rel)
	sub := &registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Filter: f,
		Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey: f.RoutingKey(rel),
	}
	s.reg.Add(sub)
	s.dropShape(st, "docs", event.Error{Code: "shape_not_authorized", Message: "hold gone"})
	if _, ok := s.hub.Get(st.StreamID()); !ok {
		t.Fatal("hold cut must leave the stream open (token_expired is the other axis)")
	}
	if len(s.reg.StreamSubscriptions(st.StreamID())) != 0 {
		t.Fatal("the cut shape must be gone")
	}
}

func TestAdminShapesDropUsesIssuerBearer(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.3", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	f, _ := shape.Parse("project_id=eq.42", rel)
	s.reg.Add(&registry.Subscription{
		Label: "docs", Sink: st, Relation: rel, Filter: f,
		Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey: f.RoutingKey(rel),
	})

	body := `{"identity":{"sub":"u1"},"schema":"public","table":"documents","equalities":{"project_id":"42"}}`
	req := httptest.NewRequest(http.MethodPost, "/sluice/v1/admin/shapes/drop", bytes.NewReader([]byte(body)))
	req.Header.Set("Authorization", "Bearer issuer-secret")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	var out struct {
		Dropped int `json:"dropped"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Dropped != 1 {
		t.Fatalf("dropped = %d", out.Dropped)
	}
}

func TestIssuerSnapshotDoesNotImpersonate(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	if s.impersonateSnapshots() {
		t.Fatal("issuer snapshots must not SET ROLE")
	}
	s = testServer(t, stubOracle{name: oracle.NameRLS})
	if !s.impersonateSnapshots() {
		t.Fatal("rls snapshots still impersonate")
	}
}

func TestDiagnosticsIssuerDoesNotShoutPolicies(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	for _, d := range s.diagnostics() {
		switch d.Code {
		case "tier_c_policy", "tier_c_subscriptions", "policy_unparseable",
			"policy_applies_to_public", "policy_function_not_wrapped", "unindexed_policy_column":
			t.Fatalf("issuer diagnostics must not treat RLS policies as the judge: %+v", d)
		}
	}
}

type tokenRow map[string]expr.Value

func (r tokenRow) Column(name string) (expr.Value, bool) {
	v, ok := r[name]
	return v, ok
}

type refreshOracle struct {
	stubOracle
	mu    sync.Mutex
	grant *oracle.Grant
	err   error
	last  oracle.Request
}

func (o *refreshOracle) Refresh(_ context.Context, req oracle.Request) (*oracle.Grant, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.last = req
	if o.err != nil {
		return nil, o.err
	}
	return o.grant, nil
}

func issuerLiveSub(t *testing.T, s *Server, st *hub.Stream, filter *shape.Filter, columns []string, holdFilter *shape.Filter) *registry.Subscription {
	t.Helper()
	rel := docsRel()
	sub := &registry.Subscription{
		Label:      "docs",
		Sink:       st,
		Relation:   rel,
		Filter:     filter,
		Columns:    append([]string(nil), columns...),
		Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
		RoutingKey: filter.RoutingKey(rel),
	}
	if !s.reg.Add(sub) {
		t.Fatal("add shape")
	}
	if !s.holds.Add(st.StreamID(), "docs", []hold.Spec{{Rel: membersRel(), Filter: holdFilter}}) {
		t.Fatal("add hold")
	}
	return sub
}

func kickHold(s *Server, st *hub.Stream) {
	cuts := s.holds.OnChange(membersRel().OID, 'D', tokenRow{
		"project_id": expr.Text("42"),
		"user_id":    expr.Text("u1"),
	}, nil)
	for _, c := range cuts {
		got, ok := s.hub.Get(c.StreamID)
		if !ok {
			got = st
		}
		s.dropShape(got, c.Label, event.Error{Code: "shape_not_authorized", Message: c.Reason})
	}
}

func TestTokenRefreshConcurrentHoldKickDoesNotLeaveShape(t *testing.T) {
	rel := docsRel()
	wide, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdF, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	grant := &oracle.Grant{
		Filter:  wide,
		Columns: []string{"id", "project_id", "title"},
		Holds:   []hold.Spec{{Rel: membersRel(), Filter: holdF}},
	}

	for i := 0; i < 200; i++ {
		s := testServer(t, stubOracle{name: oracle.NameIssuer})
		s.holdExists = func(context.Context, []hold.Spec) error { return nil }
		st := s.hub.Open("n1.token-kick", authz.Identity{Sub: "u1", Role: "authenticated"})
		issuerLiveSub(t, s, st, wide, grant.Columns, holdF)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.applyIssuerGrant(context.Background(), st, "docs", grant)
		}()
		go func() {
			defer wg.Done()
			kickHold(s, st)
		}()
		wg.Wait()

		if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 0 {
			t.Fatalf("iter %d: hold kick concurrent with /token left the shape live", i)
		}
	}
}

func TestTokenRefreshAppliesNarrowerGrant(t *testing.T) {
	rel := docsRel()
	wide, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := shape.Parse("project_id=eq.42,title=eq.public", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdF, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	orc := &refreshOracle{
		stubOracle: stubOracle{name: oracle.NameIssuer},
		grant: &oracle.Grant{
			Filter:  narrow,
			Columns: []string{"id", "project_id"},
			Holds:   []hold.Spec{{Rel: membersRel(), Filter: holdF}},
		},
	}
	s := testServer(t, orc)
	s.holdExists = func(context.Context, []hold.Spec) error { return nil }
	st := s.hub.Open("n1.token-narrow", authz.Identity{Sub: "u1", Role: "authenticated"})
	sub := issuerLiveSub(t, s, st, wide, []string{"id", "project_id", "title"}, holdF)

	if !s.refreshIssuerShape(context.Background(), st, sub, st.Identity()) {
		t.Fatal("an allowed refresh must keep the shape")
	}
	if orc.last.Action != oracle.ActionRefresh {
		t.Fatalf("action = %q, want refresh", orc.last.Action)
	}

	live := s.reg.Get(st.StreamID(), "docs")
	if live == nil {
		t.Fatal("shape must still be registered")
	}
	if live.Filter.Equalities["title"].String() != "public" {
		t.Fatalf("effective filter = %s, want title=eq.public", live.Filter.Describe())
	}
	if slices.Contains(live.Columns, "title") {
		t.Fatalf("columns = %v, title must drop from the projection", live.Columns)
	}

	lookup := func(vals map[string]string) func(string) (expr.Value, bool) {
		return func(col string) (expr.Value, bool) {
			v, ok := vals[col]
			if !ok {
				return expr.Null, false
			}
			return expr.Text(v), true
		}
	}
	if got := s.reg.Candidates(rel.OID, lookup(map[string]string{"project_id": "42"})); len(got) != 1 {
		t.Fatalf("routing must still find the shape, got %d", len(got))
	}

	vis := func(title string) bool {
		ok, unk := expr.Visible(live.Filter.Node, &expr.Context{
			Row: tokenRow{
				"project_id": expr.Text("42"),
				"title":      expr.Text(title),
			},
			ClaimsJSON: "{}",
		})
		if unk {
			t.Fatalf("filter unknown for title=%s", title)
		}
		return ok
	}
	if vis("secret") {
		t.Fatal("live filter must reject a title the join grant would have allowed")
	}
	if !vis("public") {
		t.Fatal("live filter must keep the authorized title")
	}
}
