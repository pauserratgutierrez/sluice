package server

import (
	"context"
	"strings"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// A hold whose filter reads a column outside the replica identity the WAL now
// carries is dropped when the Relation message arrives, before the reader
// dispatches another change: an UPDATE without an old tuple cuts nothing on
// the promise that no such hold exists. The catalog refresh would drop it too,
// but only on its next tick.
func TestRelationMessageDropsHoldsOutsideTheIdentity(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	members := membersRel()
	members.ReplicaIdentity = 'f' // granted while the table was FULL
	members.Columns = append(members.Columns, catalog.Column{Name: "role", TypeName: "text"})
	open := func(streamID, user, holdFilter string) *hub.Stream {
		t.Helper()
		st := s.hub.Open(streamID, "", authz.Identity{Sub: user, Role: "authenticated"})
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
		hf, err := shape.Parse(holdFilter, members)
		if err != nil {
			t.Fatal(err)
		}
		if !s.holds.Add(st.StreamID(), "docs", []hold.Spec{{Rel: members, Filter: hf}}) {
			t.Fatal("add hold")
		}
		return st
	}
	byKey := open("n1.1", "u1", "user_id=eq.u1,project_id=eq.42")
	byRole := open("n1.2", "u2", "user_id=eq.u2,role=eq.admin")

	// The table moves from FULL to its primary key; this is the first Relation
	// message the reader sees for it, as after a restart.
	s.OnRelation(nil, walRelation(t, members.OID, "public", "project_members",
		walColumn{1, "project_id", 25}, walColumn{1, "user_id", 25}, walColumn{0, "role", 25}))

	if got := s.reg.StreamSubscriptions(byRole.StreamID()); len(got) != 0 {
		t.Fatalf("a hold reading role must drop its shape, still have %d", len(got))
	}
	evs := events(byRole)
	if len(evs) == 0 {
		t.Fatal("no event for the dropped shape")
	}
	if e, ok := evs[0].Data.(event.Error); !ok || e.Code != "shape_not_authorized" {
		t.Fatalf("event = %+v, want shape_not_authorized", evs[0].Data)
	}
	if got := s.reg.StreamSubscriptions(byKey.StreamID()); len(got) != 1 {
		t.Fatalf("a hold on key columns must stay, have %d shapes", len(got))
	}
	if n := s.holds.Count(); n != 1 {
		t.Fatalf("%d hold watches left, want 1", n)
	}
}

// After the WAL has carried a replica identity that no longer covers a hold's
// columns, and before the catalog refreshes, a grant with such a hold is
// refused when it is installed and revoked when /token re-grants it.
func TestGrantsAfterAnIdentityChangeAreCheckedAgainstTheWAL(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.holdExists = func(context.Context, []hold.Spec) error { return nil }
	members := membersRel()
	members.ReplicaIdentity = 'f' // what the catalog still says
	members.Columns = append(members.Columns, catalog.Column{Name: "role", TypeName: "text"})
	s.OnRelation(nil, walRelation(t, members.OID, "public", "project_members",
		walColumn{1, "project_id", 25}, walColumn{1, "user_id", 25}, walColumn{0, "role", 25}))

	rel := docsRel()
	docs, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdOn := func(filter string) []hold.Spec {
		t.Helper()
		f, err := shape.Parse(filter, members)
		if err != nil {
			t.Fatal(err)
		}
		return []hold.Spec{{Rel: members, Filter: f}}
	}
	sub := func(st *hub.Stream) *registry.Subscription {
		return &registry.Subscription{
			Label: "docs", Sink: st, Relation: rel, Filter: docs,
			Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
			RoutingKey: docs.RoutingKey(rel),
		}
	}

	refused := s.hub.Open("n1.1", "", authz.Identity{Sub: "u2", Role: "authenticated"})
	err = s.installShape(context.Background(), sub(refused), holdOn("user_id=eq.u2,role=eq.admin"))
	if err == nil || !strings.Contains(err.Error(), "role") {
		t.Fatalf("installing a hold that reads role must be refused, got %v", err)
	}
	if got := s.reg.StreamSubscriptions(refused.StreamID()); len(got) != 0 || s.holds.Count() != 0 {
		t.Fatalf("a refused grant must leave nothing behind: %d shapes, %d watches", len(got), s.holds.Count())
	}
	covered := s.hub.Open("n1.2", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	err = s.installShape(context.Background(), sub(covered), holdOn("user_id=eq.u1,project_id=eq.42"))
	if err != nil {
		t.Fatalf("a hold on key columns must be installed, got %v", err)
	}

	renewed := s.hub.Open("n1.3", "", authz.Identity{Sub: "u3", Role: "authenticated"})
	keyHold := holdOn("user_id=eq.u3,project_id=eq.42")[0].Filter
	issuerLiveSub(t, s, renewed, docs, []string{"id", "project_id", "title"}, keyHold)
	if s.applyIssuerGrant(context.Background(), renewed, "docs", &oracle.Grant{
		Filter: docs, Columns: []string{"id", "project_id", "title"}, Holds: holdOn("user_id=eq.u3,role=eq.admin"),
	}) {
		t.Fatal("a re-grant whose hold reads role must be revoked")
	}
	if got := s.reg.StreamSubscriptions(renewed.StreamID()); len(got) != 0 {
		t.Fatalf("the revoked shape is still registered (%d)", len(got))
	}
	evs := events(renewed)
	if len(evs) == 0 {
		t.Fatal("no event for the revoked shape")
	}
	if e, ok := evs[len(evs)-1].Data.(event.Error); !ok || e.Code != "shape_not_authorized" {
		t.Fatalf("event = %+v, want shape_not_authorized", evs[len(evs)-1].Data)
	}
}
