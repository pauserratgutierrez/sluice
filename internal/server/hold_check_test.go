package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// A hold check that could not be made -- the request was cancelled, the
// database failed -- drops the shape with a retryable hold_check_unavailable,
// not a denial the client would give up on. A hold row that does not exist
// still denies.
func TestHoldCheckFailuresAreRetryableAndMissingHoldsDeny(t *testing.T) {
	rel := docsRel()
	docs, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdF, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	grant := &oracle.Grant{Filter: docs, Columns: []string{"id", "project_id", "title"},
		Holds: []hold.Spec{{Rel: membersRel(), Filter: holdF}}}

	for _, tc := range []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{"cancelled", &hold.CheckError{Rel: "public.project_members", Err: context.Canceled}, "hold_check_unavailable", true},
		{"database down", &hold.CheckError{Rel: "public.project_members", Err: fmt.Errorf("connection refused")}, "hold_check_unavailable", true},
		{"missing", &hold.MissingError{Rel: "public.project_members", Filter: "project_id=eq.42,user_id=eq.u1"}, "shape_not_authorized", false},
	} {
		t.Run("token "+tc.name, func(t *testing.T) {
			s := testServer(t, stubOracle{name: oracle.NameIssuer})
			s.holdCheck = holdsAnswer(tc.err)
			st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
			issuerLiveSub(t, s, st, docs, grant.Columns, holdF)
			if applyOne(s, st, "docs", grant) {
				t.Fatal("a grant whose holds were not confirmed must not be kept")
			}
			evs := events(st)
			if len(evs) == 0 {
				t.Fatal("no event")
			}
			e, ok := evs[len(evs)-1].Data.(event.Error)
			if !ok || e.Code != tc.code || e.Retryable != tc.retryable || (tc.retryable && e.RetryAfterMs <= 0) {
				t.Fatalf("event %+v, want code %s, retryable %v with a delay", evs[len(evs)-1].Data, tc.code, tc.retryable)
			}
		})
		t.Run("subscribe "+tc.name, func(t *testing.T) {
			s := testServer(t, stubOracle{name: oracle.NameIssuer})
			s.holdCheck = holdsAnswer(tc.err)
			st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
			sub := &registry.Subscription{Label: "docs", Sink: st, Relation: rel, Filter: docs,
				Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}), RoutingKey: docs.RoutingKey(rel)}
			res := installOne(s, sub, grant.Holds)
			if res.OK || res.Error == nil {
				t.Fatal("the install must fail")
			}
			e := res.Error
			if e.Code != tc.code || e.Retryable != tc.retryable {
				t.Fatalf("%+v, want code %s, retryable %v", e, tc.code, tc.retryable)
			}
			if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 0 || s.holds.Count() != 0 {
				t.Fatalf("a failed install must leave nothing: %d shapes, %d watches", len(got), s.holds.Count())
			}
		})
	}
}

// A shape receives no change while its holds are checked: it is registered only
// once they are found. A change dispatched during the check is before the
// shape's ready, so the client's read or a resume from its wal_lsn covers it.
// Registered first, a shape whose hold turned out missing got that change.
func TestAShapeGetsNoChangeWhileItsHoldsAreChecked(t *testing.T) {
	rel := docsRel()
	docs, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdF, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	wal := walDocs(t)
	insert := func(s *Server, id string) {
		t.Helper()
		m := &pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 100, New: row(id, "42", "t")}
		if err := s.OnChange(m, wal, 0x10, time.Now()); err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		name  string
		err   error
		after int // changes the shape gets for a row inserted after the install
	}{
		{"hold missing", &hold.MissingError{Rel: "public.project_members", Filter: "project_id=eq.42,user_id=eq.u1"}, 0},
		{"hold found", nil, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := testServer(t, stubOracle{name: oracle.NameIssuer})
			s.holdCheck = func(ctx context.Context, specs []hold.Spec) []error {
				insert(s, "1") // the reader dispatches a change while the check runs
				return holdsAnswer(tc.err)(ctx, specs)
			}
			st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
			ops, _ := shape.ParseOps(nil)
			sub := &registry.Subscription{Label: "docs", Sink: st, Relation: rel, Ops: ops, Filter: docs,
				Columns:  []string{"id", "project_id", "title"},
				Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}), RoutingKey: docs.RoutingKey(rel)}
			if res := installOne(s, sub, []hold.Spec{{Rel: membersRel(), Filter: holdF}}); res.OK != (tc.err == nil) {
				t.Fatalf("install: ok %v, %+v", res.OK, res.Error)
			}
			if got := changesOf(st); len(got) != 0 {
				t.Fatalf("the shape got %d change(s) dispatched while its holds were checked", len(got))
			}
			insert(s, "2")
			if got := changesOf(st); len(got) != tc.after {
				t.Fatalf("%d change(s) after the install, want %d", len(got), tc.after)
			}
		})
	}
}

// The shapes of a request are decided on their own holds: a missing hold
// refuses the shapes it holds, and the others are installed.
func TestAMissingHoldRefusesOnlyTheShapesItHolds(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.holdCheck = func(_ context.Context, specs []hold.Spec) []error {
		out := make([]error, len(specs))
		for i, sp := range specs {
			if sp.Filter.Equalities["project_id"].String() == "43" {
				out[i] = &hold.MissingError{Rel: sp.Rel.FullName(), Filter: sp.Filter.Describe()}
			}
		}
		return out
	}
	st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	rel := docsRel()
	plan := func(label, project string) plannedShape {
		t.Helper()
		f, err := shape.Parse("project_id=eq."+project, rel)
		if err != nil {
			t.Fatal(err)
		}
		hf, err := shape.Parse("project_id=eq."+project+",user_id=eq.u1", membersRel())
		if err != nil {
			t.Fatal(err)
		}
		return plannedShape{
			sub: &registry.Subscription{Label: label, Sink: st, Relation: rel, Filter: f,
				Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}), RoutingKey: f.RoutingKey(rel)},
			grant: &oracle.Grant{Holds: []hold.Spec{{Rel: membersRel(), Filter: hf}}},
			res:   subResult{Sub: label},
		}
	}
	res := s.installShapes(context.Background(), st, []plannedShape{plan("a", "42"), plan("b", "43"), plan("c", "42")})
	if !res[0].OK || !res[2].OK {
		t.Fatalf("shapes held by an existing row must be installed: %+v, %+v", res[0].Error, res[2].Error)
	}
	if res[1].OK || res[1].Error.Code != "shape_not_authorized" {
		t.Fatalf("the shape held by a missing row: %+v, want shape_not_authorized", res[1])
	}
	if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 2 || s.holds.Count() != 2 {
		t.Fatalf("%d shapes and %d watches, want 2 of each", len(got), s.holds.Count())
	}
}
