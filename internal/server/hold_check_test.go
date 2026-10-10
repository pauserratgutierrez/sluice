package server

import (
	"context"
	"fmt"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
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
			s.holdExists = func(context.Context, []hold.Spec) error { return tc.err }
			st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
			issuerLiveSub(t, s, st, docs, grant.Columns, holdF)
			if s.applyIssuerGrant(context.Background(), st, "docs", grant) {
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
			s.holdExists = func(context.Context, []hold.Spec) error { return tc.err }
			st := s.hub.Open("n1.1", "", authz.Identity{Sub: "u1", Role: "authenticated"})
			sub := &registry.Subscription{Label: "docs", Sink: st, Relation: rel, Filter: docs,
				Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}), RoutingKey: docs.RoutingKey(rel)}
			err := s.installShape(context.Background(), sub, grant.Holds)
			if err == nil {
				t.Fatal("installShape must fail")
			}
			e := holdCheckFailed(err)
			if e.Code != tc.code || e.Retryable != tc.retryable {
				t.Fatalf("%+v, want code %s, retryable %v", e, tc.code, tc.retryable)
			}
			if got := s.reg.StreamSubscriptions(st.StreamID()); len(got) != 0 || s.holds.Count() != 0 {
				t.Fatalf("a failed install must leave nothing: %d shapes, %d watches", len(got), s.holds.Count())
			}
		})
	}
}
