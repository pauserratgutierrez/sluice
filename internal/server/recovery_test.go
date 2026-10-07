package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/requestid"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// The result says whether a resume is covered, so a client knows at once
// whether to refetch; an uncovered one is not an error.
func TestResumeIsDecidedInTheResult(t *testing.T) {
	s := testServer(t, &slowOracle{stubOracle: stubOracle{name: oracle.NameRLS}})
	s.cat.PutForTest(docsRel())
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10
	s.hub.Rings().SetStart(0x100)
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.resume", "", id)

	shapeOn := func(sub string) subSpec {
		return subSpec{Sub: sub, Shape: &shapeSpec{Table: "documents", Filter: "project_id=eq.42"}}
	}
	for _, tc := range []struct {
		sub    string
		resume map[string]string
		want   *bool
	}{
		{"covered", map[string]string{"public.documents": "0/200"}, new(true)},
		{"before-start", map[string]string{"public.documents": "0/10"}, new(false)},
		{"no-position", nil, nil},
	} {
		res := s.applySubscriptions(context.Background(), st, id, []subSpec{shapeOn(tc.sub)}, tc.resume)[0]
		if !res.OK {
			t.Fatalf("%s: %+v", tc.sub, res.Error)
		}
		if (res.Resumed == nil) != (tc.want == nil) || (res.Resumed != nil && *res.Resumed != *tc.want) {
			t.Errorf("%s: resumed = %v, want %v", tc.sub, deref(res.Resumed), deref(tc.want))
		}
	}
	for _, ev := range events(st) {
		if e, ok := ev.Data.(event.Error); ok {
			t.Fatalf("an uncovered resume must not send an error, got %+v", e)
		}
	}
}

func deref(b *bool) any {
	if b == nil {
		return nil
	}
	return *b
}

type unavailableOracle struct {
	stubOracle
	retryAfter time.Duration
}

func (o unavailableOracle) Resolve(context.Context, oracle.Request) (*oracle.Grant, error) {
	return nil, &oracle.ErrUnavailable{Reason: "shape issuer returned HTTP 503", RetryAfter: o.retryAfter}
}

// An issuer that gives no verdict refuses the shape, but retryably, with the
// delay it asked for or one spread over a few seconds.
func TestSubscribeIssuerUnavailableIsRetryable(t *testing.T) {
	for _, tc := range []struct {
		retryAfter time.Duration
		min, max   int64
	}{
		{3 * time.Second, 3000, 3000},
		{0, issuerRetryMin.Milliseconds(), (issuerRetryMin + issuerRetrySpread).Milliseconds()},
	} {
		s := testServer(t, unavailableOracle{stubOracle{name: oracle.NameIssuer}, tc.retryAfter})
		s.cat.PutForTest(docsRel())
		s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10
		id := authz.Identity{Sub: "u1", Role: "authenticated"}
		st := s.hub.Open("n1.unavailable", "", id)
		res := s.applySubscriptions(context.Background(), st, id, []subSpec{{Sub: "docs",
			Shape: &shapeSpec{Table: "documents", Filter: "project_id=eq.42"}}}, nil)[0]

		e := res.Error
		if res.OK || e == nil || e.Code != "issuer_unavailable" || !e.Retryable {
			t.Fatalf("result = %+v, want a retryable issuer_unavailable", res)
		}
		if e.RetryAfterMs < tc.min || e.RetryAfterMs > tc.max {
			t.Errorf("retry_after_ms = %d, want within [%d, %d]", e.RetryAfterMs, tc.min, tc.max)
		}
		if len(s.reg.StreamSubscriptions(st.StreamID())) != 0 {
			t.Fatal("an undecided shape must not be installed")
		}
	}
}

// /token fails closed when the issuer gives no verdict: the shape is dropped
// with a retryable error, so the client subscribes again under the new token
// instead of keeping the previous grant.
func TestTokenIssuerUnavailableDropsShapeRetryably(t *testing.T) {
	rel := docsRel()
	f, err := shape.Parse("project_id=eq.42", rel)
	if err != nil {
		t.Fatal(err)
	}
	holdF, err := shape.Parse("project_id=eq.42,user_id=eq.u1", membersRel())
	if err != nil {
		t.Fatal(err)
	}
	orc := &refreshOracle{
		stubOracle: stubOracle{name: oracle.NameIssuer},
		err:        &oracle.ErrUnavailable{Reason: "shape issuer unreachable", RetryAfter: 2 * time.Second},
	}
	s := testServer(t, orc)
	logs := logTo(s)
	st := s.hub.Open("n1.token-down", "gw-open", authz.Identity{Sub: "u1", Role: "authenticated"})
	sub := issuerLiveSub(t, s, st, f, []string{"id", "project_id"}, holdF)

	ctx := requestid.With(context.Background(), requestid.ID{Header: "X-Request-ID", Value: "gw-token"})
	if got := s.refreshIssuerShape(ctx, st, sub, st.Identity()); got != "unavailable" {
		t.Fatalf("outcome = %q, want unavailable", got)
	}
	logs.one(t, "subscription revoked", map[string]string{"level": "WARN", "request_id": "gw-token",
		"stream_request_id": "gw-open", "sub": "docs", "table": "public.documents", "code": "issuer_unavailable"})
	if s.reg.Get(st.StreamID(), "docs") != nil || s.holds.Count() != 0 {
		t.Fatal("the shape and its hold must be dropped")
	}
	evs := events(st)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want one error", len(evs))
	}
	e, ok := evs[0].Data.(event.Error)
	if !ok || e.Sub != "docs" || e.Code != "issuer_unavailable" || !e.Retryable || e.RetryAfterMs != 2000 {
		t.Fatalf("event = %+v, want issuer_unavailable for docs, retryable after 2000 ms", evs[0].Data)
	}
	if _, ok := s.hub.Get(st.StreamID()); !ok {
		t.Fatal("the stream must stay open")
	}
}

// TRUNCATE deletes every row without a DELETE per row, so it must cut every
// shape held by a row of the truncated table.
func TestTruncateOfHoldTableCutsShape(t *testing.T) {
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	st := s.hub.Open("n1.truncate", "", authz.Identity{Sub: "u1", Role: "authenticated"})
	liveDocsHold(t, s, st, membersRel())

	s.OnTruncate([]uint32{docsRel().OID})
	if s.reg.Get(st.StreamID(), "docs") == nil {
		t.Fatal("truncating the subscribed table is not a hold cut")
	}

	s.OnTruncate([]uint32{membersRel().OID})
	if s.reg.Get(st.StreamID(), "docs") != nil || s.holds.Count() != 0 {
		t.Fatal("truncating the hold table must cut the shape and its watch")
	}
	evs := events(st)
	if len(evs) != 1 {
		t.Fatalf("got %d events, want one error", len(evs))
	}
	if e, ok := evs[0].Data.(event.Error); !ok || e.Code != "shape_not_authorized" || !strings.Contains(e.Message, "truncated") {
		t.Fatalf("event = %+v, want shape_not_authorized naming the truncate", evs[0].Data)
	}
}

// A table published only for revocation is refused as a shape, before any
// oracle is asked: the pool role need not be able to read it.
func TestShapeOnRevocationTableIsRefused(t *testing.T) {
	orc := &countingOracle{slowOracle: slowOracle{stubOracle: stubOracle{name: oracle.NameIssuer}}}
	s := testServer(t, orc)
	sessions := docsRel()
	sessions.OID, sessions.Schema, sessions.Name = 400, "identity", "session"
	s.cat.PutForTest(docsRel(), sessions)
	s.cfg.RevocationEnabled = true
	s.cfg.SessionsTable = "identity.session"
	s.cfg.MaxShapesPerStream, s.cfg.MaxSubsPerStream = 10, 10
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.revocation-table", "", id)

	results := s.applySubscriptions(context.Background(), st, id, []subSpec{
		{Sub: "sessions", Shape: &shapeSpec{Schema: "identity", Table: "session", Filter: "project_id=eq.42"}},
		{Sub: "docs", Shape: &shapeSpec{Table: "documents", Filter: "project_id=eq.42"}},
	}, nil)
	if r := results[0]; r.OK || r.Error == nil || r.Error.Code != "shape_not_authorized" {
		t.Fatalf("shape on the sessions table = %+v, want shape_not_authorized", r)
	}
	if !results[1].OK {
		t.Fatalf("other shapes are unaffected: %+v", results[1].Error)
	}
	if n := orc.asked.Load(); n != 1 {
		t.Fatalf("the oracle was asked about %d shapes, want only the documents one", n)
	}
}

type countingOracle struct {
	slowOracle
	asked atomic.Int32
}

func (o *countingOracle) Resolve(ctx context.Context, req oracle.Request) (*oracle.Grant, error) {
	o.asked.Add(1)
	return o.slowOracle.Resolve(ctx, req)
}

// With the lookup on, a token whose session row is gone cannot open a stream,
// even though this process never saw the row deleted.
func TestSessionLookupRefusesSignedOutSession(t *testing.T) {
	const (
		user   = "0199b1c2-0000-7000-8000-000000000001"
		live   = "0199b1c2-7a3e-7c11-9f00-6e5d4c3b2a10"
		gone   = "0199b1c2-7a3e-7c11-9f00-6e5d4c3b2a11"
		broken = "0199b1c2-7a3e-7c11-9f00-6e5d4c3b2a12"
	)
	s := testServer(t, stubOracle{name: oracle.NameIssuer})
	s.cfg.MaxStreams = 10
	s.cfg.WriteTimeout = 5 * time.Second
	s.revoker = auth.NewRevoker(0)
	verify, sign := sidVerifier(t)
	s.verify = verify
	lookups := map[string]int{}
	s.sessions = &sessionLookup{
		exists: func(_ context.Context, sid string) (bool, error) {
			lookups[sid]++
			if sid == broken {
				return false, errors.New("connection refused")
			}
			return sid == live, nil
		},
		found: map[string]time.Time{},
	}
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)

	status := func(sid string) int {
		res := postStream(t, srv, sign(user, sid))
		res.Body.Close()
		return res.StatusCode
	}
	if got := status(gone); got != http.StatusUnauthorized {
		t.Fatalf("signed-out session: %d, want 401", got)
	}
	if !s.revoker.Revoked(authz.Identity{SessionID: gone}) {
		t.Fatal("a session found missing must be remembered as revoked")
	}
	if got := status(gone); got != http.StatusUnauthorized || lookups[gone] != 1 {
		t.Fatalf("second attempt: %d after %d lookups, want 401 without asking again", got, lookups[gone])
	}
	if got := status(broken); got != http.StatusServiceUnavailable {
		t.Fatalf("lookup failure: %d, want 503", got)
	}
	if got := status(live); got != http.StatusOK {
		t.Fatalf("live session: %d, want 200", got)
	}
	if got := status(live); got != http.StatusOK || lookups[live] != 1 {
		t.Fatalf("live session again: %d after %d lookups, want 200 from the cache", got, lookups[live])
	}
}
