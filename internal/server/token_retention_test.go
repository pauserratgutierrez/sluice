package server

import (
	"context"
	"fmt"
	"runtime"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// freshGrantOracle answers every refresh with a grant parsed anew, as an
// issuer does: a new filter and new holds each time, never the same objects.
type freshGrantOracle struct {
	stubOracle
	project map[string]int // sub -> project
}

func (o *freshGrantOracle) Refresh(_ context.Context, req oracle.Request) (*oracle.Grant, error) {
	p := o.project[req.Identity.Sub]
	f, err := shape.Parse(fmt.Sprintf("project_id=eq.%d", p), docsRel())
	if err != nil {
		return nil, err
	}
	h, err := shape.Parse(fmt.Sprintf("user_id=eq.%s,project_id=eq.%d", req.Identity.Sub, p), membersRel())
	if err != nil {
		return nil, err
	}
	return &oracle.Grant{
		Filter:  f,
		Columns: []string{"id", "project_id", "title"},
		Holds:   []hold.Spec{{Rel: membersRel(), Filter: h}},
	}, nil
}

// Every POST /token re-grants each shape: Replace in the hold index, Rebind in
// the registry. Thousands of renewals must leave the live heap and both
// indexes where the first renewal left them.
func TestTokenRenewalsRetainNothing(t *testing.T) {
	if testing.Short() {
		t.Skip("renews tens of thousands of shapes")
	}
	const streams, shapes, rounds = 300, 7, 30
	orc := &freshGrantOracle{stubOracle: stubOracle{name: oracle.NameIssuer}, project: map[string]int{}}
	s := testServer(t, orc)
	s.holdExists = func(context.Context, []hold.Spec) error { return nil }

	var all []*hub.Stream
	for i := range streams {
		sub := fmt.Sprintf("u%d", i)
		orc.project[sub] = i % 40
		st := s.hub.Open(fmt.Sprintf("n1.%d", i), "", authz.Identity{Sub: sub, Role: "authenticated"})
		all = append(all, st)
		for j := range shapes {
			g, _ := orc.Refresh(context.Background(), oracle.Request{Identity: st.Identity()})
			label := fmt.Sprintf("s%d", j)
			if !s.reg.Add(&registry.Subscription{
				Label: label, Sink: st, Relation: docsRel(), Filter: g.Filter, Columns: g.Columns,
				Decision:   authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
				RoutingKey: g.Filter.RoutingKey(docsRel()),
			}) || !s.holds.Add(st.StreamID(), label, g.Holds) {
				t.Fatal("add")
			}
		}
	}
	renewAll := func() {
		for _, st := range all {
			for _, sub := range s.reg.StreamSubscriptions(st.StreamID()) {
				if got := s.refreshIssuerShape(context.Background(), st, sub, st.Identity()); got != "held" {
					t.Fatalf("refresh of %s/%s: %s", st.StreamID(), sub.Label, got)
				}
			}
		}
	}
	measure := func() (uint64, hold.Size, registry.IndexSize) {
		runtime.GC()
		runtime.GC()
		var ms runtime.MemStats
		runtime.ReadMemStats(&ms)
		return ms.HeapAlloc, s.holds.Size(), s.reg.IndexSize()
	}

	renewAll() // every shape has been re-granted once
	heap0, holds0, reg0 := measure()
	for range rounds {
		renewAll()
	}
	heap1, holds1, reg1 := measure()

	t.Logf("%d renewals: live heap %d → %d KiB; holds %+v → %+v; registry %+v → %+v",
		streams*shapes*rounds, heap0/1024, heap1/1024, holds0, holds1, reg0, reg1)
	if holds1 != holds0 {
		t.Errorf("the hold index changed size: %+v → %+v", holds0, holds1)
	}
	if reg1 != reg0 {
		t.Errorf("the subscription index changed size: %+v → %+v", reg0, reg1)
	}
	// Retaining 100 bytes per renewal would add 6 MiB; allow 512 KiB of noise.
	if heap1 > heap0+512<<10 {
		t.Errorf("live heap grew by %d KiB over %d renewals", (heap1-heap0)/1024, streams*shapes*rounds)
	}
}
