package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
)

// slowOracle grants every shape after a delay, except the filters in deny, and
// records how many resolutions overlapped.
type slowOracle struct {
	stubOracle
	delay        time.Duration
	deny         map[string]bool
	active, peak atomic.Int32
}

func (o *slowOracle) Resolve(_ context.Context, req oracle.Request) (*oracle.Grant, error) {
	n := o.active.Add(1)
	defer o.active.Add(-1)
	for p := o.peak.Load(); n > p && !o.peak.CompareAndSwap(p, n); p = o.peak.Load() {
	}
	time.Sleep(o.delay)
	if o.deny[req.Filter.Raw] {
		return nil, &oracle.ErrDenied{Reason: "denied for the test"}
	}
	return &oracle.Grant{
		Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}).Load(),
		Filter:   req.Filter,
		Columns:  []string{"id", "project_id", "title"},
	}, nil
}

// A stream opened with several shapes resolves them together, but admits and
// installs them in request order with the same limits as one at a time: a
// shape that fails leaves its place to a later one.
func TestShapesResolveConcurrentlyAndInstallInOrder(t *testing.T) {
	orc := &slowOracle{
		stubOracle: stubOracle{name: oracle.NameRLS},
		delay:      30 * time.Millisecond,
		deny:       map[string]bool{"project_id=eq.1": true},
	}
	s := testServer(t, orc)
	s.cat.PutForTest(docsRel())
	s.cfg.MaxShapesPerStream = 3
	s.cfg.MaxSubsPerStream = 10
	id := authz.Identity{Sub: "u1", Role: "authenticated"}
	st := s.hub.Open("n1.1", "", id)

	var specs []subSpec
	for i := 1; i <= 5; i++ {
		specs = append(specs, subSpec{Sub: fmt.Sprint("s", i), Shape: &shapeSpec{
			Table: "documents", Filter: fmt.Sprintf("project_id=eq.%d", i),
		}})
	}
	results := s.applySubscriptions(context.Background(), st, id, specs, nil)

	want := []struct {
		ok   bool
		code string
	}{{false, "not_authorized"}, {true, ""}, {true, ""}, {true, ""}, {false, "too_many_shapes"}}
	for i, r := range results {
		if r.Sub != specs[i].Sub || r.OK != want[i].ok {
			t.Fatalf("result %d = %+v, want %s ok=%v", i, r, specs[i].Sub, want[i].ok)
		}
		if !r.OK && want[i].code == "too_many_shapes" && r.Error.Code != "too_many_shapes" {
			t.Fatalf("result %d error = %q, want too_many_shapes", i, r.Error.Code)
		}
	}
	if got := len(s.reg.StreamSubscriptions(st.StreamID())); got != 3 {
		t.Fatalf("%d shapes installed, want 3", got)
	}
	if orc.peak.Load() < 2 {
		t.Fatal("the shapes were resolved one at a time")
	}
}
