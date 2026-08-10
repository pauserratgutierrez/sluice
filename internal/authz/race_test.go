package authz

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

type raceRow map[string]expr.Value

func (r raceRow) Column(name string) (expr.Value, bool) {
	v, ok := r[name]
	return v, ok
}

// TestRefreshDoesNotRaceDispatch pins the reason Decision is published through a
// Handle instead of being edited in place.
//
// RefreshLeases re-resolves on a timer while the dispatch goroutine reads the
// decision for every change, and nothing sits between them but the registry
// mutex, which protects the map rather than the struct behind the pointer.
//
// Editing in place raced on two levels. The scalar fields tore -- Predicate is an
// interface and PredicateSQL a string, both two words wide, so a reader could
// pair one generation's pointer with another's type or length. Worse, assigning
// Predicate published a pointer to a freshly built expression tree with no
// release barrier, letting a reader follow it into nodes whose field writes it
// had no guarantee of seeing; the detector reported that as reads inside
// expr.Eval racing writes from mapChildren constructing the new tree.
//
// Run under -race, which needs cgo and therefore a Linux container on a Windows
// host:
//
//	docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=1 \
//	  golang:1.26 go test -race ./internal/authz/
//
// Without the detector this test passes either way, so it is worth nothing on
// its own.
func TestRefreshDoesNotRaceDispatch(t *testing.T) {
	cat := catalog.New(nil)
	a := New(nil, cat, Options{Lease: time.Minute, TierC: "allow", MaxProbesPerSec: 1000})

	parse := func(sql string) expr.Node {
		t.Helper()
		n, err := expr.Parse(sql)
		if err != nil {
			t.Fatalf("parse %q: %v", sql, err)
		}
		return n
	}

	// Two relations differing only in policy text. Alternating between them is
	// what a CREATE/DROP POLICY looks like from here, and it makes successive
	// generations differ in every field rather than writing back identical
	// values. They are never mutated, so the only contended memory is the
	// Decision itself.
	relFor := func(using string) *catalog.Relation {
		return &catalog.Relation{
			Schema: "public", Name: "posts", RLSEnabled: true,
			Columns: []catalog.Column{
				{Name: "owner_id", TypeName: "text", AttNum: 1},
				{Name: "visibility", TypeName: "text", AttNum: 2},
			},
			Policies: []catalog.Policy{{
				Name: "p", Permissive: true, Roles: []string{"authenticated"},
				Using: using, Parsed: parse(using),
			}},
		}
	}
	gens := []*catalog.Relation{
		relFor("owner_id = 'alice'"),
		relFor("visibility = 'public' OR owner_id = 'alice'"),
	}
	rel := gens[0]

	id := Identity{Role: "authenticated", Claims: expr.Claims{"sub": "alice"}, ClaimsRaw: `{"sub":"alice"}`}

	d, err := a.Resolve(context.Background(), rel, id, nil)
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	// Keep the cross-check off: it needs a database, and it is not what this
	// test is about.
	d.verify.left.Store(0)
	h := NewHandle(d)

	row := raceRow{"owner_id": expr.Text("alice"), "visibility": expr.Text("private")}
	// Tier B reads the row, not the tuple; the tuple is only consulted by the
	// cross-check, which is disabled above.
	tuple := &Tuple{Op: 'I'}

	var readers sync.WaitGroup
	stop := make(chan struct{})

	// The dispatch goroutines: read the decision on every change.
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				dec := h.Load()
				_ = dec.EffectiveTier()
				_, _ = a.Visible(context.Background(), dec, id, rel, row, tuple)
			}
		}()
	}

	// The RefreshLeases side: re-resolve and republish.
	for i := 0; i < 2000; i++ {
		if _, err := a.Refresh(context.Background(), h, gens[i%2], id, nil); err != nil {
			t.Fatalf("refresh: %v", err)
		}
	}

	close(stop)
	readers.Wait()
}
