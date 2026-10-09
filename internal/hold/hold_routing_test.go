package hold

import (
	"fmt"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// checkConsistent asserts the index's invariant: every watch it routes to is
// a live watch, registered for its stream and label.
func checkConsistent(t *testing.T, x *Index) {
	t.Helper()
	x.mu.RLock()
	defer x.mu.RUnlock()
	live := map[*Watch]bool{}
	for _, subs := range x.byStream {
		for _, w := range subs {
			live[w] = true
		}
	}
	for oid, ri := range x.rels {
		for w := range ri.all {
			if !live[w] {
				t.Fatalf("relation %d: a watch of %s/%s is in all but not registered", oid, w.StreamID, w.Label)
			}
		}
		for col, byConst := range ri.byColumn {
			for val, list := range byConst {
				for _, w := range list {
					if !live[w] || !ri.all[w] {
						t.Fatalf("relation %d: %s=%s routes to a watch of %s/%s that is no longer live", oid, col, val, w.StreamID, w.Label)
					}
				}
			}
		}
		for _, w := range ri.unindexed {
			if !live[w] {
				t.Fatalf("relation %d: an unindexed watch of %s/%s is no longer live", oid, w.StreamID, w.Label)
			}
		}
	}
}

// A hold filter with two columns that both lead an index and are in the
// replica identity can be routed by either. Replace and Remove must take the
// watch out of whichever list it was put in; a watch left behind is kept alive
// for as long as the relation has other watches, and is a candidate on every
// later change to its constant.
func TestReplaceAndRemoveLeaveNothingIndexed(t *testing.T) {
	rel := membersRel()
	x := New()
	// Another stream's watch keeps the relation's index alive, as in a
	// process with more than one stream.
	if !x.Add("other", "docs", []Spec{holdSpec(t, rel, "project_id=eq.7,user_id=eq.u9")}) {
		t.Fatal("add other")
	}
	if !x.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")}) {
		t.Fatal("add s1")
	}
	for range 200 {
		x.Replace("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
		checkConsistent(t, x)
	}
	x.Remove("s1", "docs")
	checkConsistent(t, x)
}

// A watch left behind by a refresh that changed the hold must not cut the
// grant that replaced it: deleting the old hold row is not a revocation of the
// new one. Run many times, since how a two-column hold is routed must not
// matter.
func TestOldHoldDoesNotRevokeNewGrant(t *testing.T) {
	rel := membersRel()
	for trial := range 64 {
		x := New()
		x.Add("other", "docs", []Spec{holdSpec(t, rel, "project_id=eq.7,user_id=eq.u9")})
		x.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
		// The refreshed grant is held by another row.
		x.Replace("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.43,user_id=eq.u1")})

		cuts := x.OnChange(rel.OID, 'D', row{
			"project_id": expr.Text("42"),
			"user_id":    expr.Text("u1"),
		}, nil)
		if len(cuts) != 0 {
			t.Fatalf("trial %d: deleting the old hold row cut %+v; the grant is held by project 43 now", trial, cuts)
		}
		if x.Count() != 2 {
			t.Fatalf("trial %d: %d watches left, want 2", trial, x.Count())
		}
		// The current hold still cuts.
		cuts = x.OnChange(rel.OID, 'D', row{
			"project_id": expr.Text("43"),
			"user_id":    expr.Text("u1"),
		}, nil)
		if len(cuts) != 1 || cuts[0].StreamID != "s1" {
			t.Fatalf("trial %d: deleting the current hold row cut %+v, want s1/docs", trial, cuts)
		}
	}
}

// Thousands of streams joining, refreshing and leaving with two-column holds
// leave the index consistent throughout and holding nothing of theirs.
func TestChurnLeavesIndexConsistent(t *testing.T) {
	rel := membersRel()
	x := New()
	x.Add("other", "docs", []Spec{holdSpec(t, rel, "project_id=eq.7,user_id=eq.u9")})
	const streams = 2000
	for i := range streams {
		f := fmt.Sprintf("project_id=eq.%d,user_id=eq.u%d", i%40, i)
		for _, label := range []string{"a", "b"} {
			if !x.Add(fmt.Sprintf("s%d", i), label, []Spec{holdSpec(t, rel, f)}) {
				t.Fatal("add")
			}
		}
	}
	for round := range 3 {
		for i := range streams {
			f := fmt.Sprintf("project_id=eq.%d,user_id=eq.u%d", (i+round)%40, i)
			x.Replace(fmt.Sprintf("s%d", i), "a", []Spec{holdSpec(t, rel, f)})
		}
		checkConsistent(t, x)
	}
	for i := range streams {
		x.RemoveStream(fmt.Sprintf("s%d", i))
	}
	checkConsistent(t, x)
	if n := x.Count(); n != 1 {
		t.Fatalf("%d watches left, want the other stream's", n)
	}
}
