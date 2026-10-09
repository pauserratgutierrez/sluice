package hold

import (
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// A watch that is not the stream's current watch for its label -- one a later
// grant replaced, were it still indexed -- is dropped from the index when its
// hold breaks, and cuts nothing: the current grant has its own holds.
func TestOnlyTheCurrentWatchCuts(t *testing.T) {
	rel := membersRel()
	x := New()
	x.Add("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.43")})
	stale := newWatch("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.42")})
	x.mu.Lock()
	x.indexWatchLocked(stale)
	x.mu.Unlock()

	cuts := x.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"),
		"user_id":    expr.Text("u1"),
	}, nil)
	if len(cuts) != 0 {
		t.Fatalf("a replaced watch cut %+v", cuts)
	}
	if x.Count() != 1 {
		t.Fatalf("the current watch must stay: %d watches", x.Count())
	}
	checkConsistent(t, x)

	// TRUNCATE cuts the current watch once, and only it.
	stale = newWatch("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.41")})
	x.mu.Lock()
	x.indexWatchLocked(stale)
	x.mu.Unlock()
	cuts = x.OnTruncate(rel.OID)
	if len(cuts) != 1 || cuts[0].StreamID != "s1" {
		t.Fatalf("truncate cut %+v, want s1/docs once", cuts)
	}
	checkConsistent(t, x)
}
