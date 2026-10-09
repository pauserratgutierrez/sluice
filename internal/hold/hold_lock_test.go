package hold

import (
	"sync"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// stallingRow stops when a hold filter reads one column, until released.
type stallingRow struct {
	row
	col      string
	reached  chan struct{}
	release  chan struct{}
	reachedO *sync.Once
}

func (r stallingRow) Column(name string) (expr.Value, bool) {
	if name == r.col {
		r.reachedO.Do(func() { close(r.reached) })
		<-r.release
	}
	return r.row.Column(name)
}

// Evaluating the candidates of a change is most of a hold check; a stream
// opening (Add) or a /token (Replace) must not wait for it.
func TestHoldCheckDoesNotBlockAddOrReplace(t *testing.T) {
	rel := membersRel()
	x := New()
	// Listed under user_id (the first of two columns no list tells apart yet),
	// so finding the candidates reads user_id and evaluating them project_id.
	x.Add("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.42")})
	deleted := stallingRow{
		row:     row{"project_id": expr.Text("42"), "user_id": expr.Text("u1")},
		col:     "project_id",
		reached: make(chan struct{}), release: make(chan struct{}), reachedO: &sync.Once{},
	}
	result := make(chan []Cut)
	go func() { result <- x.OnChange(rel.OID, 'D', deleted, nil) }()
	<-deleted.reached

	done := make(chan struct{})
	go func() {
		x.Add("s2", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u2,project_id=eq.42")})
		x.Replace("s3", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u3,project_id=eq.42")})
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		close(deleted.release)
		t.Fatal("Add and Replace waited for a hold check to finish")
	}
	close(deleted.release)
	if cuts := <-result; len(cuts) != 1 || cuts[0].StreamID != "s1" {
		t.Fatalf("cut %+v, want s1", cuts)
	}
	if x.Count() != 2 {
		t.Fatalf("%d watches left, want 2", x.Count())
	}
	checkConsistent(t, x)
}
