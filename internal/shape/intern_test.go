package shape

import (
	"runtime"
	"testing"
	"time"
)

// A filter with the same text on the same relation is one object; another
// relation, or a relation reloaded after DDL, gets its own.
func TestParseSharesIdenticalFilters(t *testing.T) {
	r := rel()
	a, err := Parse("title=eq.interned", r)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := Parse("title=eq.interned", r)
	if a != b {
		t.Fatal("the same filter on the same relation must be shared")
	}
	if c, _ := Parse("title=eq.interned", rel()); c == a {
		t.Fatal("another relation must get its own filter")
	}
}

// The table does not keep a filter alive: once nothing uses it, it is
// collected and its entry removed.
func TestParseForgetsFiltersNothingUses(t *testing.T) {
	r := rel()
	f, err := Parse("title=eq.forgotten", r)
	if err != nil {
		t.Fatal(err)
	}
	k := internKey{rel: r, raw: "title=eq.forgotten"}
	if _, ok := interned.Load(k); !ok {
		t.Fatal("the filter was not recorded")
	}
	f = nil
	_ = f
	deadline := time.Now().Add(5 * time.Second)
	for {
		runtime.GC()
		if _, ok := interned.Load(k); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("an unused filter is still in the table")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
