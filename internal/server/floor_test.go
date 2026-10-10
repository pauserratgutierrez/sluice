package server

import (
	"runtime"
	rtmetrics "runtime/metrics"
	"testing"
)

// The floor holds the live heap and in-use stacks, and leaves out free and
// unused heap: it is at least the live heap and at most what the runtime holds.
func TestMemoryFloorIsLiveHeapPlusRuntime(t *testing.T) {
	runtime.GC()
	floor, gcs := readLiveHeap()
	s := []rtmetrics.Sample{{Name: "/gc/heap/live:bytes"}, {Name: "/memory/classes/total:bytes"}}
	rtmetrics.Read(s)
	live, total := s[0].Value.Uint64(), s[1].Value.Uint64()
	if gcs == 0 || floor < live || floor > total {
		t.Fatalf("floor %d, live %d, total %d, gcs %d", floor, live, total, gcs)
	}
}
