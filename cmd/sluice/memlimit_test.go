package main

import (
	"math"
	"runtime/debug"
	"testing"
)

func TestParseCgroupMemory(t *testing.T) {
	for _, c := range []struct {
		in   string
		want int64
		ok   bool
	}{
		{"536870912\n", 536870912, true},
		{"max\n", 0, false},
		{"9223372036854771712", 0, false}, // cgroup v1 "unlimited"
		{"", 0, false},
		{"garbage", 0, false},
	} {
		got, ok := parseCgroupMemory(c.in)
		if got != c.want || ok != c.ok {
			t.Errorf("parseCgroupMemory(%q) = %d, %v; want %d, %v", c.in, got, ok, c.want, c.ok)
		}
	}
}

// The resume buffer's budget is its share of the Go memory limit, a fixed
// fallback when no limit is known, and nothing when it is turned off.
func TestResumeBufferBudget(t *testing.T) {
	prev := debug.SetMemoryLimit(-1)
	t.Cleanup(func() { debug.SetMemoryLimit(prev) })

	debug.SetMemoryLimit(math.MaxInt64)
	if got := resumeBufferBytes(0.1); got != noLimitResumeBuffer {
		t.Errorf("without a limit: %d, want %d", got, noLimitResumeBuffer)
	}
	debug.SetMemoryLimit(460 << 20)
	if got := resumeBufferBytes(0.1); got != 46<<20 {
		t.Errorf("a tenth of 460 MiB: %d, want %d", got, 46<<20)
	}
	if got := resumeBufferBytes(0); got != 0 {
		t.Errorf("turned off: %d, want 0", got)
	}
}
