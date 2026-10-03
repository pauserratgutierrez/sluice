package main

import (
	"log/slog"
	"os"
	"runtime/debug"
	"strconv"
	"strings"
)

// memoryLimitShare is the fraction of the container's memory limit given to the
// Go heap. The rest covers goroutine stacks, socket buffers and the runtime.
const memoryLimitShare = 0.9

// cgroupMemoryFiles are the container memory limit under cgroup v2 and v1.
var cgroupMemoryFiles = []string{
	"/sys/fs/cgroup/memory.max",
	"/sys/fs/cgroup/memory/memory.limit_in_bytes",
}

// applyMemoryLimit sets the Go runtime's soft memory limit from the container's,
// unless GOMEMLIMIT is set.
//
// The runtime sizes GOMAXPROCS from the cgroup CPU quota but knows nothing of
// its memory limit, so by default the heap may grow to twice its live size
// before a collection. A burst of streams can then cross the limit and get the
// process OOM-killed while plenty of that memory was garbage.
func applyMemoryLimit(log *slog.Logger) {
	if os.Getenv("GOMEMLIMIT") != "" {
		return
	}
	for _, path := range cgroupMemoryFiles {
		b, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		limit, ok := parseCgroupMemory(string(b))
		if !ok {
			continue
		}
		soft := int64(float64(limit) * memoryLimitShare)
		debug.SetMemoryLimit(soft)
		log.Info("memory limit from the container", "cgroup_bytes", limit, "gomemlimit_bytes", soft)
		return
	}
}

// parseCgroupMemory reads a cgroup memory limit. "max" (v2) and the near-2^63
// sentinel (v1) both mean unlimited.
func parseCgroupMemory(s string) (int64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "max" {
		return 0, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n <= 0 || n >= 1<<62 {
		return 0, false
	}
	return n, true
}
