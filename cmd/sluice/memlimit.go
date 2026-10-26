package main

import (
	"log/slog"
	"math"
	"os"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/pauserratgutierrez/sluice/internal/config"
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

// resumeBufferBytes is the resume buffer's budget: SLUICE_RING_MEMORY of the Go
// memory limit, or noLimitResumeBuffer when none is known; 0 when disabled.
func resumeBufferBytes(share float64) int {
	limit := debug.SetMemoryLimit(-1)
	switch {
	case share == 0:
		return 0
	case limit <= 0 || limit == math.MaxInt64:
		return noLimitResumeBuffer
	}
	return int(float64(limit) * share)
}

const noLimitResumeBuffer = 64 << 20

// logAdmission says how new streams will be admitted: refused when memory
// nears the Go memory limit (SLUICE_ADMIT_MEMORY), and at a fixed number of
// streams when SLUICE_MAX_STREAMS is set.
func logAdmission(log *slog.Logger, cfg *config.Config) {
	limit := debug.SetMemoryLimit(-1)
	switch {
	case cfg.AdmitMemory > 0 && limit > 0 && limit != math.MaxInt64:
		log.Info("stream admission", "refuse_from_memory_share", cfg.AdmitMemory,
			"gomemlimit_bytes", limit, "max_streams", cfg.MaxStreams)
	case cfg.MaxStreams > 0:
		log.Info("stream admission", "max_streams", cfg.MaxStreams,
			"memory", "no limit to measure against, or SLUICE_ADMIT_MEMORY=0")
	default:
		log.Warn("stream admission is unbounded: no memory limit is known and SLUICE_MAX_STREAMS is 0; set a container memory limit or GOMEMLIMIT, or SLUICE_MAX_STREAMS")
	}
}
