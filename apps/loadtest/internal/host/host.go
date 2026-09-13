package host

import (
	"os"
	"runtime"
	"strconv"
	"strings"
)

type Resources struct {
	CPUs        float64 `json:"cpus"`
	MemoryBytes int64   `json:"memory_bytes"`
	GoMaxProcs  int     `json:"gomaxprocs"`
}

func Detect() Resources {
	r := Resources{
		CPUs:       float64(runtime.NumCPU()),
		GoMaxProcs: runtime.GOMAXPROCS(0),
	}
	if c := cgroupCPUs(); c > 0 {
		r.CPUs = c
	}
	if m := cgroupMemory(); m > 0 {
		r.MemoryBytes = m
	} else {
		r.MemoryBytes = procMemTotal()
	}
	return r
}

// StreamCap is a conservative ceiling for concurrent SSE connections on this
// container: half of visible memory at ~80 KiB/conn, 1000 conns per CPU, and
// an absolute 40k so a laptop Docker VM is not frozen.
func StreamCap(r Resources) int {
	const perConn = 80 * 1024
	byMem := int(r.MemoryBytes / perConn * 50 / 100)
	byCPU := int(r.CPUs * 1000)
	n := min(byMem, byCPU, 40000)
	if n < 100 {
		n = 100
	}
	return n
}

func cgroupMemory() int64 {
	for _, p := range []string{
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes",
	} {
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		s := strings.TrimSpace(string(b))
		if s == "" || s == "max" {
			continue
		}
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil || n <= 0 || n >= 1<<62 {
			continue
		}
		return n
	}
	return 0
}

func cgroupCPUs() float64 {
	b, err := os.ReadFile("/sys/fs/cgroup/cpu.max")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(b))
	if len(fields) != 2 || fields[0] == "max" {
		return 0
	}
	quota, err1 := strconv.ParseFloat(fields[0], 64)
	period, err2 := strconv.ParseFloat(fields[1], 64)
	if err1 != nil || err2 != nil || period <= 0 {
		return 0
	}
	return quota / period
}

func procMemTotal() int64 {
	b, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0
	}
	for _, line := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(line, "MemTotal:") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 2 {
			return 0
		}
		n, _ := strconv.ParseInt(fields[1], 10, 64)
		return n * 1024
	}
	return 0
}
