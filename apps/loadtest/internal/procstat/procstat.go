// Package procstat reads this process's CPU time and resident memory from
// /proc, so the runner and the issuer can report whether they were the limit.
package procstat

import (
	"os"
	"strconv"
	"strings"
)

// clockTicks is USER_HZ, which is 100 on every Linux the containers run.
const clockTicks = 100

// Self returns the CPU time (user + system) this process has used, and its
// resident set size. Both are zero where /proc is not available.
func Self() (cpuSeconds float64, rssBytes int64) {
	if b, err := os.ReadFile("/proc/self/stat"); err == nil {
		s := string(b)
		// The command name may contain spaces; the fields after it do not.
		if i := strings.LastIndexByte(s, ')'); i >= 0 {
			f := strings.Fields(s[i+1:])
			if len(f) > 12 {
				ut, _ := strconv.ParseFloat(f[11], 64)
				st, _ := strconv.ParseFloat(f[12], 64)
				cpuSeconds = (ut + st) / clockTicks
			}
		}
	}
	if b, err := os.ReadFile("/proc/self/status"); err == nil {
		for _, line := range strings.Split(string(b), "\n") {
			if v, ok := strings.CutPrefix(line, "VmRSS:"); ok {
				if f := strings.Fields(v); len(f) > 0 {
					kb, _ := strconv.ParseInt(f[0], 10, 64)
					rssBytes = kb * 1024
				}
			}
		}
	}
	return cpuSeconds, rssBytes
}

// CPUModel is the first "model name" in /proc/cpuinfo.
func CPUModel() string {
	b, err := os.ReadFile("/proc/cpuinfo")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(b), "\n") {
		if k, v, ok := strings.Cut(line, ":"); ok && strings.TrimSpace(k) == "model name" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// Kernel is the running kernel's release.
func Kernel() string {
	b, err := os.ReadFile("/proc/sys/kernel/osrelease")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(b))
}
