package main

import "testing"

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
