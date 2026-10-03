package runner

import "testing"

func TestFillResources(t *testing.T) {
	before := parseProm("go_memstats_heap_inuse_bytes 10485760\ngo_memstats_stack_inuse_bytes 1048576\ngo_goroutines 20\nprocess_cpu_seconds_total 1.5\nprocess_resident_memory_bytes 31457280\n")
	opened := parseProm("go_memstats_heap_inuse_bytes 20971520\ngo_memstats_stack_inuse_bytes 3145728\ngo_goroutines 2020\n")
	end := parseProm("go_memstats_heap_inuse_bytes 15728640\ngo_memstats_stack_inuse_bytes 3145728\ngo_goroutines 2020\nprocess_cpu_seconds_total 3.5\nprocess_resident_memory_bytes 52428800\n" +
		`go_gc_duration_seconds{quantile="1"} 0.0025` + "\n")

	r := &resources{}
	for _, m := range []map[string]float64{before, opened, end} {
		r.observe(m)
	}
	st := Step{Opened: 1000, Received: 4000}
	fillResources(&st, r, before, before, opened, end)

	// (23 MiB - 11 MiB) over 1000 streams.
	if got, want := st.KiBPerStream, 12.0*1024/1000; got != want {
		t.Errorf("KiB/stream = %v, want %v", got, want)
	}
	if st.GoroutinesPerStream != 2 {
		t.Errorf("goroutines/stream = %v, want 2", st.GoroutinesPerStream)
	}
	if st.LivePeakMiB != 23 || st.RSSPeakMiB != 50 {
		t.Errorf("peaks = %v MiB live, %v MiB RSS", st.LivePeakMiB, st.RSSPeakMiB)
	}
	if st.CPUSeconds != 2 || st.CPUMsPer1kEvents != 500 {
		t.Errorf("cpu = %v s, %v ms/1k events", st.CPUSeconds, st.CPUMsPer1kEvents)
	}
	if st.GCPauseMax != "2.5ms" {
		t.Errorf("gc pause = %q", st.GCPauseMax)
	}
}
