package hold

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// A /token (Replace) while the reader checks holds on a busy table back to
// back. Holds on room_id alone, 250 members with 7 each, so every member's
// update is checked against 1 750 holds whatever column is chosen. Reports the
// latency of Replace: before hold checks left the index lock, it waited for
// whichever check held it.
func BenchmarkReplaceDuringHoldChecks(b *testing.B) {
	rel := roomMembersRel()
	x := New()
	for m := range 250 {
		for s := range 7 {
			if !x.Add(fmt.Sprintf("m%d", m), fmt.Sprintf("s%d", s), []Spec{specOf(b, rel, "room_id=eq.1")}) {
				b.Fatal("add")
			}
		}
	}
	member := row{"room_id": expr.Text("1"), "user_id": expr.Text("m0"), "last_message_at": expr.Text("2026-10-10 00:00:00+00")}
	stop, done := make(chan struct{}), make(chan struct{})
	checks := 0
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				x.OnChange(rel.OID, 'U', member, member)
				checks++
			}
		}
	}()
	spec := specOf(b, rel, "room_id=eq.2,user_id=eq.r")
	lat := make([]time.Duration, 0, b.N)
	b.ResetTimer()
	for range b.N {
		t0 := time.Now()
		x.Replace("r", "s0", []Spec{spec})
		lat = append(lat, time.Since(t0))
	}
	b.StopTimer()
	close(stop)
	<-done
	slices.Sort(lat)
	q := func(p float64) float64 { return float64(lat[int(p*float64(len(lat)-1))].Nanoseconds()) / 1e3 }
	b.ReportMetric(q(0.5), "replace-p50-µs")
	b.ReportMetric(q(0.99), "replace-p99-µs")
	b.ReportMetric(q(0.999), "replace-p99.9-µs")
	b.ReportMetric(q(1), "replace-max-µs")
	slow := 0
	for _, d := range lat {
		if d > 100*time.Microsecond {
			slow++
		}
	}
	b.ReportMetric(100*float64(slow)/float64(len(lat)), "%>100µs")
	b.ReportMetric(float64(checks)/float64(b.N), "checks/replace")
}
