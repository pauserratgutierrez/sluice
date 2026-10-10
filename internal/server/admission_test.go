package server

import (
	"io"
	"math"
	"net/http"
	"strings"
	"testing"
)

// fakeMemory is a memory limit and a live heap that only changes when a
// collection is said to have run.
type fakeMemory struct {
	limit     int64
	live, gcs uint64
}

func (m *fakeMemory) admission(threshold float64) *admission {
	a := newAdmission(threshold)
	a.limit = func() int64 { return m.limit }
	a.read = func() (uint64, uint64) { return m.live, m.gcs }
	return a
}

func (m *fakeMemory) collect(live uint64) { m.live, m.gcs = live, m.gcs+1 }

const mib = 1 << 20

func TestAdmissionRefusesNearTheLimitAndResumesBelowIt(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib}
	a := m.admission(0.85)

	m.collect(500 * mib) // 1 000 streams at 0.5 MiB each
	if ok, share := a.admit(1000); !ok || share != 0.5 {
		t.Fatalf("at half the limit: admit %v, share %.2f", ok, share)
	}
	m.collect(860 * mib)
	if ok, _ := a.admit(1720); ok {
		t.Fatal("at 86% of the limit a new stream must be refused")
	}
	// Hysteresis: refusing goes on until the projection is 5 points lower.
	m.collect(820 * mib)
	if ok, _ := a.admit(1640); ok {
		t.Fatal("at 82%, within the hysteresis, streams must still be refused")
	}
	m.collect(790 * mib)
	if ok, _ := a.admit(1580); !ok {
		t.Fatal("at 79% streams must be admitted again")
	}
}

// Between two collections the live heap does not move. A storm of opens is
// projected at the live heap per stream measured at the last one, so it is
// refused before the next collection shows it.
func TestAdmissionProjectsStreamsOpenedSinceTheLastCollection(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib}
	a := m.admission(0.85)
	m.collect(400 * mib)
	if ok, _ := a.admit(1000); !ok { // 0.4 MiB per stream
		t.Fatal("first stream refused")
	}
	// 1 100 more opens without a collection: 400 + 1100*0.4 = 840 MiB.
	if ok, share := a.admit(2100); !ok || share < 0.83 {
		t.Fatalf("2 100 streams: admit %v, share %.3f", ok, share)
	}
	if ok, share := a.admit(2130); ok {
		t.Fatalf("2 130 streams project to %.3f of the limit and must be refused", share)
	}
}

func TestAdmissionWithoutALimitAdmitsEverything(t *testing.T) {
	m := &fakeMemory{limit: math.MaxInt64}
	a := m.admission(0.85)
	m.collect(1 << 40)
	if ok, _ := a.admit(1_000_000); !ok {
		t.Fatal("with no memory limit, admission must not refuse")
	}
	off := (&fakeMemory{limit: 1000 * mib}).admission(0)
	if ok, _ := off.admit(1_000_000); !ok {
		t.Fatal("threshold 0 turns automatic admission off")
	}
}

// Opening a stream near the memory limit is refused with 503
// too_many_streams and a Retry-After; below it, it opens; a fixed
// SLUICE_MAX_STREAMS still applies on its own.
func TestStreamOpenIsRefusedNearTheMemoryLimit(t *testing.T) {
	s, srv, token := streamServer(t)
	s.cfg.MaxStreams = 0
	m := &fakeMemory{limit: 1000 * mib}
	s.admission = m.admission(0.85)

	m.collect(900 * mib)
	res := postStream(t, srv, token)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "too_many_streams") {
		t.Fatalf("near the limit: %d %s", res.StatusCode, body)
	}
	if ra := res.Header.Get("Retry-After"); ra < "1" || ra > "5" || len(ra) != 1 {
		t.Fatalf("Retry-After = %q, want 1-5 seconds", ra)
	}

	m.collect(100 * mib)
	if res := postStream(t, srv, token); res.StatusCode != http.StatusOK {
		t.Fatalf("well below the limit: %d", res.StatusCode)
	}

	s.cfg.MaxStreams = 1 // one stream is open now
	res = postStream(t, srv, token)
	body, _ = io.ReadAll(res.Body)
	if res.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "configured stream limit") {
		t.Fatalf("at SLUICE_MAX_STREAMS: %d %s", res.StatusCode, body)
	}
}
