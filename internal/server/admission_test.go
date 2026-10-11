package server

import (
	"io"
	"math"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// fakeMemory is a memory limit and a live heap that only changes when a
// collection is said to have run.
type fakeMemory struct {
	limit     int64
	live, gcs uint64
	now       time.Time
	forced    atomic.Int64 // collections admission asked for
}

func (m *fakeMemory) admission(threshold float64) *admission {
	a := newAdmission(threshold)
	a.limit = func() int64 { return m.limit }
	a.read = func() (uint64, uint64) { return m.live, m.gcs }
	a.now = func() time.Time { return m.now }
	a.collect = func() { m.forced.Add(1) }
	return a
}

func (m *fakeMemory) collect(live uint64) { m.live, m.gcs = live, m.gcs+1 }

const mib = 1 << 20

func TestAdmissionRefusesNearTheLimitAndResumesBelowIt(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib}
	a := m.admission(0.85)

	m.collect(500 * mib) // 1 000 streams at 0.5 MiB each
	if ok, share := a.admit(1000, 0); !ok || share != 0.5 {
		t.Fatalf("at half the limit: admit %v, share %.2f", ok, share)
	}
	m.collect(860 * mib)
	if ok, _ := a.admit(1720, 0); ok {
		t.Fatal("at 86% of the limit a new stream must be refused")
	}
	// Hysteresis: refusing goes on until the projection is 5 points lower.
	m.collect(820 * mib)
	if ok, _ := a.admit(1640, 0); ok {
		t.Fatal("at 82%, within the hysteresis, streams must still be refused")
	}
	m.collect(790 * mib)
	if ok, _ := a.admit(1580, 0); !ok {
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
	if ok, _ := a.admit(1000, 0); !ok { // 0.4 MiB per stream
		t.Fatal("first stream refused")
	}
	// 1 100 more opens without a collection: 400 + 1100*0.4 = 840 MiB.
	if ok, share := a.admit(2100, 0); !ok || share < 0.83 {
		t.Fatalf("2 100 streams: admit %v, share %.3f", ok, share)
	}
	if ok, share := a.admit(2130, 0); ok {
		t.Fatalf("2 130 streams project to %.3f of the limit and must be refused", share)
	}
}

func TestAdmissionWithoutALimitAdmitsEverything(t *testing.T) {
	m := &fakeMemory{limit: math.MaxInt64}
	a := m.admission(0.85)
	m.collect(1 << 40)
	if ok, _ := a.admit(1_000_000, 0); !ok {
		t.Fatal("with no memory limit, admission must not refuse")
	}
	off := (&fakeMemory{limit: 1000 * mib}).admission(0)
	if ok, _ := off.admit(1_000_000, 0); !ok {
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
	if ra := res.Header.Get("Retry-After"); ra < "1" || ra > "5" || len(ra) != 1 {
		t.Fatalf("at SLUICE_MAX_STREAMS, Retry-After = %q, want 1-5 seconds", ra)
	}
}

// The opens of a storm all arrive before any is in the hub. Counted only by
// open streams, 5,000 of them would all pass at 3,000 streams: with the
// reservations each holds, admission stops near the threshold.
func TestAdmissionCountsOpensInProgress(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib}
	a := m.admission(0.5)
	m.collect(400 * mib) // 3,000 streams at 0.13 MiB each
	admitted := 0
	for pending := range 5000 {
		if ok, _ := a.admit(3000, pending); ok {
			admitted++
		}
	}
	// 100 MiB of room at 0.13 MiB per stream: about 750.
	if admitted < 700 || admitted > 800 {
		t.Fatalf("admitted %d of 5,000 simultaneous opens, want about 750", admitted)
	}
	if _, share := a.admit(3000, 5000); share < 1.0 {
		t.Fatalf("5,000 opens in progress project to %.2f of the limit, want above 1", share)
	}
}

// After a mass disconnect, the floor the last collection measured still holds
// the departed streams. The projection takes them off, so their reconnections
// are not refused until the next collection.
func TestAdmissionAfterAMassDisconnect(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib}
	a := m.admission(0.5)
	m.collect(520 * mib)
	if ok, share := a.admit(3000, 0); ok || share < 0.5 {
		t.Fatalf("full node: admit %v, share %.2f", ok, share)
	}
	if ok, share := a.admit(0, 0); !ok || share > 0.2 {
		t.Fatalf("every stream gone, no collection yet: admit %v, share %.2f; want admitted near the 20%% minimum", ok, share)
	}
}

// A reconnect storm's peak stops admission short of what fits, and the floor
// then settles between the threshold and the hysteresis. Admission opens again
// once the projection has stayed below the threshold for admissionHold; a
// projection back at the threshold starts the wait over.
func TestAdmissionResumesAfterAPeakSettlesWithinTheHysteresis(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib, now: time.Unix(1000, 0)}
	a := m.admission(0.5)
	m.collect(520 * mib) // the storm's peak, at 4,800 streams
	if ok, _ := a.admit(4800, 0); ok {
		t.Fatal("at the peak a new stream must be refused")
	}
	m.collect(470 * mib) // settled: 0.47, inside the hysteresis
	m.now = m.now.Add(admissionHold - time.Second)
	if ok, _ := a.admit(4800, 0); ok {
		t.Fatal("within admissionHold of the peak, streams must still be refused")
	}
	m.now = m.now.Add(time.Second)
	if ok, share := a.admit(4800, 0); !ok {
		t.Fatalf("below the threshold for admissionHold (share %.2f), streams must be admitted again", share)
	}
	if ok, _ := a.admit(5200, 0); ok { // projects to 0.509
		t.Fatal("back at the threshold, streams must be refused")
	}
	m.now = m.now.Add(admissionHold / 2)
	if ok, _ := a.admit(5000, 0); ok { // 0.49, but the threshold was reached since
		t.Fatal("reaching the threshold again must start the wait over")
	}
}

// Refusing with far fewer streams than the last collection saw asks for a
// collection, at most one every staleGCInterval.
func TestAdmissionForcesACollectionWhenItsFloorIsStale(t *testing.T) {
	m := &fakeMemory{limit: 1000 * mib, now: time.Unix(1000, 0)}
	a := m.admission(0.4)
	m.collect(520 * mib)
	a.admit(3000, 0) // refusing from here
	for range 10 {
		if ok, _ := a.admit(2600, 0); ok {
			t.Fatal("at 2,600 streams the projection (0.45) is still past 0.35")
		}
	}
	waitFor(t, func() bool { return m.forced.Load() == 1 })
	m.now = m.now.Add(staleGCInterval)
	a.admit(2600, 0)
	waitFor(t, func() bool { return m.forced.Load() == 2 })
	if a.admit(2950, 0); m.forced.Load() != 2 {
		t.Fatal("within 10% of the streams the last collection saw, no collection is forced")
	}
}

// SLUICE_MAX_STREAMS counts opens in progress too: with one open admitted and
// not yet in the hub, a cap of 1 refuses the next.
func TestMaxStreamsCountsOpensInProgress(t *testing.T) {
	s, srv, token := streamServer(t)
	s.cfg.MaxStreams = 1
	s.opening.Add(1)
	res := postStream(t, srv, token)
	body, _ := io.ReadAll(res.Body)
	if res.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(body), "configured stream limit") {
		t.Fatalf("an open in progress at a cap of 1: %d %s", res.StatusCode, body)
	}
	s.opening.Add(-1)
	if res := postStream(t, srv, token); res.StatusCode != http.StatusOK {
		t.Fatalf("no open in progress: %d", res.StatusCode)
	}
}

// A refused or failed open gives its reservation back; one that opened holds
// none, since the hub counts it.
func TestStreamOpensReleaseTheirReservations(t *testing.T) {
	s, srv, token := streamServer(t)
	s.cfg.MaxStreams = 0
	if res := postStream(t, srv, "not-a-token"); res.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad token: %d", res.StatusCode)
	}
	if n := s.opening.Load(); n != 0 {
		t.Fatalf("%d reservations held after a refused open", n)
	}
	if res := postStream(t, srv, token); res.StatusCode != http.StatusOK {
		t.Fatalf("open: %d", res.StatusCode)
	}
	if n := s.opening.Load(); n != 0 {
		t.Fatalf("%d reservations held by an open stream", n)
	}
}
