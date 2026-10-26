package server

import (
	"strings"
	"testing"
)

func slotAt(retained, unconfirmed, safe int64) slotStatus {
	return slotStatus{retained: retained, unconfirmed: unconfirmed, safe: &safe}
}

// The slot is reported from half of what max_slot_wal_keep_size lets it keep
// and again from 80%, and each level is left 10 points below where it starts.
// A share hovering on a threshold is reported once.
func TestRetentionLevels(t *testing.T) {
	prev := &retention{}
	for _, step := range []struct {
		retained int64 // of 100
		level    int
	}{
		{30, 0}, {50, 1}, {45, 1}, {79, 1}, {80, 2}, {75, 2}, {69, 1}, {41, 1}, {39, 0}, {120, 2},
	} {
		r := assessRetention(slotAt(step.retained, step.retained, 100-step.retained), prev)
		if r.level != step.level {
			t.Fatalf("at %d%% after level %d: level %d, want %d", step.retained, prev.level, r.level, step.level)
		}
		prev = r
	}
	if r := assessRetention(slotStatus{retained: 1 << 40}, nil); r.level != 0 {
		t.Fatalf("a slot without a bound is level %d, want 0", r.level)
	}
}

// The slot retains WAL from its restart point. When Sluice has confirmed most
// of it, an open transaction holds that point back, and the warning names it;
// otherwise Sluice is behind.
func TestRetentionSaysWhatHoldsTheWAL(t *testing.T) {
	held := assessRetention(slotAt(60, 1, 40), nil)
	held.holder = "pid 90, open for 4m0s"
	d := held.diagnostic()
	if !held.byTransaction || d.Severity != "medium" || !strings.Contains(d.Reason, "pid 90") || !strings.Contains(d.Remedy, "pg_terminate_backend") {
		t.Fatalf("held by a transaction: %+v", d)
	}

	behind := assessRetention(slotAt(85<<20, 80<<20, 15<<20), nil)
	d = behind.diagnostic()
	if behind.byTransaction || d.Severity != "high" || !strings.Contains(d.Reason, "last 80 MiB") {
		t.Fatalf("Sluice behind: %+v", d)
	}

	ended := assessRetention(slotAt(60, 1, 40), nil)
	if d := ended.diagnostic(); !strings.Contains(d.Reason, "none open any more") {
		t.Fatalf("held by a transaction that has ended: %+v", d)
	}
}
