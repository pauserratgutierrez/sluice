package reader

import "testing"

// A slot is gone when PostgreSQL says it can no longer stream from its
// confirmed position: it does not exist, it was invalidated for any reason, or
// the WAL it needs was removed. A slot merely close to its limit is not gone.
func TestSlotGone(t *testing.T) {
	for _, tc := range []struct {
		name string
		st   slotState
		gone bool
	}{
		{"healthy", slotState{exists: true, walStatus: "reserved"}, false},
		{"near the limit", slotState{exists: true, walStatus: "unreserved"}, false},
		{"missing", slotState{}, true},
		{"invalidated, PostgreSQL 17+", slotState{exists: true, walStatus: "lost", invalidation: "wal_removed"}, true},
		{"idle timeout", slotState{exists: true, invalidation: "idle_timeout"}, true},
		{"lost, PostgreSQL 16", slotState{exists: true, walStatus: "lost"}, true},
	} {
		if got := tc.st.gone() != ""; got != tc.gone {
			t.Errorf("%s: gone = %v (%q), want %v", tc.name, got, tc.st.gone(), tc.gone)
		}
	}
}

// Only an invalidated slot is ever replaced, only when allowed, and never one
// another process holds.
func TestSlotAction(t *testing.T) {
	healthy := slotState{exists: true, walStatus: "reserved"}
	lost := slotState{exists: true, walStatus: "lost", invalidation: "wal_removed"}
	held := lost
	held.active = true
	for _, tc := range []struct {
		name     string
		st       slotState
		recreate bool
		want     slotAction
	}{
		{"healthy", healthy, false, slotUse},
		{"healthy, recreate allowed", healthy, true, slotUse},
		{"missing", slotState{}, false, slotCreate},
		{"invalidated", lost, false, slotRefuse},
		{"invalidated, recreate allowed", lost, true, slotRecreate},
		{"invalidated and held by another process", held, true, slotRefuse},
	} {
		if got := tc.st.action(tc.recreate); got != tc.want {
			t.Errorf("%s: action = %d, want %d", tc.name, got, tc.want)
		}
	}
}
