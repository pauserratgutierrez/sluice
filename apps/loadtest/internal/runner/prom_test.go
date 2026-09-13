package runner

import "testing"

func TestParseProm(t *testing.T) {
	m := parseProm("# TYPE sluice_streams gauge\nsluice_streams 12\nsluice_wal_lag_bytes 3.5\n")
	if m["sluice_streams"] != 12 {
		t.Fatalf("%v", m)
	}
	if m["sluice_wal_lag_bytes"] != 3.5 {
		t.Fatalf("%v", m)
	}
}
