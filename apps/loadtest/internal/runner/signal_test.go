package runner

import "testing"

func TestApplyPresenceStateAndDiff(t *testing.T) {
	r := map[string]struct{}{}
	applyPresence(r, []byte(`{"type":"state","members":{"a":{},"b":{}}}`))
	if len(r) != 2 {
		t.Fatalf("state size %d", len(r))
	}
	applyPresence(r, []byte(`{"type":"diff","joins":{"c":{}},"leaves":{"a":{}}}`))
	if _, ok := r["a"]; ok {
		t.Fatal("a should have left")
	}
	if _, ok := r["c"]; !ok {
		t.Fatal("c should have joined")
	}
	if len(r) != 2 {
		t.Fatalf("after diff size %d", len(r))
	}
}

func TestBroadcastMeta(t *testing.T) {
	origin, seq, ok := broadcastMeta([]byte(`{"origin":"database","payload":{"n":7}}`))
	if !ok || origin != "database" || seq != 7 {
		t.Fatalf("got %s %d %v", origin, seq, ok)
	}
	origin, seq, ok = broadcastMeta([]byte(`{"origin":"client","payload":{}}`))
	if origin != "client" || ok || seq != 0 {
		t.Fatalf("client origin %s seq=%d ok=%v", origin, seq, ok)
	}
}

func TestErrorCode(t *testing.T) {
	if errorCode([]byte(`{"code":"shape_not_authorized","message":"x"}`)) != "shape_not_authorized" {
		t.Fatal("kick code")
	}
	if errorCode([]byte(`{"code":"stream_lagging"}`)) != "stream_lagging" {
		t.Fatal("lag code")
	}
}

func TestFloorMin(t *testing.T) {
	if floorMin(0, 0.9) != 0 || floorMin(1, 0.9) != 1 || floorMin(100, 0.9) != 90 {
		t.Fatalf("floorMin")
	}
}

func TestSubsAccepted(t *testing.T) {
	if subsAccepted(&readyEvent{}) {
		t.Fatal("empty")
	}
	if !subsAccepted(&readyEvent{Subscriptions: []readySub{{OK: true}, {OK: true}}}) {
		t.Fatal("all ok")
	}
	if subsAccepted(&readyEvent{Subscriptions: []readySub{{OK: true}, {OK: false}}}) {
		t.Fatal("second denied")
	}
}
