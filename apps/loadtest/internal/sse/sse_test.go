package sse

import "testing"

func TestParseNamedEvent(t *testing.T) {
	raw := []byte("event: ready\ndata: {\"ok\":true}\n\nevent: change\nid: 1\ndata: {\"op\":\"INSERT\"}\n\n: heartbeat\n\n")
	got := Parse(raw)
	if len(got) != 2 {
		t.Fatalf("events=%d want 2: %+v", len(got), got)
	}
	if got[0].Name != "ready" || string(got[0].Data) != `{"ok":true}` {
		t.Fatalf("ready = %+v", got[0])
	}
	if got[1].Name != "change" || got[1].ID != "1" {
		t.Fatalf("change = %+v", got[1])
	}
}
