package sse

import (
	"io"
	"strings"
	"testing"
	"time"
)

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

func TestReadStopsWhenAsked(t *testing.T) {
	raw := "event: a\ndata: 1\n\nevent: b\ndata: 2\n\n"
	n := 0
	_ = Read(strings.NewReader(raw), func(Event) bool { n++; return false })
	if n != 1 {
		t.Fatalf("callbacks = %d, want 1", n)
	}
}

// The channel form copies each event's data, since Read reuses its buffer.
func TestStartKeepsData(t *testing.T) {
	raw := "event: a\ndata: first\n\nevent: b\ndata: second\n\n"
	s := StartN(io.NopCloser(strings.NewReader(raw)), 0)
	a, err := s.Next(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Next(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(a.Data) != "first" || string(b.Data) != "second" {
		t.Fatalf("%q %q", a.Data, b.Data)
	}
}
