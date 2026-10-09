package sse

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"
)

type Event struct {
	Name string
	ID   string
	Data []byte
}

type Stream struct {
	Body   io.ReadCloser
	Events chan Event

	closeOnce sync.Once
}

// DefaultBuffer is how many parsed events a Stream holds for its consumer. A
// Stream's goroutine and its consumer run side by side, so a few are enough;
// each slot costs 56 bytes on every connection the runner holds, which at
// 1024 was 57 KiB per stream before a single event arrived.
const DefaultBuffer = 16

// Start parses body on its own goroutine into a channel of DefaultBuffer events.
func Start(body io.ReadCloser) *Stream { return StartN(body, DefaultBuffer) }

// StartN is Start with a channel of n events.
func StartN(body io.ReadCloser, n int) *Stream {
	s := &Stream{
		Body:   body,
		Events: make(chan Event, max(n, 0)),
	}
	go s.read()
	return s
}

func (s *Stream) read() {
	defer close(s.Events)
	_ = Read(s.Body, func(ev Event) bool {
		// Read reuses ev.Data for the next event.
		ev.Data = append([]byte(nil), ev.Data...)
		s.Events <- ev
		return true
	})
}

// Read parses body on the calling goroutine and calls fn for every event,
// until the body ends, fails, or fn returns false. ev.Data is only valid
// during the call. It returns the error that ended the body, if any.
func Read(body io.Reader, fn func(ev Event) bool) error {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 4*1024), 1*1024*1024)
	var cur Event
	for sc.Scan() {
		line := sc.Bytes()
		switch {
		case len(line) == 0:
			if cur.Name != "" || len(cur.Data) > 0 {
				if !fn(cur) {
					return nil
				}
			}
			cur = Event{Data: cur.Data[:0]}
		case line[0] == ':':
		case bytes.HasPrefix(line, []byte("event:")):
			cur.Name = strings.TrimSpace(string(line[len("event:"):]))
		case bytes.HasPrefix(line, []byte("id:")):
			cur.ID = strings.TrimSpace(string(line[len("id:"):]))
		case bytes.HasPrefix(line, []byte("data:")):
			payload := bytes.TrimPrefix(line[len("data:"):], []byte(" "))
			if len(cur.Data) > 0 {
				cur.Data = append(cur.Data, '\n')
			}
			cur.Data = append(cur.Data, payload...)
		}
	}
	return sc.Err()
}

func (s *Stream) Next(d time.Duration) (Event, error) {
	select {
	case ev, ok := <-s.Events:
		if !ok {
			return Event{}, fmt.Errorf("sse: closed")
		}
		return ev, nil
	case <-time.After(d):
		return Event{}, fmt.Errorf("sse: timeout")
	}
}

func (s *Stream) Close() {
	s.closeOnce.Do(func() {
		_ = s.Body.Close()
	})
}

// Parse is used by tests; it consumes a complete SSE buffer.
func Parse(raw []byte) []Event {
	var out []Event
	_ = Read(bytes.NewReader(raw), func(ev Event) bool {
		ev.Data = append([]byte(nil), ev.Data...)
		out = append(out, ev)
		return true
	})
	return out
}
