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

func Start(body io.ReadCloser) *Stream {
	s := &Stream{
		Body:   body,
		Events: make(chan Event, 1024),
	}
	go s.read()
	return s
}

func (s *Stream) read() {
	defer close(s.Events)
	sc := bufio.NewScanner(s.Body)
	sc.Buffer(make([]byte, 0, 4*1024), 1*1024*1024)
	var cur Event
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if cur.Name != "" || len(cur.Data) > 0 {
				s.Events <- cur
			}
			cur = Event{}
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			cur.Name = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "id:"):
			cur.ID = strings.TrimSpace(line[len("id:"):])
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if len(cur.Data) > 0 {
				cur.Data = append(cur.Data, '\n')
			}
			cur.Data = append(cur.Data, payload...)
		}
	}
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
	sc := bufio.NewScanner(bytes.NewReader(raw))
	var cur Event
	flush := func() {
		if cur.Name != "" || len(cur.Data) > 0 {
			out = append(out, cur)
		}
		cur = Event{}
	}
	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			flush()
		case strings.HasPrefix(line, ":"):
		case strings.HasPrefix(line, "event:"):
			cur.Name = strings.TrimSpace(line[len("event:"):])
		case strings.HasPrefix(line, "id:"):
			cur.ID = strings.TrimSpace(line[len("id:"):])
		case strings.HasPrefix(line, "data:"):
			payload := strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " ")
			if len(cur.Data) > 0 {
				cur.Data = append(cur.Data, '\n')
			}
			cur.Data = append(cur.Data, payload...)
		}
	}
	flush()
	return out
}
