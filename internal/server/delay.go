package server

import (
	"context"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/event"
)

// watchDelay tells every stream when changes start and stop arriving late,
// checking the reader's lag once a second off the reader's goroutine. Without
// it a client cannot tell late changes from none: heartbeats come from their
// own timer and keep arriving while the reader is behind.
func (s *Server) watchDelay(ctx context.Context) {
	if s.reader == nil || s.cfg.LagDegraded <= 0 {
		return
	}
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.checkDelay(s.reader.Lag())
		}
	}
}

// checkDelay sends every stream a delay event when the lag reaches
// SLUICE_LAG_DEGRADED, and one with lag_ms 0 when it falls below half of it.
// The state changes before the streams are listed, so a stream opened at the
// same time either is listed or reads the new state in its ready (see
// delayMs); events queued before a ready are written after it, so the last
// word a client gets is the current one.
func (s *Server) checkDelay(lag time.Duration) {
	delayed := s.delayMs.Load() > 0
	var ms int64
	switch {
	case !delayed && lag >= s.cfg.LagDegraded:
		ms = max(1, lag.Milliseconds())
	case delayed && lag < s.cfg.LagDegraded/2:
	default:
		return
	}
	s.delayMs.Store(ms)
	for _, st := range s.hub.Streams() {
		st.Send(event.Event{Kind: event.KindDelay, Data: event.Delay{LagMs: ms}})
	}
}
