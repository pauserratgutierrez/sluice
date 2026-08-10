package hub

import "time"

// bucket is a token bucket with a burst equal to one second of its rate.
type bucket struct {
	tokens float64
	last   time.Time
}

// Allow reports whether one control request of the given kind may proceed.
//
// The budget is per stream, not per node, because the thing being protected is
// the work a single client can make the server do: a subscribe re-resolves
// authorization, a publish fans out to every member of a channel, and a
// presence update schedules a broadcast. A per-node limit would let one client
// spend everybody else's budget.
//
// A rate of zero or less disables the limit, which is what makes this safe to
// call unconditionally from the handlers.
func (s *Stream) Allow(kind string, perSecond int) bool {
	if perSecond <= 0 {
		return true
	}
	burst := float64(perSecond)
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.buckets == nil {
		s.buckets = map[string]*bucket{}
	}
	b := s.buckets[kind]
	if b == nil {
		b = &bucket{tokens: burst, last: now}
		s.buckets[kind] = b
	}
	b.tokens += now.Sub(b.last).Seconds() * burst
	b.last = now
	if b.tokens > burst {
		b.tokens = burst
	}
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
