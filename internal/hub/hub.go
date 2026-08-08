// Package hub owns live streams and the in-process fan-out.
//
// Backpressure policy is per plane and explicit, because the right answer
// differs. A silently truncated change stream is worse than a closed one, so a
// lagging change consumer is disconnected and told to resnapshot. Broadcast is
// documented as best-effort, so it drops oldest. Presence is coalescible, so a
// newer diff supersedes an older one.
package hub

import (
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
)

// Stream is one live SSE connection.
type Stream struct {
	id       string
	identity atomic.Pointer[authz.Identity]

	queue chan event.Event
	done  chan struct{}

	closeOnce sync.Once
	closeCode atomic.Pointer[string]

	dropped atomic.Int64
	created time.Time

	// channels this stream subscribes to on the signalling plane:
	// channel name -> subscription label
	mu       sync.RWMutex
	channels map[string]string
}

func (s *Stream) StreamID() string { return s.id }

func (s *Stream) Identity() authz.Identity {
	if p := s.identity.Load(); p != nil {
		return *p
	}
	return authz.Identity{}
}

// SetIdentity rebinds the stream after a token refresh. The caller is
// responsible for having verified that the new token names the same subject.
func (s *Stream) SetIdentity(id authz.Identity) { s.identity.Store(&id) }

// Send enqueues an event. Returns false when the event was dropped or the stream
// is closed.
func (s *Stream) Send(ev event.Event) bool {
	select {
	case <-s.done:
		return false
	default:
	}
	select {
	case s.queue <- ev:
		return true
	default:
	}

	// Queue full. Apply the per-plane policy.
	switch ev.Kind {
	case event.KindChange:
		s.dropped.Add(1)
		s.CloseWith("stream_lagging")
		return false
	case event.KindPresence:
		// Coalesce: discard one older presence event to make room. A newer
		// snapshot or diff carries strictly more recent truth.
		select {
		case old := <-s.queue:
			if old.Kind != event.KindPresence {
				// Do not sacrifice a change or an error to make room for
				// presence; put it back and drop the presence event instead.
				select {
				case s.queue <- old:
				default:
				}
				s.dropped.Add(1)
				return false
			}
		default:
		}
		select {
		case s.queue <- ev:
			return true
		default:
			s.dropped.Add(1)
			return false
		}
	default:
		s.dropped.Add(1)
		return false
	}
}

// Events is the channel the SSE writer reads.
func (s *Stream) Events() <-chan event.Event { return s.queue }

// Done closes when the stream is finished.
func (s *Stream) Done() <-chan struct{} { return s.done }

// CloseWith terminates the stream, recording a reason for metrics.
func (s *Stream) CloseWith(code string) {
	s.closeOnce.Do(func() {
		c := code
		s.closeCode.Store(&c)
		close(s.done)
	})
}

// CloseCode returns why the stream ended, or "" if it is still open.
func (s *Stream) CloseCode() string {
	if p := s.closeCode.Load(); p != nil {
		return *p
	}
	return ""
}

func (s *Stream) Dropped() int64 { return s.dropped.Load() }

// TrackChannel records a signalling-plane subscription.
func (s *Stream) TrackChannel(channel, label string) {
	s.mu.Lock()
	s.channels[channel] = label
	s.mu.Unlock()
}

func (s *Stream) UntrackChannel(channel string) {
	s.mu.Lock()
	delete(s.channels, channel)
	s.mu.Unlock()
}

// ChannelLabel returns the subscription label for a channel, if subscribed.
func (s *Stream) ChannelLabel(channel string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	l, ok := s.channels[channel]
	return l, ok
}

func (s *Stream) Channels() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.channels))
	for c := range s.channels {
		out = append(out, c)
	}
	return out
}

// Hub holds every stream and the signalling-plane state.
type Hub struct {
	queueSize int

	mu      sync.RWMutex
	streams map[string]*Stream
	// channel -> set of streams, sharded only by map access for now; a
	// single-node hub at 100k streams does not need more, and the seam for a
	// sharded implementation is this type's method set.
	byChannel map[string]map[string]*Stream

	presence *Presence
	rings    *Rings
}

func New(queueSize, ringEvents int, ringMaxAge, presenceTick time.Duration) *Hub {
	h := &Hub{
		queueSize: queueSize,
		streams:   map[string]*Stream{},
		byChannel: map[string]map[string]*Stream{},
		rings:     NewRings(ringEvents, ringMaxAge),
	}
	h.presence = NewPresence(h, presenceTick)
	return h
}

func (h *Hub) Rings() *Rings       { return h.rings }
func (h *Hub) Presence() *Presence { return h.presence }

// Open registers a new stream.
func (h *Hub) Open(id string, identity authz.Identity) *Stream {
	s := &Stream{
		id:       id,
		queue:    make(chan event.Event, h.queueSize),
		done:     make(chan struct{}),
		created:  time.Now(),
		channels: map[string]string{},
	}
	s.identity.Store(&identity)
	h.mu.Lock()
	h.streams[id] = s
	h.mu.Unlock()
	return s
}

// Close removes a stream and everything it held.
func (h *Hub) Close(id, code string) {
	h.mu.Lock()
	s := h.streams[id]
	delete(h.streams, id)
	if s != nil {
		for ch := range s.channels {
			if set := h.byChannel[ch]; set != nil {
				delete(set, id)
				if len(set) == 0 {
					delete(h.byChannel, ch)
				}
			}
		}
	}
	h.mu.Unlock()
	if s == nil {
		return
	}
	// Presence leave is driven by the transport close, with no client message
	// required. Borrowed from MCP's "closing the stream is the cancellation" rule.
	h.presence.RemoveStream(id)
	s.CloseWith(code)
}

func (h *Hub) Get(id string) (*Stream, bool) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	s, ok := h.streams[id]
	return s, ok
}

func (h *Hub) Count() int {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return len(h.streams)
}

// Streams returns a snapshot for iteration.
func (h *Hub) Streams() []*Stream {
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]*Stream, 0, len(h.streams))
	for _, s := range h.streams {
		out = append(out, s)
	}
	return out
}

// JoinChannel adds a stream to a channel's fan-out set.
func (h *Hub) JoinChannel(channel string, s *Stream, label string) {
	h.mu.Lock()
	set := h.byChannel[channel]
	if set == nil {
		set = map[string]*Stream{}
		h.byChannel[channel] = set
	}
	set[s.id] = s
	h.mu.Unlock()
	s.TrackChannel(channel, label)
}

// LeaveChannel removes a stream from a channel.
func (h *Hub) LeaveChannel(channel string, s *Stream) {
	h.mu.Lock()
	if set := h.byChannel[channel]; set != nil {
		delete(set, s.id)
		if len(set) == 0 {
			delete(h.byChannel, channel)
		}
	}
	h.mu.Unlock()
	s.UntrackChannel(channel)
	h.presence.Untrack(channel, s.id)
}

// ChannelMembers returns the streams subscribed to a channel.
func (h *Hub) ChannelMembers(channel string) []*Stream {
	h.mu.RLock()
	defer h.mu.RUnlock()
	set := h.byChannel[channel]
	out := make([]*Stream, 0, len(set))
	for _, s := range set {
		out = append(out, s)
	}
	return out
}

// PublishBroadcast fans a message out to a channel.
//
// Unlike Supabase, there is no mode in which this silently vanishes: the caller
// is an HTTP request that receives a synchronous 403 or 413 when the channel or
// payload is rejected, and the delivered count is returned so the caller knows
// what happened.
func (h *Hub) PublishBroadcast(channel, evName, from, origin, commitLSN string, payload json.RawMessage, includeSelf bool, selfID string) int {
	members := h.ChannelMembers(channel)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	n := 0
	for _, s := range members {
		if !includeSelf && s.id == selfID {
			continue
		}
		label, ok := s.ChannelLabel(channel)
		if !ok {
			continue
		}
		if s.Send(event.Event{Kind: event.KindBroadcast, Data: event.Broadcast{
			Sub: label, Channel: channel, Event: evName, Payload: payload,
			From: from, Origin: origin, CommitLSN: commitLSN, At: now,
		}}) {
			n++
		}
	}
	return n
}

// SendError delivers an error event to a stream.
func SendError(s *Stream, e event.Error) {
	s.Send(event.Event{Kind: event.KindError, Data: e})
}

// SendWarning delivers a warning event to a stream.
func SendWarning(s *Stream, w event.Warning) {
	s.Send(event.Event{Kind: event.KindWarning, Data: w})
}
