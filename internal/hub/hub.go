// Package hub owns live streams and the in-process fan-out.
//
// Each stream has one bounded queue, and the policy when it is full depends on
// the kind of event. A silently truncated change stream is worse than a closed
// one, so a change (or snapshot_end) that does not fit closes the stream as
// stream_lagging and the client resumes or resnapshots. Every other kind --
// broadcast, presence, warnings -- is best-effort and the new event is dropped.
// Nothing already queued is ever displaced, because that would reorder the
// stream.
//
// Snapshot rows and resume replays are the exception to closing: they are
// produced off the replication path, as fast as memory allows, so they wait for
// room instead (SendBackfill). They only ever fill half the queue, leaving the
// rest to live events, which never wait.
package hub

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
)

// Stream is one live SSE connection.
type Stream struct {
	id       string
	identity atomic.Pointer[authz.Identity]

	// The queue grows as events arrive, up to limit, so an idle stream holds no
	// buffer. A preallocated channel of the same capacity cost every stream
	// about 12 KiB whether or not anything was ever sent to it.
	qmu   sync.Mutex
	queue []event.Event
	limit int
	// ready is signalled when an event is queued; room when the writer takes
	// the queue. Both hold at most one pending signal.
	ready chan struct{}
	room  chan struct{}
	done  chan struct{}

	closeOnce sync.Once
	closeCode atomic.Pointer[string]

	created time.Time

	// channels this stream subscribes to on the signalling plane
	mu       sync.RWMutex
	channels map[string]channelJoin
	buckets  map[string]*bucket
}

type channelJoin struct {
	label string
	// recheckAt is when the join's authorization expires and must be asked
	// again. Zero means it never expires (public and owner namespaces).
	recheckAt time.Time
}

// DueChannel is a join whose authorization has expired.
type DueChannel struct {
	Channel, Label string
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
	queued, closed := s.push(ev, s.limit)
	if queued || closed {
		return queued
	}
	metrics.StreamDropped.WithLabelValues(string(ev.Kind)).Inc()
	if ev.Kind == event.KindChange || ev.Kind == event.KindSnapshotEnd {
		s.CloseWith("stream_lagging")
	}
	return false
}

// SendBackfill enqueues a snapshot row or a replayed change, waiting while the
// queue is half full rather than closing the stream. Returns false when the
// stream or ctx ended first.
func (s *Stream) SendBackfill(ctx context.Context, ev event.Event) bool {
	for {
		queued, closed := s.push(ev, max(1, s.limit/2))
		if queued || closed {
			return queued
		}
		select {
		case <-s.room:
		case <-s.done:
			return false
		case <-ctx.Done():
			return false
		}
	}
}

// push appends ev while the queue holds fewer than capacity events.
func (s *Stream) push(ev event.Event, capacity int) (queued, closed bool) {
	select {
	case <-s.done:
		return false, true
	default:
	}
	s.qmu.Lock()
	if len(s.queue) >= capacity {
		s.qmu.Unlock()
		return false, false
	}
	s.queue = append(s.queue, ev)
	s.qmu.Unlock()
	select {
	case s.ready <- struct{}{}:
	default:
	}
	return true, false
}

// Ready is signalled when events are waiting to be taken.
func (s *Stream) Ready() <-chan struct{} { return s.ready }

// Take removes every queued event, oldest first, and gives the stream spare to
// queue into next. Passing back the slice an earlier Take returned, once its
// events are written, keeps the stream to two buffers.
func (s *Stream) Take(spare []event.Event) []event.Event {
	s.qmu.Lock()
	q := s.queue
	s.queue = spare[:0]
	s.qmu.Unlock()
	if len(q) > 0 {
		select {
		case s.room <- struct{}{}:
		default:
		}
	}
	return q
}

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

// TrackChannel records a signalling-plane subscription.
func (s *Stream) TrackChannel(channel, label string, recheckAt time.Time) {
	s.mu.Lock()
	s.channels[channel] = channelJoin{label: label, recheckAt: recheckAt}
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
	j, ok := s.channels[channel]
	return j.label, ok
}

// DueChannels lists the joins whose authorization expired at or before now.
func (s *Stream) DueChannels(now time.Time) []DueChannel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []DueChannel
	for ch, j := range s.channels {
		if !j.recheckAt.IsZero() && !j.recheckAt.After(now) {
			out = append(out, DueChannel{Channel: ch, Label: j.label})
		}
	}
	return out
}

// SetChannelRecheck moves a join's expiry, if the channel is still joined
// under the same label. A join replaced in the meantime keeps its own.
func (s *Stream) SetChannelRecheck(channel, label string, at time.Time) {
	s.mu.Lock()
	if j, ok := s.channels[channel]; ok && j.label == label {
		j.recheckAt = at
		s.channels[channel] = j
	}
	s.mu.Unlock()
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

	// mu guards streams and byChannel. When a Stream's own lock is also needed,
	// it is taken after this one.
	mu        sync.RWMutex
	streams   map[string]*Stream
	byChannel map[string]map[string]*Stream // channel -> stream id -> stream

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
		limit:    max(1, h.queueSize),
		ready:    make(chan struct{}, 1),
		room:     make(chan struct{}, 1),
		done:     make(chan struct{}),
		created:  time.Now(),
		channels: map[string]channelJoin{},
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
		s.mu.RLock()
		for ch := range s.channels {
			if set := h.byChannel[ch]; set != nil {
				delete(set, id)
				if len(set) == 0 {
					delete(h.byChannel, ch)
				}
			}
		}
		s.mu.RUnlock()
	}
	h.mu.Unlock()
	if s == nil {
		return
	}
	// Presence leave is driven by the transport close, with no client message
	// required.
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

// JoinChannel adds a stream to a channel's fan-out set. It returns false when
// the stream has already been closed, so a join racing a disconnect cannot
// leave a closed stream in the set. recheckAt is when the join's authorization
// expires; zero means never.
func (h *Hub) JoinChannel(channel string, s *Stream, label string, recheckAt time.Time) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.streams[s.id] != s {
		return false
	}
	set := h.byChannel[channel]
	if set == nil {
		set = map[string]*Stream{}
		h.byChannel[channel] = set
	}
	set[s.id] = s
	s.TrackChannel(channel, label, recheckAt)
	return true
}

// LeaveChannel removes a stream from a channel.
func (h *Hub) LeaveChannel(channel string, s *Stream) {
	h.mu.Lock()
	h.leaveLocked(channel, s)
	h.mu.Unlock()
	h.presence.Untrack(channel, s.id)
}

// LeaveChannelLabel removes a stream from a channel only if it is still joined
// under label, and reports whether it was. A revocation decided about one join
// must not remove a newer join of the same channel.
func (h *Hub) LeaveChannelLabel(channel string, s *Stream, label string) bool {
	h.mu.Lock()
	if l, ok := s.ChannelLabel(channel); !ok || l != label {
		h.mu.Unlock()
		return false
	}
	h.leaveLocked(channel, s)
	h.mu.Unlock()
	h.presence.Untrack(channel, s.id)
	return true
}

func (h *Hub) leaveLocked(channel string, s *Stream) {
	if set := h.byChannel[channel]; set != nil {
		delete(set, s.id)
		if len(set) == 0 {
			delete(h.byChannel, channel)
		}
	}
	s.UntrackChannel(channel)
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

// PublishBroadcast fans a message out to a channel and returns how many streams
// it was queued on. A stream whose queue is full does not count.
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
