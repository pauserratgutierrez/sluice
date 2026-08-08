package hub

import (
	"encoding/json"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/event"
)

// Presence is the ephemeral, keyed, last-write-wins membership state.
//
// It is a separate primitive from broadcast, not broadcast with extra steps: the
// server MUST hold state, because a joining client needs the current full state
// and a disconnect must produce a `leave` even though nobody sent a message.
//
// It lives in memory only. Never in the database.
//
// Diffs are coalesced on a tick (default 1500ms, matching Phoenix.Tracker's
// broadcast period) so that a client calling track() on every mouse move cannot
// amplify into N-squared messages -- the documented failure mode of Supabase
// Presence.
type Presence struct {
	hub  *Hub
	tick time.Duration

	mu       sync.Mutex
	channels map[string]*channelState

	stop chan struct{}
	once sync.Once
}

type channelState struct {
	members map[string]*member
	joins   map[string]event.Member
	leaves  map[string]event.Member
	dirty   bool
}

type member struct {
	streamID string
	meta     json.RawMessage
	since    time.Time
	ref      string
}

func NewPresence(h *Hub, tick time.Duration) *Presence {
	if tick <= 0 {
		tick = 1500 * time.Millisecond
	}
	return &Presence{
		hub:      h,
		tick:     tick,
		channels: map[string]*channelState{},
		stop:     make(chan struct{}),
	}
}

// Run flushes coalesced diffs until the context is cancelled.
func (p *Presence) Run() {
	t := time.NewTicker(p.tick)
	defer t.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-t.C:
			p.flush()
		}
	}
}

func (p *Presence) Stop() { p.once.Do(func() { close(p.stop) }) }

// Track adds or replaces a member. Returns the number of keys the channel now
// holds so the caller can enforce a cap.
func (p *Presence) Track(channel, key, streamID string, meta json.RawMessage) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	cs := p.channelLocked(channel)
	ref := time.Now().UTC().Format("20060102T150405.000000000")
	cs.members[key] = &member{streamID: streamID, meta: meta, since: time.Now(), ref: ref}
	cs.joins[key] = event.Member{Meta: meta, Since: cs.members[key].since.UTC().Format(time.RFC3339Nano), Ref: ref}
	delete(cs.leaves, key)
	cs.dirty = true
	return len(cs.members)
}

// Untrack removes every key a stream owns from one channel.
func (p *Presence) Untrack(channel, streamID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	cs := p.channels[channel]
	if cs == nil {
		return
	}
	for key, m := range cs.members {
		if m.streamID != streamID {
			continue
		}
		delete(cs.members, key)
		delete(cs.joins, key)
		cs.leaves[key] = event.Member{Meta: m.meta, Ref: m.ref}
		cs.dirty = true
	}
}

// UntrackKey removes one specific key, if the stream owns it.
func (p *Presence) UntrackKey(channel, key, streamID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	cs := p.channels[channel]
	if cs == nil {
		return false
	}
	m := cs.members[key]
	if m == nil || m.streamID != streamID {
		return false
	}
	delete(cs.members, key)
	delete(cs.joins, key)
	cs.leaves[key] = event.Member{Meta: m.meta, Ref: m.ref}
	cs.dirty = true
	return true
}

// RemoveStream removes a stream from every channel it appears in. Driven by the
// transport close, which is the only leave signal Sluice needs.
func (p *Presence) RemoveStream(streamID string) {
	p.mu.Lock()
	var channels []string
	for name := range p.channels {
		channels = append(channels, name)
	}
	p.mu.Unlock()
	for _, c := range channels {
		p.Untrack(c, streamID)
	}
}

// State returns the full membership of a channel, for a joining client.
func (p *Presence) State(channel string) map[string]event.Member {
	p.mu.Lock()
	defer p.mu.Unlock()
	cs := p.channels[channel]
	if cs == nil {
		return map[string]event.Member{}
	}
	out := make(map[string]event.Member, len(cs.members))
	for k, m := range cs.members {
		out[k] = event.Member{
			Meta:  m.meta,
			Since: m.since.UTC().Format(time.RFC3339Nano),
			Ref:   m.ref,
		}
	}
	return out
}

// Count returns the number of members in a channel.
func (p *Presence) Count(channel string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cs := p.channels[channel]; cs != nil {
		return len(cs.members)
	}
	return 0
}

func (p *Presence) channelLocked(name string) *channelState {
	cs := p.channels[name]
	if cs == nil {
		cs = &channelState{
			members: map[string]*member{},
			joins:   map[string]event.Member{},
			leaves:  map[string]event.Member{},
		}
		p.channels[name] = cs
	}
	return cs
}

// flush emits one diff per dirty channel and clears the accumulators.
func (p *Presence) flush() {
	type pending struct {
		channel string
		joins   map[string]event.Member
		leaves  map[string]event.Member
	}
	var out []pending

	p.mu.Lock()
	for name, cs := range p.channels {
		if !cs.dirty {
			continue
		}
		pd := pending{channel: name}
		if len(cs.joins) > 0 {
			pd.joins = cs.joins
			cs.joins = map[string]event.Member{}
		}
		if len(cs.leaves) > 0 {
			pd.leaves = cs.leaves
			cs.leaves = map[string]event.Member{}
		}
		cs.dirty = false
		if len(cs.members) == 0 && pd.joins == nil {
			delete(p.channels, name)
		}
		out = append(out, pd)
	}
	p.mu.Unlock()

	for _, pd := range out {
		if pd.joins == nil && pd.leaves == nil {
			continue
		}
		for _, s := range p.hub.ChannelMembers(pd.channel) {
			label, ok := s.ChannelLabel(pd.channel)
			if !ok {
				continue
			}
			s.Send(event.Event{Kind: event.KindPresence, Data: event.Presence{
				Sub: label, Channel: pd.channel, Type: "diff",
				Joins: pd.joins, Leaves: pd.leaves,
			}})
		}
	}
}
