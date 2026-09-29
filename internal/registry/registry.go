// Package registry holds subscriptions and routes changes to them.
//
// The routing index is the mechanism that makes Sluice scale with the write rate
// rather than the subscriber count. Subscriptions are indexed by the CONSTANT
// their filter pins a column to, so a change is matched with a map lookup
// instead of a scan.
package registry

import (
	"slices"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// Sink receives events for one stream.
type Sink interface {
	StreamID() string
	Send(event.Event) bool
	Identity() authz.Identity
}

// Subscription is one client interest in one relation.
//
// Once added to a Registry a Subscription is never modified, because dispatch
// reads it without holding the registry lock: Rebind publishes a copy instead.
// Only Decision (an atomic handle) and the event sequence change in place, and
// both are safe for concurrent use.
type Subscription struct {
	// Label is the client-chosen name, unique within a stream.
	Label string
	Sink  Sink

	Relation    *catalog.Relation
	Ops         shape.Ops
	Filter      *shape.Filter
	Columns     []string // projection, already intersected with column grants
	Transitions bool

	Decision *authz.Handle

	// RoutingKey is the column this subscription is indexed by, or "" when it is
	// unindexed and therefore scanned for every change to the relation.
	RoutingKey string

	// seq numbers the subscription's change events. It is shared by every copy
	// Rebind makes, and advanced by the reader, snapshots and replays alike.
	seq *atomic.Int64
}

// Indexed reports whether this subscription avoids the per-change scan.
func (s *Subscription) Indexed() bool { return s.RoutingKey != "" }

// NextSeq returns the next event sequence number for this subscription.
func (s *Subscription) NextSeq() int { return int(s.seq.Add(1)) }

// relIndex is the per-relation routing structure.
type relIndex struct {
	// byColumn[column][constant] -> subscriptions
	byColumn map[string]map[string][]*Subscription
	// unindexed subscriptions must be considered for every change
	unindexed []*Subscription
	// all is used for teardown and diagnostics
	all map[*Subscription]bool
}

func newRelIndex() *relIndex {
	return &relIndex{
		byColumn: map[string]map[string][]*Subscription{},
		all:      map[*Subscription]bool{},
	}
}

// Registry is the whole index, keyed by relation OID.
type Registry struct {
	mu   sync.RWMutex
	rels map[uint32]*relIndex

	// byStream lets a disconnect remove everything a stream held in O(subs).
	byStream map[string]map[string]*Subscription

	unindexedCount int
}

func New() *Registry {
	return &Registry{
		rels:     map[uint32]*relIndex{},
		byStream: map[string]map[string]*Subscription{},
	}
}

// Add registers a subscription. Returns false if the stream already has a
// subscription with that label.
func (r *Registry) Add(s *Subscription) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	sid := s.Sink.StreamID()
	subs := r.byStream[sid]
	if subs == nil {
		subs = map[string]*Subscription{}
		r.byStream[sid] = subs
	}
	if _, exists := subs[s.Label]; exists {
		return false
	}
	if s.seq == nil {
		s.seq = new(atomic.Int64)
	}
	subs[s.Label] = s
	r.indexRelLocked(s)
	return true
}

func (r *Registry) indexRelLocked(s *Subscription) {
	if s.Relation == nil {
		return
	}
	ri := r.rels[s.Relation.OID]
	if ri == nil {
		ri = newRelIndex()
		r.rels[s.Relation.OID] = ri
	}
	ri.all[s] = true

	if s.RoutingKey == "" {
		ri.unindexed = append(ri.unindexed, s)
		r.unindexedCount++
		return
	}
	key := s.Filter.Equalities[s.RoutingKey].String()
	byConst := ri.byColumn[s.RoutingKey]
	if byConst == nil {
		byConst = map[string][]*Subscription{}
		ri.byColumn[s.RoutingKey] = byConst
	}
	byConst[key] = append(byConst[key], s)
}

// Remove drops one subscription.
func (r *Registry) Remove(streamID, label string) *Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.removeLocked(streamID, label)
}

func (r *Registry) removeLocked(streamID, label string) *Subscription {
	subs := r.byStream[streamID]
	if subs == nil {
		return nil
	}
	s := subs[label]
	if s == nil {
		return nil
	}
	delete(subs, label)
	if len(subs) == 0 {
		delete(r.byStream, streamID)
	}
	r.unindexRelLocked(s)
	return s
}

func (r *Registry) unindexRelLocked(s *Subscription) {
	if s.Relation == nil {
		return
	}
	ri := r.rels[s.Relation.OID]
	if ri == nil {
		return
	}
	delete(ri.all, s)

	if s.RoutingKey == "" {
		ri.unindexed = slices.DeleteFunc(ri.unindexed, func(e *Subscription) bool { return e == s })
		r.unindexedCount--
	} else {
		key := s.Filter.Equalities[s.RoutingKey].String()
		if byConst := ri.byColumn[s.RoutingKey]; byConst != nil {
			byConst[key] = slices.DeleteFunc(byConst[key], func(e *Subscription) bool { return e == s })
			if len(byConst[key]) == 0 {
				delete(byConst, key)
			}
			if len(byConst) == 0 {
				delete(ri.byColumn, s.RoutingKey)
			}
		}
	}
	if len(ri.all) == 0 {
		delete(r.rels, s.Relation.OID)
	}
}

// Get returns the live subscription for a stream label, or nil.
func (r *Registry) Get(streamID, label string) *Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.byStream[streamID][label]
}

// Rebind replaces a live subscription with a copy carrying a new effective
// filter, projection, and routing key, under one lock. Remove+Add would leave an
// interval where Candidates misses the shape (under-delivery) or, if a caller
// also swapped holds across two lock acquisitions, a DELETE could miss the
// watch. A change already being dispatched keeps the version it started with.
func (r *Registry) Rebind(streamID, label string, filter *shape.Filter, columns []string) *Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	old := r.byStream[streamID][label]
	if old == nil || filter == nil {
		return nil
	}
	nu := *old
	nu.Filter = filter
	nu.Columns = append([]string(nil), columns...)
	nu.RoutingKey = filter.RoutingKey(old.Relation)
	r.unindexRelLocked(old)
	r.byStream[streamID][label] = &nu
	r.indexRelLocked(&nu)
	return &nu
}

// RemoveStream drops everything a stream held. Called on disconnect, where the
// transport-level close is the only signal -- there is no unsubscribe message to
// wait for.
func (r *Registry) RemoveStream(streamID string) []*Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	subs := r.byStream[streamID]
	if subs == nil {
		return nil
	}
	labels := make([]string, 0, len(subs))
	for l := range subs {
		labels = append(labels, l)
	}
	out := make([]*Subscription, 0, len(labels))
	for _, l := range labels {
		if s := r.removeLocked(streamID, l); s != nil {
			out = append(out, s)
		}
	}
	return out
}

// StreamSubscriptions returns a stream's subscriptions.
func (r *Registry) StreamSubscriptions(streamID string) []*Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Subscription
	for _, s := range r.byStream[streamID] {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Label < out[j].Label })
	return out
}

// Candidates returns the subscriptions that could match a change, using the
// routing index.
//
// Each lookup reads a column value from one tuple of the change -- the new and
// the old one for an UPDATE, so a row leaving a shape is still routed to it. A
// lookup returns known=false for a value the WAL did not carry. When no tuple
// carries the routing column, its index cannot be consulted and every
// subscription under it is added conservatively -- correctness before speed,
// since the residual filter and the authorizer still run.
//
// Every subscription sits in exactly one list (one constant of its routing
// column, or unindexed), and each list is added at most once, so the result
// has no duplicates without needing a set.
func (r *Registry) Candidates(oid uint32, lookups ...func(column string) (v expr.Value, known bool)) []*Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ri := r.rels[oid]
	if ri == nil {
		return nil
	}

	var out []*Subscription
	for col, byConst := range ri.byColumn {
		var first string
		found := false
		for _, lookup := range lookups {
			v, known := lookup(col)
			if !known {
				continue
			}
			key := v.String()
			if found && key == first {
				continue
			}
			out = append(out, byConst[key]...)
			if !found {
				first, found = key, true
			}
		}
		if !found {
			for _, list := range byConst {
				out = append(out, list...)
			}
		}
	}
	return append(out, ri.unindexed...)
}

// UnindexedCount is the number of subscriptions scanned for every change to
// their relation.
func (r *Registry) UnindexedCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.unindexedCount
}

// Stats describes the index, for metrics and diagnostics.
type Stats struct {
	Relations      int
	Subscriptions  int
	Unindexed      int
	Streams        int
	ByTier         map[authz.Tier]int
	ByRelationTier map[string]map[authz.Tier]int
}

func (r *Registry) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	st := Stats{
		Relations:      len(r.rels),
		Unindexed:      r.unindexedCount,
		Streams:        len(r.byStream),
		ByTier:         map[authz.Tier]int{},
		ByRelationTier: map[string]map[authz.Tier]int{},
	}
	for _, ri := range r.rels {
		for s := range ri.all {
			st.Subscriptions++
			t := authz.TierA
			if s.Decision != nil {
				if d := s.Decision.Load(); d != nil {
					t = d.Tier
				}
			}
			st.ByTier[t]++
			name := s.Relation.FullName()
			if st.ByRelationTier[name] == nil {
				st.ByRelationTier[name] = map[authz.Tier]int{}
			}
			st.ByRelationTier[name][t]++
		}
	}
	return st
}

// All returns every subscription, for lease refresh and diagnostics.
func (r *Registry) All() []*Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []*Subscription
	for _, ri := range r.rels {
		for s := range ri.all {
			out = append(out, s)
		}
	}
	return out
}
