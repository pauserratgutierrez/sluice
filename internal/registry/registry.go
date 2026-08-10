// Package registry holds subscriptions and routes changes to them.
//
// The routing index is the mechanism that makes Sluice scale with the write rate
// rather than the subscriber count. Subscriptions are indexed by the CONSTANT
// their filter pins a column to, so a change is matched with a map lookup
// instead of a scan.
//
// ElectricSQL measured the difference on the same workload: 1,400 changes/sec at
// 10 shapes and 140 at 100 shapes without constant indexing, versus a flat
// ~5,000 changes/sec at any shape count with it.
package registry

import (
	"slices"
	"sort"
	"sync"

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

	// Warnings raised at subscribe time, replayed in the ready event.
	Warnings []event.Warning

	// Seq numbers change events within a commit so that (commit_lsn, seq) is a
	// total order the client can dedupe against. At-least-once delivery is
	// inherent to logical decoding -- PostgreSQL persists slot position only at
	// checkpoint, so a crash can replay -- and this is what makes it tolerable.
	seq int
}

// Indexed reports whether this subscription avoids the per-change scan.
func (s *Subscription) Indexed() bool { return s.RoutingKey != "" }

// NextSeq is called by the reader while holding no registry lock; each
// subscription is only ever advanced by the single reader goroutine.
func (s *Subscription) NextSeq() int { s.seq++; return s.seq }

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
	subs[s.Label] = s

	ri := r.rels[s.Relation.OID]
	if ri == nil {
		ri = newRelIndex()
		r.rels[s.Relation.OID] = ri
	}
	ri.all[s] = true

	if s.RoutingKey == "" {
		ri.unindexed = append(ri.unindexed, s)
		r.unindexedCount++
		return true
	}
	key := s.Filter.Equalities[s.RoutingKey].String()
	byConst := ri.byColumn[s.RoutingKey]
	if byConst == nil {
		byConst = map[string][]*Subscription{}
		ri.byColumn[s.RoutingKey] = byConst
	}
	byConst[key] = append(byConst[key], s)
	return true
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

	ri := r.rels[s.Relation.OID]
	if ri == nil {
		return s
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
	return s
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

// Candidates returns the subscriptions that could match a tuple, using the
// routing index.
//
// `lookup` reads a column value from the tuple. It returns known=false for
// values the WAL did not carry, in which case that column's index cannot be
// consulted and the subscriptions under it are added conservatively -- correctness
// before speed, since the residual filter and the authorizer still run.
func (r *Registry) Candidates(oid uint32, lookup func(column string) (v expr.Value, known bool)) []*Subscription {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ri := r.rels[oid]
	if ri == nil {
		return nil
	}

	seen := make(map[*Subscription]bool, 8)
	var out []*Subscription
	add := func(list []*Subscription) {
		for _, s := range list {
			if !seen[s] {
				seen[s] = true
				out = append(out, s)
			}
		}
	}

	for col, byConst := range ri.byColumn {
		v, known := lookup(col)
		if !known {
			// The routing column is absent (unchanged TOAST, or not in the
			// replica identity for an old tuple). Fall back to considering every
			// subscription indexed on this column.
			for _, list := range byConst {
				add(list)
			}
			continue
		}
		add(byConst[v.String()])
	}
	add(ri.unindexed)
	return out
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
