// Package hold tracks the rows that keep an issuer grant alive.
//
// A hold is not "the subscribed table already has rows". It is a row the
// application already deletes (or updates out of a filter) when it revokes
// access. Sluice watches those rows on the same slot and cuts the shape when
// the first hold stops matching. The index is separate from the shape registry.
package hold

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// Spec is one hold the issuer named: a published table plus an equality filter.
type Spec struct {
	Rel    *catalog.Relation
	Filter *shape.Filter
}

// Watch is the holds that keep one shape subscription alive.
type Watch struct {
	StreamID string
	Label    string
	Holds    []Spec

	// routes[i] is where Holds[i] is indexed. Unindexing goes by it rather
	// than working the place out again, so a hold always leaves the list it
	// was put in.
	routes []route
}

// route is where one hold is indexed: its relation, and the column and
// constant it is listed under (no column: scanned on every change).
type route struct {
	indexed bool
	oid     uint32
	col     string
	val     string
}

func newWatch(streamID, label string, holds []Spec) *Watch {
	return &Watch{
		StreamID: streamID, Label: label,
		Holds:  append([]Spec(nil), holds...),
		routes: make([]route, len(holds)),
	}
}

// Cut is a shape that must be dropped because a hold stopped matching.
type Cut struct {
	StreamID string
	Label    string
	Reason   string
}

type relIndex struct {
	byColumn  map[string]map[string][]*Watch
	unindexed []*Watch
	all       map[*Watch]bool
}

func newRelIndex() *relIndex {
	return &relIndex{
		byColumn: map[string]map[string][]*Watch{},
		all:      map[*Watch]bool{},
	}
}

// Index routes WAL changes to hold watches in O(1) on an equality constant.
type Index struct {
	mu       sync.RWMutex
	rels     map[uint32]*relIndex
	byStream map[string]map[string]*Watch
	// identity is each hold table's replica identity as the WAL last
	// described it (OutsideIdentity), which the catalog may not show yet.
	identity map[uint32]*catalog.Relation
}

func New() *Index {
	return &Index{
		rels:     map[uint32]*relIndex{},
		byStream: map[string]map[string]*Watch{},
		identity: map[uint32]*catalog.Relation{},
	}
}

// Add registers a watch. Returns false if the stream already has that label.
// Callers must Add the watch before EXISTS so a concurrent DELETE is applied
// by OnChange rather than racing past a subscribe that has not yet appeared.
func (x *Index) Add(streamID, label string, holds []Spec) bool {
	x.mu.Lock()
	defer x.mu.Unlock()

	subs := x.byStream[streamID]
	if subs == nil {
		subs = map[string]*Watch{}
		x.byStream[streamID] = subs
	}
	if _, exists := subs[label]; exists {
		return false
	}
	w := newWatch(streamID, label, holds)
	subs[label] = w
	x.indexWatchLocked(w)
	return true
}

// Replace swaps a live watch's holds under one lock. There is no interval
// where OnChange can miss a DELETE: the previous watch is unindexed and the
// new one is indexed before the mutex is released. If no watch exists,
// Replace registers one (same as Add). Callers must Replace before EXISTS,
// the same join invariant: a DELETE already in the reader is applied by the
// watch rather than racing past a refresh that has not yet reappeared.
func (x *Index) Replace(streamID, label string, holds []Spec) {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.removeLocked(streamID, label)
	w := newWatch(streamID, label, holds)
	subs := x.byStream[streamID]
	if subs == nil {
		subs = map[string]*Watch{}
		x.byStream[streamID] = subs
	}
	subs[label] = w
	x.indexWatchLocked(w)
}

func (x *Index) indexWatchLocked(w *Watch) {
	for i := range w.Holds {
		x.indexLocked(w, i)
	}
}

func (x *Index) unindexWatchLocked(w *Watch) {
	for i := range w.Holds {
		x.unindexLocked(w, i)
	}
}

// indexLocked lists hold i of w under its relation's routing key, and records
// where in w.routes.
func (x *Index) indexLocked(w *Watch, i int) {
	spec := w.Holds[i]
	if spec.Rel == nil || spec.Filter == nil {
		return
	}
	oid := spec.Rel.OID
	ri := x.rels[oid]
	if ri == nil {
		ri = newRelIndex()
		x.rels[oid] = ri
	}
	ri.all[w] = true
	r := route{indexed: true, oid: oid, col: spec.Filter.RoutingKey(spec.Rel)}
	if r.col == "" {
		if !slices.Contains(ri.unindexed, w) {
			ri.unindexed = append(ri.unindexed, w)
		}
	} else {
		r.val = spec.Filter.Equalities[r.col].String()
		byConst := ri.byColumn[r.col]
		if byConst == nil {
			byConst = map[string][]*Watch{}
			ri.byColumn[r.col] = byConst
		}
		if !slices.Contains(byConst[r.val], w) {
			byConst[r.val] = append(byConst[r.val], w)
		}
	}
	w.routes[i] = r
}

// Remove drops one watch.
func (x *Index) Remove(streamID, label string) *Watch {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.removeLocked(streamID, label)
}

func (x *Index) removeLocked(streamID, label string) *Watch {
	subs := x.byStream[streamID]
	if subs == nil {
		return nil
	}
	w := subs[label]
	if w == nil {
		return nil
	}
	delete(subs, label)
	if len(subs) == 0 {
		delete(x.byStream, streamID)
	}
	x.unindexWatchLocked(w)
	return w
}

// unindexLocked takes hold i of w out of the list w.routes says it is in.
func (x *Index) unindexLocked(w *Watch, i int) {
	r := w.routes[i]
	if !r.indexed {
		return
	}
	w.routes[i] = route{}
	ri := x.rels[r.oid]
	if ri == nil {
		return
	}
	delete(ri.all, w)
	if r.col == "" {
		ri.unindexed = slices.DeleteFunc(ri.unindexed, func(e *Watch) bool { return e == w })
	} else if byConst := ri.byColumn[r.col]; byConst != nil {
		byConst[r.val] = slices.DeleteFunc(byConst[r.val], func(e *Watch) bool { return e == w })
		if len(byConst[r.val]) == 0 {
			delete(byConst, r.val)
		}
		if len(byConst) == 0 {
			delete(ri.byColumn, r.col)
		}
	}
	if len(ri.all) == 0 {
		delete(x.rels, r.oid)
	}
}

// RemoveStream drops every watch a stream held.
func (x *Index) RemoveStream(streamID string) {
	x.mu.Lock()
	defer x.mu.Unlock()
	subs := x.byStream[streamID]
	if subs == nil {
		return
	}
	labels := make([]string, 0, len(subs))
	for l := range subs {
		labels = append(labels, l)
	}
	for _, l := range labels {
		x.removeLocked(streamID, l)
	}
}

// All returns every live watch.
func (x *Index) All() []*Watch {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var out []*Watch
	for _, subs := range x.byStream {
		for _, w := range subs {
			out = append(out, w)
		}
	}
	return out
}

// OutsideIdentity cuts every watch with a hold on rel whose filter reads a
// column outside rel's replica identity, which OnChange relies on not
// existing. rel is the relation as the WAL now describes it, which can differ
// from the catalog the holds were granted against until the next refresh; it
// is kept for CheckIdentity. The watches are removed here, as in OnChange; the
// caller drops the shapes.
func (x *Index) OutsideIdentity(rel *catalog.Relation) []Cut {
	x.mu.Lock()
	defer x.mu.Unlock()
	x.identity[rel.OID] = rel
	ri := x.rels[rel.OID]
	if ri == nil {
		return nil
	}
	var cuts []Cut
	for w := range ri.all {
		if !x.current(w) {
			continue
		}
		for _, h := range w.Holds {
			if h.Rel == nil || h.Rel.OID != rel.OID {
				continue
			}
			if missing := MissingReplicaIdentity(rel, h.Filter); len(missing) > 0 {
				cuts = append(cuts, Cut{StreamID: w.StreamID, Label: w.Label, Reason: ReplicaIdentityReason(rel, missing)})
				break
			}
		}
	}
	for _, c := range cuts {
		x.removeLocked(c.StreamID, c.Label)
	}
	return cuts
}

// CheckIdentity is called after Add or Replace, whose holds were checked
// against the catalog when they were granted. Until the catalog refreshes, the
// WAL may already describe a replica identity that does not cover a hold's
// columns; such a watch is removed and the reason returned. It also fails when
// the watch is gone, cut by a change or by OutsideIdentity since it was added.
// OutsideIdentity runs under the same lock, so whichever comes second sees the
// other.
func (x *Index) CheckIdentity(streamID, label string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	w := x.byStream[streamID][label]
	if w == nil {
		return errors.New("a hold of this grant was cut while it was being installed")
	}
	for _, h := range w.Holds {
		if h.Rel == nil {
			continue
		}
		id := x.identity[h.Rel.OID]
		if id == nil {
			continue
		}
		if missing := MissingReplicaIdentity(id, h.Filter); len(missing) > 0 {
			x.removeLocked(streamID, label)
			return errors.New(ReplicaIdentityReason(id, missing))
		}
	}
	return nil
}

// Count returns the number of live watches.
func (x *Index) Count() int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	n := 0
	for _, subs := range x.byStream {
		n += len(subs)
	}
	return n
}

// RefreshRels swaps each hold's Rel for a fresh catalog pointer under the same
// mutex as OnChange. Callers must not write Watch.Holds from outside the lock
// (All only takes RLock).
func (x *Index) RefreshRels(streamID, label string, rels []*catalog.Relation) {
	x.mu.Lock()
	defer x.mu.Unlock()
	subs := x.byStream[streamID]
	if subs == nil {
		return
	}
	w := subs[label]
	if w == nil || len(rels) != len(w.Holds) {
		return
	}
	for i, rel := range rels {
		if rel == nil {
			continue
		}
		x.unindexLocked(w, i)
		w.Holds[i].Rel = rel
		x.indexLocked(w, i)
	}
}

// current reports whether w is the watch registered for its stream and label.
// Only that one speaks for the shape: a watch that a later grant replaced, if
// one were still indexed, must not cut the grant that replaced it.
func (x *Index) current(w *Watch) bool {
	return x.byStream[w.StreamID][w.Label] == w
}

// OnChange returns watches whose hold on this relation no longer holds after
// the WAL change. DELETE of a matching row, or UPDATE that leaves the filter,
// cuts. An UPDATE that omits the old tuple cuts nothing: no replica-identity
// column changed, and a hold's filter reads only those columns (checked when
// it is granted, on each catalog refresh and when the relation's definition
// changes), so every hold matches the row as it did before. The watch is
// removed from the index here so a later change cannot recut; the caller still
// drops the shape from the registry.
func (x *Index) OnChange(oid uint32, op byte, oldRow, newRow expr.Row) []Cut {
	if op == 'U' && oldRow == nil {
		return nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()

	ri := x.rels[oid]
	if ri == nil {
		return nil
	}

	seen := map[*Watch]bool{}
	var candidates []*Watch
	add := func(list []*Watch) {
		for _, w := range list {
			if !seen[w] {
				seen[w] = true
				candidates = append(candidates, w)
			}
		}
	}
	lookup := func(row expr.Row) func(string) (expr.Value, bool) {
		return func(col string) (expr.Value, bool) {
			if row == nil {
				return expr.Null, false
			}
			return row.Column(col)
		}
	}
	for col, byConst := range ri.byColumn {
		matched := false
		if newRow != nil {
			if v, ok := lookup(newRow)(col); ok {
				add(byConst[v.String()])
				matched = true
			} else {
				for _, list := range byConst {
					add(list)
				}
				matched = true
			}
		}
		if oldRow != nil {
			if v, ok := lookup(oldRow)(col); ok {
				add(byConst[v.String()])
			} else if !matched {
				for _, list := range byConst {
					add(list)
				}
			}
		}
	}
	add(ri.unindexed)

	var cuts []Cut
	for _, w := range candidates {
		reason, cut := watchBroken(w, oid, op, oldRow, newRow)
		if !cut {
			continue
		}
		if !x.current(w) {
			// Not the stream's watch for that label any more: drop it from
			// the index and cut nothing.
			x.unindexWatchLocked(w)
			continue
		}
		x.removeLocked(w.StreamID, w.Label)
		cuts = append(cuts, Cut{StreamID: w.StreamID, Label: w.Label, Reason: reason})
	}
	return cuts
}

// OnTruncate returns every watch with a hold on a truncated relation: TRUNCATE
// removes every row without a DELETE for each. Like OnChange, it removes the
// watches from the index; the caller drops the shapes.
func (x *Index) OnTruncate(oid uint32) []Cut {
	x.mu.Lock()
	defer x.mu.Unlock()

	ri := x.rels[oid]
	if ri == nil {
		return nil
	}
	cuts := make([]Cut, 0, len(ri.all))
	var stale []*Watch
	for w := range ri.all {
		if !x.current(w) {
			stale = append(stale, w)
			continue
		}
		name := ""
		for _, spec := range w.Holds {
			if spec.Rel != nil && spec.Rel.OID == oid {
				name = spec.Rel.FullName()
				break
			}
		}
		cuts = append(cuts, Cut{StreamID: w.StreamID, Label: w.Label,
			Reason: "a hold on " + name + " no longer exists: the table was truncated"})
	}
	for _, w := range stale {
		x.unindexWatchLocked(w)
	}
	for _, c := range cuts {
		x.removeLocked(c.StreamID, c.Label)
	}
	return cuts
}

func watchBroken(w *Watch, oid uint32, op byte, oldRow, newRow expr.Row) (string, bool) {
	for _, spec := range w.Holds {
		if spec.Rel == nil || spec.Rel.OID != oid || spec.Filter == nil {
			continue
		}
		switch op {
		case 'D':
			match, unk := visible(spec.Filter, oldRow)
			if unk || match {
				return "a hold on " + spec.Rel.FullName() + " no longer exists", true
			}
		case 'U':
			// Protocol: Update carries 'K' (old key) or 'O' (old full) or
			// neither, never both. 'K' only if replica-identity columns changed;
			// 'N' is always present. With neither, no column a hold reads
			// changed (see OnChange): nothing to cut. Deciding from the new row
			// alone would cut other rows' holds, since a watch is a candidate
			// when one routed column matches, not the whole identity.
			if oldRow == nil {
				continue
			}
			matchOld, _ := visible(spec.Filter, oldRow)
			matchNew, unkNew := visible(spec.Filter, newRow)
			if matchOld && (unkNew || !matchNew) {
				return "a hold on " + spec.Rel.FullName() + " no longer matches", true
			}
		}
	}
	return "", false
}

func visible(f *shape.Filter, row expr.Row) (bool, bool) {
	if f == nil || f.Node == nil || row == nil {
		return false, true
	}
	return expr.Visible(f.Node, &expr.Context{
		Row: row, Claims: expr.Claims{}, ClaimsJSON: "{}", Now: time.Now(),
	})
}

// MissingReplicaIdentity lists filter columns that will not appear in old
// tuples. Without them a DELETE cannot cut the hold. This is PostgreSQL
// REPLICA IDENTITY, not a Sluice convention.
func MissingReplicaIdentity(rel *catalog.Relation, f *shape.Filter) []string {
	if rel == nil || f == nil {
		return nil
	}
	var out []string
	for _, c := range f.ColumnsNeeded() {
		if !rel.InReplicaIdentity(c) {
			out = append(out, c)
		}
	}
	return out
}

// ReplicaIdentityReason is the join-deny and catalog-tick cut message when hold
// filter columns are missing from the replica identity. Join and RefreshLeases
// must use this exact text.
func ReplicaIdentityReason(rel *catalog.Relation, missing []string) string {
	if rel == nil || len(missing) == 0 {
		return ""
	}
	pk := strings.Join(rel.ReplicaIdentityColumns, ", ")
	if pk == "" {
		pk = "id"
	}
	idx := rel.Name + "_ri"
	return fmt.Sprintf(
		"hold filter column(s) %s are not in the replica identity of %s, so a DELETE cannot cut this grant (this is PostgreSQL REPLICA IDENTITY, not a Sluice convention). Remedy: CREATE UNIQUE INDEX %s ON %s (%s, %s); ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;",
		strings.Join(missing, ", "), rel.FullName(),
		idx, rel.FullName(), strings.Join(missing, ", "), pk, rel.FullName(), idx)
}

// ExistsSQL is the EXISTS used at join. It reads only the hold relation, never
// the subscribed table.
func ExistsSQL(spec Spec) (string, []any) {
	where, args := spec.Filter.SQL(1)
	sql := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`,
		catalog.QuoteQualified(spec.Rel.Schema, spec.Rel.Name), where)
	return sql, args
}

// Querier is the subset of pgxpool.Pool Exists needs.
type Querier interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Exists verifies every hold row is present. All holds must exist (AND). A
// missing hold is not "empty shape"; it is a denied grant. Zero rows on the
// subscribed table are irrelevant here and are never queried.
func Exists(ctx context.Context, q Querier, specs []Spec) error {
	for _, spec := range specs {
		if spec.Rel == nil || spec.Filter == nil {
			return fmt.Errorf("hold is missing a relation or filter")
		}
		sql, args := ExistsSQL(spec)
		var ok bool
		if err := q.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
			return fmt.Errorf("hold exists on %s: %w", spec.Rel.FullName(), err)
		}
		if !ok {
			return fmt.Errorf("hold on %s with filter %s does not exist", spec.Rel.FullName(), spec.Filter.Describe())
		}
	}
	return nil
}
