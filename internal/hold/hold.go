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
	// counted are the candidate columns this hold was counted under in
	// relIndex.cols, taken out again by unindexLocked.
	counted []string
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
	// cols counts, for each candidate routing column, the holds that have an
	// equality on it and how many share each constant, whichever column each
	// hold is listed under: what its lists would hold if every hold used it.
	cols map[string]*colCount
}

type colCount struct {
	holds   int
	byConst map[string]int
}

// meanList is the mean number of holds per constant: the expected length of
// the list a change to one row looks through when holds are listed by this
// column.
func (c *colCount) meanList() float64 {
	if c == nil || len(c.byConst) == 0 {
		return 0
	}
	return float64(c.holds) / float64(len(c.byConst))
}

func newRelIndex() *relIndex {
	return &relIndex{
		byColumn: map[string]map[string][]*Watch{},
		all:      map[*Watch]bool{},
		cols:     map[string]*colCount{},
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
// Callers must Add the watch before checking its holds, so a concurrent DELETE
// is applied by OnChange rather than racing past a subscribe that has not yet
// appeared, and register the shape with Activate once the holds were found.
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

// indexLocked lists hold i of w under the routing column of its relation whose
// lists are shortest, and records where in w.routes.
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
	r := route{indexed: true, oid: oid, counted: spec.Filter.RoutingCandidates(spec.Rel)}
	// Count the hold under every candidate column, then list it under the one
	// whose lists are shortest on average: a change to one row then looks
	// through the fewest holds, whatever order the issuer wrote the columns
	// in. Ties go to the first in the filter.
	best := -1.0
	for _, col := range r.counted {
		c := ri.cols[col]
		if c == nil {
			c = &colCount{byConst: map[string]int{}}
			ri.cols[col] = c
		}
		c.holds++
		c.byConst[spec.Filter.Equalities[col].String()]++
		if m := c.meanList(); best < 0 || m < best {
			r.col, best = col, m
		}
	}
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
	spec := w.Holds[i]
	for _, col := range r.counted {
		c := ri.cols[col]
		if c == nil {
			continue
		}
		c.holds--
		val := spec.Filter.Equalities[col].String()
		if c.byConst[val]--; c.byConst[val] <= 0 {
			delete(c.byConst, val)
		}
		if c.holds <= 0 {
			delete(ri.cols, col)
		}
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

// All returns every live watch, as copies that are safe to read without the
// lock: RefreshRels replaces a watch's Holds while others may be reading.
func (x *Index) All() []*Watch {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var out []*Watch
	for _, subs := range x.byStream {
		for _, w := range subs {
			out = append(out, &Watch{StreamID: w.StreamID, Label: w.Label, Holds: w.Holds})
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

// CheckIdentity is called after Replace, whose holds were checked against the
// catalog when they were granted. Until the catalog refreshes, the WAL may
// already describe a replica identity that does not cover a hold's columns;
// such a watch is removed and the reason returned. It also fails when the
// watch is gone, cut by a change or by OutsideIdentity since it was added.
// OutsideIdentity runs under the same lock, so whichever comes second sees the
// other.
func (x *Index) CheckIdentity(streamID, label string) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	return x.checkLocked(streamID, label)
}

// Activate registers the shape a watch added with Add keeps alive, once its
// holds were found. register runs under the index's lock, and only after the
// checks of CheckIdentity pass: the watch is still the one added, and its holds
// read columns of the replica identity the WAL last described. A change that
// cuts the watch takes the same lock. If it comes first, the shape is never
// registered; if it comes after, its cut drops the registered shape before the
// reader routes another change. So the shape receives no change unless its
// holds existed when they were checked and none was cut since. When register
// fails, the watch is removed and register's error returned.
func (x *Index) Activate(streamID, label string, register func() error) error {
	x.mu.Lock()
	defer x.mu.Unlock()
	if err := x.checkLocked(streamID, label); err != nil {
		return err
	}
	if err := register(); err != nil {
		x.removeLocked(streamID, label)
		return err
	}
	return nil
}

func (x *Index) checkLocked(streamID, label string) error {
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
	// A new slice, not an edit in place: OnChange evaluates holds it read under
	// the read lock after letting go of it.
	holds := slices.Clone(w.Holds)
	for i, rel := range rels {
		if rel == nil {
			continue
		}
		x.unindexLocked(w, i)
		holds[i].Rel = rel
	}
	w.Holds = holds
	for i, rel := range rels {
		if rel != nil {
			x.indexLocked(w, i)
		}
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
//
// The candidates are found under the read lock and evaluated with no lock
// held: evaluating is most of the work, and stream opens and /token (Add,
// Replace) must not wait on it. Only removing a broken watch takes the write
// lock.
func (x *Index) OnChange(oid uint32, op byte, oldRow, newRow expr.Row) []Cut {
	if op == 'U' && oldRow == nil {
		return nil
	}
	var broken []candidate
	for _, c := range x.candidates(oid, oldRow, newRow) {
		if reason, cut := watchBroken(c.holds, oid, op, oldRow, newRow); cut {
			c.reason = reason
			broken = append(broken, c)
		}
	}
	if len(broken) == 0 {
		return nil
	}
	x.mu.Lock()
	defer x.mu.Unlock()
	var cuts []Cut
	for _, c := range broken {
		w := x.byStream[c.w.StreamID][c.w.Label]
		if w != c.w {
			// Removed or replaced since it was read; a leftover, if one were
			// still indexed, leaves the index and cuts nothing.
			x.unindexWatchLocked(c.w)
			if w == nil {
				continue
			}
			// The change answers to the replacement, as it would have had the
			// replacement been indexed first. (A replacement's grant also
			// runs EXISTS after Replace, which sees any change the reader has
			// already received.)
			reason, cut := watchBroken(w.Holds, oid, op, oldRow, newRow)
			if !cut {
				continue
			}
			c.reason = reason
		}
		x.removeLocked(w.StreamID, w.Label)
		cuts = append(cuts, Cut{StreamID: w.StreamID, Label: w.Label, Reason: c.reason})
	}
	return cuts
}

// candidate is a watch that may be affected by a change, with the holds it
// had when it was read.
type candidate struct {
	w      *Watch
	holds  []Spec
	reason string
}

// candidates finds the watches listed under the change's values, and the
// unindexed ones, under the read lock.
func (x *Index) candidates(oid uint32, oldRow, newRow expr.Row) []candidate {
	x.mu.RLock()
	defer x.mu.RUnlock()
	ri := x.rels[oid]
	if ri == nil {
		return nil
	}

	seen := map[*Watch]bool{}
	var out []candidate
	add := func(list []*Watch) {
		for _, w := range list {
			if !seen[w] {
				seen[w] = true
				out = append(out, candidate{w: w, holds: w.Holds})
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
	return out
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

func watchBroken(holds []Spec, oid uint32, op byte, oldRow, newRow expr.Row) (string, bool) {
	for _, spec := range holds {
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

// MissingError is a hold row that does not exist: the grant is denied. Any other
// error from Check is a check that could not be made.
type MissingError struct {
	Rel, Filter string
}

func (e *MissingError) Error() string {
	return fmt.Sprintf("hold on %s with filter %s does not exist", e.Rel, e.Filter)
}

// CheckError is an EXISTS that could not be run or did not finish: it says
// nothing about the hold.
type CheckError struct {
	Rel string
	Err error
}

func (e *CheckError) Error() string { return fmt.Sprintf("hold exists on %s: %v", e.Rel, e.Err) }
func (e *CheckError) Unwrap() error { return e.Err }

// Querier is the subset of pgxpool.Pool Check needs.
type Querier interface {
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// Check verifies that the row of each hold is present, with one round trip:
// every distinct hold is asked about once, however many grants of a request
// name it. It returns one error per spec, in order: nil when the row exists, a
// *MissingError when it does not (a denied grant, not an empty shape), or a
// *CheckError when the check could not be made. Zero rows on the subscribed
// table are irrelevant here and are never queried.
func Check(ctx context.Context, q Querier, specs []Spec) []error {
	errs := make([]error, len(specs))
	asked := map[string]int{}
	query := make([]int, len(specs)) // the query answering each spec
	var first []int                  // the first spec each query answers
	var b pgx.Batch
	for i, spec := range specs {
		if spec.Rel == nil || spec.Filter == nil {
			errs[i], query[i] = fmt.Errorf("hold is missing a relation or filter"), -1
			continue
		}
		sql, args := ExistsSQL(spec)
		key := sql + "\x00" + fmt.Sprintf("%q", args)
		j, ok := asked[key]
		if !ok {
			j = b.Len()
			asked[key] = j
			first = append(first, i)
			b.Queue(sql, args...)
		}
		query[i] = j
	}
	if b.Len() == 0 {
		return errs
	}
	answers := make([]error, b.Len())
	br := q.SendBatch(ctx, &b)
	for j, i := range first {
		spec := specs[i]
		var ok bool
		if err := br.QueryRow().Scan(&ok); err != nil {
			answers[j] = &CheckError{Rel: spec.Rel.FullName(), Err: err}
		} else if !ok {
			answers[j] = &MissingError{Rel: spec.Rel.FullName(), Filter: spec.Filter.Describe()}
		}
	}
	// Every answer was read above; Close's error repeats the first failure.
	_ = br.Close()
	for i, j := range query {
		if j >= 0 {
			errs[i] = answers[j]
		}
	}
	return errs
}

// Size counts what the index holds, for tests and diagnostics: watches by
// stream, constants and entries in the routing lists (with their capacity),
// and unindexed watches.
type Size struct {
	Watches, Relations, Constants, Routed, RoutedCap, Unindexed int
	// Counted is the constants counted per candidate column (relIndex.cols).
	Counted int
}

func (x *Index) Size() Size {
	x.mu.RLock()
	defer x.mu.RUnlock()
	var s Size
	for _, subs := range x.byStream {
		s.Watches += len(subs)
	}
	s.Relations = len(x.rels)
	for _, ri := range x.rels {
		for _, byConst := range ri.byColumn {
			s.Constants += len(byConst)
			for _, list := range byConst {
				s.Routed += len(list)
				s.RoutedCap += cap(list)
			}
		}
		s.Unindexed += len(ri.unindexed)
		for _, c := range ri.cols {
			s.Counted += len(c.byConst)
		}
	}
	return s
}
