package server

import (
	"cmp"
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/encode"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// tupleRow adapts a decoded tuple to the expression evaluator.
//
// The `known` return is the important part. An unchanged TOASTed value reports
// known=false, which propagates as expr.ErrUnknown and forces the caller to mark
// the event degraded instead of silently deciding. Reporting it as NULL would be
// a correctness bug in whichever direction the policy happened to compare.
//
// Text is read in place: a value's string is a view of the tuple's bytes, which
// the decoder never writes again (pgoutput.Column), so reading a column copies
// nothing. A string kept past the change must be copied (strings.Clone), or it
// keeps the whole message alive.
type tupleRow struct {
	rel *pgoutput.Relation
	t   *pgoutput.Tuple
}

func (r tupleRow) Column(name string) (expr.Value, bool) {
	if r.t == nil {
		return expr.Null, false
	}
	i := r.rel.ColumnIndex(name)
	if i < 0 || i >= len(r.t.Columns) {
		return expr.Null, false
	}
	c := r.t.Columns[i]
	switch c.Kind {
	case pgoutput.ColNull:
		return expr.Null, true
	case pgoutput.ColUnchanged:
		return expr.Null, false
	case pgoutput.ColText:
		return expr.ParseText(r.rel.Columns[i].TypeName, inPlace(c.Data)), true
	}
	// Binary format is not requested; carry it through as unknown rather than
	// guessing at an encoding.
	return expr.Null, false
}

// key reads a column as the routing indexes list it, by the Value.String() of
// its value. The text of most types already is that, and is returned in place,
// without parsing; bool, float and numeric columns are parsed.
func (r tupleRow) key(name string) (string, bool) {
	if r.t == nil {
		return "", false
	}
	i := r.rel.ColumnIndex(name)
	if i < 0 || i >= len(r.t.Columns) {
		return "", false
	}
	if c := r.t.Columns[i]; c.Kind == pgoutput.ColText && expr.TextIsCanonical(r.rel.Columns[i].TypeName) {
		return inPlace(c.Data), true
	}
	v, ok := r.Column(name)
	return v.String(), ok
}

// inPlace is a tuple's text as a string, without copying it (see tupleRow).
func inPlace(b []byte) string { return unsafe.String(unsafe.SliceData(b), len(b)) }

// OnStart fixes the start of what the resume buffer can cover, and lets
// streams open from the first start on.
func (s *Server) OnStart(lsn uint64) {
	s.wakeQueued()
	s.txnBegan = time.Time{}
	if lsn != 0 {
		s.hub.Rings().SetStart(lsn)
	}
	s.replicated.Store(true)
}

func (s *Server) OnBegin(lsn uint64, commitTime time.Time, xid uint32) {
	metrics.WALMessages.WithLabelValues("begin").Inc()
	s.txnBegan = time.Now()
}

func (s *Server) OnCommit(lsn uint64, commitTime time.Time) {
	s.wakeQueued()
	s.txnBegan = time.Time{}
	metrics.WALMessages.WithLabelValues("commit").Inc()
	if s.reader == nil {
		return
	}
	metrics.WALLsn.WithLabelValues("confirmed").Set(float64(s.reader.ConfirmedLSN()))
	metrics.WALLsn.WithLabelValues("received").Set(float64(s.reader.ReceivedLSN()))
	if r, c := s.reader.ReceivedLSN(), s.reader.ConfirmedLSN(); r > c {
		metrics.WALLagBytes.Set(float64(r - c))
	} else {
		metrics.WALLagBytes.Set(0)
	}
}

// OnRelation reacts to a schema change.
//
// Because DDL is not replicated, a re-sent Relation message is the only in-band
// signal that the schema moved. Sluice forces every subscription to be
// re-resolved on the next catalog tick, and tells the affected subscriptions
// rather than letting them quietly serve a stale projection.
func (s *Server) OnRelation(old, nw *pgoutput.Relation) {
	metrics.WALMessages.WithLabelValues("relation").Inc()
	delete(s.relMetrics, nw.OID)

	// Resolve the revocation tables to OIDs here, once, so the per-change check
	// is an integer comparison.
	if s.cfg.RevocationEnabled {
		switch name := nw.FullName(); {
		case name == s.cfg.SessionsTable:
			s.sessionsOID.Store(nw.OID)
		case s.cfg.UsersTable != "" && name == s.cfg.UsersTable:
			s.usersOID.Store(nw.OID)
		}
	}

	// A hold may read only replica-identity columns: an UPDATE without an old
	// tuple cuts nothing on that promise. The catalog refresh re-checks every
	// hold, but later and on its own goroutine; this checks the holds on nw
	// against the identity the WAL carries now, before the reader dispatches
	// another change. It runs on the first Relation message after a restart
	// too, when there is no previous definition to compare with.
	if s.holds != nil {
		cuts := s.holds.OutsideIdentity(&catalog.Relation{
			OID: nw.OID, Schema: nw.Namespace, Name: nw.Name,
			ReplicaIdentity: nw.ReplicaIdentity, ReplicaIdentityColumns: nw.KeyColumns(),
		})
		if len(cuts) > 0 {
			s.log.Warn("hold filters read columns outside the replica identity; their shapes were dropped",
				"relation", nw.FullName(), "replica_identity", string(nw.ReplicaIdentity), "shapes", len(cuts))
			s.applyHoldCuts(cuts)
		}
	}

	if old == nil {
		return
	}
	if sameColumns(old, nw) && old.ReplicaIdentity == nw.ReplicaIdentity {
		return
	}
	s.log.Warn("relation definition changed",
		"relation", nw.FullName(),
		"replica_identity_before", string(old.ReplicaIdentity),
		"replica_identity_after", string(nw.ReplicaIdentity))

	s.scheduleCatalogRefresh()

	for _, sub := range s.reg.All() {
		if sub.Relation.OID != nw.OID {
			continue
		}
		hub.SendWarning(streamOf(sub), event.Warning{
			Sub:     sub.Label,
			Code:    "schema_changed",
			Message: "the definition of " + nw.FullName() + " changed; authorization for this subscription is re-resolved on the next catalog refresh",
			Effect:  "columns that no longer exist are absent from future events; new columns are not added to the projection",
			Remedy:  "resubscribe to pick up new columns",
		})
	}
}

func sameColumns(a, b *pgoutput.Relation) bool {
	if len(a.Columns) != len(b.Columns) {
		return false
	}
	for i := range a.Columns {
		if a.Columns[i].Name != b.Columns[i].Name ||
			a.Columns[i].TypeOID != b.Columns[i].TypeOID ||
			a.Columns[i].IsKey != b.Columns[i].IsKey {
			return false
		}
	}
	return true
}

// OnTruncate cuts the shapes held by a row of a truncated table, then delivers
// TRUNCATE to every subscription on a truncated table that asked for it.
func (s *Server) OnTruncate(rels []uint32) {
	metrics.WALMessages.WithLabelValues("truncate").Inc()
	for _, oid := range rels {
		s.applyHoldCuts(s.holds.OnTruncate(oid))
		for _, sub := range s.reg.All() {
			if sub.Relation.OID != oid || !sub.Ops.Truncate {
				continue
			}
			streamOf(sub).Send(event.Event{Kind: event.KindChange, Data: event.Change{
				Sub: sub.Label, Op: "TRUNCATE",
				Schema: sub.Relation.Schema, Table: sub.Relation.Name,
			}})
		}
	}
}

// applyHoldCuts drops the shapes whose holds no longer hold. The watches are
// already out of the hold index.
func (s *Server) applyHoldCuts(cuts []hold.Cut) {
	for _, cut := range cuts {
		if st, ok := s.hub.Get(cut.StreamID); ok {
			s.dropShape(st, cut.Label, event.Error{
				Code:    "shape_not_authorized",
				Message: cut.Reason,
			})
		} else if removed := s.reg.Remove(cut.StreamID, cut.Label); removed != nil {
			s.decSubMetric(removed)
		}
	}
}

// OnMessage handles pg_logical_emit_message.
//
// This is Sluice's database-originated broadcast, and it needs no table, no
// outbox worker and no LISTEN/NOTIFY. A transactional message is ordered inside
// its transaction and atomic with the DML, so if the transaction rolls back the
// message never existed -- the dual-write problem solved for free.
func (s *Server) OnMessage(m *pgoutput.Message, commitLSN uint64) error {
	metrics.WALMessages.WithLabelValues("message").Inc()
	if !strings.HasPrefix(m.MessagePrefix, s.cfg.MessagePrefix) {
		// Not ours. Other tools may use the same slot-independent mechanism.
		return nil
	}
	channel := strings.TrimPrefix(m.MessagePrefix, s.cfg.MessagePrefix)
	if channel == "" {
		return nil
	}

	// Content that is not JSON is delivered as a JSON string. A JSON object
	// with an "event" field is an envelope: that field names the broadcast
	// event and "payload", if present, becomes its payload.
	payload := json.RawMessage(m.MessageContent)
	if !json.Valid(payload) {
		b, _ := json.Marshal(string(m.MessageContent))
		payload = b
	}

	evName := "message"
	var envelope struct {
		Event   string          `json:"event"`
		Payload json.RawMessage `json:"payload"`
	}
	if err := json.Unmarshal(payload, &envelope); err == nil && envelope.Event != "" {
		evName = envelope.Event
		if envelope.Payload != nil {
			payload = envelope.Payload
		}
	}

	lsn := ""
	if m.MessageTransactional {
		lsn = reader.FormatLSN(commitLSN)
	}
	n := s.hub.PublishBroadcast(channel, evName, "", "database", lsn, payload, true, "")
	metrics.BroadcastPublished.WithLabelValues(namespaceOf(channel), "database").Inc()
	if n == 0 {
		s.log.Debug("database broadcast had no subscribers", "channel", channel)
	}
	return nil
}

// OnChange is the hot path. There is deliberately no database access in it for
// Tier A and Tier B subscriptions.
func (s *Server) OnChange(m *pgoutput.Message, rel *pgoutput.Relation, commitLSN uint64, commitTime time.Time) error {
	op := opName(m.Type)
	// Events a transaction queued wait for its commit, or at most txnWakeDelay
	// and one row: checked at every row, delivered or not.
	if len(s.wakes) > 0 && time.Since(s.txnBegan) >= txnWakeDelay {
		s.wakeQueued()
	}
	rm := s.metricsOf(rel)
	rm.wal[opIndex(m.Type)].Inc()
	rm.changes[opIndex(m.Type)].Inc()

	// Session revocation rides the same slot, so a sign-out reaches Sluice in
	// milliseconds with no polling and no extra query.
	if s.cfg.RevocationEnabled {
		s.handleRevocation(m, rel, op, commitLSN)
	}

	start := time.Now()
	defer func() { rm.dispatch.Observe(time.Since(start).Seconds()) }()

	newRow := tupleRow{rel: rel, t: m.New}
	oldRow := tupleRow{rel: rel, t: m.Old}

	if s.holds != nil {
		var oldR, newR expr.Row
		if m.Old != nil {
			oldR = oldRow
		}
		if m.New != nil {
			newR = newRow
		}
		s.applyHoldCuts(s.holds.OnChange(rel.OID, m.Type, oldR, newR))
	}

	// Buffer for reconnect replay regardless of who is currently listening.
	// Before routing: a subscription installed in between is then either
	// routed this change or finds it in the buffer when it replays. Not the
	// revocation tables, which no shape may be on: nothing would replay them.
	var seq uint64
	if !s.revocationTable(rel.OID) {
		seq = s.hub.Rings().Append(hub.RingEntry{
			LSN: commitLSN, CommitTime: commitTime, Op: m.Type,
			Relation: rel, New: m.New, Old: m.Old, OldIsKey: m.OldIsKey,
		})
	}

	// Route on both tuples. Routing only on the new one would miss a row that
	// left a shape (supabase/walrus#64).
	candidates := s.reg.Candidates(rel.OID, newRow.key, oldRow.key)
	rm.candidates.Observe(float64(len(candidates)))

	// Built once per change and shared by every subscriber: the encodings it
	// memoises are only computed if some subscription actually needs them.
	tuples := s.newTuples(rel, m)

	for _, sub := range candidates {
		if !wants(sub, m.Type) || !sub.Backfill.Deliver(seq) {
			continue
		}
		s.deliver(sub, m, rel, newRow, oldRow, tuples, commitLSN, commitTime, op, false)
	}
	return nil
}

// relMetrics are the metrics a relation's changes update, each looked up by its
// labels once rather than for every change. Reader goroutine only; a Relation
// message drops them, since it may rename the relation.
type relMetrics struct {
	wal, changes         [3]prometheus.Counter // by opIndex
	dispatch, candidates prometheus.Observer
}

func (s *Server) metricsOf(rel *pgoutput.Relation) *relMetrics {
	if rm := s.relMetrics[rel.OID]; rm != nil {
		return rm
	}
	rm := &relMetrics{
		dispatch:   metrics.DispatchSeconds.WithLabelValues(rel.Namespace, rel.Name),
		candidates: metrics.RoutingCandidates.WithLabelValues(rel.Namespace, rel.Name),
	}
	for i, typ := range [3]byte{pgoutput.MsgInsert, pgoutput.MsgUpdate, pgoutput.MsgDelete} {
		op := opName(typ)
		rm.wal[i] = metrics.WALMessages.WithLabelValues(strings.ToLower(op))
		rm.changes[i] = metrics.Changes.WithLabelValues(rel.Namespace, rel.Name, op)
	}
	if s.relMetrics == nil {
		s.relMetrics = map[uint32]*relMetrics{}
	}
	s.relMetrics[rel.OID] = rm
	return rm
}

// opIndex is a row change's place in relMetrics.
func opIndex(typ byte) int {
	switch typ {
	case pgoutput.MsgInsert:
		return 0
	case pgoutput.MsgUpdate:
		return 1
	}
	return 2
}

// wants reports whether a subscription takes changes of this kind: its ops,
// and updates that may enter or leave it when it asked for transitions.
func wants(sub *registry.Subscription, typ byte) bool {
	return opWanted(sub.Ops, typ) || (sub.Transitions && typ == pgoutput.MsgUpdate)
}

// sendChange queues a change. A live change never waits: the reader serves
// every stream, so one slow client must not hold it. A backfilled one -- a
// replay off the replication path -- waits for room instead of overflowing.
func (s *Server) sendChange(st *hub.Stream, ev event.Event, backfill bool) {
	if backfill {
		st.SendBackfill(s.ctx, ev)
		return
	}
	// Inside a transaction, a stream's writer is woken at the commit, so that
	// the events the transaction brings it go out in one write and one flush;
	// once the transaction has been dispatching for txnWakeDelay, at once.
	if !s.txnBegan.IsZero() {
		if time.Since(s.txnBegan) < txnWakeDelay {
			if st.SendDeferred(ev) && !s.woken[st] {
				if s.woken == nil {
					s.woken = map[*hub.Stream]bool{}
				}
				s.woken[st] = true
				s.wakes = append(s.wakes, st)
			}
			return
		}
		s.wakeQueued()
	}
	st.Send(ev)
}

// txnWakeDelay bounds how long the events of a transaction wait for its
// commit in a stream's queue: past it, they are woken at the next row the
// reader dispatches, and later events at once.
const txnWakeDelay = 5 * time.Millisecond

// wakeQueued wakes the writers of the streams the current transaction queued
// events for.
func (s *Server) wakeQueued() {
	for _, st := range s.wakes {
		st.Wake()
	}
	s.wakes = s.wakes[:0]
	clear(s.woken)
}

// tuplePair is one change as every subscriber sees it: the tuples the
// authorizer reads (the new one for INSERT/UPDATE, the old one for DELETE and
// shape-exit detection), and each tuple's column values encoded for the wire.
// The encoding is done once, on first use, and shared by every subscriber's
// projection; so is each distinct projection. A tuplePair is used by one
// goroutine at a time.
type tuplePair struct {
	New, Old *authz.Tuple

	rel              *pgoutput.Relation
	types            map[uint32]*encode.Type
	newVals, oldVals []any
	newProj, oldProj []projection
}

// projection is one tuple projected onto one column list.
type projection struct {
	columns   []string
	rec       *event.Row
	unchanged []string
	size      int
}

// maxProjections bounds how many distinct column lists a change memoises.
// Subscriptions to a table almost always project one of a few lists; past the
// bound, projections are built per subscription.
const maxProjections = 8

func (s *Server) newTuples(rel *pgoutput.Relation, m *pgoutput.Message) *tuplePair {
	key := cmp.Or(m.New, m.Old)
	p := &tuplePair{rel: rel}
	if s.cat != nil {
		p.types = s.cat.Types()
	}
	if m.New != nil {
		p.New = &authz.Tuple{Op: m.Type, Rel: rel, Row: m.New, Key: key}
	}
	if m.Old != nil {
		p.Old = &authz.Tuple{Op: m.Type, Rel: rel, Row: m.Old, Key: key}
	}
	return p
}

// values returns a tuple's encoded column values, by column position.
func (p *tuplePair) values(t *pgoutput.Tuple, cache *[]any) []any {
	if *cache == nil && t != nil {
		*cache = encodeTuple(p.rel, t, p.types)
	}
	return *cache
}

func (p *tuplePair) newValues() []any {
	if p.New == nil {
		return nil
	}
	return p.values(p.New.Row, &p.newVals)
}

func (p *tuplePair) oldValues() []any {
	if p.Old == nil {
		return nil
	}
	return p.values(p.Old.Row, &p.oldVals)
}

func (p *tuplePair) newProjection(columns []string) projection {
	if p.New == nil {
		return projection{}
	}
	return p.projection(p.New.Row, p.newValues(), columns, &p.newProj)
}

func (p *tuplePair) oldProjection(columns []string) projection {
	if p.Old == nil {
		return projection{}
	}
	return p.projection(p.Old.Row, p.oldValues(), columns, &p.oldProj)
}

func (p *tuplePair) projection(t *pgoutput.Tuple, vals []any, columns []string, memo *[]projection) projection {
	for _, pr := range *memo {
		if slices.Equal(pr.columns, columns) {
			return pr
		}
	}
	rec, unchanged, size := project(p.rel, t, vals, columns)
	pr := projection{columns: columns, rec: event.NewRow(rec), unchanged: unchanged, size: size}
	if len(*memo) < maxProjections {
		*memo = append(*memo, pr)
	}
	return pr
}

// encodeTuple encodes every text column of a tuple as to_jsonb would.
func encodeTuple(rel *pgoutput.Relation, t *pgoutput.Tuple, types map[uint32]*encode.Type) []any {
	out := make([]any, len(t.Columns))
	for i, c := range t.Columns {
		if c.Kind != pgoutput.ColText || i >= len(rel.Columns) {
			continue
		}
		out[i] = typeOf(types, rel.Columns[i].TypeOID).Value(string(c.Data))
	}
	return out
}

func typeOf(types map[uint32]*encode.Type, oid uint32) *encode.Type {
	if t := types[oid]; t != nil {
		return t
	}
	return encode.Builtin(oid)
}

// deliver evaluates one subscription against one change and emits at most one
// event. backfill is set for replays from the resume buffer; see sendChange.
func (s *Server) deliver(
	sub *registry.Subscription,
	m *pgoutput.Message,
	rel *pgoutput.Relation,
	newRow, oldRow tupleRow,
	tuples *tuplePair,
	commitLSN uint64,
	commitTime time.Time,
	op string,
	backfill bool,
) {
	st := streamOf(sub)
	if st == nil {
		return
	}
	id := st.Identity()

	// Pushed revocation beats any cached decision.
	if s.revoker != nil && s.revoker.Revoked(id) {
		metrics.Revocations.WithLabelValues("session").Inc()
		hub.SendError(st, event.Error{Code: "session_revoked",
			Message: "the session backing this stream no longer exists"})
		s.hub.Close(st.StreamID(), "session_revoked")
		return
	}

	fctx := &expr.Context{Claims: id.Claims, ClaimsJSON: id.ClaimsRaw, Now: time.Now()}
	visible := func(r tupleRow) (bool, bool) {
		fctx.Row = r
		return expr.Visible(sub.Filter.Node, fctx)
	}

	matchNew, unknownNew := false, false
	if m.New != nil {
		matchNew, unknownNew = visible(newRow)
	}
	matchOld, unknownOld := false, false
	switch {
	case m.Old != nil:
		matchOld, unknownOld = visible(oldRow)
	case m.Type == pgoutput.MsgUpdate:
		// No old tuple means the replica identity key did not change. The
		// columns the filter reads may still have (only a replica identity
		// that covers them would say), so presume the row was already in the
		// shape: a matching row is an UPDATE, not an entry, and a leave cannot
		// be detected -- which is what replica_identity_insufficient warns.
		matchOld, unknownOld = matchNew, unknownNew
	}

	emitOp := op
	transition := ""
	var authRow tupleRow
	var authTuple *authz.Tuple

	switch m.Type {
	case pgoutput.MsgInsert:
		if !matchNew {
			return
		}
		authRow, authTuple = newRow, tuples.New

	case pgoutput.MsgDelete:
		// With REPLICA IDENTITY DEFAULT the old tuple carries only the primary
		// key, so a filter on any other column cannot be evaluated. Rather than
		// guess, withhold and say why.
		if unknownOld {
			s.noteUnknown(rel, "DELETE")
			if sub.Filter.Node != nil && !expr.IsAlwaysTrue(sub.Filter.Node) {
				return
			}
		} else if !matchOld {
			return
		}
		authRow, authTuple = oldRow, tuples.Old

	case pgoutput.MsgUpdate:
		switch {
		case matchNew && matchOld:
			authRow, authTuple = newRow, tuples.New
		case matchNew && !matchOld:
			// The row entered the shape. Present it as an INSERT so a client that
			// keyed a local cache does the right thing.
			if !sub.Transitions && !sub.Ops.Update {
				return
			}
			emitOp, transition = "INSERT", "enter"
			authRow, authTuple = newRow, tuples.New
		case !matchNew && matchOld:
			if !sub.Transitions {
				return
			}
			emitOp, transition = "DELETE", "leave"
			authRow, authTuple = oldRow, tuples.Old
		default:
			if unknownNew || unknownOld {
				s.noteUnknown(rel, "UPDATE")
			}
			return
		}
		if !sub.Ops.Update && transition == "" {
			return
		}
	}

	// Authorization. Tier A is a field read; Tier B is an in-process evaluation;
	// only Tier C touches the database.
	//
	// Loaded once: a refresh can publish a new decision at any tick, and every
	// field read for this change has to belong to the same generation.
	dec := sub.Decision.Load()
	tier := dec.EffectiveTier()
	allowed, unknown := s.authz.Visible(s.ctx, dec, id, sub.Relation, authRow, authTuple)
	if tier == authz.TierC {
		metrics.TierCProbes.WithLabelValues(rel.Namespace, rel.Name).Inc()
		if unknown {
			metrics.TierCWithheld.WithLabelValues(rel.Namespace, rel.Name).Inc()
		}
	}
	if unknown {
		// The decision could not be made -- almost always a DELETE whose old tuple
		// is too narrow for the policy, under a Tier C predicate that cannot be
		// evaluated in process.
		//
		// Withholding is the default. Delivering would tell the subscriber that a
		// row matching their filter was deleted without ever checking whether they
		// were allowed to know that, which is exactly the leak walrus avoided by
		// truncating to primary keys. Operators who prefer upstream's trade-off
		// can opt in with SLUICE_DEGRADED_DELETES=deliver.
		s.noteUnknown(rel, emitOp)
		if !(s.cfg.DegradedDeletes == "deliver" && m.Type == pgoutput.MsgDelete) {
			return
		}
	} else if !allowed {
		return
	}
	degraded := ""
	if unknown {
		degraded = "delete_authz_unavailable"
	}

	newProj, oldProj := tuples.newProjection(sub.Columns), tuples.oldProjection(sub.Columns)
	rec, oldRec, unchangedNew := newProj.rec, oldProj.rec, newProj.unchanged
	for _, c := range unchangedNew {
		metrics.ToastUnchanged.WithLabelValues(rel.Namespace, rel.Name, c).Inc()
	}

	// One enormous row must not be able to evict a stream's whole queue. Trim to
	// the key columns so the client still learns which row changed and can
	// refetch it, and say so rather than delivering a silently partial record.
	if n := s.cfg.MaxChangeBytes; n > 0 && newProj.size+oldProj.size > n {
		rec = keyOnly(rec, sub.Relation.KeyColumns)
		oldRec = keyOnly(oldRec, sub.Relation.KeyColumns)
		unchangedNew = nil
		degraded = "change_too_large"
		metrics.ChangesTruncated.WithLabelValues(rel.Namespace, rel.Name).Inc()
	}

	ch := event.Change{
		Sub:        sub.Label,
		Op:         emitOp,
		Schema:     rel.Namespace,
		Table:      rel.Name,
		CommitLSN:  reader.FormatLSN(commitLSN),
		Seq:        sub.NextSeq(),
		Record:     rec,
		Old:        oldRec,
		Unchanged:  unchangedNew,
		Transition: transition,
		Degraded:   degraded,
	}
	if !commitTime.IsZero() {
		ch.CommitTime = commitTime.UTC().Format(time.RFC3339Nano)
	}

	s.sendChange(st, event.Event{
		Kind: event.KindChange,
		ID:   ch.CommitLSN + ":" + strconv.Itoa(ch.Seq),
		Data: ch,
	}, backfill)
}

func (s *Server) noteUnknown(rel *pgoutput.Relation, op string) {
	metrics.AuthzUnknown.WithLabelValues(rel.Namespace, rel.Name, op).Inc()
}

// revocationTable reports whether a relation is one of the revocation tables.
func (s *Server) revocationTable(oid uint32) bool {
	return s.cfg.RevocationEnabled && (oid == s.sessionsOID.Load() || oid == s.usersOID.Load())
}

// handleRevocation watches the sessions table and, when one is configured, the
// users table on the same slot.
//
// Verified against supabase/auth v2.195.0: sign-out DELETEs the auth.sessions
// row (LogoutSession/Logout/LogoutAllExceptMe are all DELETE), and the
// `session_id` JWT claim is that row's id. Watching refresh_tokens.revoked
// instead would miss every sign-out, because those rows vanish by cascade. Any
// identity service that deletes the session row on sign-out works the same way;
// SLUICE_JWT_SESSION_CLAIM names the claim that carries the row's id.
//
// The relation is matched by OID, resolved once per Relation message, so the
// per-change cost is an integer comparison rather than two string comparisons.
//
// No request asked for a revocation, so its log line has no request ID: it
// names the commit, and the session when there is one. Each stream it closes
// logs its own line under the ID of the request that opened it.
func (s *Server) handleRevocation(m *pgoutput.Message, rel *pgoutput.Relation, op string, commitLSN uint64) {
	switch rel.OID {
	case s.sessionsOID.Load():
		if op != "DELETE" {
			return
		}
		if v, ok := (tupleRow{rel: rel, t: m.Old}).Column("id"); ok {
			// The revoker keeps it, so it is copied out of the message.
			id := strings.Clone(v.String())
			s.revoker.RevokeSession(id)
			if n := s.closeStreamsForSession(id); n > 0 {
				s.log.Info("session revoked; its streams were closed",
					"session_id", strings.ToLower(id), "streams", n, "commit_lsn", reader.FormatLSN(commitLSN))
			}
		}
	case s.usersOID.Load():
		if op != "UPDATE" {
			return
		}
		row := tupleRow{rel: rel, t: m.New}
		idv, ok := row.Column("id")
		if !ok || idv.IsNull() {
			return
		}
		banned, ok := row.Column(s.cfg.UsersBanColumn)
		if !ok {
			return
		}
		until, active := banExpiry(banned, time.Now())
		if !active {
			s.revoker.UnbanUser(idv.String())
			return
		}
		// The revoker keeps it, so it is copied out of the message.
		id := strings.Clone(idv.String())
		s.revoker.BanUser(id, until)
		if n := s.closeStreamsForUser(id); n > 0 {
			s.log.Info("user banned; their streams were closed",
				"streams", n, "commit_lsn", reader.FormatLSN(commitLSN))
		}
	}
}

// banExpiry reads the ban column (GoTrue: auth.users.banned_until) and reports
// whether the ban is in force, and until when.
//
// A ban whose time has passed is no ban. The auth service leaves the column set
// when a timed ban runs out, so the next unrelated UPDATE of that user (a
// sign-in sets last_sign_in_at) must not close their streams. A value that
// cannot be read as a time is treated as a ban in force, the direction that
// cannot leak.
func banExpiry(v expr.Value, now time.Time) (time.Time, bool) {
	// The revoker caps every ban at its TTL, so "forever" only has to be later
	// than that.
	const forever = 100 * 365 * 24 * time.Hour
	if v.IsNull() {
		return time.Time{}, false
	}
	switch s := v.String(); s {
	case "infinity":
		return now.Add(forever), true
	case "-infinity":
		return time.Time{}, false
	default:
		until, ok := encode.ParseTimestampTZ(s)
		if !ok {
			return now.Add(forever), true
		}
		return until, until.After(now)
	}
}

// closeStreamsForSession closes the session's streams and returns how many.
func (s *Server) closeStreamsForSession(sessionID string) int {
	if sessionID == "" {
		return 0
	}
	sessionID = strings.ToLower(sessionID)
	n := 0
	for _, st := range s.hub.Streams() {
		if st.Identity().SessionID != sessionID {
			continue
		}
		metrics.Revocations.WithLabelValues("session").Inc()
		hub.SendError(st, event.Error{Code: "session_revoked",
			Message: "the session backing this stream was signed out"})
		s.hub.Close(st.StreamID(), "session_revoked")
		n++
	}
	return n
}

// closeStreamsForUser closes the user's streams and returns how many.
func (s *Server) closeStreamsForUser(userID string) int {
	userID = strings.ToLower(userID)
	if userID == "" {
		return 0
	}
	n := 0
	for _, st := range s.hub.Streams() {
		if strings.ToLower(st.Identity().Sub) != userID {
			continue
		}
		metrics.Revocations.WithLabelValues("ban").Inc()
		hub.SendError(st, event.Error{Code: "user_banned",
			Message: "this user has been banned"})
		s.hub.Close(st.StreamID(), "user_banned")
		n++
	}
	return n
}

// project builds the emitted record from exactly the subscription's columns and
// reports unchanged-TOAST columns explicitly. The key columns are already part
// of the projection when the caller may read them (see the oracles); nothing
// outside it is ever emitted, because the projection is what column grants and
// an issuer allowlist were intersected into.
//
// vals are the tuple's encoded values by column position. size approximates
// the record's encoded size from the text it came from, without a marshal.
func project(rel *pgoutput.Relation, t *pgoutput.Tuple, vals []any, columns []string) (rec map[string]any, unchanged []string, size int) {
	if t == nil {
		return nil, nil, 0
	}
	rec = make(map[string]any, len(columns))
	for i, col := range t.Columns {
		if i >= len(rel.Columns) {
			break
		}
		meta := rel.Columns[i]
		if !slices.Contains(columns, meta.Name) {
			continue
		}
		size += len(meta.Name) + len(col.Data) + 4
		switch col.Kind {
		case pgoutput.ColUnchanged:
			unchanged = append(unchanged, meta.Name)
		case pgoutput.ColText:
			rec[meta.Name] = vals[i]
		default:
			rec[meta.Name] = nil
		}
	}
	return rec, unchanged, size
}

// keyOnly reduces a projected row to the columns that identify it.
func keyOnly(rec *event.Row, keys []string) *event.Row {
	if rec == nil {
		return nil
	}
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		if v, ok := rec.Values[k]; ok {
			out[k] = v
		}
	}
	return event.NewRow(out)
}

// messageFromRing rebuilds a message from a buffered ring entry so that replay
// goes through exactly the same delivery path as live changes -- including
// re-filtering and re-authorization.
func messageFromRing(e hub.RingEntry) *pgoutput.Message {
	return &pgoutput.Message{
		Type:        e.Op,
		RelationOID: e.Relation.OID,
		New:         e.New,
		Old:         e.Old,
		OldIsKey:    e.OldIsKey,
		CommitLSN:   e.LSN,
		CommitTime:  e.CommitTime,
	}
}

func opName(t byte) string {
	switch t {
	case pgoutput.MsgInsert:
		return "INSERT"
	case pgoutput.MsgUpdate:
		return "UPDATE"
	case pgoutput.MsgDelete:
		return "DELETE"
	case pgoutput.MsgTruncate:
		return "TRUNCATE"
	}
	return "UNKNOWN"
}

func opWanted(o shape.Ops, t byte) bool {
	switch t {
	case pgoutput.MsgInsert:
		return o.Insert
	case pgoutput.MsgUpdate:
		return o.Update
	case pgoutput.MsgDelete:
		return o.Delete
	case pgoutput.MsgTruncate:
		return o.Truncate
	}
	return false
}

func streamOf(sub *registry.Subscription) *hub.Stream {
	st, _ := sub.Sink.(*hub.Stream)
	return st
}
