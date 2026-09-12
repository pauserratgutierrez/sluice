package server

import (
	"encoding/json"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/expr"
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
		return expr.ParseText(r.rel.Columns[i].TypeName, string(c.Data)), true
	}
	// Binary format is not requested; carry it through as unknown rather than
	// guessing at an encoding.
	return expr.Null, false
}

func (s *Server) OnBegin(lsn uint64, commitTime time.Time, xid uint32) {
	metrics.WALMessages.WithLabelValues("begin").Inc()
}

func (s *Server) OnCommit(lsn uint64, commitTime time.Time) {
	metrics.WALMessages.WithLabelValues("commit").Inc()
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
// signal that the schema moved. Sluice uses it to revalidate replica identity and
// to invalidate the policy cache, and it tells affected subscriptions rather than
// letting them quietly serve a stale projection.
func (s *Server) OnRelation(old, nw *pgoutput.Relation) {
	metrics.WALMessages.WithLabelValues("relation").Inc()

	// Resolve the revocation tables to OIDs here, once, so the per-change check
	// is an integer comparison.
	if s.cfg.RevocationEnabled {
		switch nw.FullName() {
		case s.cfg.SessionsTable:
			s.sessionsOID.Store(nw.OID)
		case s.cfg.UsersTable:
			s.usersOID.Store(nw.OID)
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
			Message: "the definition of " + nw.FullName() + " changed; the projection and authorization for this subscription have been re-resolved",
			Effect:  "columns that no longer exist are dropped from future events",
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

func (s *Server) OnTruncate(rels []uint32) {
	metrics.WALMessages.WithLabelValues("truncate").Inc()
	for _, oid := range rels {
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

	// A database-originated payload is trusted to be JSON but must not be able to
	// corrupt the SSE frame if it is not.
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
	ns := channel
	if i := strings.IndexByte(channel, ':'); i >= 0 {
		ns = channel[:i]
	}
	metrics.BroadcastPublished.WithLabelValues(ns, "database").Inc()
	if n == 0 {
		s.log.Debug("database broadcast had no subscribers", "channel", channel)
	}
	return nil
}

// OnChange is the hot path. There is deliberately no database access in it for
// Tier A and Tier B subscriptions.
func (s *Server) OnChange(m *pgoutput.Message, rel *pgoutput.Relation, commitLSN uint64, commitTime time.Time) error {
	op := opName(m.Type)
	metrics.WALMessages.WithLabelValues(strings.ToLower(op)).Inc()
	metrics.Changes.WithLabelValues(rel.Namespace, rel.Name, op).Inc()

	// Session revocation rides the same slot, so a sign-out reaches Sluice in
	// milliseconds with no polling and no extra query.
	if s.cfg.RevocationEnabled {
		s.handleRevocation(m, rel, op)
	}

	start := time.Now()
	defer func() {
		metrics.DispatchSeconds.WithLabelValues(rel.Namespace, rel.Name).
			Observe(time.Since(start).Seconds())
	}()

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
		for _, cut := range s.holds.OnChange(rel.OID, m.Type, oldR, newR) {
			if st, ok := s.hub.Get(cut.StreamID); ok {
				s.dropShape(st, cut.Label, event.Error{
					Code:    "shape_not_authorized",
					Message: cut.Reason,
				})
			} else {
				s.holds.Remove(cut.StreamID, cut.Label)
				if removed := s.reg.Remove(cut.StreamID, cut.Label); removed != nil {
					s.decSubMetric(removed)
				}
			}
		}
	}

	// Route on both tuples. Routing only on the new one would miss a row that
	// left a shape, which is supabase/walrus#64 -- still open upstream.
	candidates := s.reg.Candidates(rel.OID, func(col string) (expr.Value, bool) {
		if v, ok := newRow.Column(col); ok {
			return v, true
		}
		return oldRow.Column(col)
	})
	if m.Old != nil {
		for _, extra := range s.reg.Candidates(rel.OID, oldRow.Column) {
			if !slices.Contains(candidates, extra) {
				candidates = append(candidates, extra)
			}
		}
	}
	metrics.RoutingCandidates.WithLabelValues(rel.Namespace, rel.Name).Observe(float64(len(candidates)))

	// Buffer for reconnect replay regardless of who is currently listening.
	s.hub.Rings().Append(hub.RingEntry{
		LSN: commitLSN, CommitTime: commitTime, Op: m.Type,
		Relation: rel, New: m.New, Old: m.Old, OldIsKey: m.OldIsKey,
	})

	// Built once per change and shared by every subscriber: the JSON encoding it
	// memoises is only computed if some subscription actually needs it.
	tuples := newTuples(rel, m)

	for _, sub := range candidates {
		if !opWanted(sub.Ops, m.Type) && !(sub.Transitions && m.Type == pgoutput.MsgUpdate) {
			continue
		}
		s.deliver(sub, m, rel, newRow, oldRow, tuples, commitLSN, commitTime, op, false)
	}
	return nil
}

// tuplePair carries the authorization view of a change: the new tuple for
// INSERT/UPDATE, the old one for DELETE and for shape-exit detection.
type tuplePair struct{ New, Old *authz.Tuple }

func newTuples(rel *pgoutput.Relation, m *pgoutput.Message) tuplePair {
	pk := primaryKeyOf(rel, m)
	var p tuplePair
	if m.New != nil {
		p.New = &authz.Tuple{Op: m.Type, PK: pk, Rel: rel, Row: m.New}
	}
	if m.Old != nil {
		p.Old = &authz.Tuple{Op: m.Type, PK: pk, Rel: rel, Row: m.Old}
	}
	return p
}

// deliver evaluates one subscription against one change and emits at most one event.
func (s *Server) deliver(
	sub *registry.Subscription,
	m *pgoutput.Message,
	rel *pgoutput.Relation,
	newRow, oldRow tupleRow,
	tuples tuplePair,
	commitLSN uint64,
	commitTime time.Time,
	op string,
	snapshot bool,
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

	filterCtx := func(r tupleRow) *expr.Context {
		return &expr.Context{Row: r, Claims: id.Claims, ClaimsJSON: id.ClaimsRaw, Now: time.Now()}
	}

	matchNew, unknownNew := false, false
	if m.New != nil {
		matchNew, unknownNew = expr.Visible(sub.Filter.Node, filterCtx(newRow))
	}
	matchOld, unknownOld := false, false
	if m.Old != nil {
		matchOld, unknownOld = expr.Visible(sub.Filter.Node, filterCtx(oldRow))
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
	visible, unknown := s.authz.Visible(s.ctx, dec, id, sub.Relation, authRow, authTuple)
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
	} else if !visible {
		return
	}
	degraded := ""
	if unknown {
		degraded = "delete_authz_unavailable"
	}

	rec, unchangedNew := project(rel, m.New, sub.Columns)
	oldRec, _ := project(rel, m.Old, sub.Columns)
	for _, c := range unchangedNew {
		metrics.ToastUnchanged.WithLabelValues(rel.Namespace, rel.Name, c).Inc()
	}

	// One enormous row must not be able to evict a stream's whole queue. Trim to
	// the replica identity so the client still learns which row changed and can
	// refetch it, and say so rather than delivering a silently partial record.
	if n := s.cfg.MaxChangeBytes; n > 0 && approxSize(rec)+approxSize(oldRec) > n {
		rec = keyOnly(rec, sub.Relation.ReplicaIdentityColumns)
		oldRec = keyOnly(oldRec, sub.Relation.ReplicaIdentityColumns)
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
		Snapshot:   snapshot,
	}
	if !commitTime.IsZero() {
		ch.CommitTime = commitTime.UTC().Format(time.RFC3339Nano)
	}

	if !st.Send(event.Event{
		Kind: event.KindChange,
		ID:   ch.CommitLSN + ":" + strconv.Itoa(ch.Seq),
		Data: ch,
	}) {
		metrics.StreamDropped.WithLabelValues("change", st.CloseCode()).Inc()
	}
}

func (s *Server) noteUnknown(rel *pgoutput.Relation, op string) {
	metrics.AuthzUnknown.WithLabelValues(rel.Namespace, rel.Name, op).Inc()
}

// handleRevocation watches auth.sessions and auth.users on the same slot.
//
// Verified against supabase/auth v2.195.0: sign-out DELETEs the auth.sessions
// row (LogoutSession/Logout/LogoutAllExceptMe are all DELETE), and the
// `session_id` JWT claim is that row's id. Watching refresh_tokens.revoked
// instead would miss every sign-out, because those rows vanish by cascade.
//
// The relation is matched by OID, resolved once per Relation message, so the
// per-change cost is an integer comparison rather than two string comparisons.
func (s *Server) handleRevocation(m *pgoutput.Message, rel *pgoutput.Relation, op string) {
	switch rel.OID {
	case s.sessionsOID.Load():
		if op != "DELETE" {
			return
		}
		if v, ok := (tupleRow{rel: rel, t: m.Old}).Column("id"); ok {
			s.revoker.RevokeSession(v.String())
			s.closeStreamsForSession(v.String())
		}
	case s.usersOID.Load():
		if op != "UPDATE" {
			return
		}
		row := tupleRow{rel: rel, t: m.New}
		banned, ok := row.Column("banned_until")
		if !ok || banned.IsNull() {
			return
		}
		if idv, ok := row.Column("id"); ok {
			s.revoker.BanUser(idv.String())
			s.closeStreamsForUser(idv.String())
		}
	}
}

func (s *Server) closeStreamsForSession(sessionID string) {
	if sessionID == "" {
		return
	}
	for _, st := range s.hub.Streams() {
		if st.Identity().SessionID != sessionID {
			continue
		}
		metrics.Revocations.WithLabelValues("session").Inc()
		hub.SendError(st, event.Error{Code: "session_revoked",
			Message: "the session backing this stream was signed out"})
		s.hub.Close(st.StreamID(), "session_revoked")
	}
}

func (s *Server) closeStreamsForUser(userID string) {
	userID = strings.ToLower(userID)
	if userID == "" {
		return
	}
	for _, st := range s.hub.Streams() {
		if strings.ToLower(st.Identity().Sub) != userID {
			continue
		}
		metrics.Revocations.WithLabelValues("ban").Inc()
		hub.SendError(st, event.Error{Code: "user_banned",
			Message: "this user has been banned"})
		s.hub.Close(st.StreamID(), "user_banned")
	}
}

// project builds the emitted record, honouring the column projection and
// reporting unchanged-TOAST columns explicitly.
func project(rel *pgoutput.Relation, t *pgoutput.Tuple, columns []string) (map[string]any, []string) {
	if t == nil {
		return nil, nil
	}
	want := map[string]bool{}
	for _, c := range columns {
		want[c] = true
	}
	out := make(map[string]any, len(t.Columns))
	var unchanged []string

	for i, col := range t.Columns {
		if i >= len(rel.Columns) {
			break
		}
		meta := rel.Columns[i]
		// Replica identity columns are always included: without them the client
		// cannot identify the row at all.
		if len(want) > 0 && !want[meta.Name] && !meta.IsKey {
			continue
		}
		switch col.Kind {
		case pgoutput.ColNull:
			out[meta.Name] = nil
		case pgoutput.ColUnchanged:
			unchanged = append(unchanged, meta.Name)
		case pgoutput.ColText:
			out[meta.Name] = jsonValue(meta.TypeName, string(col.Data))
		default:
			out[meta.Name] = nil
		}
	}
	return out, unchanged
}

// approxSize estimates the encoded size of a projected row without paying for a
// marshal on the per-change path. Only strings and raw JSON can be large enough
// to matter, so everything else is charged a flat few bytes.
func approxSize(rec map[string]any) int {
	n := 0
	for k, v := range rec {
		n += len(k) + 4
		switch t := v.(type) {
		case string:
			n += len(t)
		case json.RawMessage:
			n += len(t)
		default:
			n += 8
		}
	}
	return n
}

// keyOnly reduces a projected row to the columns that identify it.
func keyOnly(rec map[string]any, keys []string) map[string]any {
	if rec == nil {
		return nil
	}
	out := make(map[string]any, len(keys))
	for _, k := range keys {
		if v, ok := rec[k]; ok {
			out[k] = v
		}
	}
	return out
}

// jsonValue converts a PostgreSQL text datum into a JSON-native value where that
// is lossless, and leaves it as a string otherwise. Numerics that cannot be
// represented exactly stay strings rather than silently losing precision.
func jsonValue(typeName, s string) any {
	v := expr.ParseText(typeName, s)
	switch v.Kind {
	case expr.KindBool:
		return v.Bool
	case expr.KindInt:
		return v.Int
	case expr.KindFloat:
		return v.Float
	case expr.KindJSON:
		if json.Valid([]byte(v.Str)) {
			return json.RawMessage(v.Str)
		}
		return v.Str
	default:
		return v.Str
	}
}

func primaryKeyOf(rel *pgoutput.Relation, m *pgoutput.Message) map[string]expr.Value {
	src := m.New
	if src == nil {
		src = m.Old
	}
	if src == nil {
		return nil
	}
	row := tupleRow{rel: rel, t: src}
	out := map[string]expr.Value{}
	for _, name := range rel.KeyColumns() {
		if v, ok := row.Column(name); ok && !v.IsNull() {
			out[name] = v
		}
	}
	return out
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
