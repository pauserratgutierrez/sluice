// Package server exposes Sluice's HTTP surface and wires the pieces together.
//
// The transport shape follows MCP's 2026-07-28 revision rather than its earlier
// one, because MCP built the naive POST+SSE hybrid, hit the problems, and
// documented them:
//
//   - POST only. The long-lived stream is a POST whose response is
//     text/event-stream, not an EventSource. That is what makes
//     `Authorization: Bearer` possible and removes every token-in-URL hack.
//   - Closing the stream IS the signal. No unsubscribe-on-disconnect message.
//   - Routing metadata mirrored into headers, and REJECTED when it disagrees
//     with the body -- a load balancer routing on the header while the server
//     acts on the body is a real vulnerability class.
package server

import (
	"cmp"
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/pauserratgutierrez/sluice/internal/auth"
	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
	"github.com/pauserratgutierrez/sluice/internal/timer"
)

type Server struct {
	cfg  *config.Config
	log  *slog.Logger
	pool *pgxpool.Pool

	cat     *catalog.Cache
	authz   *Authorizer
	oracle  oracle.Oracle
	holds   *hold.Index
	reg     *registry.Registry
	hub     *hub.Hub
	verify  *auth.Verifier
	revoker *auth.Revoker
	reader  *reader.Reader

	ctx context.Context

	wheel   *timer.Wheel
	snapSem chan struct{}
	hooks   *hookCache

	streamSeq atomic.Uint64
	refreshAt atomic.Int64
	// authzVer is the catalog.AuthzVersion the live decisions were resolved
	// against. See RefreshLeases.
	authzVer atomic.Uint64

	// Revocation tables, resolved to OIDs on their first Relation message so the
	// per-change check costs an integer comparison rather than string building.
	sessionsOID atomic.Uint32
	usersOID    atomic.Uint32

	// holdExists, if set, replaces hold.Exists. Tests inject it so /token can
	// verify holds without a database pool.
	holdExists func(ctx context.Context, specs []hold.Spec) error
}

// Authorizer is aliased so the package reads naturally and so the concrete type
// stays swappable in tests.
type Authorizer = authz.Authorizer

type Options struct {
	Config  *config.Config
	Logger  *slog.Logger
	Pool    *pgxpool.Pool
	Catalog *catalog.Cache
	Authz   *Authorizer
	Oracle  oracle.Oracle
	Hub     *hub.Hub
	Verify  *auth.Verifier
	Revoker *auth.Revoker
}

func New(ctx context.Context, o Options) *Server {
	s := &Server{
		cfg: o.Config, log: o.Logger, pool: o.Pool,
		cat: o.Catalog, authz: o.Authz, oracle: o.Oracle,
		holds: hold.New(),
		reg:   registry.New(), hub: o.Hub,
		verify: o.Verify, revoker: o.Revoker,
		ctx: ctx,
		// 64 buckets spreads a heartbeat period into batches rather than waking
		// every stream at once.
		wheel:   timer.New(o.Config.Heartbeat, 64),
		snapSem: make(chan struct{}, max(1, o.Config.SnapshotMaxConc)),
		hooks:   newHookCache(o.Config.HookTTL, o.Config.HookTimeout),
	}
	if o.Catalog != nil {
		// The catalog is loaded before the server exists, so start level with it
		// rather than spending the first tick re-resolving against a version
		// nothing was resolved against.
		s.authzVer.Store(o.Catalog.AuthzVersion())
	}
	if s.oracle == nil && o.Authz != nil && o.Catalog != nil {
		s.oracle = oracle.NewRLS(o.Authz, o.Catalog)
	}
	return s
}

// Run drives the server's background loops until the context is cancelled.
func (s *Server) Run(ctx context.Context) { s.wheel.Run(ctx) }

// SetReader wires the reader after construction, because the reader needs the
// server as its handler.
func (s *Server) SetReader(r *reader.Reader) { s.reader = r }

func (s *Server) Registry() *registry.Registry { return s.reg }

// Handler builds the HTTP mux.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	p := s.cfg.PathPrefix

	mux.HandleFunc("POST "+p+"/stream", s.handleStream)
	mux.HandleFunc("POST "+p+"/subscribe", s.handleSubscribe)
	mux.HandleFunc("POST "+p+"/unsubscribe", s.handleUnsubscribe)
	mux.HandleFunc("POST "+p+"/publish", s.handlePublish)
	mux.HandleFunc("POST "+p+"/presence", s.handlePresence)
	mux.HandleFunc("POST "+p+"/token", s.handleToken)
	mux.HandleFunc("POST "+p+"/admin/jwks/refresh", s.handleJWKSRefresh)
	mux.HandleFunc("POST "+p+"/admin/shapes/drop", s.handleAdminShapesDrop)

	mux.HandleFunc("GET "+p+"/healthz", s.handleHealthz)
	mux.HandleFunc("GET "+p+"/readyz", s.handleReadyz)
	// Also unprefixed, so a container healthcheck does not need to know the prefix.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	mux.HandleFunc("GET /readyz", s.handleReadyz)

	if s.cfg.MetricsEnabled {
		mux.Handle("GET "+p+"/metrics", promhttp.Handler())
		mux.Handle("GET /metrics", promhttp.Handler())
	}
	if s.cfg.DiagnosticsEnabled {
		mux.HandleFunc("GET "+p+"/diagnostics", s.handleDiagnostics)
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "not_found", "message": "no such endpoint",
		})
	})
	return mux
}

// ---------------------------------------------------------------------------
// Request/response shapes
// ---------------------------------------------------------------------------

type shapeSpec struct {
	Schema      string   `json:"schema"`
	Table       string   `json:"table"`
	Ops         []string `json:"ops"`
	Filter      string   `json:"filter"`
	Columns     []string `json:"columns"`
	Initial     string   `json:"initial"` // "none" | "snapshot"
	Transitions bool     `json:"transitions"`
}

type subSpec struct {
	Sub      string     `json:"sub"`
	Shape    *shapeSpec `json:"shape,omitempty"`
	Channel  string     `json:"channel,omitempty"`
	Presence bool       `json:"presence,omitempty"`
}

type streamReq struct {
	Subscriptions []subSpec         `json:"subscriptions"`
	Resume        map[string]string `json:"resume"`
}

type subscribeReq struct {
	StreamID      string    `json:"stream_id"`
	Subscriptions []subSpec `json:"subscriptions"`
}

type subResult struct {
	Sub        string          `json:"sub"`
	OK         bool            `json:"ok"`
	Oracle     string          `json:"oracle,omitempty"`
	Tier       string          `json:"tier,omitempty"`
	Filter     string          `json:"filter,omitempty"`
	Indexed    *bool           `json:"indexed,omitempty"`
	RoutingKey string          `json:"routing_key,omitempty"`
	Reason     string          `json:"reason,omitempty"`
	Error      *event.Error    `json:"error,omitempty"`
	Warnings   []event.Warning `json:"warnings,omitempty"`
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	if s.hub.Count() >= s.cfg.MaxStreams {
		writeErr(w, http.StatusServiceUnavailable, "too_many_streams",
			"this node is at its configured stream limit")
		return
	}

	var req streamReq
	if r.Body != nil {
		_ = json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req)
	}

	// stream_id carries the node so that a control POST landing on any node can be
	// forwarded to the owning one. That is the alternative to sticky sessions,
	// which MCP SEP-2575 documents as the thing that made stateful HTTP hard to
	// scale.
	streamID := fmt.Sprintf("%s.%d-%d", s.cfg.NodeID, time.Now().UnixNano(), s.streamSeq.Add(1))

	st := s.hub.Open(streamID, id)
	metrics.Streams.Set(float64(s.hub.Count()))
	defer func() {
		s.holds.RemoveStream(streamID)
		for _, sub := range s.reg.RemoveStream(streamID) {
			s.decSubMetric(sub)
		}
		s.hub.Close(streamID, cmp.Or(st.CloseCode(), "client_closed"))
		metrics.StreamClosed.WithLabelValues(cmp.Or(st.CloseCode(), "client_closed")).Inc()
		metrics.Streams.Set(float64(s.hub.Count()))
	}()

	// SSE headers. X-Accel-Buffering is normative in MCP's spec and is a
	// documented, first-class nginx feature; without it a proxy accumulates
	// events before forwarding them.
	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-store")
	h.Set("Connection", "keep-alive")
	h.Set("X-Accel-Buffering", "no")
	h.Set("Vary", "Authorization")
	h.Set("Sluice-Stream-Id", streamID)
	w.WriteHeader(http.StatusOK)

	rc := http.NewResponseController(w)
	_ = rc.Flush()

	results := s.applySubscriptions(r.Context(), st, id, req.Subscriptions, req.Resume)

	ready := map[string]any{
		"stream_id":     streamID,
		"server_time":   time.Now().UTC().Format(time.RFC3339Nano),
		"heartbeat_ms":  s.cfg.Heartbeat.Milliseconds(),
		"subscriptions": results,
	}
	if s.reader != nil {
		ready["wal_lsn"] = reader.FormatLSN(s.reader.ConfirmedLSN())
	}
	s.writeEvent(w, rc, event.Event{Kind: event.KindReady, Data: ready})

	// One shared wheel instead of a timer per connection. The callback only
	// enqueues; all writing stays on this goroutine, which is what keeps the
	// ResponseWriter single-threaded.
	cancelTick := s.wheel.Add(func() { s.tick(st) })
	defer cancelTick()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-st.Events():
			if !ok {
				return
			}
			if err := s.writeEvent(w, rc, ev); err != nil {
				st.CloseWith("write_failed")
				return
			}
		case <-st.Done():
			// Drain what is already queued so a final error event reaches the
			// client before the connection goes away.
			for {
				select {
				case ev := <-st.Events():
					_ = s.writeEvent(w, rc, ev)
					continue
				default:
				}
				return
			}
		}
	}
}

// tick runs once per heartbeat period per stream, off the writer goroutine.
func (s *Server) tick(st *hub.Stream) {
	id := st.Identity()
	if exp := auth.Expiry(id); !exp.IsZero() && time.Now().After(exp) {
		hub.SendError(st, event.Error{Code: "token_expired",
			Message: "the access token expired and was not refreshed"})
		st.CloseWith("token_expired")
		return
	}
	if s.revoker != nil && s.revoker.Revoked(id) {
		hub.SendError(st, event.Error{Code: "session_revoked",
			Message: "the session backing this stream no longer exists"})
		st.CloseWith("session_revoked")
		return
	}
	st.Send(event.Event{Kind: event.KindHeartbeat})
}

func (s *Server) writeEvent(w http.ResponseWriter, rc *http.ResponseController, ev event.Event) error {
	// Bounding each write is what stops a slow consumer from pinning a goroutine
	// forever, without needing a watchdog per connection.
	_ = rc.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))

	// A bare comment, exactly as the WHATWG spec recommends for keeping proxies
	// from dropping an idle stream.
	if ev.Kind == event.KindHeartbeat {
		if _, err := w.Write([]byte(": hb\n\n")); err != nil {
			return err
		}
		return rc.Flush()
	}

	body, err := json.Marshal(ev.Data)
	if err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("event: ")
	b.WriteString(string(ev.Kind))
	b.WriteByte('\n')
	if ev.ID != "" {
		b.WriteString("id: ")
		b.WriteString(ev.ID)
		b.WriteByte('\n')
	}
	b.WriteString("data: ")
	b.Write(body)
	// The blank line is what dispatches the event; an event without it is
	// discarded by the client.
	b.WriteString("\n\n")

	if _, err := w.Write([]byte(b.String())); err != nil {
		return err
	}
	return rc.Flush()
}

// ---------------------------------------------------------------------------
// Subscribe / unsubscribe
// ---------------------------------------------------------------------------

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req subscribeReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	if !st.Allow("subscribe", s.cfg.SubscribeRate) {
		writeErr(w, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may issue at most %d subscribe requests per second", s.cfg.SubscribeRate))
		return
	}
	results := s.applySubscriptions(r.Context(), st, id, req.Subscriptions, nil)
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func (s *Server) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req struct {
		StreamID string   `json:"stream_id"`
		Subs     []string `json:"subs"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	removed := 0
	for _, label := range req.Subs {
		if sub := s.reg.Remove(st.StreamID(), label); sub != nil {
			s.holds.Remove(st.StreamID(), label)
			s.decSubMetric(sub)
			removed++
		}
		for _, ch := range st.Channels() {
			if l, ok := st.ChannelLabel(ch); ok && l == label {
				s.hub.LeaveChannel(ch, st)
				removed++
			}
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"removed": removed})
}

// applySubscriptions resolves a batch. A partial failure is not a request
// failure: each subscription gets its own result, so one bad shape does not take
// down a client's whole stream.
func (s *Server) applySubscriptions(
	ctx context.Context,
	st *hub.Stream,
	id authz.Identity,
	specs []subSpec,
	resume map[string]string,
) []subResult {

	out := make([]subResult, 0, len(specs))
	shapes := len(s.reg.StreamSubscriptions(st.StreamID()))
	existing := shapes + len(st.Channels())

	for _, spec := range specs {
		if spec.Sub == "" {
			out = append(out, subResult{OK: false, Error: &event.Error{
				Code: "invalid_subscription", Message: "each subscription needs a `sub` label"}})
			continue
		}
		if existing >= s.cfg.MaxSubsPerStream {
			out = append(out, subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
				Code: "too_many_subscriptions", Message: "this stream is at its subscription limit"}})
			continue
		}

		switch {
		case spec.Shape != nil:
			// Shapes are capped separately from channels: a shape costs an
			// authorization resolution and a registry entry consulted on every
			// change to its relation, where a channel costs a map entry.
			if shapes >= s.cfg.MaxShapesPerStream {
				out = append(out, subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
					Code: "too_many_shapes", Message: fmt.Sprintf(
						"this stream is at its limit of %d shape subscriptions", s.cfg.MaxShapesPerStream)}})
				continue
			}
			res := s.subscribeShape(ctx, st, id, spec, resume)
			out = append(out, res)
			if res.OK {
				existing++
				shapes++
			}
		case spec.Channel != "":
			res := s.subscribeChannel(st, id, spec)
			out = append(out, res)
			if res.OK {
				existing++
			}
		default:
			out = append(out, subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
				Code: "invalid_subscription", Message: "provide either `shape` or `channel`"}})
		}
	}
	return out
}

func (s *Server) subscribeShape(
	ctx context.Context,
	st *hub.Stream,
	id authz.Identity,
	spec subSpec,
	resume map[string]string,
) subResult {

	res := subResult{Sub: spec.Sub}
	sp := spec.Shape

	schema := cmp.Or(sp.Schema, "public")
	rel, ok := s.cat.Lookup(schema, sp.Table)
	if !ok {
		res.Error = &event.Error{Code: "relation_not_published", Message: fmt.Sprintf(
			"%s.%s is not in publication %q; add it with: ALTER PUBLICATION %s ADD TABLE %s.%s",
			schema, sp.Table, s.cfg.Publication, s.cfg.Publication, schema, sp.Table)}
		return res
	}

	ops, err := shape.ParseOps(sp.Ops)
	if err != nil {
		res.Error = &event.Error{Code: "invalid_ops", Message: err.Error()}
		return res
	}

	filter, err := shape.Parse(sp.Filter, rel)
	if err != nil {
		res.Error = &event.Error{Code: "invalid_filter", Message: err.Error()}
		return res
	}

	if s.oracle == nil {
		res.Error = &event.Error{Code: "internal", Message: "shape oracle is not configured"}
		return res
	}

	start := time.Now()
	grant, err := s.oracle.Resolve(ctx, oracle.Request{
		Action:   oracle.ActionSubscribe,
		Identity: id,
		Relation: rel,
		Filter:   filter,
		Columns:  sp.Columns,
		Ops:      sp.Ops,
	})
	if err != nil {
		s.mapOracleErr(&res, err)
		return res
	}
	decision := grant.Decision
	filter = grant.Filter
	tierLabel := s.tierLabel(decision)
	metrics.AuthzResolveSeconds.WithLabelValues(tierLabel).Observe(time.Since(start).Seconds())
	metrics.AuthzResolutions.WithLabelValues(tierLabel, "granted").Inc()
	if decision != nil && decision.Tier == authz.TierC && s.oracle.Name() == oracle.NameRLS {
		metrics.AuthzCompileFailures.WithLabelValues(rel.Schema, rel.Name, truncate(decision.Reason, 60)).Inc()
	}

	sub := &registry.Subscription{
		Label:       spec.Sub,
		Sink:        st,
		Relation:    rel,
		Ops:         ops,
		Filter:      filter,
		Columns:     grant.Columns,
		Transitions: sp.Transitions,
		Decision:    authz.NewHandle(decision),
		RoutingKey:  filter.RoutingKey(rel),
	}

	// Warnings. Every one carries a remedy that is a runnable statement, because
	// a warning nobody can act on is noise.
	if len(grant.Denied) > 0 {
		remedy := fmt.Sprintf("GRANT SELECT (%s) ON %s TO %s;", strings.Join(grant.Denied, ", "), rel.FullName(), id.Role)
		if s.oracle.Name() == oracle.NameIssuer {
			remedy = fmt.Sprintf("GRANT SELECT (%s) ON %s TO the Sluice role, or ask the issuer to allow those columns;", strings.Join(grant.Denied, ", "), rel.FullName())
		}
		sub.Warnings = append(sub.Warnings, event.Warning{
			Sub: spec.Sub, Code: "columns_not_granted",
			Message: "these columns were dropped from the projection: " + strings.Join(grant.Denied, ", "),
			Effect:  "events will not contain them",
			Remedy:  remedy,
		})
	}
	if sub.RoutingKey == "" {
		sub.Warnings = append(sub.Warnings, event.Warning{
			Sub: spec.Sub, Code: "unindexed_shape",
			Message: "this shape has no equality filter on an indexed column",
			Effect:  "it is scanned for every change to " + rel.FullName() + " instead of being found by a map lookup",
			Remedy:  "add an equality filter on an indexed column, or create an index on the filtered column",
		})
	}
	if ops.Delete || sp.Transitions {
		if missing := missingFromReplicaIdentity(rel, filter.ColumnsNeeded()); len(missing) > 0 {
			ri := "DEFAULT (primary key only)"
			switch rel.ReplicaIdentity {
			case 'f':
				ri = "FULL"
			case 'i':
				ri = "USING INDEX"
			case 'n':
				ri = "NOTHING"
			}
			pk := strings.Join(rel.ReplicaIdentityColumns, ", ")
			if pk == "" {
				pk = "id"
			}
			idx := rel.Name + "_ri"
			w := event.Warning{
				Sub: spec.Sub, Code: "replica_identity_insufficient",
				Message: fmt.Sprintf(
					"DELETE events for this shape cannot be filtered or authorized: column(s) %s are not in the replica identity of %s (currently %s)",
					strings.Join(missing, ", "), rel.FullName(), ri),
				Effect: "DELETE events will be withheld or marked degraded",
				Remedy: fmt.Sprintf(
					"CREATE UNIQUE INDEX %s ON %s (%s, %s); ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;",
					idx, rel.FullName(), strings.Join(missing, ", "), pk, rel.FullName(), idx),
			}
			if s.cfg.ReplicaIdentity == "strict" {
				res.Error = &event.Error{Code: w.Code, Message: w.Message + ". Remedy: " + w.Remedy}
				return res
			}
			sub.Warnings = append(sub.Warnings, w)
		}
	}
	if rel.ReplicaIdentity == 'f' && rel.HasToastableColumn {
		sub.Warnings = append(sub.Warnings, event.Warning{
			Sub: spec.Sub, Code: "replica_identity_full_with_toast",
			Message: rel.FullName() + " uses REPLICA IDENTITY FULL and has a TOAST-able column",
			Effect:  "every UPDATE and DELETE inlines the whole column into the old tuple; measured 15x WAL amplification and 3000x larger messages",
			Remedy:  "switch to REPLICA IDENTITY USING INDEX over only the columns actually needed",
		})
	}
	if !rel.ReplicaIdentityOK {
		sub.Warnings = append(sub.Warnings, event.Warning{
			Sub: spec.Sub, Code: "replica_identity_broken",
			Message: rel.FullName() + " has an inadequate replica identity",
			Effect:  "the APPLICATION's own UPDATE and DELETE statements on this table are failing, not just replication",
			Remedy:  "ALTER TABLE " + rel.FullName() + " REPLICA IDENTITY FULL; -- or recreate the index named by relreplident",
		})
	}

	// An unindexed shape is consulted for every change to its relation, so the
	// cost of admitting one is paid by every other subscriber to that table.
	// Past a threshold the node stops being O(1) in subscription count, which is
	// the property the whole design rests on.
	if sub.RoutingKey == "" && s.reg.Stats().Unindexed >= s.cfg.UnindexedMax {
		res.Error = &event.Error{Code: "too_many_unindexed_shapes", Message: fmt.Sprintf(
			"this node already has %d unindexed subscriptions, the configured maximum; "+
				"filter on an indexed column with an equality, or index the filtered column",
			s.cfg.UnindexedMax)}
		return res
	}

	if err := s.installShape(ctx, sub, grant.Holds); err != nil {
		if errors.Is(err, errDuplicateSub) {
			res.Error = &event.Error{Code: "duplicate_sub",
				Message: "this stream already has a subscription labelled " + spec.Sub}
			return res
		}
		res.Error = &event.Error{Code: "shape_not_authorized", Message: err.Error()}
		return res
	}
	s.incSubMetric(sub)

	res.OK = true
	indexed := sub.Indexed()
	res.Indexed = &indexed
	res.RoutingKey = sub.RoutingKey
	res.Reason = grant.Reason
	res.Warnings = sub.Warnings
	if s.oracle.Name() == oracle.NameIssuer {
		res.Oracle = oracle.NameIssuer
		if filter != nil {
			if d := filter.Describe(); d != "" && d != "(none)" {
				res.Filter = d
			}
		}
	} else if decision != nil {
		res.Tier = string(decision.Tier)
	}

	for _, w := range sub.Warnings {
		hub.SendWarning(st, w)
		metrics.ConfigWarnings.WithLabelValues(w.Code).Set(1)
	}

	// Resume replay, re-filtered and re-authorized. Replay is never trusted to
	// have been authorized on its first pass.
	resumed := false
	if resume != nil {
		if lsnStr, ok := resume[rel.FullName()]; ok && lsnStr != "" {
			s.replay(sub, lsnStr)
			resumed = true
		}
	}
	// A snapshot and a resume are alternatives: resuming means the client already
	// holds a base state for changes to apply to.
	if !resumed && sp.Initial == "snapshot" {
		if !s.cfg.SnapshotEnabled {
			sub.Warnings = append(sub.Warnings, event.Warning{
				Sub: spec.Sub, Code: "snapshot_disabled",
				Message: "initial snapshots are disabled on this server",
				Remedy:  "set SLUICE_SNAPSHOT_ENABLED=true, or fetch the initial state yourself",
			})
		} else {
			go s.snapshot(s.ctx, sub)
		}
	}
	return res
}

func (s *Server) replay(sub *registry.Subscription, lsnStr string) {
	from, err := reader.ParseLSN(lsnStr)
	if err != nil {
		hub.SendError(streamOf(sub), event.Error{Sub: sub.Label, Code: "invalid_resume",
			Message: "resume LSN could not be parsed", Retryable: false})
		return
	}
	s.replayFrom(sub, from)
}

func (s *Server) replayFrom(sub *registry.Subscription, from uint64) {
	st := streamOf(sub)
	entries, ok := s.hub.Rings().Replay(sub.Relation.OID, from)
	if !ok {
		hub.SendError(st, event.Error{Sub: sub.Label, Code: "resume_too_old",
			Message:   "the requested position is older than the retained buffer",
			Retryable: true, Action: "resnapshot"})
		return
	}
	for _, e := range entries {
		m := messageFromRing(e)
		s.deliver(sub, m, e.Relation,
			tupleRow{rel: e.Relation, t: e.New}, tupleRow{rel: e.Relation, t: e.Old},
			newTuples(e.Relation, m),
			e.LSN, e.CommitTime, opName(e.Op), false)
	}
}

func (s *Server) subscribeChannel(st *hub.Stream, id authz.Identity, spec subSpec) subResult {
	res := subResult{Sub: spec.Sub}
	ch, ok := s.cfg.Channel(spec.Channel)
	if !ok {
		res.Error = &event.Error{Code: "unknown_namespace", Message: fmt.Sprintf(
			"channel %q has no configured namespace; add it to SLUICE_CHANNELS", spec.Channel)}
		return res
	}
	switch ch.Mode {
	case config.ChannelPublic:
	case config.ChannelOwner:
		// Authorization by naming convention: O(1), no I/O, no policy to write.
		if id.Sub == "" || !strings.HasSuffix(spec.Channel, ":"+id.Sub) {
			res.Error = &event.Error{Code: "channel_not_authorized", Message: fmt.Sprintf(
				"namespace %q is owner-scoped, so the channel must be named %s:<your user id>",
				ch.Namespace, ch.Namespace)}
			return res
		}
	case config.ChannelHook:
		allow, reason := s.hooks.Authorize(s.ctx, ch, id, spec.Channel)
		if !allow {
			res.Error = &event.Error{Code: "channel_not_authorized", Message: reason}
			return res
		}
	}

	s.hub.JoinChannel(spec.Channel, st, spec.Sub)
	if spec.Presence {
		st.Send(event.Event{Kind: event.KindPresence, Data: event.Presence{
			Sub: spec.Sub, Channel: spec.Channel, Type: "state",
			Members: s.hub.Presence().State(spec.Channel),
		}})
	}
	res.OK = true
	return res
}

// ---------------------------------------------------------------------------
// Publish / presence / token
// ---------------------------------------------------------------------------

func (s *Server) handlePublish(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req struct {
		StreamID string          `json:"stream_id"`
		Channel  string          `json:"channel"`
		Event    string          `json:"event"`
		Payload  json.RawMessage `json:"payload"`
		Self     bool            `json:"self"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, int64(s.cfg.MaxPayloadBytes)+4096)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(req.Payload) > s.cfg.MaxPayloadBytes {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf(
			"payload is %d bytes, limit is %d", len(req.Payload), s.cfg.MaxPayloadBytes))
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	if _, subscribed := st.ChannelLabel(req.Channel); !subscribed {
		// Publishing to a channel you have not joined would bypass the subscribe-time
		// authorization check, so it is refused rather than re-checked here.
		writeErr(w, http.StatusForbidden, "channel_not_subscribed",
			"subscribe to a channel before publishing to it")
		return
	}
	if !st.Allow("publish", s.cfg.PublishRate) {
		writeErr(w, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may publish at most %d messages per second", s.cfg.PublishRate))
		return
	}
	n := s.hub.PublishBroadcast(req.Channel, cmp.Or(req.Event, "message"),
		id.Sub, "client", "", req.Payload, req.Self, st.StreamID())

	ns := req.Channel
	if i := strings.IndexByte(ns, ':'); i >= 0 {
		ns = ns[:i]
	}
	metrics.BroadcastPublished.WithLabelValues(ns, "client").Inc()
	writeJSON(w, http.StatusOK, map[string]any{"delivered": n})
}

func (s *Server) handlePresence(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req struct {
		StreamID string          `json:"stream_id"`
		Channel  string          `json:"channel"`
		Action   string          `json:"action"`
		Key      string          `json:"key"`
		Meta     json.RawMessage `json:"meta"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	if _, subscribed := st.ChannelLabel(req.Channel); !subscribed {
		writeErr(w, http.StatusForbidden, "channel_not_subscribed",
			"subscribe to a channel before tracking presence on it")
		return
	}
	if !st.Allow("presence", s.cfg.PresenceRate) {
		writeErr(w, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may send at most %d presence updates per second", s.cfg.PresenceRate))
		return
	}
	key := cmp.Or(req.Key, id.Sub)
	// A client may not claim someone else's identity in a presence roster.
	if id.Sub != "" && key != id.Sub {
		writeErr(w, http.StatusForbidden, "presence_key_not_allowed",
			"a presence key must equal the caller's subject")
		return
	}

	switch req.Action {
	case "track", "update", "":
		n := s.hub.Presence().Track(req.Channel, key, st.StreamID(), req.Meta)
		if n > s.cfg.PresenceMaxKeys {
			s.hub.Presence().UntrackKey(req.Channel, key, st.StreamID())
			writeErr(w, http.StatusTooManyRequests, "presence_too_many_keys", fmt.Sprintf(
				"channel %q is at its %d-key limit", req.Channel, s.cfg.PresenceMaxKeys))
			return
		}
		metrics.PresenceMembers.WithLabelValues(req.Channel).Set(float64(n))
	case "untrack":
		s.hub.Presence().UntrackKey(req.Channel, key, st.StreamID())
		metrics.PresenceMembers.WithLabelValues(req.Channel).
			Set(float64(s.hub.Presence().Count(req.Channel)))
	default:
		writeErr(w, http.StatusBadRequest, "bad_request", "action must be track, update or untrack")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// handleToken rebinds a stream to a refreshed access token and re-resolves every
// authorization decision on it, because the claims may have changed.
func (s *Server) handleToken(w http.ResponseWriter, r *http.Request) {
	var req struct {
		StreamID    string `json:"stream_id"`
		AccessToken string `json:"access_token"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	newID, err := s.verify.Verify(r.Context(), "Bearer "+req.AccessToken)
	if err != nil {
		writeErr(w, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	st, ok := s.hub.Get(req.StreamID)
	if !ok {
		writeErr(w, http.StatusNotFound, "unknown_stream", "no such stream on this node")
		return
	}
	// A stream may not change identity mid-flight.
	if cur := st.Identity(); cur.Sub != "" && cur.Sub != newID.Sub {
		writeErr(w, http.StatusForbidden, "subject_mismatch",
			"the refreshed token names a different subject")
		return
	}
	st.SetIdentity(newID)

	revoked := 0
	for _, sub := range s.reg.StreamSubscriptions(st.StreamID()) {
		if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
			if !s.refreshIssuerShape(r.Context(), st, sub, newID) {
				metrics.AuthzLeaseRefreshes.WithLabelValues("revoked").Inc()
				revoked++
				continue
			}
			metrics.AuthzLeaseRefreshes.WithLabelValues("held").Inc()
			continue
		}
		eqs := sub.Filter.Equalities
		held, err := s.oracle.LeaseTick(r.Context(), sub.Decision, sub.Relation, newID, eqs)
		if err != nil || !held {
			metrics.AuthzLeaseRefreshes.WithLabelValues("revoked").Inc()
			s.dropShape(st, sub.Label, event.Error{Code: "shape_not_authorized",
				Message: "the refreshed token no longer grants access to this shape"})
			revoked++
			continue
		}
		metrics.AuthzLeaseRefreshes.WithLabelValues("held").Inc()
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revoked_subscriptions": revoked})
}

func (s *Server) handleJWKSRefresh(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil || id.Role != "service_role" {
		writeErr(w, http.StatusForbidden, "forbidden", "service_role required")
		return
	}
	if err := s.verify.Refresh(r.Context()); err != nil {
		var warn *auth.WarnSymmetricKey
		if !errors.As(err, &warn) {
			writeErr(w, http.StatusBadGateway, "jwks_refresh_failed", err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "warning": warn.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// ---------------------------------------------------------------------------
// Health and diagnostics
// ---------------------------------------------------------------------------

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	ready := s.cat.LoadedAt().IsZero() == false
	code := http.StatusOK
	if !ready {
		code = http.StatusServiceUnavailable
	}
	body := map[string]any{"catalog_loaded": ready, "streams": s.hub.Count()}
	if s.reader != nil {
		body["confirmed_lsn"] = reader.FormatLSN(s.reader.ConfirmedLSN())
	}
	writeJSON(w, code, body)
}

func (s *Server) scheduleCatalogRefresh() {
	s.refreshAt.Store(time.Now().UnixNano())
}

// RefreshLeases re-evaluates every decision whose lease has expired, and every
// decision on a relation whose policies may have changed.
//
// This is the half of authorization that a subscribe-time model must not forget:
// resolving once is only safe if a revocation can still take effect. A predicate
// that reads another table or the clock can flip without any row or claim
// changing, so those decisions carry a lease; stable ones do not and are only
// re-resolved when the catalog itself moves.
//
// "Moves" has to mean the catalog CONTENTS, not a WAL Relation message. DROP
// POLICY and ALTER ROLE ... NOBYPASSRLS change what a caller may see and emit no
// Relation message at all, so refreshAt alone never fires for them -- and the
// decisions they affect are exactly the stable Tier A and Tier B ones that carry
// no lease. That combination made a revoked policy unenforceable on an open
// stream for as long as it stayed open. Comparing the catalog's authorization
// version closes it; re-resolving is pure CPU, since Resolve reads the cached
// catalog and makes no database round trip.
func (s *Server) RefreshLeases(ctx context.Context) {
	now := time.Now()
	subs := s.reg.All()
	catalogMoved := s.refreshAt.Swap(0) != 0
	if v := s.cat.AuthzVersion(); s.authzVer.Swap(v) != v {
		catalogMoved = true
		s.log.Info("authorization catalog changed; re-resolving every subscription",
			"authz_version", v, "subscriptions", len(subs))
	}

	for _, sub := range subs {
		st := streamOf(sub)
		if st == nil {
			continue
		}
		// Re-read the relation: the cached pointer may predate a refresh.
		rel, ok := s.cat.Lookup(sub.Relation.Schema, sub.Relation.Name)
		if !ok {
			s.dropShape(st, sub.Label, event.Error{Code: "relation_unpublished",
				Message: sub.Relation.FullName() + " is no longer in the publication"})
			continue
		}
		sub.Relation = rel

		if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
			continue
		}
		if s.oracle == nil {
			continue
		}
		if !catalogMoved && !sub.Decision.Load().Expired(now) {
			continue
		}

		held, err := s.oracle.LeaseTick(ctx, sub.Decision, rel, st.Identity(), sub.Filter.Equalities)
		if err != nil || !held {
			metrics.AuthzLeaseRefreshes.WithLabelValues("revoked").Inc()
			s.dropShape(st, sub.Label, event.Error{Code: "shape_not_authorized",
				Message: "access to this shape has been revoked"})
			continue
		}
		metrics.AuthzLeaseRefreshes.WithLabelValues("held").Inc()
	}

	for _, w := range s.holds.All() {
		unpublished := false
		riReason := ""
		fresh := make([]*catalog.Relation, len(w.Holds))
		for i, h := range w.Holds {
			if h.Rel == nil {
				unpublished = true
				break
			}
			rel, ok := s.cat.Lookup(h.Rel.Schema, h.Rel.Name)
			if !ok {
				unpublished = true
				break
			}
			if missing := hold.MissingReplicaIdentity(rel, h.Filter); len(missing) > 0 {
				riReason = hold.ReplicaIdentityReason(rel, missing)
				break
			}
			fresh[i] = rel
		}
		if !unpublished && riReason == "" {
			s.holds.RefreshRels(w.StreamID, w.Label, fresh)
			continue
		}
		st, ok := s.hub.Get(w.StreamID)
		if !ok {
			s.holds.Remove(w.StreamID, w.Label)
			continue
		}
		if unpublished {
			s.dropShape(st, w.Label, event.Error{Code: "relation_unpublished",
				Message: "a hold table is no longer in the publication"})
			continue
		}
		s.dropShape(st, w.Label, event.Error{Code: "shape_not_authorized", Message: riReason})
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Server) identify(r *http.Request) (authz.Identity, error) {
	return s.verify.Verify(r.Context(), r.Header.Get("Authorization"))
}

// resolveStream finds the stream a control request targets, and enforces the
// header/body agreement rule. Mirroring routing metadata into a header without
// checking it against the body is a documented vulnerability class: an
// intermediary routes on one value while the server acts on another.
func (s *Server) resolveStream(w http.ResponseWriter, r *http.Request, bodyID string, id authz.Identity) (*hub.Stream, bool) {
	headerID := r.Header.Get("Sluice-Stream-Id")
	if headerID != "" && bodyID != "" && headerID != bodyID {
		writeErr(w, http.StatusBadRequest, "stream_id_mismatch",
			"Sluice-Stream-Id does not match the stream_id in the body")
		return nil, false
	}
	streamID := cmp.Or(bodyID, headerID)
	if streamID == "" {
		writeErr(w, http.StatusBadRequest, "bad_request", "stream_id is required")
		return nil, false
	}
	st, ok := s.hub.Get(streamID)
	if !ok {
		// In a multi-node deployment this is where the request would be forwarded
		// to the owning node, using the node prefix in the stream id.
		writeErr(w, http.StatusNotFound, "unknown_stream", "no such stream on this node")
		return nil, false
	}
	if cur := st.Identity(); cur.Sub != id.Sub || cur.Role != id.Role {
		writeErr(w, http.StatusForbidden, "forbidden", "this token does not own that stream")
		return nil, false
	}
	return st, true
}

func (s *Server) incSubMetric(sub *registry.Subscription) {
	tier := "-"
	if d := sub.Decision.Load(); d != nil {
		tier = s.tierLabel(d)
	}
	metrics.Subscriptions.WithLabelValues(sub.Relation.Schema, sub.Relation.Name,
		tier, boolLabel(sub.Indexed())).Inc()
}

func (s *Server) decSubMetric(sub *registry.Subscription) {
	tier := "-"
	if d := sub.Decision.Load(); d != nil {
		tier = s.tierLabel(d)
	}
	metrics.Subscriptions.WithLabelValues(sub.Relation.Schema, sub.Relation.Name,
		tier, boolLabel(sub.Indexed())).Dec()
}

func missingFromReplicaIdentity(rel *catalog.Relation, needed []string) []string {
	var out []string
	for _, c := range needed {
		if !rel.InReplicaIdentity(c) {
			out = append(out, c)
		}
	}
	return out
}

func boolLabel(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}

var errDuplicateSub = errors.New("duplicate subscription label")

func (s *Server) tierLabel(d *authz.Decision) string {
	if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
		return oracle.NameIssuer
	}
	if d == nil {
		return "-"
	}
	return string(d.EffectiveTier())
}

func (s *Server) impersonateSnapshots() bool {
	if s.oracle == nil {
		return true
	}
	return s.oracle.SnapshotImpersonate()
}

func (s *Server) mapOracleErr(res *subResult, err error) {
	var denied *authz.ErrDenied
	var issuerDenied *oracle.ErrDenied
	var tierC *authz.ErrTierCDisabled
	switch {
	case errors.As(err, &denied):
		metrics.AuthzResolutions.WithLabelValues("-", "denied").Inc()
		res.Error = &event.Error{Code: "shape_not_authorized", Message: denied.Reason}
	case errors.As(err, &issuerDenied):
		metrics.AuthzResolutions.WithLabelValues("-", "denied").Inc()
		res.Error = &event.Error{Code: issuerDenied.WireCode(), Message: issuerDenied.Reason}
	case errors.As(err, &tierC):
		metrics.AuthzResolutions.WithLabelValues("C", "refused").Inc()
		res.Error = &event.Error{Code: "policy_requires_impersonation", Message: tierC.Reason}
	default:
		res.Error = &event.Error{Code: "internal", Message: err.Error()}
	}
}

// refreshIssuerShape is the /token path for the issuer oracle: re-ask the
// issuer, then apply the grant without a window where the shape is live and
// unwatched.
func (s *Server) refreshIssuerShape(ctx context.Context, st *hub.Stream, sub *registry.Subscription, id authz.Identity) bool {
	grant, err := s.oracle.Refresh(ctx, oracle.Request{
		Action:   oracle.ActionRefresh,
		Identity: id,
		Relation: sub.Relation,
		Filter:   sub.Filter,
		Columns:  sub.Columns,
	})
	if err != nil || grant == nil {
		s.dropShape(st, sub.Label, event.Error{Code: "shape_not_authorized",
			Message: "the refreshed token no longer grants access to this shape"})
		return false
	}
	return s.applyIssuerGrant(ctx, st, sub.Label, grant)
}

// applyIssuerGrant installs a refresh grant on a live shape. Holds are
// replaced under one lock (never Remove then Add), then EXISTS, then the
// effective filter, columns, and routing key are rebound. A shape already
// cut during the issuer round-trip is not resurrected.
func (s *Server) applyIssuerGrant(ctx context.Context, st *hub.Stream, label string, grant *oracle.Grant) bool {
	if grant == nil || grant.Filter == nil || len(grant.Columns) == 0 || len(grant.Holds) == 0 {
		s.dropShape(st, label, event.Error{Code: "shape_not_authorized",
			Message: "the refreshed token no longer grants access to this shape"})
		return false
	}
	if s.reg.Get(st.StreamID(), label) == nil {
		s.holds.Remove(st.StreamID(), label)
		return false
	}
	s.holds.Replace(st.StreamID(), label, grant.Holds)
	if err := s.verifyHolds(ctx, grant.Holds); err != nil {
		s.dropShape(st, label, event.Error{Code: "shape_not_authorized", Message: err.Error()})
		return false
	}
	live := s.reg.Get(st.StreamID(), label)
	if live == nil {
		s.holds.Remove(st.StreamID(), label)
		return false
	}
	oldIndexed := live.Indexed()
	schema, table := live.Relation.Schema, live.Relation.Name
	tier := "-"
	if live.Decision != nil {
		if d := live.Decision.Load(); d != nil {
			tier = s.tierLabel(d)
		}
	}
	rebound := s.reg.Rebind(st.StreamID(), label, grant.Filter, grant.Columns)
	if rebound == nil {
		s.holds.Remove(st.StreamID(), label)
		return false
	}
	if oldIndexed != rebound.Indexed() {
		metrics.Subscriptions.WithLabelValues(schema, table, tier, boolLabel(oldIndexed)).Dec()
		s.incSubMetric(rebound)
	}
	return true
}

func (s *Server) verifyHolds(ctx context.Context, specs []hold.Spec) error {
	if len(specs) == 0 {
		return fmt.Errorf("issuer grant has no holds")
	}
	if s.holdExists != nil {
		return s.holdExists(ctx, specs)
	}
	if s.pool == nil {
		return fmt.Errorf("cannot verify holds without a database pool")
	}
	return hold.Exists(ctx, s.pool, specs)
}

// installShape registers hold watches and the shape atomically, then EXISTS
// the holds. A DELETE already in the reader is applied by the watch.
func (s *Server) installShape(ctx context.Context, sub *registry.Subscription, holds []hold.Spec) error {
	sid := sub.Sink.StreamID()
	if len(holds) > 0 {
		if !s.holds.Add(sid, sub.Label, holds) {
			return errDuplicateSub
		}
	}
	if !s.reg.Add(sub) {
		s.holds.Remove(sid, sub.Label)
		return errDuplicateSub
	}
	if len(holds) == 0 {
		return nil
	}
	if s.pool == nil {
		s.reg.Remove(sid, sub.Label)
		s.holds.Remove(sid, sub.Label)
		return fmt.Errorf("cannot verify holds without a database pool")
	}
	if err := hold.Exists(ctx, s.pool, holds); err != nil {
		s.reg.Remove(sid, sub.Label)
		s.holds.Remove(sid, sub.Label)
		return err
	}
	return nil
}

func (s *Server) dropShape(st *hub.Stream, label string, err event.Error) {
	if st == nil {
		return
	}
	if removed := s.reg.Remove(st.StreamID(), label); removed != nil {
		s.decSubMetric(removed)
	}
	s.holds.Remove(st.StreamID(), label)
	if err.Code == "" {
		return
	}
	err.Sub = label
	hub.SendError(st, err)
}

func (s *Server) authorizeAdmin(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	token := strings.TrimSpace(strings.TrimPrefix(header, "Bearer "))
	if s.cfg != nil && s.cfg.IssuerMode() && s.cfg.IssuerBearer != "" &&
		hmac.Equal([]byte(token), []byte(s.cfg.IssuerBearer)) {
		return true
	}
	id, err := s.identify(r)
	return err == nil && id.Role == "service_role"
}

// handleAdminShapesDrop is ops: it walks this node's subscriptions and cuts
// matches. It is not the hot path.
func (s *Server) handleAdminShapesDrop(w http.ResponseWriter, r *http.Request) {
	if !s.authorizeAdmin(r) {
		writeErr(w, http.StatusForbidden, "forbidden", "service_role or the issuer bearer is required")
		return
	}
	var req struct {
		Identity struct {
			Sub  string `json:"sub"`
			Role string `json:"role"`
		} `json:"identity"`
		Schema     string            `json:"schema"`
		Table      string            `json:"table"`
		Equalities map[string]string `json:"equalities"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	schema := cmp.Or(req.Schema, "public")
	if req.Table == "" || len(req.Equalities) == 0 {
		writeErr(w, http.StatusBadRequest, "bad_request", "table and equalities are required")
		return
	}
	dropped := 0
	for _, sub := range s.reg.All() {
		if sub.Relation.Schema != schema || sub.Relation.Name != req.Table {
			continue
		}
		st := streamOf(sub)
		if st == nil {
			continue
		}
		id := st.Identity()
		if req.Identity.Sub != "" && id.Sub != req.Identity.Sub {
			continue
		}
		if req.Identity.Role != "" && id.Role != req.Identity.Role {
			continue
		}
		match := true
		for col, val := range req.Equalities {
			got, ok := sub.Filter.Equalities[col]
			if !ok || got.String() != val {
				match = false
				break
			}
		}
		if !match {
			continue
		}
		s.dropShape(st, sub.Label, event.Error{Code: "shape_not_authorized",
			Message: "this shape was dropped by an operator"})
		dropped++
	}
	writeJSON(w, http.StatusOK, map[string]any{"dropped": dropped})
}

func writeJSON(w http.ResponseWriter, code int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Vary", "Authorization")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(body)
}

func writeErr(w http.ResponseWriter, code int, errCode, msg string) {
	writeJSON(w, code, map[string]any{"error": errCode, "message": msg})
}
