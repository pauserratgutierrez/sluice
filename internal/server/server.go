// Package server exposes Sluice's HTTP surface and wires the pieces together.
//
// Transport rules:
//
//   - POST only. The long-lived stream is a POST whose response is
//     text/event-stream, not an EventSource, so the token travels in
//     `Authorization: Bearer` and never in a URL.
//   - Closing the stream is the signal. There is no unsubscribe-on-disconnect
//     message; everything the stream held is released when it closes.
//   - The stream id may be mirrored into the Sluice-Stream-Id header, and a
//     request whose header and body disagree is rejected, so an intermediary
//     routing on the header cannot act on a different stream than the server.
package server

import (
	"cmp"
	"context"
	"crypto/hmac"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
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
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/requestid"
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
	// sessions is nil unless SLUICE_REVOCATION_SESSION_LOOKUP is on.
	sessions *sessionLookup

	ctx context.Context

	wheel   *timer.Wheel
	snapSem chan struct{}
	hooks   *hookCache
	// hookRecheck is held while a round of hook re-checks runs, so a slow
	// endpoint cannot stack rounds up across ticks.
	hookRecheck atomic.Bool

	// admission refuses new streams when memory nears the Go memory limit.
	admission   *admission
	capacityLog refusalLog
	// opening counts the stream opens admitted but not yet in the hub.
	opening atomic.Int64

	// replicated is set the first time this process starts replicating, and
	// never cleared. Until then, new streams are refused; see handleStream.
	replicated        atomic.Bool
	notReplicatingLog refusalLog

	// delayMs is the reader's lag when changes started arriving late, 0 while
	// they are not; see checkDelay. A stream's ready carries it.
	delayMs atomic.Int64

	// Reader goroutine only: when the transaction being dispatched began, and
	// the streams it queued events for without waking their writers, which
	// its commit wakes (see sendChange).
	txnBegan time.Time
	wakes    []*hub.Stream
	woken    map[*hub.Stream]bool

	streamSeq atomic.Uint64
	refreshAt atomic.Int64
	// draining is set once shutdown begins; see Drain.
	draining atomic.Bool
	// shutdownCause says why the process is stopping when that is not a
	// signal; see DrainFor.
	shutdownCause atomic.Pointer[string]
	// authzVer is the catalog.AuthzVersion the live decisions were resolved
	// against. See RefreshLeases.
	authzVer atomic.Uint64

	// Revocation tables, resolved to OIDs on their first Relation message so the
	// per-change check costs an integer comparison rather than string building.
	sessionsOID atomic.Uint32
	usersOID    atomic.Uint32

	// startupWarnings are the non-fatal findings of startup validation, kept so
	// /diagnostics and sluice_config_warnings report them for the process's
	// whole life.
	startupWarnings []Diagnostic
	// retention is the slot's WAL retention as of the last catalog tick; see
	// noteRetention.
	retention atomic.Pointer[retention]

	// holdCheck, if set, replaces hold.Check. Tests inject it so holds can be
	// checked without a database pool.
	holdCheck func(ctx context.Context, specs []hold.Spec) []error
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
	// StartupWarnings are reported by /diagnostics alongside the live findings.
	StartupWarnings []Diagnostic
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
		wheel:           timer.New(o.Config.Heartbeat, 64),
		snapSem:         make(chan struct{}, max(1, o.Config.SnapshotMaxConc)),
		hooks:           newHookCache(o.Config.HookTTL, o.Config.HookTimeout, o.Config.IssuerIdleTimeout, o.Config.HookBearer),
		admission:       newAdmission(o.Config.AdmitMemory),
		startupWarnings: o.StartupWarnings,
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
	if o.Config.SessionLookup && o.Pool != nil {
		s.sessions = newSessionLookup(o.Pool, o.Config.SessionsTable)
	}
	return s
}

// Run drives the server's background loops until the context is cancelled.
func (s *Server) Run(ctx context.Context) {
	go s.watchDelay(ctx)
	s.wheel.Run(ctx)
}

// SetReader wires the reader after construction, because the reader needs the
// server as its handler.
func (s *Server) SetReader(r *reader.Reader) {
	s.reader = r
	metrics.SetReaderLag(func() float64 { return r.Lag().Seconds() })
}

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
		metricsHandler := s.withIndexGauges(promhttp.Handler())
		mux.Handle("GET "+p+"/metrics", metricsHandler)
		mux.Handle("GET /metrics", metricsHandler)
	}
	if s.cfg.DiagnosticsEnabled {
		mux.HandleFunc("GET "+p+"/diagnostics", s.handleDiagnostics)
	}

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusNotFound, map[string]any{
			"error": "not_found", "message": "no such endpoint",
		})
	})
	return s.withRequestID(mux)
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
	// Resumed is set when the request carried a resume position for the
	// shape's table: true when every change since it is being replayed, false
	// when the buffer does not reach back that far.
	Resumed *bool `json:"resumed,omitempty"`

	// warn marks a refusal that is not the caller's doing (an issuer or hook
	// that gave no verdict, a node-wide limit), so it is logged as a warning.
	warn bool
}

// ---------------------------------------------------------------------------
// Stream
// ---------------------------------------------------------------------------

func (s *Server) handleStream(w http.ResponseWriter, r *http.Request) {
	// Capacity is decided before the token is verified: a refused storm of
	// reconnects should cost as little as possible, and a 503 says nothing
	// about the caller.
	if s.draining.Load() {
		s.refuse(w, r, http.StatusServiceUnavailable, "server_shutdown",
			"this node is shutting down; reconnect to another or retry shortly")
		return
	}
	// A stream is live from its ready, so it opens only once this process
	// replicates. While the slot is being created, a transaction that commits
	// before the slot's consistent point is never decoded: a client told it
	// was live would read its state, then miss every change committed between
	// that read and the consistent point, with nothing to say so. A process
	// standing by for the reader lock would accept streams and deliver nothing
	// until it took over. Once replication has started, a reader reconnecting
	// later resumes from the slot's confirmed position and loses nothing, so
	// streams keep opening.
	if s.reader != nil && !s.replicated.Load() {
		s.refuseLater(w, &s.notReplicatingLog, "not_replicating", "not_replicating",
			"this node is not replicating yet; retry shortly")
		return
	}
	// Opens in progress hold a reservation from here until hub.Open counts them,
	// so the opens of a storm, all arriving before any is counted, are not all
	// measured against the same number of streams.
	open, pending := s.hub.Count(), int(s.opening.Add(1))-1
	reserved := true
	release := func() {
		if reserved {
			reserved = false
			s.opening.Add(-1)
		}
	}
	defer release()
	if s.cfg.MaxStreams > 0 && open+pending >= s.cfg.MaxStreams {
		s.refuseCapacity(w, "max_streams", "this node is at its configured stream limit")
		return
	}
	ok, share := s.admission.admit(open, pending)
	metrics.AdmissionMemoryShare.Set(share)
	if !ok {
		s.refuseCapacity(w, "memory", fmt.Sprintf("this node is near its memory limit (%.0f%% in use, refusing new streams from %.0f%%)", share*100, s.cfg.AdmitMemory*100))
		return
	}
	id, err := s.identify(r)
	if err != nil {
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	if !s.checkSession(w, r, id) {
		return
	}

	// An empty body opens a stream with no subscriptions; a malformed one is an
	// error, not an empty stream the client would wait on forever.
	var req streamReq
	if r.Body != nil {
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
			return
		}
	}

	// The node id prefix says which process owns the stream; control requests
	// must reach that process.
	streamID := fmt.Sprintf("%s.%d-%d", s.cfg.NodeID, time.Now().UnixNano(), s.streamSeq.Add(1))

	st := s.hub.Open(streamID, requestid.From(r.Context()).Value, id)
	release()
	if s.draining.Load() {
		// Drain may have listed the open streams before this one was added.
		st.CloseWith("server_shutdown")
	}
	metrics.Streams.Set(float64(s.hub.Count()))
	var writeFailure error
	defer func() {
		s.holds.RemoveStream(streamID)
		for _, sub := range s.reg.RemoveStream(streamID) {
			s.decSubMetric(sub)
		}
		s.hub.Close(streamID, cmp.Or(st.CloseCode(), "client_closed"))
		code := cmp.Or(st.CloseCode(), "client_closed")
		metrics.StreamClosed.WithLabelValues(code).Inc()
		metrics.Streams.Set(float64(s.hub.Count()))
		// A client going away is how a stream normally ends. Any other ending
		// is logged under the ID of the request that opened the stream.
		if code != "client_closed" {
			s.logStreamEnd(r.Context(), st, code, writeFailure)
		}
	}()

	// SSE headers. X-Accel-Buffering: no stops nginx-style proxies from
	// accumulating events before forwarding them.
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

	liveFrom := s.liveFrom()
	results := s.applySubscriptions(r.Context(), st, id, req.Subscriptions, req.Resume)

	ready := map[string]any{
		"stream_id":     streamID,
		"server_time":   time.Now().UTC().Format(time.RFC3339Nano),
		"heartbeat_ms":  s.cfg.Heartbeat.Milliseconds(),
		"subscriptions": results,
	}
	if liveFrom != "" {
		ready["wal_lsn"] = liveFrom
	}
	if ms := s.delayMs.Load(); ms > 0 {
		ready["lag_ms"] = ms
	}
	_ = s.writeEvents(w, rc, []event.Event{{Kind: event.KindReady, Data: ready}})

	// One shared wheel instead of a timer per connection. The callback only
	// enqueues; all writing stays on this goroutine, which is what keeps the
	// ResponseWriter single-threaded.
	cancelTick := s.wheel.Add(func() { s.tick(st) })
	defer cancelTick()

	// Everything queued is written together and flushed once, so a burst costs
	// a stream one flush rather than one per event.
	var batch []event.Event
	for {
		select {
		case <-r.Context().Done():
			return
		case <-st.Ready():
			batch = st.Take(batch)
			if err := s.writeEvents(w, rc, batch); err != nil {
				writeFailure = err
				st.CloseWith("write_failed")
				return
			}
			batch = recycleBatch(batch)
		case <-st.Done():
			// Write what is already queued so a final error event reaches the
			// client before the connection goes away, then the reason the
			// server ended the stream when it could not be queued.
			batch = st.Take(batch)
			if ev, ok := s.closingError(st.CloseCode()); ok {
				batch = append(batch, event.Event{Kind: event.KindError, Data: ev})
			}
			_ = s.writeEvents(w, rc, batch)
			return
		}
	}
}

// recycleBatch empties a written batch for reuse, unless a burst grew it past
// what is worth keeping for the life of the stream.
func recycleBatch(batch []event.Event) []event.Event {
	clear(batch)
	if cap(batch) > 64 {
		return nil
	}
	return batch[:0]
}

// closingError is the error a stream gets last when the server ended it for a
// reason it could not queue: the queue was full, or the process is stopping.
func (s *Server) closingError(code string) (event.Error, bool) {
	switch code {
	case "stream_lagging":
		return event.Error{
			Code:      "stream_lagging",
			Message:   "the client did not keep up with the change stream",
			Retryable: true, Action: "resnapshot",
		}, true
	case "server_shutdown":
		e := event.Error{
			Code:      "server_shutdown",
			Message:   "the server is shutting down",
			Retryable: true,
		}
		if spread := s.cfg.ReconnectSpread; spread > 0 {
			e.RetryAfterMs = rand.N(spread).Milliseconds()
		}
		return e, true
	}
	return event.Error{}, false
}

// Drain ends every open stream with server_shutdown and refuses new ones.
//
// http.Server.Shutdown waits for handlers to return, and an SSE handler only
// returns when its stream ends. Without this a shutdown always ran out its
// grace period and the process was killed with every stream still open, so no
// client learned why or when to come back.
func (s *Server) Drain() {
	s.draining.Store(true)
	for _, st := range s.hub.Streams() {
		st.CloseWith("server_shutdown")
	}
}

// DrainFor is Drain when the process stops for a reason other than a signal.
// Clients still get server_shutdown; the cause is in the line logged for each
// stream.
func (s *Server) DrainFor(cause string) {
	s.shutdownCause.Store(&cause)
	s.Drain()
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

// writeEvents writes a batch of events and flushes once. The ResponseWriter
// buffers what is written to it, so frames go straight in and reach the socket
// as that buffer fills, and the flush sends the rest.
func (s *Server) writeEvents(w http.ResponseWriter, rc *http.ResponseController, evs []event.Event) error {
	if len(evs) == 0 {
		return nil
	}
	for _, ev := range evs {
		// Bounding each write is what stops a slow consumer from pinning a
		// goroutine forever, without needing a watchdog per connection. The
		// flush runs under the last frame's deadline.
		_ = rc.SetWriteDeadline(time.Now().Add(s.cfg.WriteTimeout))
		if err := writeFrame(w, ev); err != nil {
			return err
		}
	}
	return rc.Flush()
}

// writeFrame writes one event in SSE framing.
func writeFrame(w io.Writer, ev event.Event) error {
	// A bare comment, exactly as the WHATWG spec recommends for keeping proxies
	// from dropping an idle stream.
	if ev.Kind == event.KindHeartbeat {
		_, err := io.WriteString(w, ": hb\n\n")
		return err
	}

	body, err := json.Marshal(ev.Data)
	if err != nil {
		return err
	}
	frame := make([]byte, 0, len(body)+len(ev.Kind)+len(ev.ID)+24)
	frame = append(frame, "event: "...)
	frame = append(frame, ev.Kind...)
	frame = append(frame, '\n')
	if ev.ID != "" {
		frame = append(frame, "id: "...)
		frame = append(frame, ev.ID...)
		frame = append(frame, '\n')
	}
	frame = append(frame, "data: "...)
	frame = append(frame, body...)
	// The blank line is what dispatches the event; an event without it is
	// discarded by the client.
	frame = append(frame, "\n\n"...)
	_, err = w.Write(frame)
	return err
}

// ---------------------------------------------------------------------------
// Subscribe / unsubscribe
// ---------------------------------------------------------------------------

func (s *Server) handleSubscribe(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req subscribeReq
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	if !st.Allow("subscribe", s.cfg.SubscribeRate, time.Second) {
		s.refuseStream(w, r, st, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may issue at most %d subscribe requests per second", s.cfg.SubscribeRate))
		return
	}
	liveFrom := s.liveFrom()
	results := s.applySubscriptions(r.Context(), st, id, req.Subscriptions, nil)
	body := map[string]any{"results": results}
	if liveFrom != "" {
		body["wal_lsn"] = liveFrom
	}
	writeJSON(w, http.StatusOK, body)
}

// liveFrom is a resume position for subscriptions installed after this call:
// resuming from it later replays everything they may have missed. It is the
// reader's confirmed LSN taken BEFORE installing -- the same floor a snapshot
// uses. Every change dispatched after this point commits at or after it, so a
// change dispatched in the instant before a subscription existed is still
// covered; one dispatched earlier committed before the subscription, and the
// client's own read sees it.
func (s *Server) liveFrom() string {
	if s.reader == nil {
		return ""
	}
	return reader.FormatLSN(s.reader.ConfirmedLSN())
}

func (s *Server) handleUnsubscribe(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	var req struct {
		StreamID string   `json:"stream_id"`
		Subs     []string `json:"subs"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
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
//
// Shapes are installed together once every subscription has been looked at
// (installShapes), so that their holds are checked in one round trip. Until
// then a planned shape holds its place against the stream's limits; one whose
// holds turn out missing leaves its place to the next request.
func (s *Server) applySubscriptions(
	ctx context.Context,
	st *hub.Stream,
	id authz.Identity,
	specs []subSpec,
	resume map[string]string,
) []subResult {

	out := make([]subResult, len(specs))
	shapes := len(s.reg.StreamSubscriptions(st.StreamID()))
	existing := shapes + len(st.Channels())
	prepared := s.prepareShapes(ctx, id, specs,
		min(s.cfg.MaxShapesPerStream-shapes, s.cfg.MaxSubsPerStream-existing))

	var planned []plannedShape
	unindexed := 0
	for i, spec := range specs {
		if spec.Sub == "" {
			out[i] = subResult{OK: false, Error: &event.Error{
				Code: "invalid_subscription", Message: "each subscription needs a `sub` label"}}
			continue
		}
		if existing >= s.cfg.MaxSubsPerStream {
			out[i] = subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
				Code: "too_many_subscriptions", Message: "this stream is at its subscription limit"}}
			continue
		}

		switch {
		case spec.Shape != nil:
			// Shapes are capped separately from channels: a shape costs an
			// authorization resolution and a registry entry consulted on every
			// change to its relation, where a channel costs a map entry.
			if shapes >= s.cfg.MaxShapesPerStream {
				out[i] = subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
					Code: "too_many_shapes", Message: fmt.Sprintf(
						"this stream is at its limit of %d shape subscriptions", s.cfg.MaxShapesPerStream)}}
				continue
			}
			prep, ok := prepared[i]
			if !ok {
				prep = s.prepareShape(ctx, id, spec)
			}
			p, res := s.planShape(st, id, spec, prep, resume, unindexed)
			if res.Error != nil {
				out[i] = res
				continue
			}
			p.at = i
			planned = append(planned, p)
			if !p.sub.Indexed() {
				unindexed++
			}
			existing++
			shapes++
		case spec.Channel != "":
			res := s.subscribeChannel(ctx, st, id, spec)
			out[i] = res
			if res.OK {
				existing++
			}
		default:
			out[i] = subResult{Sub: spec.Sub, OK: false, Error: &event.Error{
				Code: "invalid_subscription", Message: "provide either `shape` or `channel`"}}
		}
	}
	for k, res := range s.installShapes(ctx, st, planned) {
		out[planned[k].at] = res
	}
	s.logRefusals(ctx, st, specs, out)
	return out
}

// prepareShapes checks the first `room` shapes of a request -- as many as the
// stream's limits could admit -- and resolves them with one oracle call: in
// issuer mode, one round trip for all of them. Resolving is the slow part of a
// subscribe and touches nothing of the stream's. Admission, limits and
// installation stay in request order in applySubscriptions, which resolves any
// later shape itself if an earlier one fails and leaves room.
func (s *Server) prepareShapes(ctx context.Context, id authz.Identity, specs []subSpec, room int) map[int]preparedShape {
	out := map[int]preparedShape{}
	var idx []int
	var reqs []oracle.Request
	for i, spec := range specs {
		if len(out) >= room {
			break
		}
		if spec.Sub == "" || spec.Shape == nil {
			continue
		}
		p := s.checkShape(spec)
		out[i] = p
		if p.res.Error == nil {
			idx = append(idx, i)
			reqs = append(reqs, oracle.Request{Label: spec.Sub, Relation: p.rel,
				Filter: p.requested.Filter, Columns: p.requested.Columns, Ops: p.requested.Ops})
		}
	}
	if len(reqs) == 0 {
		return out
	}
	start := time.Now()
	results := s.oracle.Resolve(ctx, oracle.ActionSubscribe, id, reqs)
	elapsed := time.Since(start)
	for j, i := range idx {
		p := out[i]
		if err := results[j].Err; err != nil {
			s.mapOracleErr(&p.res, err)
			out[i] = p
			continue
		}
		grant := results[j].Grant
		tierLabel := s.tierLabel(grant.Decision)
		metrics.AuthzResolveSeconds.WithLabelValues(tierLabel).Observe(elapsed.Seconds())
		metrics.AuthzResolutions.WithLabelValues(tierLabel, "granted").Inc()
		if d := grant.Decision; d != nil && d.Tier == authz.TierC && s.oracle.Name() == oracle.NameRLS {
			metrics.AuthzCompileFailures.WithLabelValues(p.rel.Schema, p.rel.Name, truncate(d.Reason, 60)).Inc()
		}
		p.grant = grant
		out[i] = p
	}
	return out
}

// prepareShape checks and resolves one shape, which has a label.
func (s *Server) prepareShape(ctx context.Context, id authz.Identity, spec subSpec) preparedShape {
	return s.prepareShapes(ctx, id, []subSpec{spec}, 1)[0]
}

// preparedShape is a shape request validated and resolved by the oracle, not
// yet installed. A failure is carried in res.
type preparedShape struct {
	res       subResult
	rel       *catalog.Relation
	ops       shape.Ops
	grant     *oracle.Grant
	requested registry.Requested
}

// checkShape validates a shape request against the catalog, before the oracle
// is asked about it.
func (s *Server) checkShape(spec subSpec) preparedShape {
	p := preparedShape{res: subResult{Sub: spec.Sub}}
	sp := spec.Shape

	schema := cmp.Or(sp.Schema, "public")
	rel, ok := s.cat.Lookup(schema, sp.Table)
	if !ok {
		p.res.Error = &event.Error{Code: "relation_not_published", Message: fmt.Sprintf(
			"%s.%s is not in publication %q; add it with: ALTER PUBLICATION %s ADD TABLE %s.%s",
			schema, sp.Table, s.cfg.Publication, s.cfg.Publication, schema, sp.Table)}
		return p
	}
	// Published for revocation only: the pool role need not be able to read it,
	// and session rows are not for streaming.
	if slices.Contains(s.cfg.RevocationTables(), rel.FullName()) {
		p.res.Error = &event.Error{Code: "shape_not_authorized", Message: fmt.Sprintf(
			"%s is published for session revocation, not for subscriptions", rel.FullName())}
		return p
	}

	ops, err := shape.ParseOps(sp.Ops)
	if err != nil {
		p.res.Error = &event.Error{Code: "invalid_ops", Message: err.Error()}
		return p
	}

	filter, err := shape.Parse(sp.Filter, rel)
	if err != nil {
		p.res.Error = &event.Error{Code: "invalid_filter", Message: err.Error()}
		return p
	}
	for _, c := range sp.Columns {
		if _, ok := rel.Column(c); !ok {
			p.res.Error = &event.Error{Code: "invalid_columns", Message: fmt.Sprintf(
				"column %q does not exist on %s", c, rel.FullName())}
			return p
		}
	}

	if s.oracle == nil {
		p.res.Error = &event.Error{Code: "internal", Message: "shape oracle is not configured"}
		p.res.warn = true
		return p
	}
	p.rel, p.ops = rel, ops
	p.requested = registry.Requested{Filter: filter, Columns: sp.Columns, Ops: sp.Ops}
	return p
}

// plannedShape is a shape of a request that passed every check before its
// holds, waiting for installShapes.
type plannedShape struct {
	at       int // its place in the request
	sub      *registry.Subscription
	grant    *oracle.Grant
	warnings []event.Warning
	res      subResult // its result so far: the label, and how a refusal is logged
	// resume is the request's position for the shape's table, from as parsed.
	resume   string
	from     uint64
	fromErr  error
	snapshot bool
}

// planShape builds a resolved shape's subscription and checks it against the
// node's limits. unindexed counts the unindexed shapes planned earlier in the
// same request, not yet registered. The result carries a refusal, or only the
// label when the shape is planned.
func (s *Server) planShape(
	st *hub.Stream,
	id authz.Identity,
	spec subSpec,
	prep preparedShape,
	resume map[string]string,
	unindexed int,
) (plannedShape, subResult) {

	res := prep.res
	if res.Error != nil {
		return plannedShape{}, res
	}
	sp := spec.Shape
	rel, ops, grant := prep.rel, prep.ops, prep.grant
	decision := grant.Decision
	filter := grant.Filter

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
		Requested:   prep.requested,
	}

	// Warnings. Every one carries a remedy that is a runnable statement, because
	// a warning nobody can act on is noise.
	var warnings []event.Warning
	if len(grant.Denied) > 0 {
		remedy := fmt.Sprintf("GRANT SELECT (%s) ON %s TO %s;", strings.Join(grant.Denied, ", "), rel.FullName(), id.Role)
		if s.oracle.Name() == oracle.NameIssuer {
			remedy = fmt.Sprintf("GRANT SELECT (%s) ON %s TO the Sluice role, or ask the issuer to allow those columns;", strings.Join(grant.Denied, ", "), rel.FullName())
		}
		warnings = append(warnings, event.Warning{
			Sub: spec.Sub, Code: "columns_not_granted",
			Message: "these columns were dropped from the projection: " + strings.Join(grant.Denied, ", "),
			Effect:  "events will not contain them",
			Remedy:  remedy,
		})
	}
	if sub.RoutingKey == "" {
		warnings = append(warnings, event.Warning{
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
			pk := strings.Join(rel.KeyColumns, ", ")
			if pk == "" {
				pk = "id"
			}
			idx := rel.Name + "_ri"
			w := event.Warning{
				Sub: spec.Sub, Code: "replica_identity_insufficient",
				Message: fmt.Sprintf(
					"DELETE events and rows leaving this shape cannot be detected: column(s) %s are not in the replica identity of %s (currently %s)",
					strings.Join(missing, ", "), rel.FullName(), ri),
				Effect: "DELETE events are withheld (or delivered marked degraded with SLUICE_DEGRADED_DELETES=deliver), and transitions cannot report a leave",
				Remedy: fmt.Sprintf(
					"CREATE UNIQUE INDEX %s ON %s (%s, %s); ALTER TABLE %s REPLICA IDENTITY USING INDEX %s;",
					idx, rel.FullName(), strings.Join(missing, ", "), pk, rel.FullName(), idx),
			}
			if s.cfg.ReplicaIdentity == "strict" {
				res.Error = &event.Error{Code: w.Code, Message: w.Message + ". Remedy: " + w.Remedy}
				return plannedShape{}, res
			}
			warnings = append(warnings, w)
		}
	}
	if rel.ReplicaIdentity == 'f' && rel.HasToastableColumn {
		warnings = append(warnings, event.Warning{
			Sub: spec.Sub, Code: "replica_identity_full_with_toast",
			Message: rel.FullName() + " uses REPLICA IDENTITY FULL and has a TOAST-able column",
			Effect:  "every UPDATE and DELETE carries the whole out-of-line value in the old tuple, even when that column did not change",
			Remedy:  "switch to REPLICA IDENTITY USING INDEX over only the columns actually needed",
		})
	}
	if !rel.ReplicaIdentityOK {
		warnings = append(warnings, event.Warning{
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
	if sub.RoutingKey == "" && s.reg.UnindexedCount()+unindexed >= s.cfg.UnindexedMax {
		res.Error = &event.Error{Code: "too_many_unindexed_shapes", Message: fmt.Sprintf(
			"this node already has %d unindexed subscriptions, the configured maximum; "+
				"filter on an indexed column with an equality, or index the filtered column",
			s.cfg.UnindexedMax)}
		res.warn = true
		return plannedShape{}, res
	}

	wantSnapshot := sp.Initial == "snapshot"
	if wantSnapshot && !s.cfg.SnapshotEnabled {
		wantSnapshot = false
		warnings = append(warnings, event.Warning{
			Sub: spec.Sub, Code: "snapshot_disabled",
			Message: "initial snapshots are disabled on this server",
			Remedy:  "set SLUICE_SNAPSHOT_ENABLED=true, or fetch the initial state yourself",
		})
	}

	// A resume the buffer covers keeps the reader's changes from the
	// subscription until its replay has caught up (see registry.Backfill).
	// Decided before the install, so that none goes out ahead of the replay.
	p := plannedShape{sub: sub, grant: grant, warnings: warnings, res: res,
		resume: resume[rel.FullName()], snapshot: wantSnapshot}
	if p.resume != "" {
		if p.from, p.fromErr = reader.ParseLSN(p.resume); p.fromErr == nil && s.hub.Rings().Covers(rel.OID, p.from) {
			sub.Backfill = &registry.Backfill{}
		}
	}
	return p, res
}

// installShapes installs a request's planned shapes. The watches of every
// shape go into the hold index first; then the request's distinct holds are
// checked in one round trip; then each shape whose holds exist is registered,
// under the hold index's lock and only while its watch is current
// (hold.Index.Activate). So a DELETE racing the join is caught, by the check
// before the watch is added and by the watch after, and a shape receives no
// change before its holds are confirmed. It returns each shape's result.
func (s *Server) installShapes(ctx context.Context, st *hub.Stream, planned []plannedShape) []subResult {
	sid := st.StreamID()
	failed := make([]error, len(planned))
	watched := make([][]hold.Spec, len(planned))
	for k, p := range planned {
		if len(p.grant.Holds) == 0 {
			continue
		}
		if !s.holds.Add(sid, p.sub.Label, p.grant.Holds) {
			failed[k] = errDuplicateSub
			continue
		}
		watched[k] = p.grant.Holds
	}
	checked := s.checkHolds(ctx, watched)
	// Before any shape is registered: every change appended from here on is
	// routed to the shape or found in the buffer by its replay.
	installedAt := s.hub.Rings().Seq()

	out := make([]subResult, len(planned))
	for k, p := range planned {
		register := func() error {
			if !s.reg.Add(p.sub) {
				return errDuplicateSub
			}
			return nil
		}
		err := failed[k]
		switch {
		case err != nil:
		case checked[k] != nil:
			err = checked[k]
			s.holds.Remove(sid, p.sub.Label)
		case watched[k] != nil:
			err = s.holds.Activate(sid, p.sub.Label, register)
		default:
			err = register()
		}
		out[k] = p.res
		switch {
		case errors.Is(err, errDuplicateSub):
			out[k].Error = &event.Error{Code: "duplicate_sub",
				Message: "this stream already has a subscription labelled " + p.sub.Label}
		case err != nil:
			out[k].Error = holdCheckFailed(err)
		default:
			out[k] = s.finishShape(st, p, installedAt)
		}
	}
	return out
}

// finishShape completes the result of a shape just registered, and starts its
// replay or snapshot. installedAt is the buffer's append order before it was
// registered.
func (s *Server) finishShape(st *hub.Stream, p plannedShape, installedAt uint64) subResult {
	sub, res := p.sub, p.res
	s.incSubMetric(sub)

	res.OK = true
	indexed := sub.Indexed()
	res.Indexed = &indexed
	res.RoutingKey = sub.RoutingKey
	res.Reason = p.grant.Reason
	res.Warnings = p.warnings
	if s.oracle.Name() == oracle.NameIssuer {
		res.Oracle = oracle.NameIssuer
		if sub.Filter != nil {
			if d := sub.Filter.Describe(); d != "" && d != "(none)" {
				res.Filter = d
			}
		}
	} else if d := p.grant.Decision; d != nil {
		res.Tier = string(d.Tier)
	}

	for _, w := range p.warnings {
		hub.SendWarning(st, w)
	}

	// A resume and a snapshot are alternatives: resuming means the client
	// already holds a base state to apply changes to. Whether the buffer covers
	// the position is decided here, so the result tells the client; one it
	// cannot cover is not silently replaced by a snapshot, because only the
	// client knows to discard the state it holds.
	if p.resume != "" {
		if p.fromErr != nil {
			hub.SendError(st, event.Error{Sub: sub.Label, Code: "invalid_resume",
				Message: "resume LSN could not be parsed"})
			return res
		}
		if sub.Backfill == nil {
			res.Resumed = new(false)
			return res
		}
		entries, through, ok := s.hub.Rings().ReplayThrough(sub.Relation.OID, p.from, routedTo(sub))
		res.Resumed = &ok
		if !ok {
			// Removed since Covers looked. The client reads its state again;
			// it still gets what the reader kept from it since the install.
			entries, through = nil, installedAt
		}
		if len(entries) == 0 {
			if _, _, _, done := s.handOver(sub, through); done {
				return res
			}
		}
		// Off the request: a replay waits for the writer to make room, and
		// on a stream being opened the writer only starts after the ready
		// event.
		go s.catchUp(sub, entries, through)
		return res
	}
	if p.snapshot {
		go s.snapshot(s.ctx, sub)
	}
	return res
}

// catchUp replays a resumed subscription's entries, then what the buffer took
// since, until nothing is left; from then on the reader delivers to it (see
// registry.Backfill). A buffer that dropped changes it still had to replay
// closes the stream as stream_lagging, as a full queue does, and the client
// resumes.
func (s *Server) catchUp(sub *registry.Subscription, entries []hub.RingEntry, through uint64) {
	for {
		if !s.replay(sub, entries) {
			return
		}
		rest, next, ok, done := s.handOver(sub, through)
		if done {
			return
		}
		if !ok {
			if st := streamOf(sub); st != nil {
				st.CloseWith("stream_lagging")
			}
			return
		}
		entries, through = rest, next
	}
}

// handOver lets the reader take a resumed subscription over when the buffer
// has taken nothing for it since through; otherwise it returns what it took,
// and the append order reached. done is also true once the subscription is
// gone.
func (s *Server) handOver(sub *registry.Subscription, through uint64) (rest []hub.RingEntry, next uint64, ok, done bool) {
	cur := s.reg.Get(sub.Sink.StreamID(), sub.Label)
	if cur == nil {
		return nil, through, true, true
	}
	next, ok = through, true
	done = sub.Backfill.CatchUp(func() (uint64, bool) {
		rest, next, ok = s.hub.Rings().Since(sub.Relation.OID, through, routedTo(cur))
		return next, ok && len(rest) == 0
	})
	return rest, next, ok, done
}

// routedTo keeps the buffered changes the reader would route to a subscription,
// as Registry.Candidates does: those whose new or old row holds its routing
// key's constant, or that hold that column in neither. Nil, keeping every
// change, for an unindexed subscription.
//
// Measured before it: each subscription that resumed copied every change to
// its table since its position, 250 of them for a room's messages, and the
// thousands that resume after a cut held tens of MiB while they waited for
// their queues; with no admission to spread them, a node of 512 MB went past
// its memory limit.
func routedTo(sub *registry.Subscription) func(*hub.RingEntry) bool {
	col := sub.RoutingKey
	if col == "" {
		return nil
	}
	want := sub.Filter.Equalities[col].String()
	return func(e *hub.RingEntry) bool {
		known := false
		for _, t := range [2]*pgoutput.Tuple{e.New, e.Old} {
			if v, ok := (tupleRow{rel: e.Relation, t: t}).Column(col); ok {
				if v.String() == want {
					return true
				}
				known = true
			}
		}
		return !known
	}
}

func errResumeTooOld(label string) event.Error {
	return event.Error{Sub: label, Code: "resume_too_old",
		Message:   "the requested position is no longer in the server's buffer",
		Retryable: true, Action: "resnapshot"}
}

// replayFrom re-delivers buffered changes at or after `from`. It returns false,
// delivering nothing, when the buffer cannot cover it.
func (s *Server) replayFrom(sub *registry.Subscription, from uint64) bool {
	entries, _, ok := s.hub.Rings().ReplayThrough(sub.Relation.OID, from, routedTo(sub))
	if ok {
		s.replay(sub, entries)
	}
	return ok
}

// replay re-delivers buffered changes, re-filtered and re-authorized: replay is
// never trusted to have been authorized on its first pass. It returns false
// once the subscription is gone.
func (s *Server) replay(sub *registry.Subscription, entries []hub.RingEntry) bool {
	if s.reg.Get(sub.Sink.StreamID(), sub.Label) == nil {
		return false
	}
	for _, e := range entries {
		// A replay can outlast the subscription, or a /token can rebind it to a
		// new filter and columns while it runs; follow what is registered now.
		cur := s.reg.Get(sub.Sink.StreamID(), sub.Label)
		if cur == nil {
			return false
		}
		if !wants(cur, e.Op) {
			continue
		}
		m := messageFromRing(e)
		s.deliver(cur, m, e.Relation,
			tupleRow{rel: e.Relation, t: e.New}, tupleRow{rel: e.Relation, t: e.Old},
			s.newTuples(e.Relation, m),
			e.LSN, e.CommitTime, opName(e.Op), true)
	}
	return true
}

func (s *Server) subscribeChannel(ctx context.Context, st *hub.Stream, id authz.Identity, spec subSpec) subResult {
	res := subResult{Sub: spec.Sub}
	ch, ok := s.cfg.Channel(spec.Channel)
	if !ok {
		res.Error = &event.Error{Code: "unknown_namespace", Message: fmt.Sprintf(
			"channel %q has no configured namespace; add it to SLUICE_CHANNELS", spec.Channel)}
		return res
	}
	// A hook verdict is a lease: the join is asked about again when it expires.
	var recheckAt time.Time
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
		// Asked under the server's context, not the request's: the verdict is
		// cached for the session, so the caller going away must not leave a
		// failed one behind. The call still carries the request's ID.
		v := s.hooks.Authorize(requestid.With(s.ctx, requestid.From(ctx)), ch, id, spec.Channel, false)
		if v.outcome == hookUnavailable {
			// No verdict says nothing about the caller. The join is refused for
			// now (fail closed) and retryable, like a shape the issuer could
			// not decide, once the cached answer has expired.
			delay := max(issuerRetryMin+rand.N(issuerRetrySpread), v.expires.Sub(s.hooks.now()))
			res.Error = &event.Error{Code: "channel_hook_unavailable", Message: v.reason,
				Retryable: true, RetryAfterMs: delay.Milliseconds()}
			res.warn = true
			return res
		}
		if !v.allowed() {
			res.Error = &event.Error{Code: "channel_not_authorized", Message: v.reason}
			return res
		}
		recheckAt = v.expires
	}

	if !s.hub.JoinChannel(spec.Channel, st, spec.Sub, recheckAt) {
		res.Error = &event.Error{Code: "stream_closed", Message: "the stream closed during the subscribe"}
		return res
	}
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
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
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
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	if len(req.Payload) > s.cfg.MaxPayloadBytes {
		s.refuse(w, r, http.StatusRequestEntityTooLarge, "payload_too_large", fmt.Sprintf(
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
		s.refuseStream(w, r, st, http.StatusForbidden, "channel_not_subscribed",
			"subscribe to a channel before publishing to it", "channel", truncate(req.Channel, maxLogged))
		return
	}
	if !st.Allow("publish", s.cfg.PublishRate, time.Second) {
		s.refuseStream(w, r, st, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may publish at most %d messages per second", s.cfg.PublishRate),
			"channel", truncate(req.Channel, maxLogged))
		return
	}
	n := s.hub.PublishBroadcast(req.Channel, cmp.Or(req.Event, "message"),
		id.Sub, "client", "", req.Payload, req.Self, st.StreamID())

	metrics.BroadcastPublished.WithLabelValues(namespaceOf(req.Channel), "client").Inc()
	writeJSON(w, http.StatusOK, map[string]any{"delivered": n})
}

// namespaceOf returns the part of a channel name before the first colon.
func namespaceOf(channel string) string {
	if i := strings.IndexByte(channel, ':'); i >= 0 {
		return channel[:i]
	}
	return channel
}

func (s *Server) handlePresence(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil {
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
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
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	st, ok := s.resolveStream(w, r, req.StreamID, id)
	if !ok {
		return
	}
	channel := truncate(req.Channel, maxLogged)
	if _, subscribed := st.ChannelLabel(req.Channel); !subscribed {
		s.refuseStream(w, r, st, http.StatusForbidden, "channel_not_subscribed",
			"subscribe to a channel before tracking presence on it", "channel", channel)
		return
	}
	// A presence key is the caller's subject, so a roster can only ever list
	// identities the JWT proves. A token without a subject has nothing to track.
	if id.Sub == "" || cmp.Or(req.Key, id.Sub) != id.Sub {
		s.refuseStream(w, r, st, http.StatusForbidden, "presence_key_not_allowed",
			"presence requires a token with a sub, and the key must equal it", "channel", channel)
		return
	}
	key := id.Sub
	if !st.Allow("presence", s.cfg.PresenceRate, s.cfg.PresenceWindow) {
		s.refuseStream(w, r, st, http.StatusTooManyRequests, "rate_limited", fmt.Sprintf(
			"this stream may send at most %d presence updates per %s", s.cfg.PresenceRate, s.cfg.PresenceWindow),
			"channel", channel)
		return
	}

	switch req.Action {
	case "track", "update", "":
		if _, ok := s.hub.Presence().Track(req.Channel, key, st.StreamID(), req.Meta, s.cfg.PresenceMaxKeys); !ok {
			s.refuseStream(w, r, st, http.StatusTooManyRequests, "presence_too_many_keys", fmt.Sprintf(
				"channel %q is at its %d-key limit", req.Channel, s.cfg.PresenceMaxKeys), "channel", channel)
			return
		}
	case "untrack":
		s.hub.Presence().UntrackKey(req.Channel, key, st.StreamID())
	default:
		s.refuseStream(w, r, st, http.StatusBadRequest, "bad_request", "action must be track, update or untrack")
		return
	}
	metrics.PresenceUpdates.WithLabelValues(namespaceOf(req.Channel), cmp.Or(req.Action, "track")).Inc()
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
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	newID, err := s.verifyToken(r.Context(), "Bearer "+req.AccessToken)
	if err != nil {
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", err.Error())
		return
	}
	if !s.checkSession(w, r, newID) {
		return
	}
	st, ok := s.hub.Get(req.StreamID)
	if !ok {
		s.refuse(w, r, http.StatusNotFound, "unknown_stream", "no such stream on this node",
			"stream_id", truncate(req.StreamID, maxLogged))
		return
	}
	// A stream may not change identity mid-flight.
	if cur := st.Identity(); cur.Sub != "" && cur.Sub != newID.Sub {
		s.refuseStream(w, r, st, http.StatusForbidden, "subject_mismatch",
			"the refreshed token names a different subject")
		return
	}
	st.SetIdentity(newID)

	revoked := 0
	subs := s.reg.StreamSubscriptions(st.StreamID())
	if s.oracle != nil && s.oracle.Name() == oracle.NameIssuer {
		for _, result := range s.refreshIssuerShapes(r.Context(), st, subs, newID) {
			metrics.AuthzLeaseRefreshes.WithLabelValues(result).Inc()
			if result != "held" {
				revoked++
			}
		}
		subs = nil
	}
	for _, sub := range subs {
		eqs := sub.Filter.Equalities
		held, err := s.oracle.LeaseTick(r.Context(), sub.Decision, sub.Relation, newID, eqs)
		if err != nil || !held {
			metrics.AuthzLeaseRefreshes.WithLabelValues("revoked").Inc()
			s.revokeShape(r.Context(), st, sub.Label, false, event.Error{Code: "shape_not_authorized",
				Message: "the refreshed token no longer grants access to this shape"})
			revoked++
			continue
		}
		metrics.AuthzLeaseRefreshes.WithLabelValues("held").Inc()
	}
	revoked += s.recheckStreamHooks(r.Context(), st, newID)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "revoked_subscriptions": revoked})
}

func (s *Server) handleJWKSRefresh(w http.ResponseWriter, r *http.Request) {
	id, err := s.identify(r)
	if err != nil || id.Role != "service_role" {
		s.refuse(w, r, http.StatusForbidden, "forbidden", "service_role required")
		return
	}
	if err := s.verify.Refresh(r.Context()); err != nil {
		var warn *auth.WarnSymmetricKey
		if !errors.As(err, &warn) {
			s.refuse(w, r, http.StatusBadGateway, "jwks_refresh_failed", err.Error())
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

// handleReadyz reports whether this process should receive streams: its
// catalog is loaded and it is the one reading the replication slot. A process
// standing by for the reader lock serves no changes, so it is not ready.
func (s *Server) handleReadyz(w http.ResponseWriter, r *http.Request) {
	catalogLoaded := !s.cat.LoadedAt().IsZero()
	replicating := s.reader != nil && s.reader.Streaming()
	draining := s.draining.Load()
	code := http.StatusOK
	if !catalogLoaded || !replicating || draining {
		code = http.StatusServiceUnavailable
	}
	body := map[string]any{"catalog_loaded": catalogLoaded, "replicating": replicating, "streams": s.hub.Count()}
	if draining {
		body["draining"] = true
	}
	if s.reader != nil {
		body["confirmed_lsn"] = reader.FormatLSN(s.reader.ConfirmedLSN())
		lag := s.reader.Lag()
		body["reader_lag_seconds"] = lag.Seconds()
		// Behind by more than SLUICE_LAG_DEGRADED: still serving, but every
		// change arrives that late. Not ready only past SLUICE_READY_MAX_LAG,
		// when set: a single node should not take itself out of its balancer.
		if s.cfg.LagDegraded > 0 && lag >= s.cfg.LagDegraded {
			body["degraded"] = "reader_lag"
		}
		if s.cfg.ReadyMaxLag > 0 && lag >= s.cfg.ReadyMaxLag {
			code = http.StatusServiceUnavailable
		}
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
//
// Hook channel joins are leases too, expiring with their verdict; they are
// re-checked in the background (see recheckHookChannels).
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
		// Resolve against the current catalog entry for the same table. A
		// published subscription is never modified, so the fresh relation is
		// passed along rather than stored.
		rel, ok := s.cat.Get(sub.Relation.OID)
		if !ok {
			s.dropShape(st, sub.Label, event.Error{Code: "relation_unpublished",
				Message: sub.Relation.FullName() + " is no longer in the publication"})
			continue
		}

		if s.oracle == nil || s.oracle.Name() == oracle.NameIssuer {
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
			rel, ok := s.cat.Get(h.Rel.OID)
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

	s.startHookRechecks(ctx, now)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func (s *Server) identify(r *http.Request) (authz.Identity, error) {
	return s.verifyToken(r.Context(), r.Header.Get("Authorization"))
}

var errSessionRevoked = errors.New("auth: the session behind this token was signed out or the user was banned")

// verifyToken verifies a bearer token and refuses one whose session this
// process has already seen revoked, so a signed-out token cannot open a new
// stream or read a snapshot while it is still unexpired.
func (s *Server) verifyToken(ctx context.Context, bearer string) (authz.Identity, error) {
	id, err := s.verify.Verify(ctx, bearer)
	if err != nil {
		return id, err
	}
	if s.revoker != nil && s.revoker.Revoked(id) {
		return authz.Identity{}, errSessionRevoked
	}
	return id, nil
}

// resolveStream finds the stream a control request targets, and enforces the
// header/body agreement rule. Mirroring routing metadata into a header without
// checking it against the body is a documented vulnerability class: an
// intermediary routes on one value while the server acts on another.
func (s *Server) resolveStream(w http.ResponseWriter, r *http.Request, bodyID string, id authz.Identity) (*hub.Stream, bool) {
	headerID := r.Header.Get("Sluice-Stream-Id")
	if headerID != "" && bodyID != "" && headerID != bodyID {
		s.refuse(w, r, http.StatusBadRequest, "stream_id_mismatch",
			"Sluice-Stream-Id does not match the stream_id in the body")
		return nil, false
	}
	streamID := cmp.Or(bodyID, headerID)
	if streamID == "" {
		s.refuse(w, r, http.StatusBadRequest, "bad_request", "stream_id is required")
		return nil, false
	}
	st, ok := s.hub.Get(streamID)
	if !ok {
		// In a multi-node deployment this is where the request would be forwarded
		// to the owning node, using the node prefix in the stream id.
		s.refuse(w, r, http.StatusNotFound, "unknown_stream", "no such stream on this node",
			"stream_id", truncate(streamID, maxLogged))
		return nil, false
	}
	if cur := st.Identity(); cur.Sub != id.Sub || cur.Role != id.Role {
		s.refuseStream(w, r, st, http.StatusForbidden, "forbidden", "this token does not own that stream")
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
	var unavailable *oracle.ErrUnavailable
	var tierC *authz.ErrTierCDisabled
	switch {
	case errors.As(err, &denied):
		metrics.AuthzResolutions.WithLabelValues("-", "denied").Inc()
		res.Error = &event.Error{Code: "shape_not_authorized", Message: denied.Reason}
	case errors.As(err, &issuerDenied):
		metrics.AuthzResolutions.WithLabelValues("-", "denied").Inc()
		res.Error = &event.Error{Code: issuerDenied.WireCode(), Message: issuerDenied.Reason}
	case errors.As(err, &unavailable):
		metrics.AuthzResolutions.WithLabelValues("-", "unavailable").Inc()
		e := issuerUnavailable(unavailable)
		res.Error = &e
		res.warn = true
	case errors.As(err, &tierC):
		metrics.AuthzResolutions.WithLabelValues("C", "refused").Inc()
		res.Error = &event.Error{Code: "policy_requires_impersonation", Message: tierC.Reason}
	default:
		res.Error = &event.Error{Code: "internal", Message: err.Error()}
		res.warn = true
	}
}

// Without a Retry-After from the issuer, a retry is spread over this window, so
// the subscriptions of a reconnect wave that found the issuer down do not all
// come back at once.
const (
	issuerRetryMin    = time.Second
	issuerRetrySpread = 4 * time.Second
)

// issuerUnavailable is the error for a shape the issuer could not decide:
// retryable, after the delay the issuer asked for or a spread one.
// holdCheckFailed is the error for a grant whose holds were not confirmed. A hold
// row that does not exist denies it. A check that could not be made -- the
// request was cancelled or timed out, the database failed -- says nothing
// about the caller, so it is retryable, after a delay spread like the
// issuer's, rather than a denial the client would give up on.
func holdCheckFailed(err error) *event.Error {
	var missing *hold.MissingError
	if errors.As(err, &missing) || !isHoldQueryError(err) {
		return &event.Error{Code: "shape_not_authorized", Message: err.Error()}
	}
	return &event.Error{Code: "hold_check_unavailable", Message: err.Error(), Retryable: true,
		RetryAfterMs: (issuerRetryMin + rand.N(issuerRetrySpread)).Milliseconds()}
}

func issuerUnavailable(e *oracle.ErrUnavailable) event.Error {
	delay := e.RetryAfter
	if delay <= 0 {
		delay = issuerRetryMin + rand.N(issuerRetrySpread)
	}
	return event.Error{Code: "issuer_unavailable", Message: e.Reason,
		Retryable: true, RetryAfterMs: delay.Milliseconds()}
}

// refreshIssuerShapes is the /token path for the issuer oracle: re-ask the
// issuer about every shape of the stream in one call, then apply each grant
// without a window where the shape is live and unwatched. It returns each
// shape's lease-refresh outcome: held, revoked or unavailable.
//
// An issuer that gives no verdict fails closed: the shape is dropped with the
// retryable issuer_unavailable, and the client subscribes again. Keeping the
// previous grant instead would let a token that changed what its holder may see
// keep the old view for as long as the issuer is down.
func (s *Server) refreshIssuerShapes(ctx context.Context, st *hub.Stream, subs []*registry.Subscription, id authz.Identity) []string {
	reqs := make([]oracle.Request, len(subs))
	for k, sub := range subs {
		// Ask about what the client requested, as at subscribe. The effective
		// filter already holds the previous grant's terms; narrowing it again
		// would add them once more on every renewal, and a grant could never
		// widen back.
		req := sub.Requested
		if req.Filter == nil {
			req = registry.Requested{Filter: sub.Filter, Columns: sub.Columns}
		}
		reqs[k] = oracle.Request{Label: sub.Label, Relation: sub.Relation,
			Filter: req.Filter, Columns: req.Columns, Ops: req.Ops}
	}
	out := make([]string, len(subs))
	var grants []labeledGrant
	var at []int
	for k, res := range s.oracle.Resolve(ctx, oracle.ActionRefresh, id, reqs) {
		label := subs[k].Label
		var unavailable *oracle.ErrUnavailable
		switch {
		case errors.As(res.Err, &unavailable):
			s.revokeShape(ctx, st, label, true, issuerUnavailable(unavailable))
			out[k] = "unavailable"
		case res.Err != nil || res.Grant == nil:
			s.revokeShape(ctx, st, label, false, event.Error{Code: "shape_not_authorized",
				Message: "the refreshed token no longer grants access to this shape"})
			out[k] = "revoked"
		default:
			grants = append(grants, labeledGrant{label: label, grant: res.Grant})
			at = append(at, k)
		}
	}
	for j, held := range s.applyIssuerGrants(ctx, st, grants) {
		out[at[j]] = "revoked"
		if held {
			out[at[j]] = "held"
		}
	}
	return out
}

// labeledGrant is a refresh grant for the stream's shape with that label.
type labeledGrant struct {
	label string
	grant *oracle.Grant
}

// applyIssuerGrants installs refresh grants on live shapes and reports which
// were kept. Each shape's holds are replaced under one lock (never Remove then
// Add); then the holds of every grant are checked in one round trip; then each
// shape whose holds exist has its effective filter, columns and routing key
// rebound, and the others are revoked. A shape already cut during the issuer
// round trip is not resurrected. Until it is rebound a shape keeps its
// previous grant, under the new grant's watches.
func (s *Server) applyIssuerGrants(ctx context.Context, st *hub.Stream, grants []labeledGrant) []bool {
	held := make([]bool, len(grants))
	watched := make([][]hold.Spec, len(grants))
	for j, g := range grants {
		label, grant := g.label, g.grant
		if grant == nil || grant.Filter == nil || len(grant.Columns) == 0 || len(grant.Holds) == 0 {
			s.revokeShape(ctx, st, label, false, event.Error{Code: "shape_not_authorized",
				Message: "the refreshed token no longer grants access to this shape"})
			continue
		}
		if s.reg.Get(st.StreamID(), label) == nil {
			s.holds.Remove(st.StreamID(), label)
			continue
		}
		s.holds.Replace(st.StreamID(), label, grant.Holds)
		if err := s.holds.CheckIdentity(st.StreamID(), label); err != nil {
			s.revokeShape(ctx, st, label, false, event.Error{Code: "shape_not_authorized", Message: err.Error()})
			continue
		}
		watched[j] = grant.Holds
	}
	for j, err := range s.checkHolds(ctx, watched) {
		switch {
		case watched[j] == nil:
		case err != nil:
			e := holdCheckFailed(err)
			s.revokeShape(ctx, st, grants[j].label, e.Retryable, *e)
		default:
			held[j] = s.rebindGrant(st, grants[j].label, grants[j].grant)
		}
	}
	return held
}

// rebindGrant rebinds a live shape to a refresh grant whose holds were found.
func (s *Server) rebindGrant(st *hub.Stream, label string, grant *oracle.Grant) bool {
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

// checkHolds checks the holds of a request's grants in one round trip, each
// distinct hold once (hold.Check). It returns, per grant, nil when every hold
// row exists (or the grant names none), else the first hold's error.
func (s *Server) checkHolds(ctx context.Context, grants [][]hold.Spec) []error {
	out := make([]error, len(grants))
	var specs []hold.Spec
	for _, g := range grants {
		specs = append(specs, g...)
	}
	if len(specs) == 0 {
		return out
	}
	var errs []error
	switch {
	case s.holdCheck != nil:
		errs = s.holdCheck(ctx, specs)
	case s.pool == nil:
		errs = make([]error, len(specs))
		for i := range errs {
			errs[i] = fmt.Errorf("cannot verify holds without a database pool")
		}
	default:
		errs = hold.Check(ctx, s.pool, specs)
	}
	for i, g := range grants {
		for _, err := range errs[:len(g)] {
			if err != nil {
				out[i] = err
				break
			}
		}
		errs = errs[len(g):]
	}
	return out
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

// revokeShape drops a shape that a request's re-check no longer allows, and
// logs it. warn is set when no verdict was given.
func (s *Server) revokeShape(ctx context.Context, st *hub.Stream, label string, warn bool, err event.Error) {
	spec := subSpec{Sub: label}
	if sub := s.reg.Get(st.StreamID(), label); sub != nil {
		spec.Shape = &shapeSpec{Schema: sub.Relation.Schema, Table: sub.Relation.Name}
	}
	s.dropShape(st, label, err)
	s.logSub(ctx, "subscription revoked", st, spec, warn, err)
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
		s.refuse(w, r, http.StatusForbidden, "forbidden", "service_role or the issuer bearer is required")
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
		s.refuse(w, r, http.StatusBadRequest, "bad_request", err.Error())
		return
	}
	schema := cmp.Or(req.Schema, "public")
	if req.Table == "" || len(req.Equalities) == 0 {
		s.refuse(w, r, http.StatusBadRequest, "bad_request", "table and equalities are required")
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

// isHoldQueryError reports whether a hold check failed to run rather than
// found a hold missing.
func isHoldQueryError(err error) bool {
	var check *hold.CheckError
	return errors.As(err, &check) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
