package server

import (
	"cmp"
	"context"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/requestid"
)

// Every request gets an ID, the one its gateway set or a new one, and every
// log line about a request carries it as request_id: a refusal, a refused or
// revoked subscription, an issuer or hook that denied or gave no verdict. The
// same ID is sent to the issuer and hooks on the calls made for the request.
//
// A stream outlives the request that opened it, so it keeps that request's ID:
// the line logged when the server ends a stream carries it as request_id, and
// a line about a later request on the stream carries it as stream_request_id.
// Work no request started (fan-out, the catalog tick, revocations read from
// the slot) has no request ID and logs its own identifiers instead.
//
// A line never holds a token, a bearer secret, a payload or row data. Strings
// the client chose are clipped to maxLogged bytes, and one request logs at
// most maxRefusalLines refused subscriptions.

const (
	maxLogged       = 256
	maxRefusalLines = 20
)

// withRequestID gives every request its ID; see requestid.
func (s *Server) withRequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := requestid.Read(r, cmp.Or(s.cfg.RequestIDHeader, requestid.DefaultHeader))
		next.ServeHTTP(w, r.WithContext(requestid.With(r.Context(), id)))
	})
}

// logRequest logs a line about the request ctx belongs to. A request that acts
// on a stream adds the stream's id and the ID of the request that opened it.
func (s *Server) logRequest(ctx context.Context, level slog.Level, msg string, st *hub.Stream, args ...any) {
	if !s.log.Enabled(ctx, level) {
		return
	}
	rid := requestid.From(ctx).Value
	attrs := make([]any, 0, 6+len(args))
	attrs = append(attrs, "request_id", rid)
	if st != nil {
		attrs = append(attrs, "stream_id", st.StreamID())
		if opened := st.RequestID(); opened != "" && opened != rid {
			attrs = append(attrs, "stream_request_id", opened)
		}
	}
	s.log.Log(ctx, level, msg, append(attrs, args...)...)
}

// refuse answers a request with an error and logs it.
func (s *Server) refuse(w http.ResponseWriter, r *http.Request, status int, code, msg string, args ...any) {
	s.refuseStream(w, r, nil, status, code, msg, args...)
}

// refuseStream is refuse for a request on a stream. A 5xx is Sluice's to fix
// and is a warning, except server_shutdown, which is expected while stopping.
func (s *Server) refuseStream(w http.ResponseWriter, r *http.Request, st *hub.Stream, status int, code, msg string, args ...any) {
	level := slog.LevelInfo
	if status >= 500 && code != "server_shutdown" {
		level = slog.LevelWarn
	}
	s.logRequest(r.Context(), level, "request refused", st, append([]any{
		"path", r.URL.Path, "status", status, "code", code, "reason", truncate(msg, maxLogged),
	}, args...)...)
	writeErr(w, status, code, msg)
}

// logRefusals logs the subscriptions of a request that were refused.
func (s *Server) logRefusals(ctx context.Context, st *hub.Stream, specs []subSpec, results []subResult) {
	logged, more := 0, 0
	for i, res := range results {
		if res.OK || res.Error == nil {
			continue
		}
		if logged == maxRefusalLines {
			more++
			continue
		}
		logged++
		s.logSub(ctx, "subscription refused", st, specs[i], res.warn, *res.Error)
	}
	if more > 0 {
		s.logRequest(ctx, slog.LevelInfo, "more subscriptions refused", st, "count", more)
	}
}

// logSub logs a line about one subscription of a request. warn makes it a
// warning: the cause is Sluice's, or the issuer's or hook's, not the caller's.
func (s *Server) logSub(ctx context.Context, msg string, st *hub.Stream, spec subSpec, warn bool, e event.Error) {
	level := slog.LevelInfo
	if warn {
		level = slog.LevelWarn
	}
	args := []any{"sub", truncate(spec.Sub, maxLogged)}
	switch {
	case spec.Shape != nil:
		args = append(args, "table", truncate(cmp.Or(spec.Shape.Schema, "public")+"."+spec.Shape.Table, maxLogged))
	case spec.Channel != "":
		args = append(args, "channel", truncate(spec.Channel, maxLogged))
	}
	args = append(args, "code", e.Code, "reason", truncate(e.Message, maxLogged))
	s.logRequest(ctx, level, msg, st, args...)
}

// logStreamEnd logs a stream the server ended. err is the write that failed,
// if one did.
func (s *Server) logStreamEnd(ctx context.Context, st *hub.Stream, code string, err error) {
	args := []any{"code", code}
	if cause := s.shutdownCause.Load(); cause != nil && code == "server_shutdown" {
		args = append(args, "cause", *cause)
	}
	if err != nil {
		args = append(args, "err", err)
	}
	s.logRequest(ctx, slog.LevelInfo, "stream ended", st, args...)
}

// refusalLog counts streams refused with one code between log lines: a
// refused reconnect storm is thousands of requests a second, so it is logged
// at most once a second, with how many there were.
type refusalLog struct {
	mu      sync.Mutex
	last    time.Time
	refused int
}

// refuseCapacity answers 503 too_many_streams.
func (s *Server) refuseCapacity(w http.ResponseWriter, reason, msg string) {
	s.refuseLater(w, &s.capacityLog, "too_many_streams", reason, msg)
}

// refuseLater answers 503 with a Retry-After of 1 to 5 seconds, spreading a
// refused storm's retries, and logs it sampled.
func (s *Server) refuseLater(w http.ResponseWriter, l *refusalLog, code, reason, msg string) {
	metrics.StreamsRefused.WithLabelValues(reason).Inc()
	w.Header().Set("Retry-After", strconv.Itoa(1+rand.IntN(5)))
	writeErr(w, http.StatusServiceUnavailable, code, msg)
	l.mu.Lock()
	l.refused++
	if time.Since(l.last) < time.Second {
		l.mu.Unlock()
		return
	}
	n := l.refused
	l.refused, l.last = 0, time.Now()
	l.mu.Unlock()
	s.log.Warn("streams refused", "code", code, "reason", reason, "refused", n, "detail", msg)
}
