package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/authz"
)

// sessionFoundTTL is how long a session found in the table is trusted without
// asking again. Its deletion while this process runs arrives through the slot
// and is enforced by the revoker regardless, so this only spares a reconnect
// wave one query per stream: a session opening many streams costs one.
const sessionFoundTTL = 30 * time.Second

// sessionLookup confirms that a token's session row still exists
// (SLUICE_REVOCATION_SESSION_LOOKUP). The revoker only knows deletions the slot
// delivered since this process started; the lookup closes the rest, so a token
// whose session was signed out before a restart cannot open a stream.
type sessionLookup struct {
	exists func(ctx context.Context, sessionID string) (bool, error)

	mu    sync.Mutex
	found map[string]time.Time // session id -> when it was last found
}

func newSessionLookup(pool *pgxpool.Pool, table string) *sessionLookup {
	query := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE id = $1)`,
		pgx.Identifier(strings.SplitN(table, ".", 2)).Sanitize())
	return &sessionLookup{
		exists: func(ctx context.Context, sessionID string) (bool, error) {
			var ok bool
			err := pool.QueryRow(ctx, query, sessionID).Scan(&ok)
			// invalid_text_representation: an id that is not a valid value of
			// the column's type (a uuid column, a non-uuid claim) names no row.
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "22P02" {
				return false, nil
			}
			return ok, err
		},
		found: map[string]time.Time{},
	}
}

// check reports whether the session exists.
func (l *sessionLookup) check(ctx context.Context, sessionID string) (bool, error) {
	now := time.Now()
	l.mu.Lock()
	at, ok := l.found[sessionID]
	l.mu.Unlock()
	if ok && now.Sub(at) < sessionFoundTTL {
		return true, nil
	}
	exists, err := l.exists(ctx, sessionID)
	if err != nil || !exists {
		return false, err
	}
	l.mu.Lock()
	l.found[sessionID] = now
	l.mu.Unlock()
	return true, nil
}

// sweep forgets sessions found longer ago than the TTL, so the map holds only
// the sessions that connected recently.
func (l *sessionLookup) sweep() {
	cutoff := time.Now().Add(-sessionFoundTTL)
	l.mu.Lock()
	defer l.mu.Unlock()
	for id, at := range l.found {
		if at.Before(cutoff) {
			delete(l.found, id)
		}
	}
}

// checkSession refuses, and writes the response for, a token whose session row
// is gone (401, remembered by the revoker) or could not be looked up (503,
// retryable). Tokens without a session id, and processes without the lookup,
// pass.
func (s *Server) checkSession(w http.ResponseWriter, r *http.Request, id authz.Identity) bool {
	if s.sessions == nil || id.SessionID == "" {
		return true
	}
	ok, err := s.sessions.check(r.Context(), id.SessionID)
	if err != nil {
		s.refuse(w, r, http.StatusServiceUnavailable, "session_check_unavailable",
			"the session behind this token could not be checked; retry shortly", "err", err)
		return false
	}
	if !ok {
		if s.revoker != nil {
			s.revoker.RevokeSession(id.SessionID)
		}
		s.refuse(w, r, http.StatusUnauthorized, "unauthorized", errSessionRevoked.Error())
		return false
	}
	return true
}
