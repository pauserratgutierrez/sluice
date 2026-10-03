// Package event defines the wire events Sluice emits over SSE.
//
// Kept in its own package so that the registry, hub, reader and server can all
// refer to it without an import cycle.
package event

import (
	"encoding/json"
	"sync"
)

// Kind is the SSE `event:` name.
type Kind string

const (
	KindReady       Kind = "ready"
	KindChange      Kind = "change"
	KindBroadcast   Kind = "broadcast"
	KindPresence    Kind = "presence"
	KindSnapshotEnd Kind = "snapshot_end"
	KindWarning     Kind = "warning"
	KindError       Kind = "error"
	// KindHeartbeat is written as a bare SSE comment rather than a named event,
	// so clients never see it. It exists as a Kind only so the shared timer wheel
	// can enqueue it like anything else and keep all writing on one goroutine.
	KindHeartbeat Kind = "heartbeat"
)

// Event is one SSE frame.
type Event struct {
	Kind Kind
	// ID populates the SSE `id:` field. Only change events set it, because only
	// they are resumable.
	ID   string
	Data any
}

// Change is the replication-plane payload.
type Change struct {
	Sub        string `json:"sub"`
	Op         string `json:"op"`
	Schema     string `json:"schema"`
	Table      string `json:"table"`
	CommitLSN  string `json:"commit_lsn"`
	CommitTime string `json:"commit_time,omitempty"`
	Seq        int    `json:"seq"`
	Record     *Row   `json:"record,omitempty"`
	Old        *Row   `json:"old,omitempty"`
	// Unchanged lists columns whose value the WAL did NOT carry because they
	// hold an unchanged TOASTed value. This is the distinction wal2json throws
	// away, and getting it wrong silently blanks large columns on every
	// subscriber whenever an unrelated counter is updated.
	Unchanged []string `json:"unchanged,omitempty"`
	// Transition is "enter" or "leave" when an UPDATE moved the row into or out
	// of the shape (only for subscriptions that asked for transitions). Without
	// it a client's view goes stale on a row that no longer matches its filter.
	Transition string `json:"transition,omitempty"`
	// Degraded names a caveat that applies to this event:
	// "delete_authz_unavailable" (a DELETE delivered without an authorization
	// decision, only with SLUICE_DEGRADED_DELETES=deliver) or
	// "change_too_large" (record and old trimmed to the key columns).
	Degraded string `json:"degraded,omitempty"`
	Snapshot bool   `json:"snapshot,omitempty"`
}

// Row is a projected record. A change reaches every subscription that projects
// the same columns through the same Row, so it is encoded once however many
// streams carry it. Values must not be modified once the Row is shared.
type Row struct {
	Values map[string]any

	once sync.Once
	enc  []byte
	err  error
}

// NewRow wraps a record. An empty record is nil, which the wire omits.
func NewRow(values map[string]any) *Row {
	if len(values) == 0 {
		return nil
	}
	return &Row{Values: values}
}

func (r *Row) MarshalJSON() ([]byte, error) {
	r.once.Do(func() { r.enc, r.err = json.Marshal(r.Values) })
	return r.enc, r.err
}

// Broadcast is the signalling-plane payload.
type Broadcast struct {
	Sub       string          `json:"sub"`
	Channel   string          `json:"channel"`
	Event     string          `json:"event"`
	Payload   json.RawMessage `json:"payload,omitempty"`
	From      string          `json:"from,omitempty"`
	Origin    string          `json:"origin,omitempty"` // "client" | "database"
	CommitLSN string          `json:"commit_lsn,omitempty"`
	At        string          `json:"at,omitempty"`
}

// Presence carries either a full state snapshot or a diff.
type Presence struct {
	Sub     string            `json:"sub"`
	Channel string            `json:"channel"`
	Type    string            `json:"type"` // "state" | "diff"
	Members map[string]Member `json:"members,omitempty"`
	Joins   map[string]Member `json:"joins,omitempty"`
	Leaves  map[string]Member `json:"leaves,omitempty"`
}

type Member struct {
	Meta  json.RawMessage `json:"meta,omitempty"`
	Since string          `json:"since,omitempty"`
	Ref   string          `json:"ref,omitempty"`
}

// Error is a subscription- or stream-scoped failure.
type Error struct {
	Sub       string `json:"sub,omitempty"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Action    string `json:"action,omitempty"`
	// RetryAfterMs is how long the client should wait before reconnecting. The
	// server spreads it per stream, so a restart is not followed by every
	// client reconnecting at once.
	RetryAfterMs int64 `json:"retry_after_ms,omitempty"`
}

// Warning is non-fatal but actionable. Every warning carries a remedy that is a
// runnable statement or a concrete instruction, because a warning nobody can act
// on is noise.
type Warning struct {
	Sub     string `json:"sub,omitempty"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Effect  string `json:"effect,omitempty"`
	Remedy  string `json:"remedy,omitempty"`
}

// SnapshotEnd terminates an initial snapshot. Truncated means more rows matched
// than SLUICE_SNAPSHOT_MAX_ROWS and only that many were sent.
type SnapshotEnd struct {
	Sub       string `json:"sub"`
	Rows      int    `json:"rows"`
	FloorLSN  string `json:"floor_lsn"`
	Truncated bool   `json:"truncated,omitempty"`
}
