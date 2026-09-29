package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/event"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/reader"
	"github.com/pauserratgutierrez/sluice/internal/registry"
)

// snapshot serves a consistent initial read and then replays everything that
// happened since, closing the classic race where a client fetches initial state
// over HTTP and misses whatever changed before its subscription took effect.
//
// The ordering is what makes it gapless:
//
//  1. the subscription is already registered, so every transaction the reader
//     dispatches from here on reaches it live;
//  2. the reader's confirmed LSN is taken as the floor BEFORE the snapshot
//     transaction begins: everything dispatched up to the floor committed
//     before the snapshot started, so the snapshot contains it;
//  3. the rows are read in one REPEATABLE READ transaction (under the caller's
//     role and claims in RLS mode, so PostgreSQL applies RLS as for any query);
//  4. the relation's ring buffer is replayed from the floor, re-filtered and
//     re-authorized, so a live change delivered before an older snapshot row
//     arrives again after it.
//
// Duplicates are possible and intended: clients upsert by primary key, and
// at-least-once is already the contract. Gaps are not possible.
func (s *Server) snapshot(ctx context.Context, sub *registry.Subscription) {
	st := streamOf(sub)
	if st == nil {
		return
	}
	rel := sub.Relation

	select {
	case s.snapSem <- struct{}{}:
		defer func() { <-s.snapSem }()
	case <-ctx.Done():
		return
	}

	floor := uint64(0)
	if s.reader != nil {
		floor = s.reader.ConfirmedLSN()
	}

	start := time.Now()
	rows, truncated, err := s.readSnapshot(ctx, sub)
	if err != nil {
		hub.SendError(st, event.Error{Sub: sub.Label, Code: "snapshot_failed",
			Message: err.Error(), Retryable: true})
		return
	}
	metrics.SnapshotSeconds.WithLabelValues(rel.Schema, rel.Name).Observe(time.Since(start).Seconds())
	metrics.SnapshotRows.WithLabelValues(rel.Schema, rel.Name).Add(float64(len(rows)))

	for _, r := range rows {
		st.Send(event.Event{Kind: event.KindChange, Data: event.Change{
			Sub: sub.Label, Op: "INSERT", Schema: rel.Schema, Table: rel.Name,
			CommitLSN: reader.FormatLSN(floor), Seq: sub.NextSeq(),
			Record: r, Snapshot: true,
		}})
	}

	st.Send(event.Event{Kind: event.KindSnapshotEnd, Data: event.SnapshotEnd{
		Sub: sub.Label, Rows: len(rows), FloorLSN: reader.FormatLSN(floor), Truncated: truncated,
	}})

	// Changes after the floor may already have been delivered live before the
	// rows above, which could be older; replaying them puts the newest version
	// last. If the buffer no longer covers the floor, the client must start over.
	if !s.replayFrom(sub, floor) {
		hub.SendError(st, errResumeTooOld(sub.Label))
	}
}

// readSnapshot performs the initial consistent read.
//
// RLS mode impersonates the caller so PostgreSQL applies the same policies as
// any query. Issuer mode selects as the pool role (BYPASSRLS or RLS off) using
// the effective filter; zero rows is success.
//
// Rows are encoded by to_jsonb, so their values follow PostgreSQL's JSON
// conversion (ISO 8601 timestamps, JSON arrays), and numbers are decoded
// without passing through float64.
func (s *Server) readSnapshot(ctx context.Context, sub *registry.Subscription) (rows []map[string]any, truncated bool, err error) {
	rel := sub.Relation

	// The impersonation runs as its own statement with its own parameters, so the
	// filter can number from $1 without colliding.
	where, args := sub.Filter.SQL(1)
	cols := make([]string, 0, len(sub.Columns))
	for _, c := range sub.Columns {
		cols = append(cols, catalog.QuoteIdent(c))
	}

	// Order by the key so LIMIT is deterministic, using only key columns the
	// caller may read.
	var order []string
	for _, c := range rel.KeyColumns {
		if slices.Contains(sub.Columns, c) {
			order = append(order, catalog.QuoteIdent(c))
		}
	}
	if len(order) == 0 {
		order = []string{"1"}
	}

	// One row past the cap tells a full result from a truncated one.
	limit := s.cfg.SnapshotMaxRows
	sql := fmt.Sprintf(`SELECT to_jsonb(t) FROM (SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d) t`,
		strings.Join(cols, ", "), catalog.QuoteQualified(rel.Schema, rel.Name), where,
		strings.Join(order, ", "), limit+1)

	// A single statement, so it sees one consistent state. READ ONLY so an
	// accidental write is impossible even under a misbehaving policy; the
	// transaction is what scopes the impersonation.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if s.impersonateSnapshots() {
		id := streamOf(sub).Identity()
		if _, err := tx.Exec(ctx,
			`SELECT set_config('role', $1, true), set_config('request.jwt.claims', $2, true)`,
			id.Role, id.ClaimsRaw); err != nil {
			return nil, false, err
		}
	}

	qrows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, false, fmt.Errorf("snapshot query: %w", err)
	}
	defer qrows.Close()

	rows = make([]map[string]any, 0, 64)
	for qrows.Next() {
		if len(rows) == limit {
			truncated = true
			break
		}
		var raw []byte
		if err := qrows.Scan(&raw); err != nil {
			return nil, false, err
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		var m map[string]any
		if err := dec.Decode(&m); err != nil {
			return nil, false, err
		}
		rows = append(rows, m)
	}
	return rows, truncated, qrows.Err()
}
