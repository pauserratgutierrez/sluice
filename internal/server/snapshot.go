package server

import (
	"context"
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
//  1. take the reader's CONFIRMED LSN as the replay floor, BEFORE opening the
//     snapshot transaction. The reader is by definition at or behind the WAL
//     head, so no transaction the snapshot can see committed below that point;
//  2. read the rows in a REPEATABLE READ transaction under the caller's own role
//     and claims, so PostgreSQL applies RLS exactly as it would for any query;
//  3. replay the relation's ring buffer from the floor, re-filtered and
//     re-authorized.
//
// Duplicates between (2) and (3) are possible and intended: clients upsert by
// primary key, and at-least-once is already the contract because PostgreSQL only
// persists slot position at checkpoint. Gaps are not possible, which is the half
// that matters.
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
	rows, err := s.readSnapshot(ctx, sub)
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
		Sub: sub.Label, Rows: len(rows), FloorLSN: reader.FormatLSN(floor),
	}})

	s.replayFrom(sub, floor)
}

// readSnapshot performs the impersonated, RLS-enforced read.
func (s *Server) readSnapshot(ctx context.Context, sub *registry.Subscription) ([]map[string]any, error) {
	rel := sub.Relation
	id := streamOf(sub).Identity()

	// The impersonation runs as its own statement with its own parameters, so the
	// filter can number from $1 without colliding.
	where, args := sub.Filter.SQL(1)
	cols := make([]string, 0, len(sub.Columns))
	for _, c := range sub.Columns {
		cols = append(cols, catalog.QuoteIdent(c))
	}
	// The replica identity columns are always included so the client can key the
	// row, matching what live change events guarantee.
	for _, c := range rel.ReplicaIdentityColumns {
		if !slices.Contains(sub.Columns, c) {
			cols = append(cols, catalog.QuoteIdent(c))
		}
	}

	order := "1"
	if len(rel.ReplicaIdentityColumns) > 0 {
		order = ""
		for i, c := range rel.ReplicaIdentityColumns {
			if i > 0 {
				order += ", "
			}
			order += catalog.QuoteIdent(c)
		}
	}

	sql := fmt.Sprintf(`SELECT to_jsonb(t) FROM (SELECT %s FROM %s WHERE %s ORDER BY %s LIMIT %d) t`,
		strings.Join(cols, ", "), catalog.QuoteQualified(rel.Schema, rel.Name), where, order,
		s.cfg.SnapshotMaxRows)

	// REPEATABLE READ so every page sees one consistent database state, and READ
	// ONLY so an accidental write is impossible even under a misbehaving policy.
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT set_config('role', $1, true), set_config('request.jwt.claims', $2, true)`,
		id.Role, id.ClaimsRaw); err != nil {
		return nil, err
	}

	qrows, err := tx.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("snapshot query: %w", err)
	}
	defer qrows.Close()

	out := make([]map[string]any, 0, 64)
	for qrows.Next() {
		var m map[string]any
		if err := qrows.Scan(&m); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, qrows.Err()
}
