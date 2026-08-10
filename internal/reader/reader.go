// Package reader owns the single replication connection and dispatches changes.
//
// Configuration choices worth understanding before changing anything here:
//
//	proto_version = 4   declares capability. Verified empirically: the negotiated
//	                    version ALONE changes nothing on the wire -- 1, 4 and
//	                    4+parallel produced byte-identical output. The OPTIONS
//	                    determine the message set.
//	streaming = off     everything received is therefore already committed and
//	                    durable, so this loop forwards immediately and holds no
//	                    transaction buffer. With streaming=on it would have to
//	                    buffer uncommitted transactions in the Go heap.
//	binary = false      measured LARGER than text (112 vs 88 bytes) and would
//	                    require per-type binary decoders. Sluice emits JSON.
//	messages = true     delivers pg_logical_emit_message, which gives
//	                    transactional broadcast with no outbox table.
package reader

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

// Handler consumes decoded messages. The reader owns ordering and never calls a
// handler concurrently, so implementations need no locking of their own.
type Handler interface {
	OnBegin(lsn uint64, commitTime time.Time, xid uint32)
	OnChange(m *pgoutput.Message, rel *pgoutput.Relation, commitLSN uint64, commitTime time.Time) error
	OnMessage(m *pgoutput.Message, commitLSN uint64) error
	OnCommit(lsn uint64, commitTime time.Time)
	OnRelation(old, new *pgoutput.Relation)
	OnTruncate(rels []uint32)
}

type Reader struct {
	cfg  *config.Config
	pool *pgxpool.Pool
	log  *slog.Logger
	h    Handler

	dec *pgoutput.Decoder

	// confirmed is the LSN Sluice has taken responsibility for. It advances only
	// after a change has been queued to every interested stream or deliberately
	// withheld. That is the backpressure mechanism: a stalled Sluice retains WAL
	// on disk rather than losing data.
	//
	// Atomic because the reader goroutine writes them while /diagnostics,
	// /readyz and every snapshot read them.
	confirmed atomic.Uint64
	received  atomic.Uint64

	// currentCommit carries the enclosing transaction's LSN and timestamp down to
	// per-row handlers.
	currentCommit     uint64
	currentCommitTime time.Time
}

func New(cfg *config.Config, pool *pgxpool.Pool, log *slog.Logger, h Handler) *Reader {
	r := &Reader{cfg: cfg, pool: pool, log: log, h: h, dec: pgoutput.NewDecoder()}
	r.dec.OnRelation = func(old, nw *pgoutput.Relation) { h.OnRelation(old, nw) }
	return r
}

// ConfirmedLSN is the position Sluice has acknowledged. Snapshots use it as their
// replay floor, which is what closes the subscribe race without gaps.
func (r *Reader) ConfirmedLSN() uint64 { return r.confirmed.Load() }
func (r *Reader) ReceivedLSN() uint64  { return r.received.Load() }

// seedConfirmed adopts the slot's persisted position as the starting point.
//
// Without this, confirmed is zero until the first COMMIT of the session is
// dispatched, and everything derived from it is wrong in the meantime: the
// snapshot replay floor, /diagnostics, and -- worst -- the standby status
// update, which would otherwise have nothing to report but the position the
// server last told us about. Acknowledging that would discard a backlog this
// process has received but not yet delivered.
func (r *Reader) seedConfirmed(ctx context.Context) {
	var lsn pglogrepl.LSN
	err := r.pool.QueryRow(ctx,
		`SELECT coalesce(confirmed_flush_lsn, '0/0') FROM pg_replication_slots WHERE slot_name = $1`,
		r.cfg.SlotName).Scan(&lsn)
	if err != nil {
		r.log.Warn("could not read the slot's confirmed_flush_lsn", "err", err)
		return
	}
	if uint64(lsn) > r.confirmed.Load() {
		r.confirmed.Store(uint64(lsn))
	}
}

// EnsureSlot creates the replication slot if it does not exist.
//
// The slot is PERMANENT, deliberately. supabase/realtime uses a temporary slot,
// which is dropped on any error or session end -- silently losing every change
// between the failure and the reconnect. A permanent slot is crash-safe and
// resumes from confirmed_flush_lsn.
func (r *Reader) EnsureSlot(ctx context.Context) (created bool, err error) {
	var exists bool
	if err := r.pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		r.cfg.SlotName).Scan(&exists); err != nil {
		return false, fmt.Errorf("reader: check slot: %w", err)
	}
	if exists {
		return false, nil
	}

	conn, err := r.connect(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close(context.Background())

	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false}); err != nil {
		return false, fmt.Errorf("reader: create slot %q: %w", r.cfg.SlotName, err)
	}
	return true, nil
}

func (r *Reader) connect(ctx context.Context) (*pgconn.PgConn, error) {
	conn, err := pgconn.Connect(ctx, r.cfg.ReplURL)
	if err != nil {
		return nil, fmt.Errorf("reader: connect replication: %w", err)
	}
	return conn, nil
}

// Run streams until the context is cancelled, reconnecting with backoff.
func (r *Reader) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if err := r.stream(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// An invalidated slot is fatal by design. Silently resuming from a
			// new position would be silent data loss, so the operator must act.
			if isSlotInvalidated(err) {
				return fmt.Errorf("reader: replication slot %q has been invalidated; "+
					"the change stream has a gap and clients must resnapshot: %w", r.cfg.SlotName, err)
			}
			r.log.Error("replication stream failed, reconnecting", "err", err, "backoff", backoff)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		return nil
	}
}

func isSlotInvalidated(err error) bool {
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "can no longer get changes from replication slot") ||
		strings.Contains(s, "requested wal segment") && strings.Contains(s, "removed")
}

func (r *Reader) stream(ctx context.Context) error {
	conn, err := r.connect(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	sys, err := pglogrepl.IdentifySystem(ctx, conn)
	if err != nil {
		return fmt.Errorf("reader: IDENTIFY_SYSTEM: %w", err)
	}

	r.seedConfirmed(ctx)

	args := []string{
		fmt.Sprintf("proto_version '%d'", r.cfg.ProtoVersion),
		fmt.Sprintf("publication_names '%s'", strings.ReplaceAll(r.cfg.Publication, "'", "''")),
	}
	if r.cfg.Messages {
		args = append(args, "messages 'true'")
	}
	if r.cfg.Binary {
		args = append(args, "binary 'true'")
	}
	if r.cfg.Streaming != "off" {
		args = append(args, fmt.Sprintf("streaming '%s'", r.cfg.Streaming))
	}
	args = append(args, "origin 'any'")

	// Starting at 0/0 makes the server begin at the slot's confirmed_flush_lsn,
	// which is exactly what a resuming consumer wants.
	if err := pglogrepl.StartReplication(ctx, conn, r.cfg.SlotName, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: args}); err != nil {
		return fmt.Errorf("reader: START_REPLICATION: %w", err)
	}

	r.log.Info("replication started",
		"slot", r.cfg.SlotName, "publication", r.cfg.Publication,
		"proto_version", r.cfg.ProtoVersion, "streaming", r.cfg.Streaming,
		"binary", r.cfg.Binary, "system_id", sys.SystemID, "timeline", sys.Timeline)

	nextStatus := time.Now().Add(r.cfg.StatusEvery)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Now().After(nextStatus) {
			if err := r.sendStatus(ctx, conn); err != nil {
				return err
			}
			nextStatus = time.Now().Add(r.cfg.StatusEvery)
		}

		// The deadline must be comfortably inside wal_sender_timeout (default
		// 60s) so a quiet period never looks like a dead client.
		recvCtx, cancel := context.WithDeadline(ctx, nextStatus)
		raw, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("reader: receive: %w", err)
		}

		switch msg := raw.(type) {
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("reader: parse keepalive: %w", err)
				}
				if uint64(ka.ServerWALEnd) > r.received.Load() {
					r.received.Store(uint64(ka.ServerWALEnd))
				}
				if ka.ReplyRequested {
					if err := r.sendStatus(ctx, conn); err != nil {
						return err
					}
					nextStatus = time.Now().Add(r.cfg.StatusEvery)
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("reader: parse XLogData: %w", err)
				}
				if uint64(xld.ServerWALEnd) > r.received.Load() {
					r.received.Store(uint64(xld.ServerWALEnd))
				}
				if err := r.handle(xld); err != nil {
					return err
				}
			}

		case *pgproto3.ErrorResponse:
			return fmt.Errorf("reader: server error: %s: %s", msg.Severity, msg.Message)
		}
	}
}

func (r *Reader) sendStatus(ctx context.Context, conn *pgconn.PgConn) error {
	// Only ever the confirmed position. Reporting the received position instead
	// would tell PostgreSQL it may recycle WAL this process has read but not yet
	// dispatched, which is precisely the data loss the permanent slot exists to
	// prevent. Zero is a truthful answer for a reader that has confirmed
	// nothing; it simply does not advance the slot.
	lsn := r.confirmed.Load()
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: pglogrepl.LSN(lsn)}); err != nil {
		return fmt.Errorf("reader: standby status update: %w", err)
	}
	return nil
}

func (r *Reader) handle(xld pglogrepl.XLogData) error {
	m, err := r.dec.Decode(xld.WALData)
	if err != nil {
		// A decode failure means the stream and the decoder have diverged.
		// Continuing would deliver wrong data, so surface it.
		return fmt.Errorf("reader: decode: %w", err)
	}

	switch m.Type {
	case pgoutput.MsgBegin:
		r.currentCommit = m.FinalLSN
		r.currentCommitTime = m.CommitTime
		r.h.OnBegin(m.FinalLSN, m.CommitTime, m.XID)

	case pgoutput.MsgCommit:
		r.h.OnCommit(m.CommitLSN, m.CommitTime)
		// The whole transaction has been dispatched, so it is now safe to
		// acknowledge it. Acknowledging mid-transaction would risk losing the
		// remainder after a crash.
		if m.EndLSN > r.confirmed.Load() {
			r.confirmed.Store(m.EndLSN)
		}
		r.currentCommit, r.currentCommitTime = 0, time.Time{}

	case pgoutput.MsgRelation, pgoutput.MsgType:
		// Handled through the decoder's OnRelation callback.

	case pgoutput.MsgInsert, pgoutput.MsgUpdate, pgoutput.MsgDelete:
		rel, ok := r.dec.Relation(m.RelationOID)
		if !ok {
			// Protocol violation: DML before its Relation message.
			return fmt.Errorf("reader: no relation cached for OID %d", m.RelationOID)
		}
		if err := r.h.OnChange(m, rel, r.currentCommit, r.currentCommitTime); err != nil {
			return err
		}

	case pgoutput.MsgTruncate:
		r.h.OnTruncate(m.TruncateRelations)

	case pgoutput.MsgMessage:
		if err := r.h.OnMessage(m, r.currentCommit); err != nil {
			return err
		}

	case pgoutput.MsgStreamStart, pgoutput.MsgStreamStop,
		pgoutput.MsgStreamCommit, pgoutput.MsgStreamAbort:
		// Only reachable with streaming enabled, which is not the default.
		r.log.Debug("stream control message", "type", string(rune(m.Type)), "xid", m.StreamXID)

	default:
		r.log.Warn("unhandled pgoutput message", "type", string(rune(m.Type)))
	}
	return nil
}

// FormatLSN renders an LSN the way PostgreSQL does, so that a value in a log or
// an SSE id can be pasted straight into a query.
func FormatLSN(lsn uint64) string { return pglogrepl.LSN(lsn).String() }

// ParseLSN parses PostgreSQL's X/Y form.
func ParseLSN(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	l, err := pglogrepl.ParseLSN(s)
	if err != nil {
		return 0, err
	}
	return uint64(l), nil
}
