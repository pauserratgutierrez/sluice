// Package reader owns the single replication connection and dispatches changes.
//
// The options it sends to pgoutput are fixed except for two:
//
//	proto_version   SLUICE_PROTO_VERSION (default 4). With streaming off it only
//	                declares capability; the options decide the message set.
//	messages        SLUICE_MESSAGES (default true): delivers
//	                pg_logical_emit_message, which gives transactional broadcast
//	                with no outbox table.
//
// Streaming and binary are never requested. With streaming off, everything the
// reader receives is already committed, so it forwards immediately and holds no
// transaction buffer. With text format, one decoding path covers every type.
package reader

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/metrics"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

// Handler consumes decoded messages. The reader owns ordering and never calls a
// handler concurrently, so implementations need no locking of their own.
type Handler interface {
	// OnStart is called each time replication starts, with the LSN it starts
	// from: every transaction that commits after it will be delivered.
	OnStart(lsn uint64)
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
	// withheld, or -- between transactions -- to the end of WAL the server says
	// it has already decoded. That is the backpressure mechanism: a stalled
	// Sluice retains WAL on disk rather than losing data.
	//
	// Atomic because the reader goroutine writes them while /diagnostics,
	// /readyz and every snapshot read them.
	confirmed atomic.Uint64
	received  atomic.Uint64
	streaming atomic.Bool

	// inTxn is true between Begin and Commit; currentCommit carries the
	// enclosing transaction's LSN and timestamp down to per-row handlers.
	inTxn             bool
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

// Streaming reports whether the replication stream is currently running.
func (r *Reader) Streaming() bool { return r.streaming.Load() }

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

// slotState is the slot as pg_replication_slots reports it.
type slotState struct {
	exists       bool
	active       bool
	walStatus    string
	invalidation string
}

// gone says why the slot can no longer stream every change since its confirmed
// position, or "" while it still can.
func (s slotState) gone() string {
	switch {
	case !s.exists:
		return "does not exist"
	case s.invalidation != "":
		return "was invalidated (" + s.invalidation + ")"
	case s.walStatus == "lost":
		return "has lost WAL it needs"
	}
	return ""
}

type slotAction int

const (
	slotUse slotAction = iota
	slotCreate
	slotRecreate
	slotRefuse
)

// action decides what to do with the slot as found before streaming. Only an
// invalidated slot is ever replaced, only when the operator allowed it, and
// never while some other process holds it.
func (s slotState) action(recreate bool) slotAction {
	switch {
	case !s.exists:
		return slotCreate
	case s.gone() == "":
		return slotUse
	case recreate && !s.active:
		return slotRecreate
	}
	return slotRefuse
}

func (r *Reader) readSlot(ctx context.Context) (slotState, error) {
	st := slotState{exists: true}
	// invalidation_reason exists from PostgreSQL 17; through to_jsonb it reads
	// as NULL on 16 rather than failing the query.
	err := r.pool.QueryRow(ctx, `
		SELECT active, coalesce(wal_status, ''), coalesce(to_jsonb(s) ->> 'invalidation_reason', '')
		  FROM pg_replication_slots s WHERE slot_name = $1`,
		r.cfg.SlotName).Scan(&st.active, &st.walStatus, &st.invalidation)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, nil
	}
	return st, err
}

// prepareSlot readies the slot before the first START_REPLICATION. It runs in
// the process holding the reader lock, so no other Sluice is streaming it.
//
// The slot is PERMANENT, deliberately. supabase/realtime uses a temporary slot,
// which is dropped on any error or session end -- silently losing every change
// between the failure and the reconnect. A permanent slot is crash-safe and
// resumes from confirmed_flush_lsn.
//
// A missing slot is created: that is every first start, and a process whose
// slot disappeared has already exited, closing every stream. An invalidated
// slot stops the process unless SLUICE_SLOT_RECREATE allows replacing it,
// because a new slot starts after whatever the old one lost.
func (r *Reader) prepareSlot(ctx context.Context) error {
	st, err := r.readSlot(ctx)
	if err != nil {
		return fmt.Errorf("reader: read slot %q: %w", r.cfg.SlotName, err)
	}
	switch st.action(r.cfg.SlotRecreate) {
	case slotUse:
		r.log.Info("using existing replication slot", "slot", r.cfg.SlotName)
	case slotCreate:
		conn, err := r.connect(ctx)
		if err != nil {
			return err
		}
		defer conn.Close(context.Background())
		if err := r.createSlot(ctx, conn); err != nil {
			return err
		}
		r.log.Info("created replication slot", "slot", r.cfg.SlotName)
	case slotRecreate:
		conn, err := r.connect(ctx)
		if err != nil {
			return err
		}
		defer conn.Close(context.Background())
		if err := pglogrepl.DropReplicationSlot(ctx, conn, r.cfg.SlotName,
			pglogrepl.DropReplicationSlotOptions{}); err != nil {
			return fmt.Errorf("reader: drop slot %q: %w", r.cfg.SlotName, err)
		}
		if err := r.createSlot(ctx, conn); err != nil {
			return err
		}
		metrics.SlotRecreated.Inc()
		r.log.Warn("recreated the replication slot; changes made while it was unusable were not delivered",
			"slot", r.cfg.SlotName, "reason", st.gone())
	default:
		if st.active {
			return fmt.Errorf("reader: replication slot %q %s and another process holds it; not recreating it",
				r.cfg.SlotName, st.gone())
		}
		return fmt.Errorf("reader: replication slot %q %s, so the change stream has a gap. "+
			"Drop it with SELECT pg_drop_replication_slot('%s'); or set SLUICE_SLOT_RECREATE=true",
			r.cfg.SlotName, st.gone(), r.cfg.SlotName)
	}
	return nil
}

func (r *Reader) createSlot(ctx context.Context, conn *pgconn.PgConn) error {
	if _, err := pglogrepl.CreateReplicationSlot(ctx, conn, r.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false}); err != nil {
		return fmt.Errorf("reader: create slot %q: %w", r.cfg.SlotName, err)
	}
	return nil
}

// slotGone checks, after a failed stream, whether the slot itself is the
// problem. A query that fails, as while PostgreSQL restarts, is no answer, and
// the reader retries.
func (r *Reader) slotGone(ctx context.Context) string {
	st, err := r.readSlot(ctx)
	if err != nil {
		return ""
	}
	return st.gone()
}

// connect opens the replication connection with the output settings the wire
// encoder expects: pgoutput prints values with the session's output functions,
// so the date style, interval style and time zone are pinned here rather than
// inherited from whatever the server or role is configured with.
func (r *Reader) connect(ctx context.Context) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(r.cfg.ReplURL)
	if err != nil {
		return nil, fmt.Errorf("reader: parse SLUICE_DB_REPL_URL: %w", err)
	}
	cfg.RuntimeParams["DateStyle"] = "ISO"
	cfg.RuntimeParams["IntervalStyle"] = "postgres"
	cfg.RuntimeParams["TimeZone"] = "UTC"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("reader: connect replication: %w", err)
	}
	return conn, nil
}

// Run streams until the context is cancelled, reconnecting with backoff. It
// returns an error, and the process exits, when the slot is gone.
func (r *Reader) Run(ctx context.Context) error {
	if err := r.prepareSlot(ctx); err != nil {
		return err
	}
	backoff := time.Second
	for {
		started := time.Now()
		err := r.stream(ctx)
		r.streaming.Store(false)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// A slot that is gone is fatal by design: streaming on from a new
		// position would hide the gap. Exiting closes every stream, so every
		// client reconnects knowing it was not resumed.
		if why := r.slotGone(ctx); why != "" {
			return fmt.Errorf("reader: replication slot %q %s; the change stream has a gap: %w",
				r.cfg.SlotName, why, err)
		}
		// A stream that ran for a while failed on its own, not because the
		// last attempt did; start the backoff over.
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		metrics.ReaderReconnects.Inc()
		r.log.Error("replication stream failed, reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
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

	// Starting at 0/0 makes the server begin at the slot's confirmed_flush_lsn,
	// which is exactly what a resuming consumer wants.
	if err := pglogrepl.StartReplication(ctx, conn, r.cfg.SlotName, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: args}); err != nil {
		return fmt.Errorf("reader: START_REPLICATION: %w", err)
	}
	r.inTxn = false
	r.streaming.Store(true)
	r.h.OnStart(r.confirmed.Load())

	r.log.Info("replication started",
		"slot", r.cfg.SlotName, "publication", r.cfg.Publication,
		"proto_version", r.cfg.ProtoVersion, "from_lsn", FormatLSN(r.confirmed.Load()),
		"system_id", sys.SystemID, "timeline", sys.Timeline)

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
				// Between transactions, everything up to the server's WAL end
				// has been decoded and anything relevant already delivered.
				// Without this, a publication whose tables are quiet never
				// advances the slot while the rest of the database writes WAL,
				// and the slot retains it until it is invalidated.
				if !r.inTxn && uint64(ka.ServerWALEnd) > r.confirmed.Load() {
					r.confirmed.Store(uint64(ka.ServerWALEnd))
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
			return fmt.Errorf("reader: server error: %w", pgconn.ErrorResponseToPgError(msg))
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
		r.inTxn = true
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
		r.inTxn = false
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

	case pgoutput.MsgOrigin:
		// Only sent for changes that arrived through replication from another
		// node; nothing to do.

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
