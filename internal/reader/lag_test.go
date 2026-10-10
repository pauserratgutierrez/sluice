package reader

import (
	"bytes"
	"encoding/binary"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"

	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

type lagHandler struct {
	lagAtCommit time.Duration
	r           *Reader
}

func (h *lagHandler) OnStart(uint64)                    {}
func (h *lagHandler) OnBegin(uint64, time.Time, uint32) {}
func (h *lagHandler) OnChange(*pgoutput.Message, *pgoutput.Relation, uint64, time.Time) error {
	return nil
}
func (h *lagHandler) OnMessage(*pgoutput.Message, uint64) error { return nil }
func (h *lagHandler) OnCommit(uint64, time.Time)                { h.lagAtCommit = h.r.Lag() }
func (h *lagHandler) OnRelation(_, _ *pgoutput.Relation)        {}
func (h *lagHandler) OnTruncate([]uint32)                       {}

func pgMicros(t time.Time) int64 {
	return t.Sub(time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)).Microseconds()
}

// Lag is how long ago the transaction being dispatched committed, and 0
// between transactions.
func TestLagFollowsTheTransactionBeingDispatched(t *testing.T) {
	h := &lagHandler{}
	r := New(nil, nil, nil, h)
	h.r = r
	committed := time.Now().Add(-3 * time.Second)

	var begin bytes.Buffer
	begin.WriteByte(pgoutput.MsgBegin)
	_ = binary.Write(&begin, binary.BigEndian, uint64(0x100))
	_ = binary.Write(&begin, binary.BigEndian, pgMicros(committed))
	_ = binary.Write(&begin, binary.BigEndian, uint32(7))
	if err := r.handle(pglogrepl.XLogData{WALData: begin.Bytes()}); err != nil {
		t.Fatal(err)
	}
	if lag := r.Lag(); lag < 3*time.Second || lag > 4*time.Second {
		t.Fatalf("inside a transaction committed 3 s ago: lag %v", lag)
	}

	var commit bytes.Buffer
	commit.WriteByte(pgoutput.MsgCommit)
	commit.WriteByte(0)
	_ = binary.Write(&commit, binary.BigEndian, uint64(0x100))
	_ = binary.Write(&commit, binary.BigEndian, uint64(0x180))
	_ = binary.Write(&commit, binary.BigEndian, pgMicros(committed))
	if err := r.handle(pglogrepl.XLogData{WALData: commit.Bytes()}); err != nil {
		t.Fatal(err)
	}
	if h.lagAtCommit < 3*time.Second {
		t.Fatalf("while the commit was dispatched: lag %v", h.lagAtCommit)
	}
	if lag := r.Lag(); lag != 0 {
		t.Fatalf("between transactions: lag %v, want 0", lag)
	}

	// A commit time ahead of this clock is not a negative lag.
	r.dispatching.Store(time.Now().Add(time.Minute).UnixNano())
	if lag := r.Lag(); lag != 0 {
		t.Fatalf("commit time in the future: lag %v, want 0", lag)
	}
}
