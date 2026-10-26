package hold

import (
	"context"
	"errors"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// batchQuerier answers each queued EXISTS from exists, keyed by the query's
// arguments, and records how many queries each batch carried.
type batchQuerier struct {
	exists  map[string]bool
	fail    error
	batches []int
}

func (q *batchQuerier) SendBatch(_ context.Context, b *pgx.Batch) pgx.BatchResults {
	q.batches = append(q.batches, b.Len())
	return &batchResults{q: q, queued: b.QueuedQueries}
}

type batchResults struct {
	q      *batchQuerier
	queued []*pgx.QueuedQuery
	next   int
}

func (r *batchResults) Exec() (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, errors.ErrUnsupported
}
func (r *batchResults) Query() (pgx.Rows, error) { return nil, errors.ErrUnsupported }
func (r *batchResults) Close() error             { return r.q.fail }

func (r *batchResults) QueryRow() pgx.Row {
	qq := r.queued[r.next]
	r.next++
	return existsRow{ok: r.q.exists[qq.Arguments[0].(string)+","+qq.Arguments[1].(string)], err: r.q.fail}
}

type existsRow struct {
	ok  bool
	err error
}

func (r existsRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*bool) = r.ok
	return nil
}

// A request's holds are checked in one batch, each distinct hold once, and
// every grant naming a hold gets its answer.
func TestCheckAsksEachDistinctHoldOnce(t *testing.T) {
	rel := membersRel()
	member := holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")
	other := holdSpec(t, rel, "project_id=eq.43,user_id=eq.u1")
	q := &batchQuerier{exists: map[string]bool{"42,u1": true}}

	errs := Check(context.Background(), q, []Spec{member, member, other, member})
	if len(q.batches) != 1 || q.batches[0] != 2 {
		t.Fatalf("batches %v, want one batch of 2 queries", q.batches)
	}
	for _, i := range []int{0, 1, 3} {
		if errs[i] != nil {
			t.Errorf("spec %d: %v, want the hold found", i, errs[i])
		}
	}
	var missing *MissingError
	if !errors.As(errs[2], &missing) {
		t.Fatalf("spec 2: %v, want a missing hold", errs[2])
	}
}

// A batch that cannot run says nothing about the holds: every one is a
// CheckError, which the server makes retryable.
func TestCheckThatFailsIsACheckError(t *testing.T) {
	rel := membersRel()
	q := &batchQuerier{fail: context.DeadlineExceeded}
	errs := Check(context.Background(), q, []Spec{
		holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1"),
		holdSpec(t, rel, "project_id=eq.43,user_id=eq.u1"),
	})
	for i, err := range errs {
		var check *CheckError
		if !errors.As(err, &check) || !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("spec %d: %v, want a CheckError", i, err)
		}
	}
	if errs := Check(context.Background(), q, nil); len(errs) != 0 || len(q.batches) != 1 {
		t.Fatalf("no holds must send no batch: %v, batches %v", errs, q.batches)
	}
}

// Activate registers a shape only while its watch is the one added: a hold
// cut between Add and Activate leaves it unregistered, and a registration that
// fails takes the watch out again.
func TestActivateRegistersOnlyWhileTheWatchIsCurrent(t *testing.T) {
	rel := membersRel()
	registered := 0
	register := func() error { registered++; return nil }

	x := New()
	x.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
	if err := x.Activate("s1", "docs", register); err != nil || registered != 1 {
		t.Fatalf("activate: %v, %d registrations, want one", err, registered)
	}

	x.Add("s1", "cut", []Spec{holdSpec(t, rel, "project_id=eq.43,user_id=eq.u1")})
	cuts := x.OnChange(rel.OID, 'D', row{"project_id": expr.Text("43"), "user_id": expr.Text("u1")}, nil)
	if len(cuts) != 1 {
		t.Fatalf("cuts %+v, want the watch on project 43", cuts)
	}
	if err := x.Activate("s1", "cut", register); err == nil || registered != 1 {
		t.Fatalf("a cut watch must not register its shape: %v, %d registrations", err, registered)
	}

	taken := errors.New("label taken")
	x.Add("s1", "dup", []Spec{holdSpec(t, rel, "project_id=eq.44,user_id=eq.u1")})
	if err := x.Activate("s1", "dup", func() error { return taken }); !errors.Is(err, taken) {
		t.Fatalf("activate: %v, want register's error", err)
	}
	if x.Count() != 1 {
		t.Fatalf("%d watches, want only the activated one", x.Count())
	}
	checkConsistent(t, x)
}
