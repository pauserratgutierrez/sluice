// Package authz implements Sluice's three-tier authorization model.
//
// This is the core of the whole design. Authorization is resolved ONCE, at
// subscribe time, into something that costs nothing per change. Measured on
// PostgreSQL 18.4, the alternative -- per-subscriber impersonation, which is
// what supabase/walrus does -- costs ~9-13 microseconds per subscriber per
// change and caps at 108 changes/sec with 1,000 subscribers.
//
//	Tier A  the predicate reduces to a constant over the whole shape.
//	        Decided once. Zero work per change, forever.
//	Tier B  the predicate is row-dependent but compilable, so it is evaluated
//	        in process against the tuple the WAL already delivered.
//	        Zero database round trips.
//	Tier C  the predicate contains a subquery or something else uncompilable.
//	        Impersonated probe per subscriber per change: correct, slow, and
//	        loudly reported so it can be fixed.
package authz

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
)

type Tier string

const (
	TierA Tier = "A"
	TierB Tier = "B"
	TierC Tier = "C"
)

// Decision is the result of resolving a subscription's authorization.
type Decision struct {
	Tier Tier

	// Granted is meaningful for Tier A only: the constant value the predicate
	// reduced to. False means the subscription must be refused outright.
	Granted bool

	// Predicate is the compiled, folded expression Tier B evaluates per row.
	Predicate expr.Node

	// NeedsLease is true when the predicate's truth can change without any row
	// or claim changing -- a volatile function, or a Tier C subquery over
	// another table. Stable predicates need no lease at all.
	NeedsLease bool
	LeaseUntil time.Time

	// Reason explains a Tier B or Tier C classification, for /diagnostics.
	Reason string

	// PredicateSQL is PostgreSQL's own spelling of the combined predicate, used
	// for Tier C evaluation and for the Tier B cross-check.
	PredicateSQL string

	// verify is the cross-check budget and outcome. It is a property of the
	// subscription rather than of any one resolution, so it is shared by
	// reference across generations instead of being copied into each.
	verify *verifyState
}

// verifyState is the mutable half of an authorization decision, kept out of
// Decision so that Decision itself can be immutable.
type verifyState struct {
	// left counts how many more Tier B decisions to cross-check against
	// PostgreSQL. See Authorizer.Visible.
	left atomic.Int32
	// downgraded is set when a cross-check failed and the decision was forced to
	// Tier C.
	downgraded atomic.Bool
}

// Handle is the mutable holder a subscription keeps. The Decision inside it is
// replaced wholesale, never edited.
//
// Every reader is on the per-change path, and re-resolution can happen under
// them at any tick. Publishing a new Decision by storing a pointer is what makes
// that safe, and the safety is not only about the struct: Predicate points at a
// freshly built expression tree, and an unsynchronised pointer write would let a
// reader follow it into nodes whose field writes it has no guarantee of seeing.
// The atomic store/load pair supplies the happens-before edge that publishes the
// whole tree, and it costs the reader a plain load rather than the shared-cache-
// line traffic an RWMutex would put on the hottest path in the system.
type Handle struct {
	p atomic.Pointer[Decision]
}

// NewHandle publishes an initial decision.
//
// Verification state is filled in when absent, so that every Decision reachable
// through a Handle has it and the readers of EffectiveTier need no nil check on
// the hot path.
func NewHandle(d *Decision) *Handle {
	if d != nil && d.verify == nil {
		d.verify = &verifyState{}
	}
	h := &Handle{}
	h.p.Store(d)
	return h
}

// Load returns the decision currently in force. Callers deciding one change
// should load once and use that snapshot throughout, so every field they read
// belongs to the same generation.
func (h *Handle) Load() *Decision { return h.p.Load() }

// Downgraded reports whether a Tier B decision was demoted to Tier C because the
// in-process evaluator disagreed with PostgreSQL.
func (d *Decision) Downgraded() bool { return d.verify.downgraded.Load() }

// EffectiveTier is the tier actually in force, accounting for a downgrade.
func (d *Decision) EffectiveTier() Tier {
	if d.verify.downgraded.Load() {
		return TierC
	}
	return d.Tier
}

// Identity is the verified caller.
type Identity struct {
	Role      string
	Sub       string
	SessionID string
	Claims    expr.Claims
	ClaimsRaw string
}

// Row adapts a decoded tuple for expression evaluation.
type Row = expr.Row

// Tuple is the row a decision is being made about, exactly as the WAL delivered
// it. One is built per change and shared by every subscriber on that relation, so
// the JSON encoding is memoised.
type Tuple struct {
	Op  byte // 'I', 'U' or 'D'
	PK  map[string]expr.Value
	Rel *pgoutput.Relation
	Row *pgoutput.Tuple

	once     sync.Once
	js       []byte
	complete bool
}

// CompleteJSON renders the tuple for jsonb_populate_record, and reports whether
// every column is present.
//
// Completeness is not a nicety. A column the WAL omitted -- an unchanged TOASTed
// value, or a column outside a narrow replica identity -- would arrive at
// PostgreSQL as NULL and could flip the predicate in either direction. So an
// incomplete tuple disqualifies this path rather than being silently patched.
func (t *Tuple) CompleteJSON() ([]byte, bool) {
	if t == nil || t.Row == nil || t.Rel == nil {
		return nil, false
	}
	t.once.Do(func() {
		obj := make(map[string]any, len(t.Row.Columns))
		complete := len(t.Row.Columns) == len(t.Rel.Columns)
		for i, c := range t.Row.Columns {
			if i >= len(t.Rel.Columns) {
				complete = false
				break
			}
			meta := t.Rel.Columns[i]
			switch c.Kind {
			case pgoutput.ColNull:
				obj[meta.Name] = nil
			case pgoutput.ColText:
				// Text is handed back verbatim; PostgreSQL applies the column's
				// own input function, so round-tripping is exact.
				obj[meta.Name] = string(c.Data)
			default:
				complete = false
			}
		}
		if !complete {
			return
		}
		js, err := json.Marshal(obj)
		if err != nil {
			return
		}
		t.js, t.complete = js, true
	})
	return t.js, t.complete
}

// Authorizer resolves and evaluates authorization decisions.
type Authorizer struct {
	pool  *pgxpool.Pool
	cat   *catalog.Cache
	lease time.Duration
	tierC string // "allow" | "deny"

	// verifySamples is how many Tier B decisions to cross-check against
	// PostgreSQL before trusting the compiled evaluator. Zero disables it.
	verifySamples int32

	// probeBudget rate-limits Tier C. Over budget, changes are WITHHELD from
	// Tier C subscribers rather than delivered unauthorized.
	probeBudget *budget

	// OnDowngrade is called when a cross-check fails. Wired to logging, metrics
	// and a client warning, because a parser disagreeing with PostgreSQL is the
	// single most serious thing that can go wrong here.
	OnDowngrade func(rel *catalog.Relation, predicateSQL string, goSaid, pgSaid bool)
}

type Options struct {
	Lease           time.Duration
	TierC           string
	MaxProbesPerSec int
	VerifySamples   int
}

func New(pool *pgxpool.Pool, cat *catalog.Cache, o Options) *Authorizer {
	return &Authorizer{
		pool:          pool,
		cat:           cat,
		lease:         o.Lease,
		tierC:         o.TierC,
		verifySamples: int32(o.VerifySamples),
		probeBudget:   newBudget(o.MaxProbesPerSec),
	}
}

// ErrDenied means the subscription is not authorized at all and must be refused.
type ErrDenied struct{ Reason string }

func (e *ErrDenied) Error() string { return "authorization denied: " + e.Reason }

// ErrTierCDisabled means the predicate needs Tier C but the operator disabled it.
type ErrTierCDisabled struct{ Reason string }

func (e *ErrTierCDisabled) Error() string {
	return "predicate requires per-change impersonation, which is disabled: " + e.Reason
}

// Resolve classifies a subscription into a tier.
//
// `equalities` is the set of column=constant pairs from the shape's filter. It
// is what makes Tier A possible: if the policy reads only columns the shape pins
// to constants, the policy's truth value is the same for every row in the shape,
// so it can be decided once.
func (a *Authorizer) Resolve(
	ctx context.Context,
	rel *catalog.Relation,
	id Identity,
	equalities map[string]expr.Value,
) (*Decision, error) {

	bypass := a.cat.BypassesRLS(id.Role)
	pred, parseIssue, predSQL := rel.Predicate(id.Role, bypass)
	d := &Decision{Predicate: pred, PredicateSQL: predSQL, verify: &verifyState{}}

	// A role with BYPASSRLS, or a table without RLS, has nothing to evaluate.
	if expr.IsAlwaysTrue(pred) {
		d.Tier = TierA
		d.Granted = true
		// Worth distinguishing: a Tier A grant on a table that does have RLS
		// enabled looks alarming in /diagnostics until it says why.
		d.Reason = "no row-level security applies"
		if bypass && rel.RLSEnabled {
			d.Reason = fmt.Sprintf(
				"role %q bypasses row-level security, so every row of %s is visible to it",
				id.Role, rel.FullName())
		}
		return d, nil
	}
	if expr.IsAlwaysFalse(pred) {
		return nil, &ErrDenied{Reason: fmt.Sprintf(
			"row-level security is enabled on %s but no SELECT policy grants access to role %q",
			rel.FullName(), id.Role)}
	}

	evalCtx := &expr.Context{Claims: id.Claims, ClaimsJSON: id.ClaimsRaw, Now: time.Now()}

	// Fold claim-derived subtrees to literals. The canonical Supabase policy is
	//   owner_id = (current_setting('request.jwt.claims')::jsonb ->> 'sub')::uuid
	// and the entire right-hand side is constant for this subscription. Folding
	// removes JSON parsing from the per-change path entirely.
	folded := expr.Fold(pred, evalCtx)
	info := expr.Analyze(folded, rel.Name)

	// ---- Tier A: does the shape pin every column the predicate reads? -------
	if info.Compilable() && coveredBy(info.Columns, equalities) {
		reduced := expr.Substitute(folded, equalities)
		visible, unknown := expr.Visible(reduced, evalCtx)
		if unknown {
			// Should not happen once all columns are substituted, but never
			// promote an unknown into a grant.
			d.Tier = TierB
			d.Predicate = folded
			d.Reason = "predicate reduced to a constant but could not be evaluated; falling back to per-row evaluation"
			d.NeedsLease = info.Volatile
			return d, nil
		}
		if !visible {
			return nil, &ErrDenied{Reason: fmt.Sprintf(
				"the row-level security policy on %s does not grant this caller access to the requested shape",
				rel.FullName())}
		}
		d.Tier = TierA
		d.Granted = true
		d.Predicate = expr.TrueNode
		d.NeedsLease = info.Volatile
		if d.NeedsLease {
			d.LeaseUntil = time.Now().Add(a.lease)
			d.Reason = "predicate reduces to a constant over the shape, but is time-dependent so it is re-checked on a lease"
		} else {
			d.Reason = "predicate reduces to a constant over the shape and is stable"
		}
		return d, nil
	}

	// ---- Tier B: compilable, row-dependent ---------------------------------
	if info.Compilable() {
		d.Tier = TierB
		d.Predicate = folded
		d.NeedsLease = info.Volatile
		d.verify.left.Store(a.verifySamples)
		if d.NeedsLease {
			d.LeaseUntil = time.Now().Add(a.lease)
		}
		missing := missingFrom(info.Columns, equalities)
		d.Reason = fmt.Sprintf(
			"predicate depends on column(s) %s which the shape does not pin to a constant; "+
				"evaluated in process against the WAL tuple at zero database cost",
			strings.Join(missing, ", "))
		return d, nil
	}

	// ---- Tier C: not compilable --------------------------------------------
	reason := info.Unsupported
	if parseIssue != "" {
		reason = parseIssue
	}
	if a.tierC == "deny" {
		return nil, &ErrTierCDisabled{Reason: reason}
	}
	d.Tier = TierC
	d.Predicate = folded
	d.NeedsLease = true
	d.LeaseUntil = time.Now().Add(a.lease)
	d.Reason = reason
	return d, nil
}

// Visible decides whether one tuple is visible to one subscription.
//
// The unknown return is not a detail. It means the WAL did not carry a value the
// predicate needs -- an unchanged TOASTed column, or a column absent from an old
// tuple under a narrow replica identity. Callers must mark the event degraded
// rather than pretending a clean decision was made, and must never treat unknown
// as visible.
func (a *Authorizer) Visible(
	ctx context.Context,
	d *Decision,
	id Identity,
	rel *catalog.Relation,
	row Row,
	t *Tuple,
) (visible bool, unknown bool) {

	switch d.EffectiveTier() {
	case TierA:
		return d.Granted, false

	case TierB:
		v, unk := expr.Visible(d.Predicate, &expr.Context{
			Row: row, Claims: id.Claims, ClaimsJSON: id.ClaimsRaw, Now: time.Now(),
		})
		if !unk {
			a.crossCheck(ctx, d, id, rel, t, v)
		}
		return v, unk
	}

	// ---- Tier C ------------------------------------------------------------
	if !a.probeBudget.take() {
		// Withhold, do not deliver. Over-budget must never become a leak.
		return false, true
	}

	// Preferred path: evaluate the real policy expression against the tuple the
	// WAL delivered, using jsonb_populate_record to reconstitute the row.
	//
	// This is what makes a Tier C DELETE correct -- the row is gone from the
	// table, so the classic `SELECT EXISTS (... WHERE pk = ...)` probe that
	// supabase/walrus uses can only ever return false, which is precisely why
	// upstream truncates deleted rows to primary keys and calls RLS-on-DELETE
	// impossible. Reconstituting the row sidesteps that entirely, and it works
	// even for a policy containing a subquery against another table.
	//
	// It requires a COMPLETE tuple: a column the WAL omitted would arrive as
	// NULL and could flip the predicate either way. So it is used only when every
	// column is present, which for DELETE means REPLICA IDENTITY FULL.
	if js, ok := t.CompleteJSON(); ok {
		v, err := a.evalOnTuple(ctx, id, rel, d.PredicateSQL, js)
		if err == nil {
			return v, false
		}
	}

	// Fallback: probe the live row by primary key. Correct for INSERT and UPDATE;
	// impossible for DELETE, which the caller reports as degraded rather than
	// guessing.
	if t.Op == 'D' || len(t.PK) == 0 {
		return false, true
	}
	ok, err := a.probePK(ctx, id, rel, t.PK)
	if err != nil {
		return false, true
	}
	return ok, false
}

// crossCheck verifies the in-process evaluator against PostgreSQL itself.
//
// Sluice parses policy text with a pure-Go port of PostgreSQL's own grammar, so
// the shape of the tree is not in question and anything Sluice declines to
// evaluate already fails closed to Tier C. The residual risk is narrower but
// sharper: an expression Sluice does parse correctly and then evaluates
// differently -- a coercion difference, a collation-dependent comparison, a
// three-valued-logic corner.
//
// So for the first N changes on each Tier B subscription, the same predicate is
// also evaluated by PostgreSQL against the same tuple and the verdicts compared.
// A disagreement downgrades the subscription to Tier C and shouts. The cost is
// bounded and paid once per subscription, not per change.
func (a *Authorizer) crossCheck(ctx context.Context, d *Decision, id Identity, rel *catalog.Relation, t *Tuple, goSaid bool) {
	if d.verify.left.Load() <= 0 || d.verify.downgraded.Load() {
		return
	}
	js, complete := t.CompleteJSON()
	if !complete {
		return // an incomplete tuple would make PostgreSQL and Go disagree for a legitimate reason
	}
	if d.verify.left.Add(-1) < 0 {
		return
	}

	pgSaid, err := a.evalOnTuple(ctx, id, rel, d.PredicateSQL, js)
	if err != nil {
		return // a transient database error is not evidence of a compiler bug
	}
	if pgSaid == goSaid {
		return
	}

	// Fail closed: from here on this subscription pays for an impersonated probe
	// per change, which is slow but correct.
	d.verify.downgraded.Store(true)
	if a.OnDowngrade != nil {
		a.OnDowngrade(rel, d.PredicateSQL, goSaid, pgSaid)
	}
}

// evalOnTuple asks PostgreSQL to evaluate a policy predicate against a
// reconstituted row, under the caller's role and claims.
//
// The relation alias is the bare relation name because that is how pg_get_expr
// qualifies self-references (`posts.owner_id`), so the synthetic record has to
// answer to the same name.
func (a *Authorizer) evalOnTuple(ctx context.Context, id Identity, rel *catalog.Relation, predicateSQL string, rowJSON []byte) (bool, error) {
	sql := fmt.Sprintf(
		`SELECT coalesce((SELECT (%s) FROM jsonb_populate_record(NULL::%s, $1::jsonb) AS %s), false)`,
		predicateSQL,
		catalog.QuoteQualified(rel.Schema, rel.Name),
		catalog.QuoteIdent(rel.Name))
	return a.queryAs(ctx, id, sql, string(rowJSON))
}

// probePK is the classic existence check: does a row with this primary key exist
// as far as this caller is concerned?
func (a *Authorizer) probePK(ctx context.Context, id Identity, rel *catalog.Relation, pk map[string]expr.Value) (bool, error) {
	where := make([]string, 0, len(pk))
	args := make([]any, 0, len(pk))
	for col, v := range pk {
		where = append(where, fmt.Sprintf("%s = $%d", catalog.QuoteIdent(col), len(args)+1))
		args = append(args, v.String())
	}
	sql := fmt.Sprintf(`SELECT EXISTS (SELECT 1 FROM %s WHERE %s)`,
		catalog.QuoteQualified(rel.Schema, rel.Name), strings.Join(where, " AND "))
	return a.queryAs(ctx, id, sql, args...)
}

// queryAs runs a boolean query under the caller's identity.
//
// Impersonation is transaction-scoped -- set_config(..., true) -- so PostgreSQL
// unwinds it at ROLLBACK regardless of what the pool does next. A connection
// returned to the pool can never carry a stale role into another caller's query.
// Identifiers come from the catalog and values are always bound parameters.
func (a *Authorizer) queryAs(ctx context.Context, id Identity, sql string, args ...any) (bool, error) {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{AccessMode: pgx.ReadOnly})
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := tx.Exec(ctx,
		`SELECT set_config('role', $1, true), set_config('request.jwt.claims', $2, true)`,
		id.Role, id.ClaimsRaw); err != nil {
		return false, err
	}
	var ok bool
	if err := tx.QueryRow(ctx, sql, args...).Scan(&ok); err != nil {
		return false, err
	}
	return ok, nil
}

// Refresh re-resolves a subscription and publishes the result. Returns false
// when access has been revoked, so the caller can drop the subscription
// immediately.
//
// The new decision replaces the old one wholesale rather than being written into
// it field by field. Readers are on the per-change path and hold no lock, so an
// edit in place would race them -- both on the scalar fields and, more seriously,
// on the freshly built expression tree that Predicate points at.
//
// Cross-check state is carried across only when the predicate is unchanged. A
// lease renewal on the same policy text must not forget a downgrade the
// cross-check already earned; a genuinely new predicate must not inherit a
// verdict reached about the old one, so it starts its sampling over.
func (a *Authorizer) Refresh(ctx context.Context, h *Handle, rel *catalog.Relation, id Identity, equalities map[string]expr.Value) (bool, error) {
	nd, err := a.Resolve(ctx, rel, id, equalities)
	if err != nil {
		return false, err
	}
	if old := h.Load(); old != nil && old.PredicateSQL == nd.PredicateSQL {
		nd.verify = old.verify
	}
	h.p.Store(nd)
	return !(nd.EffectiveTier() == TierA && !nd.Granted), nil
}

// Expired reports whether a leased decision is due for re-evaluation.
func (d *Decision) Expired(now time.Time) bool {
	return d.NeedsLease && !d.LeaseUntil.IsZero() && now.After(d.LeaseUntil)
}

// ParseClaims decodes a claim set into the form the evaluator wants, keeping the
// raw JSON so that `current_setting('request.jwt.claims')` is byte-exact.
func ParseClaims(raw []byte) (expr.Claims, string, error) {
	var c expr.Claims
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, "", err
	}
	return c, string(raw), nil
}

func coveredBy(columns []string, equalities map[string]expr.Value) bool {
	if len(columns) == 0 {
		return true
	}
	for _, c := range columns {
		if _, ok := equalities[c]; !ok {
			return false
		}
	}
	return true
}

func missingFrom(columns []string, equalities map[string]expr.Value) []string {
	var out []string
	for _, c := range columns {
		if _, ok := equalities[c]; !ok {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		out = append(out, "(none)")
	}
	return out
}

// budget is a simple per-second token bucket.
type budget struct {
	mu     sync.Mutex
	limit  int
	used   int
	window time.Time
}

func newBudget(perSecond int) *budget {
	return &budget{limit: perSecond, window: time.Now()}
}

func (b *budget) take() bool {
	if b.limit <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if now.Sub(b.window) >= time.Second {
		b.window = now
		b.used = 0
	}
	if b.used >= b.limit {
		return false
	}
	b.used++
	return true
}
