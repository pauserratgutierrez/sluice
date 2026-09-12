// Package oracle is the subscribe-time judge of shapes.
//
// One process runs one oracle, chosen at deploy time: RLS (GRANT + policies,
// three tiers) or an HTTP issuer. The tube after a grant is the same: an
// effective AND-only filter, a no-op or tiered Decision on the hot path, and
// (in issuer mode) holds watched on the WAL.
package oracle

import (
	"context"
	"fmt"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

const (
	NameRLS    = "rls"
	NameIssuer = "issuer"
)

// Action is what the issuer endpoint is being asked to decide.
type Action string

const (
	ActionSubscribe Action = "subscribe"
	ActionRefresh   Action = "refresh"
)

// Request is one shape the caller wants to subscribe to or refresh.
type Request struct {
	Action   Action
	Identity authz.Identity
	Relation *catalog.Relation
	Filter   *shape.Filter // client-requested; may be empty
	Columns  []string      // client-requested; empty means all columns
	Ops      []string
}

// Grant is the mechanical result of a decision. Decision is internal and is
// never serialized as a product-tier on the issuer path.
type Grant struct {
	Decision *authz.Decision
	Filter   *shape.Filter
	Columns  []string
	Denied   []string // requested columns dropped from the projection
	Holds    []hold.Spec
	Reason   string
}

// Oracle resolves shapes at subscribe and token-refresh time.
type Oracle interface {
	Name() string
	Resolve(ctx context.Context, req Request) (*Grant, error)
	Refresh(ctx context.Context, req Request) (*Grant, error)
	LeaseTick(ctx context.Context, h *authz.Handle, rel *catalog.Relation, id authz.Identity, eqs map[string]expr.Value) (held bool, err error)
	SnapshotImpersonate() bool
}

// ErrDenied is a closed-door grant: the caller is not entitled to the shape.
type ErrDenied struct {
	Reason string
	Code   string // default shape_not_authorized
}

func (e *ErrDenied) Error() string { return e.Reason }

func (e *ErrDenied) WireCode() string {
	if e.Code != "" {
		return e.Code
	}
	return "shape_not_authorized"
}

// New constructs the process-wide oracle from config.
func New(cfg *config.Config, az *authz.Authorizer, cat *catalog.Cache) (Oracle, error) {
	if cfg == nil {
		return nil, fmt.Errorf("oracle: config is required")
	}
	switch cfg.ShapeOracle {
	case "", NameRLS:
		if az == nil || cat == nil {
			return nil, fmt.Errorf("oracle: rls mode needs an authorizer and a catalog")
		}
		return NewRLS(az, cat), nil
	case NameIssuer:
		return NewIssuer(cfg, cat)
	default:
		return nil, fmt.Errorf("oracle: unknown SLUICE_SHAPE_ORACLE %q", cfg.ShapeOracle)
	}
}

// grantedDecision is the internal no-op used in issuer mode so deliver can keep
// calling Visible without a branch: Tier A, granted, no lease.
func grantedDecision(reason string) *authz.Decision {
	return &authz.Decision{
		Tier:      authz.TierA,
		Granted:   true,
		Predicate: expr.TrueNode,
		Reason:    reason,
	}
}
