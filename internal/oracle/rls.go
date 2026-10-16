package oracle

import (
	"context"

	"golang.org/x/sync/errgroup"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// RLS wraps the existing GRANT + policy authorizer. LOGIN and impersonation
// are unchanged: snapshots still SET ROLE.
type RLS struct {
	az  *authz.Authorizer
	cat *catalog.Cache
}

func NewRLS(az *authz.Authorizer, cat *catalog.Cache) *RLS {
	return &RLS{az: az, cat: cat}
}

func (r *RLS) Name() string              { return NameRLS }
func (r *RLS) SnapshotImpersonate() bool { return true }

// rlsConcurrency bounds how many shapes of one request are decided at once:
// a decision is catalog work and, in Tier C, probes.
const rlsConcurrency = 4

// Resolve decides each shape locally, the same for subscribe and refresh.
func (r *RLS) Resolve(ctx context.Context, _ Action, id authz.Identity, reqs []Request) []Result {
	out := make([]Result, len(reqs))
	var g errgroup.Group
	g.SetLimit(rlsConcurrency)
	for i, req := range reqs {
		g.Go(func() error {
			out[i].Grant, out[i].Err = r.decide(ctx, id, req)
			return nil
		})
	}
	_ = g.Wait()
	return out
}

func (r *RLS) LeaseTick(ctx context.Context, h *authz.Handle, rel *catalog.Relation, id authz.Identity, eqs map[string]expr.Value) (bool, error) {
	return r.az.Refresh(ctx, h, rel, id, eqs)
}

func (r *RLS) decide(ctx context.Context, id authz.Identity, req Request) (*Grant, error) {
	rel := req.Relation
	filter := req.Filter
	if filter == nil {
		empty, err := shape.Parse("", rel)
		if err != nil {
			return nil, err
		}
		filter = empty
	}

	columns := projection(rel, req.Columns)
	granted, err := r.cat.HasColumnPrivilege(ctx, id.Role, rel, columns)
	if err != nil {
		return nil, err
	}
	var allowed, denied []string
	for _, c := range columns {
		if granted[c] {
			allowed = append(allowed, c)
		} else {
			denied = append(denied, c)
		}
	}
	if len(allowed) == 0 {
		return nil, &ErrDenied{
			Code:   "column_not_granted",
			Reason: "role " + id.Role + " may not select any of the requested columns on " + rel.FullName(),
		}
	}

	var eqs map[string]expr.Value
	if filter != nil {
		eqs = filter.Equalities
	}
	d, err := r.az.Resolve(ctx, rel, id, eqs)
	if err != nil {
		return nil, err
	}
	return &Grant{
		Decision: d,
		Filter:   filter,
		Columns:  allowed,
		Denied:   denied,
		Reason:   d.Reason,
	}, nil
}
