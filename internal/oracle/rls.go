package oracle

import (
	"context"

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

func (r *RLS) Resolve(ctx context.Context, req Request) (*Grant, error) {
	return r.decide(ctx, req)
}

func (r *RLS) Refresh(ctx context.Context, req Request) (*Grant, error) {
	return r.decide(ctx, req)
}

func (r *RLS) LeaseTick(ctx context.Context, h *authz.Handle, rel *catalog.Relation, id authz.Identity, eqs map[string]expr.Value) (bool, error) {
	return r.az.Refresh(ctx, h, rel, id, eqs)
}

func (r *RLS) decide(ctx context.Context, req Request) (*Grant, error) {
	rel := req.Relation
	filter := req.Filter
	if filter == nil {
		empty, err := shape.Parse("", rel)
		if err != nil {
			return nil, err
		}
		filter = empty
	}

	columns := req.Columns
	if len(columns) == 0 {
		for _, c := range rel.Columns {
			columns = append(columns, c.Name)
		}
	}
	granted, err := r.cat.HasColumnPrivilege(ctx, req.Identity.Role, rel.FullName(), columns)
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
			Reason: "role " + req.Identity.Role + " may not select any of the requested columns on " + rel.FullName(),
		}
	}

	var eqs map[string]expr.Value
	if filter != nil {
		eqs = filter.Equalities
	}
	d, err := r.az.Resolve(ctx, rel, req.Identity, eqs)
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
