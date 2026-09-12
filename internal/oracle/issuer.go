package oracle

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// Issuer asks an application endpoint to authorize a shape. Fail closed: a
// timeout, a non-2xx, or an unreadable body is a deny. There is no allow cache.
type Issuer struct {
	url    string
	bearer string
	client *http.Client

	lookup  func(schema, name string) (*catalog.Relation, bool)
	columns func(ctx context.Context, rel *catalog.Relation, requested []string) ([]string, []string, error)
}

// IssuerOptions wires catalog lookups and physical column grants. Tests inject
// both so the HTTP contract can be exercised without a database.
type IssuerOptions struct {
	URL     string
	Bearer  string
	Timeout time.Duration
	Client  *http.Client
	Lookup  func(schema, name string) (*catalog.Relation, bool)
	// Columns returns (allowed, denied, err) for the pool role's SELECT.
	Columns func(ctx context.Context, rel *catalog.Relation, requested []string) ([]string, []string, error)
}

func NewIssuer(cfg *config.Config, cat *catalog.Cache) (*Issuer, error) {
	if cfg == nil || !cfg.IssuerMode() {
		return nil, fmt.Errorf("oracle: issuer requires SLUICE_SHAPE_ORACLE=issuer")
	}
	timeout := cfg.IssuerTimeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var lookup func(schema, name string) (*catalog.Relation, bool)
	var cols func(ctx context.Context, rel *catalog.Relation, requested []string) ([]string, []string, error)
	if cat != nil {
		lookup = cat.Lookup
		cols = func(ctx context.Context, rel *catalog.Relation, requested []string) ([]string, []string, error) {
			return physicalColumns(ctx, cat, rel, requested)
		}
	}
	return NewIssuerWith(IssuerOptions{
		URL:     cfg.IssuerURL,
		Bearer:  cfg.IssuerBearer,
		Timeout: timeout,
		Lookup:  lookup,
		Columns: cols,
	})
}

func NewIssuerWith(o IssuerOptions) (*Issuer, error) {
	if o.URL == "" {
		return nil, fmt.Errorf("oracle: issuer URL is required")
	}
	if o.Bearer == "" {
		return nil, fmt.Errorf("oracle: issuer bearer is required")
	}
	timeout := o.Timeout
	if timeout <= 0 {
		timeout = 2 * time.Second
	}
	var client *http.Client
	if o.Client == nil {
		client = &http.Client{Timeout: timeout, CheckRedirect: errNoIssuerRedirect}
	} else {
		c := *o.Client
		c.CheckRedirect = errNoIssuerRedirect
		client = &c
	}
	return &Issuer{
		url:     o.URL,
		bearer:  o.Bearer,
		client:  client,
		lookup:  o.Lookup,
		columns: o.Columns,
	}, nil
}

// errNoIssuerRedirect stops the client from following a 302 (or any redirect).
// Following would re-send SLUICE_ISSUER_BEARER on the same host; a 302 to a
// 200 with allow:true would then pass. ErrUseLastResponse lets roundTrip see
// the 3xx and deny it like any other non-2xx.
func errNoIssuerRedirect(*http.Request, []*http.Request) error {
	return http.ErrUseLastResponse
}

func (i *Issuer) Name() string              { return NameIssuer }
func (i *Issuer) SnapshotImpersonate() bool { return false }

func (i *Issuer) Resolve(ctx context.Context, req Request) (*Grant, error) {
	if req.Action == "" {
		req.Action = ActionSubscribe
	}
	return i.decide(ctx, req)
}

func (i *Issuer) Refresh(ctx context.Context, req Request) (*Grant, error) {
	req.Action = ActionRefresh
	return i.decide(ctx, req)
}

func (i *Issuer) LeaseTick(context.Context, *authz.Handle, *catalog.Relation, authz.Identity, map[string]expr.Value) (bool, error) {
	return true, nil
}

type issuerHTTPRequest struct {
	Action    string          `json:"action"`
	Identity  issuerIdentity  `json:"identity"`
	Requested issuerRequested `json:"requested"`
}

type issuerIdentity struct {
	Role      string          `json:"role"`
	Sub       string          `json:"sub"`
	SessionID string          `json:"session_id,omitempty"`
	Claims    json.RawMessage `json:"claims"`
}

type issuerRequested struct {
	Schema  string   `json:"schema"`
	Table   string   `json:"table"`
	Filter  string   `json:"filter,omitempty"`
	Columns []string `json:"columns,omitempty"`
	Ops     []string `json:"ops,omitempty"`
}

type issuerHTTPResponse struct {
	Allow bool         `json:"allow"`
	Shape *issuerShape `json:"shape"`
	Holds []issuerHold `json:"holds"`
}

type issuerShape struct {
	Schema  string    `json:"schema"`
	Table   string    `json:"table"`
	Filter  string    `json:"filter"`
	Columns *[]string `json:"columns"`
}

type issuerHold struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
	Filter string `json:"filter"`
}

func (i *Issuer) decide(ctx context.Context, req Request) (*Grant, error) {
	body, err := i.roundTrip(ctx, req)
	if err != nil {
		return nil, err
	}
	if !body.Allow {
		return nil, &ErrDenied{Reason: "the shape issuer denied this subscription"}
	}
	if body.Shape == nil {
		return nil, &ErrDenied{Reason: "the shape issuer returned no authorized shape"}
	}
	wantSchema := req.Relation.Schema
	wantTable := req.Relation.Name
	gotSchema := cmpOr(body.Shape.Schema, "public")
	if !strings.EqualFold(gotSchema, wantSchema) || body.Shape.Table != wantTable {
		return nil, &ErrDenied{Reason: fmt.Sprintf(
			"the shape issuer authorized %s.%s, which is not the requested %s",
			gotSchema, body.Shape.Table, req.Relation.FullName())}
	}

	authorized, err := shape.Parse(body.Shape.Filter, req.Relation)
	if err != nil {
		return nil, &ErrDenied{Reason: "the shape issuer returned an invalid filter: " + err.Error()}
	}
	if !authorized.Concrete() {
		return nil, &ErrDenied{Reason: "the shape issuer returned a filter with no equality; a grant for the whole table is refused"}
	}

	effective, err := shape.Narrow(authorized, req.Filter, req.Relation)
	if err != nil {
		return nil, &ErrDenied{Reason: err.Error()}
	}

	requested := req.Columns
	if len(requested) == 0 {
		for _, c := range req.Relation.Columns {
			requested = append(requested, c.Name)
		}
	}
	var allowlist []string
	if body.Shape.Columns == nil {
		allowlist = requested
	} else {
		allowlist = *body.Shape.Columns
		if len(allowlist) == 0 {
			return nil, &ErrDenied{Reason: "the shape issuer returned an empty column allowlist"}
		}
	}
	intersected := intersect(requested, allowlist)
	if len(intersected) == 0 {
		return nil, &ErrDenied{Reason: "column intersection of the request and the issuer allowlist is empty"}
	}
	allowed, denied, err := i.grantColumns(ctx, req.Relation, intersected)
	if err != nil {
		return nil, err
	}
	if len(allowed) == 0 {
		return nil, &ErrDenied{Reason: "the Sluice role may not SELECT any of the authorized columns on " + req.Relation.FullName()}
	}

	if len(body.Holds) == 0 {
		return nil, &ErrDenied{Reason: "the shape issuer must name at least one hold"}
	}
	holds, err := i.parseHolds(body.Holds)
	if err != nil {
		return nil, err
	}

	reason := "authorized by the shape issuer"
	return &Grant{
		Decision: grantedDecision(reason),
		Filter:   effective,
		Columns:  allowed,
		Denied:   denied,
		Holds:    holds,
		Reason:   reason,
	}, nil
}

func (i *Issuer) grantColumns(ctx context.Context, rel *catalog.Relation, requested []string) ([]string, []string, error) {
	if i.columns != nil {
		return i.columns(ctx, rel, requested)
	}
	return requested, nil, nil
}

func (i *Issuer) parseHolds(raw []issuerHold) ([]hold.Spec, error) {
	if i.lookup == nil {
		return nil, &ErrDenied{Reason: "cannot resolve hold tables without a catalog"}
	}
	out := make([]hold.Spec, 0, len(raw))
	for _, h := range raw {
		schema := cmpOr(h.Schema, "public")
		rel, ok := i.lookup(schema, h.Table)
		if !ok {
			return nil, &ErrDenied{Reason: fmt.Sprintf(
				"hold table %s.%s is not in the publication; add it with ALTER PUBLICATION ... ADD TABLE %s.%s",
				schema, h.Table, schema, h.Table)}
		}
		f, err := shape.Parse(h.Filter, rel)
		if err != nil {
			return nil, &ErrDenied{Reason: "hold filter is invalid: " + err.Error()}
		}
		if !f.Concrete() {
			return nil, &ErrDenied{Reason: fmt.Sprintf("hold on %s must include at least one equality", rel.FullName())}
		}
		if missing := hold.MissingReplicaIdentity(rel, f); len(missing) > 0 {
			return nil, &ErrDenied{Reason: hold.ReplicaIdentityReason(rel, missing)}
		}
		out = append(out, hold.Spec{Rel: rel, Filter: f})
	}
	return out, nil
}

func (i *Issuer) roundTrip(ctx context.Context, req Request) (*issuerHTTPResponse, error) {
	claims := json.RawMessage([]byte("{}"))
	if req.Identity.ClaimsRaw != "" && json.Valid([]byte(req.Identity.ClaimsRaw)) {
		claims = json.RawMessage(req.Identity.ClaimsRaw)
	}
	filter := ""
	if req.Filter != nil {
		filter = req.Filter.Raw
		if filter == "" {
			filter = req.Filter.Describe()
			if filter == "(none)" {
				filter = ""
			}
		}
	}
	payload, err := json.Marshal(issuerHTTPRequest{
		Action: string(cmpOr(string(req.Action), string(ActionSubscribe))),
		Identity: issuerIdentity{
			Role:      req.Identity.Role,
			Sub:       req.Identity.Sub,
			SessionID: req.Identity.SessionID,
			Claims:    claims,
		},
		Requested: issuerRequested{
			Schema:  req.Relation.Schema,
			Table:   req.Relation.Name,
			Filter:  filter,
			Columns: req.Columns,
			Ops:     req.Ops,
		},
	})
	if err != nil {
		return nil, &ErrDenied{Reason: "could not encode the shape-issuer request"}
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, i.url, bytes.NewReader(payload))
	if err != nil {
		return nil, &ErrDenied{Reason: "invalid shape-issuer URL"}
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+i.bearer)

	resp, err := i.client.Do(httpReq)
	if err != nil {
		return nil, &ErrDenied{Reason: fmt.Sprintf("shape issuer unreachable: %v", err)}
	}
	defer resp.Body.Close()

	limited := io.LimitReader(resp.Body, 64<<10)
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, limited)
		return nil, &ErrDenied{Reason: fmt.Sprintf("shape issuer returned HTTP %d", resp.StatusCode)}
	}

	var out issuerHTTPResponse
	if err := json.NewDecoder(limited).Decode(&out); err != nil {
		return nil, &ErrDenied{Reason: "shape issuer returned an unreadable body"}
	}
	return &out, nil
}

func physicalColumns(ctx context.Context, cat *catalog.Cache, rel *catalog.Relation, requested []string) ([]string, []string, error) {
	granted, err := cat.HasColumnPrivilegeCurrent(ctx, rel.FullName(), requested)
	if err != nil {
		return nil, nil, err
	}
	var allowed, denied []string
	for _, c := range requested {
		if granted[c] {
			allowed = append(allowed, c)
		} else {
			denied = append(denied, c)
		}
	}
	return allowed, denied, nil
}

func intersect(requested, allowlist []string) []string {
	ok := map[string]bool{}
	for _, c := range allowlist {
		ok[c] = true
	}
	var out []string
	for _, c := range requested {
		if ok[c] {
			out = append(out, c)
		}
	}
	return out
}

func cmpOr(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
