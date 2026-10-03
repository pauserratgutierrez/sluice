package catalog

import (
	"context"
	"slices"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// privilegeRows answers has_column_privilege with one (column, granted) row each.
type privilegeRows struct {
	cols    []string
	granted map[string]bool
	i       int
}

func (r *privilegeRows) Close()                                       {}
func (r *privilegeRows) Err() error                                   { return nil }
func (r *privilegeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *privilegeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *privilegeRows) Values() ([]any, error)                       { return nil, nil }
func (r *privilegeRows) RawValues() [][]byte                          { return nil }
func (r *privilegeRows) Conn() *pgx.Conn                              { return nil }
func (r *privilegeRows) Next() bool                                   { r.i++; return r.i <= len(r.cols) }
func (r *privilegeRows) Scan(dest ...any) error {
	col := r.cols[r.i-1]
	*dest[0].(*string) = col
	*dest[1].(*bool) = r.granted[col]
	return nil
}

// A reconnect wave resubscribes the same shapes, so each column's privilege is
// asked once per refresh, not once per subscription.
func TestColumnPrivilegesAreCachedUntilRefresh(t *testing.T) {
	c := New(nil)
	rel := &Relation{OID: 7, Schema: "public", Name: "posts"}
	granted := map[string]bool{"id": true, "title": true}
	var asked [][]string
	query := func(_ context.Context, missing []string) (pgx.Rows, error) {
		asked = append(asked, slices.Clone(missing))
		return &privilegeRows{cols: missing, granted: granted}, nil
	}
	check := func(cols ...string) map[string]bool {
		t.Helper()
		got, err := c.columnPrivileges(context.Background(), "", rel, cols, query)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}

	if got := check("id", "secret"); !got["id"] || got["secret"] {
		t.Fatalf("privileges = %v", got)
	}
	check("id", "secret")
	if got := check("id", "title"); !got["title"] {
		t.Fatalf("privileges = %v", got)
	}
	if want := [][]string{{"id", "secret"}, {"title"}}; !slices.EqualFunc(asked, want, slices.Equal) {
		t.Fatalf("queried %v, want only the columns not yet cached: %v", asked, want)
	}

	granted["secret"] = true
	c.forgetPrivileges()
	if got := check("secret"); !got["secret"] {
		t.Fatal("a GRANT is not seen after a refresh")
	}
}
