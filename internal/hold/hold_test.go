package hold

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

type row map[string]expr.Value

func (r row) Column(name string) (expr.Value, bool) {
	v, ok := r[name]
	return v, ok
}

func membersRel() *catalog.Relation {
	return &catalog.Relation{
		OID: 200, Schema: "public", Name: "project_members",
		ReplicaIdentity:        'i',
		ReplicaIdentityColumns: []string{"project_id", "user_id"},
		Columns: []catalog.Column{
			{Name: "project_id", TypeName: "text"},
			{Name: "user_id", TypeName: "text"},
			{Name: "role", TypeName: "text"},
		},
		IndexedColumns: map[string]bool{"project_id": true, "user_id": true},
	}
}

func holdSpec(t *testing.T, rel *catalog.Relation, filter string) Spec {
	t.Helper()
	f, err := shape.Parse(filter, rel)
	if err != nil {
		t.Fatal(err)
	}
	return Spec{Rel: rel, Filter: f}
}

func TestExistsSQLReadsOnlyTheHoldTable(t *testing.T) {
	rel := membersRel()
	spec := holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")
	sql, args := ExistsSQL(spec)
	if !strings.Contains(sql, `"project_members"`) {
		t.Fatalf("EXISTS must read the hold table: %s", sql)
	}
	if strings.Contains(sql, "documents") {
		t.Fatalf("EXISTS must never read the subscribed table: %s", sql)
	}
	if len(args) != 2 {
		t.Fatalf("args = %v, want the two equalities as parameters", args)
	}
}

func TestOnChangeCutsDeleteOfHold(t *testing.T) {
	idx := New()
	rel := membersRel()
	if !idx.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")}) {
		t.Fatal("add")
	}
	cuts := idx.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"),
		"user_id":    expr.Text("u1"),
	}, nil)
	if len(cuts) != 1 || cuts[0].Label != "docs" {
		t.Fatalf("cuts = %+v, want the docs shape", cuts)
	}
	if idx.Count() != 0 {
		t.Fatal("the watch must be removed so a later change cannot recut")
	}
}

func TestOnChangeCutsUpdateLeavingHold(t *testing.T) {
	idx := New()
	rel := membersRel()
	idx.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
	cuts := idx.OnChange(rel.OID, 'U',
		row{"project_id": expr.Text("42"), "user_id": expr.Text("u1")},
		row{"project_id": expr.Text("99"), "user_id": expr.Text("u1")},
	)
	if len(cuts) != 1 {
		t.Fatalf("update leaving the hold must cut, got %+v", cuts)
	}
}

func TestOnChangeKeepsUpdateStayingInHold(t *testing.T) {
	idx := New()
	rel := membersRel()
	idx.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
	cuts := idx.OnChange(rel.OID, 'U',
		row{"project_id": expr.Text("42"), "user_id": expr.Text("u1"), "role": expr.Text("viewer")},
		row{"project_id": expr.Text("42"), "user_id": expr.Text("u1"), "role": expr.Text("admin")},
	)
	if len(cuts) != 0 {
		t.Fatalf("an update that stays in the hold must not cut: %+v", cuts)
	}
}

func TestOnChangeIgnoresInsert(t *testing.T) {
	idx := New()
	rel := membersRel()
	idx.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")})
	cuts := idx.OnChange(rel.OID, 'I', nil, row{
		"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
	})
	if len(cuts) != 0 {
		t.Fatalf("insert is not a kick: %+v", cuts)
	}
}

func TestKickVsJoinRegisterThenDeleteCuts(t *testing.T) {
	// Subscribe registers the watch first, then EXISTS. A DELETE already in the
	// reader is applied by the watch, so the grant cannot survive a kick that
	// raced the join.
	idx := New()
	rel := membersRel()
	if !idx.Add("s1", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")}) {
		t.Fatal("add")
	}
	cuts := idx.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
	}, nil)
	if len(cuts) != 1 {
		t.Fatal("a kick that lands after register and before EXISTS must cut")
	}
}

func TestRefreshRelsMissingStreamNoPanic(t *testing.T) {
	idx := New()
	idx.RefreshRels("gone", "docs", []*catalog.Relation{membersRel()})
}

func TestReplaceThenDeleteCuts(t *testing.T) {
	idx := New()
	rel := membersRel()
	spec := holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")
	if !idx.Add("s1", "docs", []Spec{spec}) {
		t.Fatal("add")
	}
	idx.Replace("s1", "docs", []Spec{spec})
	cuts := idx.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
	}, nil)
	if len(cuts) != 1 {
		t.Fatal("a watch left by Replace must still cut on DELETE")
	}
}

func TestReplaceRaceStillIndexed(t *testing.T) {
	idx := New()
	rel := membersRel()
	spec := holdSpec(t, rel, "project_id=eq.42,user_id=eq.u1")
	if !idx.Add("s1", "docs", []Spec{spec}) {
		t.Fatal("add")
	}
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			idx.Replace("s1", "docs", []Spec{spec})
		}()
		go func() {
			defer wg.Done()
			idx.OnChange(rel.OID, 'D', row{
				"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
			}, nil)
		}()
	}
	wg.Wait()
	if idx.Count() == 0 {
		return
	}
	cuts := idx.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
	}, nil)
	if len(cuts) == 0 {
		t.Fatal("a watch left after concurrent Replace must still be indexed; DELETE did not cut")
	}
}

func TestIndexIsConstantNotLinear(t *testing.T) {
	idx := New()
	rel := membersRel()
	for i := 0; i < 200; i++ {
		label := fmt.Sprintf("s%d", i)
		filter := fmt.Sprintf("project_id=eq.%d,user_id=eq.u1", i)
		if !idx.Add("st", label, []Spec{holdSpec(t, rel, filter)}) {
			t.Fatal("add")
		}
	}
	cuts := idx.OnChange(rel.OID, 'D', row{
		"project_id": expr.Text("42"), "user_id": expr.Text("u1"),
	}, nil)
	if len(cuts) != 1 || cuts[0].Label != "s42" {
		t.Fatalf("only the matching hold should cut, got %+v", cuts)
	}
}

func TestMissingReplicaIdentity(t *testing.T) {
	rel := membersRel()
	rel.ReplicaIdentity = 'd'
	rel.ReplicaIdentityColumns = []string{"id"}
	rel.Columns = append(rel.Columns, catalog.Column{Name: "id", TypeName: "bigint"})
	f, _ := shape.Parse("project_id=eq.42,user_id=eq.u1", rel)
	got := MissingReplicaIdentity(rel, f)
	if len(got) != 2 {
		t.Fatalf("missing = %v, want project_id and user_id", got)
	}
	reason := ReplicaIdentityReason(rel, got)
	if !strings.Contains(reason, "REPLICA IDENTITY") || !strings.Contains(reason, "project_id") {
		t.Fatalf("reason = %q", reason)
	}
}
