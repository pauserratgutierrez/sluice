package hold

import (
	"slices"
	"strings"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
)

// With REPLICA IDENTITY DEFAULT or USING INDEX, an UPDATE that leaves the
// identity alone carries no old tuple. A hold reads only identity columns, so
// such an UPDATE cannot break one; before, it was decided from the new row
// alone and cut every candidate the row did not match, other rows' holds
// included (a watch is a candidate when one routed column matches).
func TestHoldsUnderKeyReplicaIdentity(t *testing.T) {
	byDefault := membersRel()
	byDefault.ReplicaIdentity = 'd' // the primary key (project_id, user_id)
	for _, rel := range []*catalog.Relation{byDefault, membersRel()} {
		t.Run("replica identity "+string(rel.ReplicaIdentity), func(t *testing.T) {
			holds := map[string]string{
				"s1": "user_id=eq.u1,project_id=eq.42",
				"s2": "user_id=eq.u2,project_id=eq.42",
				"s3": "project_id=eq.43,user_id=eq.u1",
				"s4": "project_id=eq.42",
			}
			index := func(t *testing.T) *Index {
				x := New()
				for s, f := range holds {
					if !x.Add(s, "docs", []Spec{holdSpec(t, rel, f)}) {
						t.Fatal("add")
					}
				}
				return x
			}
			member := func(project, user, role string) row {
				return row{"project_id": expr.Text(project), "user_id": expr.Text(user), "role": expr.Text(role)}
			}
			key := func(project, user string) row {
				return row{"project_id": expr.Text(project), "user_id": expr.Text(user)}
			}
			cutStreams := func(cuts []Cut) []string {
				var out []string
				for _, c := range cuts {
					out = append(out, c.StreamID)
				}
				slices.Sort(out)
				return out
			}

			t.Run("UPDATE of a column outside the key cuts nothing", func(t *testing.T) {
				x := index(t)
				for _, r := range []row{member("42", "u2", "editor"), member("42", "u1", "editor"), member("43", "u1", "viewer")} {
					if cuts := x.OnChange(rel.OID, 'U', nil, r); len(cuts) != 0 {
						t.Fatalf("UPDATE of %v cut %v", r, cutStreams(cuts))
					}
				}
				if x.Count() != len(holds) {
					t.Fatalf("%d watches left, want %d", x.Count(), len(holds))
				}
				checkConsistent(t, x)
			})

			t.Run("UPDATE of the key cuts the holds it leaves", func(t *testing.T) {
				x := index(t)
				// u1 leaves project 42 for 44: the 'K' old tuple is the old key.
				cuts := x.OnChange(rel.OID, 'U', key("42", "u1"), member("44", "u1", "editor"))
				if got := cutStreams(cuts); !slices.Equal(got, []string{"s1", "s4"}) {
					t.Fatalf("cut %v, want [s1 s4]", got)
				}
				// u2 becomes u3 in project 42: project 42's hold still matches.
				x = index(t)
				cuts = x.OnChange(rel.OID, 'U', key("42", "u2"), member("42", "u3", "editor"))
				if got := cutStreams(cuts); !slices.Equal(got, []string{"s2"}) {
					t.Fatalf("cut %v, want [s2]", got)
				}
				checkConsistent(t, x)
			})

			t.Run("DELETE cuts the holds on the row", func(t *testing.T) {
				x := index(t)
				cuts := x.OnChange(rel.OID, 'D', key("42", "u2"), nil)
				if got := cutStreams(cuts); !slices.Equal(got, []string{"s2", "s4"}) {
					t.Fatalf("cut %v, want [s2 s4]", got)
				}
				checkConsistent(t, x)
			})
		})
	}
}

// A relation whose identity no longer covers a hold's columns cuts that hold
// at once, so OnChange's promise holds between catalog refreshes.
func TestOutsideIdentityCutsHoldsTheIdentityNoLongerCovers(t *testing.T) {
	rel := membersRel()
	rel.ReplicaIdentity = 'f'
	x := New()
	x.Add("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.42")})
	x.Add("s2", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u2,role=eq.admin")})
	x.Add("s3", "docs", []Spec{holdSpec(t, rel, "project_id=eq.42")})

	full := *rel
	if cuts := x.OutsideIdentity(&full); len(cuts) != 0 {
		t.Fatalf("FULL covers every column; cut %+v", cuts)
	}
	byKey := *rel
	byKey.ReplicaIdentity = 'd'
	byKey.ReplicaIdentityColumns = []string{"project_id", "user_id"}
	cuts := x.OutsideIdentity(&byKey)
	if len(cuts) != 1 || cuts[0].StreamID != "s2" {
		t.Fatalf("cut %+v, want s2 (role is outside the key)", cuts)
	}
	if x.Count() != 2 {
		t.Fatalf("%d watches left, want 2", x.Count())
	}
	checkConsistent(t, x)
	nothing := *rel
	nothing.ReplicaIdentity = 'n'
	nothing.ReplicaIdentityColumns = nil
	if cuts := x.OutsideIdentity(&nothing); len(cuts) != 2 {
		t.Fatalf("REPLICA IDENTITY NOTHING covers no column; cut %+v", cuts)
	}
}

// A hold granted against a catalog that has not caught up with a replica
// identity change the WAL already carried is refused once it is in the index.
func TestCheckIdentityRefusesHoldsTheWALIdentityDoesNotCover(t *testing.T) {
	rel := membersRel()
	rel.ReplicaIdentity = 'f' // the catalog still says FULL
	x := New()
	byKey := *rel
	byKey.ReplicaIdentity = 'd'
	byKey.ReplicaIdentityColumns = []string{"project_id", "user_id"}
	x.OutsideIdentity(&byKey) // the WAL has moved to the primary key

	x.Add("s1", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u1,project_id=eq.42")})
	if err := x.CheckIdentity("s1", "docs"); err != nil {
		t.Fatalf("a hold on key columns is covered: %v", err)
	}
	x.Add("s2", "docs", []Spec{holdSpec(t, rel, "user_id=eq.u2,role=eq.admin")})
	if err := x.CheckIdentity("s2", "docs"); err == nil || !strings.Contains(err.Error(), "role") {
		t.Fatalf("a hold reading role must be refused, got %v", err)
	}
	if x.Count() != 1 {
		t.Fatalf("%d watches left, want 1", x.Count())
	}
	if err := x.CheckIdentity("s3", "docs"); err == nil {
		t.Fatal("a watch that is gone must fail the check")
	}
	checkConsistent(t, x)
}
