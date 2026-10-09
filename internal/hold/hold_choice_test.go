package hold

import (
	"fmt"
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/expr"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

func roomMembersRel() *catalog.Relation {
	return &catalog.Relation{
		OID: 300, Schema: "public", Name: "room_members",
		ReplicaIdentity: 'f',
		Columns: []catalog.Column{
			{Name: "room_id", TypeName: "bigint"},
			{Name: "user_id", TypeName: "text"},
			{Name: "last_message_at", TypeName: "timestamptz"},
		},
		IndexedColumns: map[string]bool{"room_id": true, "user_id": true},
	}
}

func specOf(tb testing.TB, rel *catalog.Relation, filter string) Spec {
	tb.Helper()
	f, err := shape.Parse(filter, rel)
	if err != nil {
		tb.Fatal(err)
	}
	return Spec{Rel: rel, Filter: f}
}

func memberOf(room int, user string) string { return fmt.Sprintf("u%s-r%d", user, room) }

// membershipHolds indexes holds the way an issuer of rooms grants them: each
// of a room's streams holds its shapes on its own membership row.
func membershipHolds(tb testing.TB, x *Index, rel *catalog.Relation, rooms, streams, shapes int, userFirst bool) {
	tb.Helper()
	for r := range rooms {
		for m := range streams {
			user := memberOf(r, fmt.Sprint(m))
			f := fmt.Sprintf("room_id=eq.%d,user_id=eq.%s", r, user)
			if userFirst {
				f = fmt.Sprintf("user_id=eq.%s,room_id=eq.%d", user, r)
			}
			spec := specOf(tb, rel, f)
			for s := range shapes {
				if !x.Add(user, fmt.Sprintf("s%d", s), []Spec{spec}) {
					tb.Fatal("add")
				}
			}
		}
	}
}

// listedUnder counts the holds of rel listed under each column.
func listedUnder(x *Index, rel *catalog.Relation) map[string]int {
	x.mu.RLock()
	defer x.mu.RUnlock()
	out := map[string]int{}
	for col, byConst := range x.rels[rel.OID].byColumn {
		for _, list := range byConst {
			out[col] += len(list)
		}
	}
	return out
}

// Whatever order the issuer writes a membership hold in, it is listed under
// the member, the column with the shortest lists: a change to one member's row
// then looks through that member's holds, not every hold in the room.
func TestHoldsAreListedUnderTheMostSelectiveColumn(t *testing.T) {
	rel := roomMembersRel()
	for _, userFirst := range []bool{false, true} {
		x := New()
		membershipHolds(t, x, rel, 20, 25, 7, userFirst)
		got := listedUnder(x, rel)
		// The first holds of the first room cannot tell the columns apart yet;
		// after that every hold goes under user_id.
		if got["user_id"] < 20*25*7-25*7 {
			t.Fatalf("user first %v: listed %v, want nearly all under user_id", userFirst, got)
		}
		checkConsistent(t, x)
	}
}

// The counts follow holds out: after every stream leaves, the index keeps
// nothing, counts included.
func TestColumnCountsLeaveWithTheirHolds(t *testing.T) {
	rel := roomMembersRel()
	x := New()
	membershipHolds(t, x, rel, 5, 10, 3, false)
	for r := range 5 {
		for m := range 10 {
			x.RemoveStream(memberOf(r, fmt.Sprint(m)))
		}
	}
	if s := x.Size(); s != (Size{}) {
		t.Fatalf("index not empty after every stream left: %+v", s)
	}
}

// One member's row changing at the unicorn's scale: 400 rooms of 28 streams,
// 7 holds each on the member's row; a member UPDATE with FULL's old tuple.
func BenchmarkMemberUpdate(b *testing.B) {
	rel := roomMembersRel()
	for _, order := range []struct {
		name      string
		userFirst bool
	}{{"room-first", false}, {"user-first", true}} {
		b.Run(order.name, func(b *testing.B) {
			x := New()
			membershipHolds(b, x, rel, 400, 28, 7, order.userFirst)
			rows := make([]row, 0, 400*28)
			for r := range 400 {
				for m := range 28 {
					rows = append(rows, row{
						"room_id": expr.Text(fmt.Sprint(r)), "user_id": expr.Text(memberOf(r, fmt.Sprint(m))),
						"last_message_at": expr.Text("2026-10-10 00:00:00+00"),
					})
				}
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				r := rows[i%len(rows)]
				if cuts := x.OnChange(rel.OID, 'U', r, r); len(cuts) != 0 {
					b.Fatal("an unchanged membership must not cut")
				}
			}
		})
	}
}
