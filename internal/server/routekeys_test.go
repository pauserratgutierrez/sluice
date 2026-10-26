package server

import (
	"testing"

	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

// The routing keys a buffered change carries only spare a replay from reading
// its tuples: with or without them, routedTo keeps the same changes. Among
// them a row that left the shape, which only its old tuple shows, and a row
// whose routing column the change does not carry.
func TestBufferedRoutingKeysNeverChangeAReplaysAnswer(t *testing.T) {
	rel := walRelation(t, 301, "public", "room_members",
		walColumn{1, "room_id", 20}, walColumn{1, "user_id", 2950}, walColumn{1, "last_message_at", 1184})
	cat := &catalog.Relation{OID: 301, Schema: "public", Name: "room_members", ReplicaIdentity: 'f',
		ReplicaIdentityColumns: []string{"room_id", "user_id", "last_message_at"}, KeyColumns: []string{"room_id", "user_id"},
		Columns: []catalog.Column{{Name: "room_id", TypeName: "bigint", TypeOID: 20},
			{Name: "user_id", TypeName: "uuid", TypeOID: 2950}, {Name: "last_message_at", TypeName: "timestamptz", TypeOID: 1184}},
		IndexedColumns: map[string]bool{"room_id": true, "user_id": true}}
	me, other := uid(1), uid(2)
	f, err := shape.Parse("user_id=eq."+me, cat)
	if err != nil {
		t.Fatal(err)
	}
	sub := &registry.Subscription{Label: "me", Relation: cat, Filter: f, RoutingKey: f.RoutingKey(cat)}
	at := "2026-10-26 10:00:00+00"
	unchanged := &pgoutput.Tuple{Columns: []pgoutput.Column{
		{Kind: pgoutput.ColText, Data: []byte("1")}, {Kind: pgoutput.ColUnchanged}, {Kind: pgoutput.ColText, Data: []byte(at)}}}

	for _, c := range []struct {
		name     string
		old, new *pgoutput.Tuple
		want     bool
	}{
		{"its row", text("1", me, at), text("1", me, at), true},
		{"another row", text("1", other, at), text("1", other, at), false},
		{"its row leaving", text("1", me, at), text("1", other, at), true},
		{"a row entering", text("1", other, at), text("1", me, at), true},
		{"an insert of its row", nil, text("1", me, at), true},
		{"a delete of another row", text("1", other, at), nil, false},
		{"no routing column", nil, unchanged, true},
	} {
		for _, cols := range [][]string{nil, {"user_id"}, {"room_id", "user_id"}, {"room_id", "last_message_at", "user_id"}} {
			e := hub.RingEntry{Op: pgoutput.MsgUpdate, Relation: rel, New: c.new, Old: c.old}
			e.Keys = routeKeys(rel, cols, tupleRow{rel, c.new}, tupleRow{rel, c.old})
			if got := routedTo(sub)(&e); got != c.want {
				t.Errorf("%s, keys of %v: kept %v, want %v", c.name, cols, got, c.want)
			}
		}
	}
}
