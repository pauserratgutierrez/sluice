package server

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"testing"
	"time"

	"github.com/pauserratgutierrez/sluice/internal/authz"
	"github.com/pauserratgutierrez/sluice/internal/catalog"
	"github.com/pauserratgutierrez/sluice/internal/config"
	"github.com/pauserratgutierrez/sluice/internal/hold"
	"github.com/pauserratgutierrez/sluice/internal/hub"
	"github.com/pauserratgutierrez/sluice/internal/oracle"
	"github.com/pauserratgutierrez/sluice/internal/pgoutput"
	"github.com/pauserratgutierrez/sluice/internal/registry"
	"github.com/pauserratgutierrez/sluice/internal/shape"
)

type roomsBench struct {
	s                *Server
	msgs, mem, notif *catalog.Relation
	walMsgs, walMem  *pgoutput.Relation
	rooms            []benchRoom
}

type benchRoom struct {
	id      int64
	members []string // every member's user id
	streams []*hub.Stream
}

func uid(i int) string { return fmt.Sprintf("00000000-0000-4000-8000-%012d", i) }

// newRoomsBench builds the rooms workload's registry and hold index: per room,
// `members` members of whom `online` hold a stream with 7 shapes, each held by
// the member's room_members row.
func newRoomsBench(tb testing.TB, classes [][3]int, ringBytes int) *roomsBench {
	cfg := &config.Config{
		PathPrefix: "/sluice/v1", ShapeOracle: oracle.NameIssuer, Heartbeat: 20 * time.Second,
		UnindexedMax: 200, ReplicaIdentity: "warn", MaxChangeBytes: 1 << 20,
	}
	h := hub.New(256, ringBytes, time.Minute, time.Second)
	s := New(context.Background(), Options{Config: cfg, Logger: slog.New(slog.DiscardHandler),
		Catalog: catalog.New(nil), Oracle: stubOracle{name: oracle.NameIssuer}, Hub: h})
	rb := &roomsBench{s: s}
	rb.msgs = &catalog.Relation{OID: 300, Schema: "public", Name: "room_messages", ReplicaIdentity: 'd',
		ReplicaIdentityColumns: []string{"id"}, KeyColumns: []string{"id"}, ReplicaIdentityOK: true,
		Columns: []catalog.Column{{Name: "id", TypeName: "bigint", TypeOID: 20}, {Name: "room_id", TypeName: "bigint", TypeOID: 20},
			{Name: "seq", TypeName: "bigint", TypeOID: 20}, {Name: "author_id", TypeName: "uuid", TypeOID: 2950},
			{Name: "body", TypeName: "text", TypeOID: 25}, {Name: "created_at", TypeName: "timestamptz", TypeOID: 1184}},
		IndexedColumns: map[string]bool{"id": true, "room_id": true}}
	rb.mem = &catalog.Relation{OID: 301, Schema: "public", Name: "room_members", ReplicaIdentity: 'f',
		ReplicaIdentityColumns: []string{"room_id", "user_id", "last_message_at"}, KeyColumns: []string{"room_id", "user_id"}, ReplicaIdentityOK: true,
		Columns: []catalog.Column{{Name: "room_id", TypeName: "bigint", TypeOID: 20}, {Name: "user_id", TypeName: "uuid", TypeOID: 2950},
			{Name: "last_message_at", TypeName: "timestamptz", TypeOID: 1184}},
		IndexedColumns: map[string]bool{"room_id": true, "user_id": true}}
	rb.notif = &catalog.Relation{OID: 302, Schema: "public", Name: "notifications", ReplicaIdentity: 'd',
		ReplicaIdentityColumns: []string{"id"}, KeyColumns: []string{"id"}, ReplicaIdentityOK: true,
		Columns: []catalog.Column{{Name: "id", TypeName: "bigint", TypeOID: 20}, {Name: "user_id", TypeName: "uuid", TypeOID: 2950},
			{Name: "kind", TypeName: "text", TypeOID: 25}, {Name: "body", TypeName: "text", TypeOID: 25},
			{Name: "created_at", TypeName: "timestamptz", TypeOID: 1184}},
		IndexedColumns: map[string]bool{"id": true, "user_id": true}}
	rb.walMsgs = walRelation(tb, 300, "public", "room_messages",
		walColumn{1, "id", 20}, walColumn{0, "room_id", 20}, walColumn{0, "seq", 20},
		walColumn{0, "author_id", 2950}, walColumn{0, "body", 25}, walColumn{0, "created_at", 1184})
	rb.walMem = walRelation(tb, 301, "public", "room_members",
		walColumn{1, "room_id", 20}, walColumn{1, "user_id", 2950}, walColumn{1, "last_message_at", 1184})

	cols := func(r *catalog.Relation) []string {
		var out []string
		for _, c := range r.Columns {
			out = append(out, c.Name)
		}
		return out
	}
	mustParse := func(f string, r *catalog.Relation) *shape.Filter {
		p, err := shape.Parse(f, r)
		if err != nil {
			tb.Fatal(err)
		}
		return p
	}
	allOps, _ := shape.ParseOps(nil)
	ins, _ := shape.ParseOps([]string{"INSERT"})
	del, _ := shape.ParseOps([]string{"DELETE"})
	user, room := 0, int64(0)
	for _, c := range classes {
		count, members, online := c[0], c[1], c[2]
		for range count {
			room++
			br := benchRoom{id: room}
			for m := range members {
				user++
				u := uid(user)
				br.members = append(br.members, u)
				if m >= online {
					continue
				}
				st := h.Open(fmt.Sprintf("n1.%d", user), "", authz.Identity{Sub: u, Role: "authenticated"})
				br.streams = append(br.streams, st)
				holdF := mustParse("user_id=eq."+u+",room_id=eq."+strconv.FormatInt(room, 10), rb.mem)
				add := func(label string, rel *catalog.Relation, filter string, ops shape.Ops) {
					f := mustParse(filter, rel)
					if !s.holds.Add(st.StreamID(), label, []hold.Spec{{Rel: rb.mem, Filter: holdF}}) {
						tb.Fatal("hold add")
					}
					if !s.reg.Add(&registry.Subscription{Label: label, Sink: st, Relation: rel, Ops: ops, Filter: f,
						Columns: cols(rel), Decision: authz.NewHandle(&authz.Decision{Tier: authz.TierA, Granted: true}),
						RoutingKey: f.RoutingKey(rel)}) {
						tb.Fatal("reg add")
					}
				}
				add("msgs", rb.msgs, "room_id=eq."+strconv.FormatInt(room, 10), allOps)
				add("me", rb.mem, "user_id=eq."+u, allOps)
				add("nt", rb.notif, "user_id=eq."+u, ins)
				for i := 3; i < 7; i++ {
					add(fmt.Sprintf("f%d", i), rb.mem, "user_id=eq."+u, del)
				}
			}
			rb.rooms = append(rb.rooms, br)
		}
	}
	return rb
}

func text(vals ...string) *pgoutput.Tuple {
	t := &pgoutput.Tuple{Columns: make([]pgoutput.Column, len(vals))}
	for i, v := range vals {
		t.Columns[i] = pgoutput.Column{Kind: pgoutput.ColText, Data: []byte(v)}
	}
	return t
}

var benchBody = "The quick brown fox jumps over the lazy dog while the room keeps chatting about the release plan."

// message dispatches one message the way the rooms workload writes it: one
// INSERT into room_messages and one UPDATE of every member's last_message_at,
// in one transaction. It returns the changes dispatched.
func (rb *roomsBench) message(r *benchRoom, n int, lsn uint64) int {
	s := rb.s
	now := time.Now()
	at := now.UTC().Format("2006-01-02 15:04:05.999999+00")
	prev := now.Add(-15 * time.Second).UTC().Format("2006-01-02 15:04:05.999999+00")
	s.OnBegin(lsn, now, uint32(n))
	room := strconv.FormatInt(r.id, 10)
	_ = s.OnChange(&pgoutput.Message{Type: pgoutput.MsgInsert, RelationOID: 300,
		New: text(strconv.Itoa(n), room, strconv.Itoa(n), r.members[0], benchBody, at)}, rb.walMsgs, lsn, now)
	for _, u := range r.members {
		_ = s.OnChange(&pgoutput.Message{Type: pgoutput.MsgUpdate, RelationOID: 301,
			Old: text(room, u, prev), New: text(room, u, at)}, rb.walMem, lsn, now)
	}
	s.OnCommit(lsn, now)
	for _, st := range r.streams {
		st.Take(nil)
	}
	return 1 + len(r.members)
}

var unicorn = [][3]int{{380, 80, 28}, {16, 250, 85}}

// BenchmarkRoomMessage is one message of the unicorn's mix (12,000 streams of
// 7 shapes), on the reader: ns/op is per message; changes/s is the reader's
// ceiling for this mix on one core.
func BenchmarkRoomMessage(b *testing.B) {
	rb := newRoomsBench(b, unicorn, 64<<20)
	b.ReportAllocs()
	changes := 0
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r := &rb.rooms[i%len(rb.rooms)]
		changes += rb.message(r, i, uint64(0x1000+i))
	}
	b.ReportMetric(float64(changes)/b.Elapsed().Seconds(), "changes/s")
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(changes), "ns/change")
}

// BenchmarkReplay is one resumed subscription replaying a busy room_members
// buffer: the last `since` changes are at or after its position, and 1 in 400
// of them is its user's.
func BenchmarkReplay(b *testing.B) {
	rb := newRoomsBench(b, nil, 1<<30)
	at := "2026-10-26 10:00:00.123456+00"
	for _, c := range []struct{ size, since int }{{4096, 4096}, {131072, 1024}, {131072, 131072}} {
		b.Run(fmt.Sprintf("ring=%d/since=%d", c.size, c.since), func(b *testing.B) {
			rings := hub.NewRings(1<<40, time.Hour)
			rings.SetStart(1)
			me := uid(1)
			for i := range c.size {
				u := uid(2 + i%12000)
				if i%400 == 0 {
					u = me
				}
				e := hub.RingEntry{LSN: uint64(0x1000 + i), Op: pgoutput.MsgUpdate, Relation: rb.walMem,
					Old: text("1", u, at), New: text("1", u, at)}
				e.Keys = routeKeys(rb.walMem, []string{"user_id"}, tupleRow{rb.walMem, e.New}, tupleRow{rb.walMem, e.Old})
				rings.Append(e)
			}
			f, err := shape.Parse("user_id=eq."+me, rb.mem)
			if err != nil {
				b.Fatal(err)
			}
			match := routedTo(&registry.Subscription{Label: "me", Relation: rb.mem, Filter: f, RoutingKey: f.RoutingKey(rb.mem)})
			from := uint64(0x1000 + c.size - c.since)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if entries, _, ok := rings.ReplayThrough(301, from, match); !ok || len(entries) == 0 {
					b.Fatal("not covered")
				}
			}
		})
	}
}
