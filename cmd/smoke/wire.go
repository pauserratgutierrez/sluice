package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"
)

// wireInsert writes one row of public.wire_types covering every kind of value
// Sluice encodes: scalars, NaN, time zones other than UTC, json and jsonb,
// domains, an enum, a composite, one- and two-dimensional arrays, a bounded
// array and a box[] (whose elements use ';').
const wireInsert = `
insert into wire_types (owner_id, b, i2, i4, i8, f4, f8, f8_nan, n, n_nan, t, vc, c, u, o,
  j, jb, d, ts, tstz, tm, tmtz, iv, bin, m, ip, pt, mood, pos, pair,
  t_arr, i_arr, i_arr2, b_arr, tstz_arr, jb_arr, n_arr, pos_arr, pair_arr, box_arr, bounded)
values ('%s', true, 1, 2, 9007199254740993, 1.5, 0.1, 'NaN', 12345678901234567.89, 'NaN',
  'hé "q" \ z', 'v', 'ab', gen_random_uuid(), 42,
  '{"b":1, "a":[1,2]}', '{"x":{"y":null}}', '2026-09-29', '2026-09-29 20:24:42.5',
  '2026-09-29 22:24:42.39579+02', '20:24:42', '20:24:42+02', '1 day 02:00',
  '\xdead', 12.5, '10.0.0.1', '(1,2)', 'ok', 5, ROW(1, 'a "b" \ c', '2026-09-29 22:00+02'),
  '{a,"b c",NULL,"NULL"}', '{1,2}', '{{1,2},{3,4}}', '{t,f}', ARRAY['2026-09-29 22:00+02'::timestamptz],
  ARRAY['{"x":1}'::jsonb], '{1.5,NULL}', '{5,6}', ARRAY[ROW(2, 'z', NULL)::wire_pair],
  ARRAY['(1,1),(0,0)'::box, '(2,2),(1,1)'::box], '[0:1]={7,8}')
returning id`

// phaseWireEncoding checks that a snapshot row and a live change are both
// encoded exactly as to_jsonb encodes the same row: the one encoding clients
// and generated Database types expect.
func phaseWireEncoding(ctx context.Context, tok, userID string) {
	fmt.Println("\n-- wire encoding matches to_jsonb --")
	snapID, err := insertReturningID(ctx, fmt.Sprintf(wireInsert, userID))
	must(err, "insert the snapshot row")

	st, err := openStream(ctx, tok, fmt.Sprintf(
		`{"subscriptions":[{"sub":"wire","shape":{"table":"wire_types","filter":"owner_id=eq.%s","initial":"snapshot"}}]}`, userID))
	must(err, "open the wire stream")
	defer st.Close()

	records := map[int64]json.RawMessage{}
	snapshotOf := map[int64]bool{}
	var seen []string
	// until reads events until done reports true or 10s pass.
	until := func(done func(name string, id int64) bool) {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			e, err := st.next(time.Until(deadline))
			if err != nil {
				return
			}
			var c struct {
				Sub      string          `json:"sub"`
				Snapshot bool            `json:"snapshot"`
				Record   json.RawMessage `json:"record"`
			}
			_ = json.Unmarshal(e.Data, &c)
			seen = append(seen, e.Name)
			var id struct{ ID int64 }
			if e.Name == "change" && c.Sub == "wire" && json.Unmarshal(c.Record, &id) == nil {
				records[id.ID] = c.Record
				snapshotOf[id.ID] = c.Snapshot
			}
			if done(e.Name, id.ID) {
				return
			}
		}
	}
	until(func(name string, _ int64) bool { return name == "snapshot_end" })
	liveID, err := insertReturningID(ctx, fmt.Sprintf(wireInsert, userID))
	must(err, "insert the live row")
	until(func(name string, id int64) bool { return name == "change" && id == liveID })

	for _, tc := range []struct {
		id       int64
		snapshot bool
		what     string
	}{
		{snapID, true, "a snapshot row is encoded exactly as to_jsonb encodes it"},
		{liveID, false, "a live change is encoded exactly as to_jsonb encodes it"},
	} {
		got, ok := records[tc.id]
		if !ok || snapshotOf[tc.id] != tc.snapshot {
			check(false, tc.what, fmt.Sprintf("row %d did not arrive with snapshot=%v; events seen: %v", tc.id, tc.snapshot, seen))
			continue
		}
		want, err := toJSONB(ctx, "wire_types", tc.id)
		must(err, "read to_jsonb of the row")
		diff := jsonDiff(decode(got), decode(want))
		check(len(diff) == 0, tc.what, strings.Join(diff, "; "))
	}
}

func decode(raw []byte) map[string]any {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var m map[string]any
	_ = d.Decode(&m)
	return m
}

// jsonDiff lists the top-level keys whose values differ. Numbers compare by
// value (to_jsonb may print 1e+30 as 1000…000); everything else exactly.
func jsonDiff(got, want map[string]any) []string {
	var out []string
	keys := map[string]bool{}
	for k := range got {
		keys[k] = true
	}
	for k := range want {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		if !sameJSON(got[k], want[k]) {
			g, _ := json.Marshal(got[k])
			w, _ := json.Marshal(want[k])
			out = append(out, fmt.Sprintf("%s: got %s, want %s", k, g, w))
		}
	}
	return out
}

func sameJSON(a, b any) bool {
	switch x := a.(type) {
	case json.Number:
		y, ok := b.(json.Number)
		if !ok {
			return false
		}
		rx, okx := new(big.Rat).SetString(string(x))
		ry, oky := new(big.Rat).SetString(string(y))
		return okx && oky && rx.Cmp(ry) == 0
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for k, v := range x {
			if !sameJSON(v, y[k]) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !sameJSON(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return a == b
	}
}
