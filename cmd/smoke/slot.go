package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"
	"time"
)

var slotName = envOr("SMOKE_SLOT_NAME", "sluice")

// phaseSlotLoss drops the replication slot under a running Sluice. The reader
// must find the slot gone and stop the process, ending every stream with
// server_shutdown, instead of retrying forever while its streams stay silently
// open. The restarted process creates a new slot and serves streams again.
//
// Creating a slot waits for every transaction that had an xid when it started,
// and a transaction that commits before the slot's consistent point is never
// decoded. A transaction held open across the restart keeps the new process
// in that wait: a stream opened then must be refused, not told it is live,
// since changes committed after its ready would never reach it.
//
// It runs last: it restarts the Sluice under test.
func phaseSlotLoss(ctx context.Context, tok, userID string) {
	fmt.Println("\n-- slot loss --")
	shape := fmt.Sprintf(`{"subscriptions":[{"sub":"s","shape":{"table":"documents","filter":"owner_id=eq.%s"}}]}`, userID)
	st, err := openStream(ctx, tok, shape)
	must(err, "open a stream before the slot is dropped")
	if _, err := st.next(10 * time.Second); err != nil {
		fatal("ready before the slot is dropped: %v", err)
	}

	// With an xid, so that creating the new slot waits for it.
	must(execSQL(ctx, "select 1"), "connect to the database")
	blocker, err := pool.Begin(ctx)
	must(err, "open the transaction that holds up the slot's creation")
	defer func() { _ = blocker.Rollback(context.Background()) }()
	var xid string
	must(blocker.QueryRow(ctx, "SELECT pg_current_xact_id()::text").Scan(&xid), "assign it an xid")

	// The walsender must exit before its slot can be dropped, and the reader
	// reconnects a second later: drop it in that window.
	must(execSQL(ctx, fmt.Sprintf(`DO $$
	BEGIN
	  PERFORM pg_terminate_backend(active_pid) FROM pg_replication_slots
	   WHERE slot_name = '%[1]s' AND active_pid IS NOT NULL;
	  FOR i IN 1..250 LOOP
	    BEGIN
	      PERFORM pg_drop_replication_slot('%[1]s');
	      RETURN;
	    EXCEPTION WHEN object_in_use THEN
	      PERFORM pg_sleep(0.02);
	    END;
	  END LOOP;
	  RAISE EXCEPTION 'slot %[1]s stayed active';
	END $$`, slotName)), "drop the slot under the running reader")

	var shutdown bool
	for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); {
		ev, err := st.next(time.Until(deadline))
		if err != nil {
			break
		}
		var e struct {
			Code string `json:"code"`
		}
		if ev.Name == "error" && json.Unmarshal(ev.Data, &e) == nil && e.Code == "server_shutdown" {
			shutdown = true
			break
		}
	}
	st.Close()
	check(shutdown, "with its slot gone, Sluice ends its streams with server_shutdown and stops",
		"no server_shutdown within 20s")

	// The restart policy brings the process back; it creates the slot again,
	// and waits for the open transaction to do so.
	var creating bool
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline) && !creating; time.Sleep(200 * time.Millisecond) {
		_ = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_stat_activity
		  WHERE backend_type = 'walsender' AND state = 'active' AND query ILIKE 'CREATE_REPLICATION_SLOT%')`).Scan(&creating)
	}
	check(creating, "the restarted process waits to create the slot while a transaction is open",
		"no CREATE_REPLICATION_SLOT within 60s")

	var status int
	var retryAfter, body string
	for deadline := time.Now().Add(30 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if status, retryAfter, body, err = streamStatus(ctx, tok, shape); err == nil {
			break
		}
	}
	check(status == http.StatusServiceUnavailable && strings.Contains(body, `"not_replicating"`) &&
		len(retryAfter) == 1 && retryAfter >= "1" && retryAfter <= "5",
		"while the slot is being created, a new stream is refused with 503 not_replicating and a Retry-After",
		fmt.Sprintf("got %d, Retry-After %q: %s", status, retryAfter, truncate(body, 160)))

	must(blocker.Commit(ctx), "commit the transaction holding up the slot's creation")

	var back *stream
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline) && back == nil; time.Sleep(time.Second) {
		s, err := openStream(ctx, tok, shape)
		if err != nil {
			continue
		}
		if ev, err := s.next(5 * time.Second); err == nil && ev.Name == "ready" && slotExists(ctx) {
			back = s
		} else {
			s.Close()
		}
	}
	check(back != nil, "the restarted process recreates the slot and serves streams again",
		"Sluice did not come back within 60s")
	if back == nil {
		return
	}
	defer back.Close()
	mustExec(ctx, fmt.Sprintf(`insert into documents (owner_id, title, body) values ('%s','after-recreate','x')`, userID))
	got := changeTitles(back.collect(4*time.Second), "s")
	check(slices.Contains(got, "after-recreate"),
		"a change committed after the ready of a stream on the new slot reaches it",
		fmt.Sprintf("got %v", got))
}

func slotExists(ctx context.Context) bool {
	var ok bool
	return pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		slotName).Scan(&ok) == nil && ok
}

// streamStatus opens a stream only to read its status and Retry-After, and a
// refusal's body; it closes the response either way.
func streamStatus(ctx context.Context, token, body string) (status int, retryAfter, refusal string, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sluiceURL+"/stream", strings.NewReader(body))
	if err != nil {
		return 0, "", "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{}).Do(req)
	if err != nil {
		return 0, "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		refusal = string(b)
	}
	return resp.StatusCode, resp.Header.Get("Retry-After"), refusal, nil
}
