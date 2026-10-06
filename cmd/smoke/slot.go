package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

var slotName = envOr("SMOKE_SLOT_NAME", "sluice")

// phaseSlotLoss drops the replication slot under a running Sluice. The reader
// must find the slot gone and stop the process, ending every stream with
// server_shutdown, instead of retrying forever while its streams stay silently
// open. The restarted process creates a new slot and serves streams again.
//
// It runs last: it restarts the Sluice under test.
func phaseSlotLoss(ctx context.Context, tok, userID string) {
	fmt.Println("\n-- slot loss --")
	st, err := openStream(ctx, tok, fmt.Sprintf(
		`{"subscriptions":[{"sub":"s","shape":{"table":"documents","filter":"owner_id=eq.%s"}}]}`, userID))
	must(err, "open a stream before the slot is dropped")
	if _, err := st.next(10 * time.Second); err != nil {
		fatal("ready before the slot is dropped: %v", err)
	}

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

	// The restart policy brings the process back; it creates the slot again.
	var back bool
	for deadline := time.Now().Add(60 * time.Second); time.Now().Before(deadline) && !back; time.Sleep(time.Second) {
		s, err := openStream(ctx, tok, `{"subscriptions":[]}`)
		if err != nil {
			continue
		}
		ev, err := s.next(5 * time.Second)
		s.Close()
		back = err == nil && ev.Name == "ready" && slotExists(ctx)
	}
	check(back, "the restarted process recreates the slot and serves streams again",
		"Sluice did not come back within 60s")
}

func slotExists(ctx context.Context) bool {
	var ok bool
	return pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`,
		slotName).Scan(&ok) == nil && ok
}
