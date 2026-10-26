# Phase 4 — five proposals, evaluated (2026-10-26)

**Status.** Section 3, with the fix of the gap it describes, is implemented in the working tree, not committed. Checked with `go vet`, `go test` and `go test -race` (Linux), `cmd/smoke` 48/0, `cmd/smoke-issuer` 21/0, `cmd/audit` 200/200, SDK 35/0 and `live.mjs` 19/0. Not yet measured under load: the refill plan before and after is pending.

Five ideas, each weighed against 0.10.0 as it is on `main` (`0a5b75a`): what Sluice does today, what was measured, a design, its cost, and the decisions left to the user. Every figure is measured unless marked otherwise.

How it was measured:
- Microbenchmarks and probe tests in `internal/server`, run on the host with nothing else running (checked with `docker stats` first), on one core (`-cpu 1`) unless noted. They are not in git; the files are kept apart to be added with the changes they motivate.
- PostgreSQL 18.6 in a throwaway container for the slot experiments.
- No load run. The load-test figures quoted come from earlier reports, named where used.

## Summary

| # | Proposal | Verdict | Size |
| --- | --- | --- | --- |
| 1 | Lag and slot retention | **Yes** for the slot: `safe_wal_size` and warnings at 50% and 80% that name the cause. An open transaction anywhere in the database loses the slot while Sluice is caught up (measured). **Optional** for a delay signal to clients. **Only as consistency** for refusing streams on lag: behind `SLUICE_READY_MAX_LAG`, unset by default. | small |
| 2 | Resume buffer by time | **Yes, in two steps.** First make replays cheap (6–8× faster, measured); then age within a byte budget across tables. A longer buffer costs streams on a small node: the default budget is the user's call. | medium |
| 3 | Holds once per request | **Yes.** 7 identical `EXISTS` per open and per `/token` in the load test become 1. It also closes a gap found on the way: a shape receives changes while its hold check runs (probe test). | small–medium |
| 4 | Work off the reader | **Not now.** The reader is ~8 µs per change, about a tenth of the process's CPU at the unicorn load. Cheap savings per change instead. Off-thread pays only for database round trips on the reader (RLS mode). | small (savings) / large (off-thread) |
| 5 | Guide and characterization | **Yes.** Half of the ten findings are outdated by 0.9–0.10; RLS mode, routing and replica identity are not measured at all. Outline and runs below. | large |

Proposed order: 3, 1 (slot), 2 step 1, the first phase 4 runs, 2 step 2, the guide, then the optional parts of 1 and 4.

## 1. Lag and slot retention

### Today

- Reader lag in seconds: `sluice_reader_lag_seconds`; `/readyz` adds `"degraded": "reader_lag"` past `SLUICE_LAG_DEGRADED` and answers `503` past `SLUICE_READY_MAX_LAG` (unset by default).
- `sluice_slot_retained_bytes` (WAL end − `restart_lsn`) and, in `/diagnostics`, `wal_status`, read each `SLUICE_CATALOG_REFRESH`. Startup warns when retention is unbounded.
- Nothing says how close the slot is to `max_slot_wal_keep_size`, or why it retains. Clients cannot tell late changes from no changes: heartbeats come from a separate timer and keep arriving while the reader is behind. Streams open at any lag.

### Measured: an open transaction loses the slot while the consumer is caught up

PostgreSQL 18.6, `max_slot_wal_keep_size=64MB`, `wal_keep_size=0`. A `pgoutput` slot on a publication of table `t`. Each round inserts 15 MB of rows into `t`, consumes the slot (`pg_logical_slot_get_binary_changes`) and runs `CHECKPOINT`. Meanwhile another session has inserted one row into a table **outside** the publication and keeps its transaction open.

| Round | Retained | Behind `confirmed_flush_lsn` | `wal_status` | `safe_wal_size` |
| ---: | ---: | ---: | --- | ---: |
| 1 | 15 MB | 208 B | reserved | 57 MB |
| 2 | 30 MB | 208 B | reserved | 43 MB |
| 3 | 45 MB | 208 B | reserved | 28 MB |
| 4 | 60 MB | 208 B | reserved | 13 MB |
| 5 | — | 208 B | lost | — |

`test_decoding` gave the same rounds.
- **Cause.** `restart_lsn` stays at the oldest transaction open in the database, whatever table it wrote. The slot is invalidated by retention although the consumer acknowledged everything. Sluice then exits (the slot is gone) and every client reads its state again.
- **No early warning from `wal_status`.** It went from `reserved` straight to `lost`: one checkpoint removed the segments. `safe_wal_size` fell steadily and is the signal to watch.
- **A cause the operator can fix in time.** Ending the transaction saves the slot. When Sluice itself is behind, the fix is capacity.
- **Seen on the way.** Creating a slot waited for that open transaction, as the README already says.

### Design: retention warnings (recommended)

- The slot query (each `SLUICE_CATALOG_REFRESH`) also reads `safe_wal_size` and `confirmed_flush_lsn`.
- A new gauge, `sluice_slot_safe_wal_bytes`, left unset when `max_slot_wal_keep_size = -1` (PostgreSQL returns NULL then). It is the one to alert on.
- **Share used.** retained / (retained + `safe_wal_size`). That counts `wal_keep_size` and segment rounding the way PostgreSQL does.
- **Logged on crossing, by the process holding the reader lock** (the standby runs the same tick): a warning at 50%, an error at 80%, and a line when the share falls back below 40%. `/diagnostics` adds `slot_wal_retention` (medium, then high) while above 50%, and `sluice_config_warnings` follows.
- **The cause, on the same line:**
  - If the WAL end − `confirmed_flush_lsn` is under half of what is retained, an open transaction holds the restart point. The line names the oldest one:
    - its pid, from `pg_locks` (the `transactionid` lock of `pg_snapshot_xmin(pg_current_snapshot())`), which any role can read;
    - its age, from `pg_stat_activity` when the pool role can see it (`pg_read_all_stats` for other roles' sessions);
    - prepared transactions too (`pg_prepared_xacts`).
  - Otherwise Sluice is behind, and the line gives the reader lag.
- **Docs.** README, Failures: the cause above. Operating it:
  - size `max_slot_wal_keep_size` for the WAL rate × the longest write transaction allowed;
  - set `idle_in_transaction_session_timeout` (and `transaction_timeout` on 17+);
  - alert on `sluice_slot_safe_wal_bytes`;
  - Sluice cannot warn while it is down, so PostgreSQL-side monitoring is still needed.
- **Cost.** A column and a gauge, a little state for the crossings, and one more query while the share is above 50%.

### Design: a delay signal for clients (optional)

- **The event.** A `delay` event, sent by a 1-second ticker, not the reader, to every stream:
  - `{"lag_ms": n}` when the lag reaches `SLUICE_LAG_DEGRADED`;
  - `{"lag_ms": 0}` when it falls below half of it again;
  - a stream opened while delayed gets it after its `ready`.
- **Its name.** Not `stream_lagging`, which closes a stream whose client is too slow.
- **The SDK** maps it onto `onStatusChange`: a `delayed` status alongside `open`. An app that shows `reconnecting` can then show "updates delayed".
- **Cost.** O(streams) per transition. It adds an event to the protocol and a status to the SDK.
- **Limit.** The lag only sees the transaction being dispatched: while PostgreSQL is still decoding a large transaction, before sending it, the lag reads 0.

### Design: refusing new streams on lag (only as consistency)

- **Single node: little to gain.** Refusing does not make anyone's data less late. The client shows `reconnecting` instead of a late stream, and its own read over HTTP works either way.
- **Load: already covered.** Refusing would protect the reader only when stream opens compete with it, in reconnect storms. Memory admission already spreads those, and message p99 stayed at 28 ms through the 0.10 refill (`../refill/README.md`).
- **Where it fits:** independent nodes, each with its own slot, behind a balancer. There `/readyz` already answers `503` past `SLUICE_READY_MAX_LAG`, and a node that is not ready still accepts streams.
- **Design.** While `SLUICE_READY_MAX_LAG` is set and exceeded, `/stream` answers `503 node_lagging` with `Retry-After`, counted as `sluice_streams_refused_total{reason="reader_lag"}`. No new setting. The SDK already retries any `503` after its `Retry-After`.

## 2. Resume buffer by time

### Today

- **The bounds:**
  - `SLUICE_RING_EVENTS` (4,096) entries per table;
  - `SLUICE_RING_MAX_BYTES` (64 MiB) across tables;
  - entries older than `SLUICE_RING_MAX_AGE` (60 s) swept, keeping each table's newest transaction.
- **Which one binds.** On a busy table the count does: about 12 s of coverage at 337 changes/s, about 2 s at 2,300/s (README). The byte bound is never reached: about 2 MiB per busy table.

### Measured

Entries are `room_members` UPDATEs with `REPLICA IDENTITY FULL` (3 columns, old and new tuple), the load test's busiest table.

- **Memory:**
  - 515–536 B per entry on the heap; the buffer counts 487 B;
  - the decoder keeps one copy of the WAL message per change (5 objects); the benchmark's tuples have 9.5.
- **GC.** A full collection with 131,072 entries live took 25 ms on one core, against 1 ms with 4,096.
- **Append.** 34–35 ns and no allocation, at 4,096 or 131,072 entries.
- **Replay, today:**
  - one resumed subscription: 1 in 400 entries are its own, `routedTo` filters the rest;
  - the cost is the filter: each entry's routing column becomes an `expr.Value` (a string copy, `ParseText`, `FormatInt` for integers), for the new and the old tuple;
  - comparing the column's bytes with the constant's text is exact for text, uuid and integer columns, which PostgreSQL writes in the canonical form `Value.String()` gives. That is the right column below.

| Buffer | Entries since the position | Today | Text compared as bytes |
| ---: | ---: | --- | --- |
| 4,096 | 1,024 | 200 µs | — |
| 4,096 | 4,096 | 763 µs, 8,186 allocations | 98 µs, 5 allocations |
| 32,768 | 32,768 | 6.3 ms | — |
| 131,072 | 131,072 | 26 ms, 261,826 allocations | 4.2 ms, 10 allocations |

### What a buffer by time costs

Extrapolated from the figures above. Coverage = budget / (changes per second × bytes per entry); at about 500 B per entry:

| Changes/s on busy tables | 60 s of coverage needs |
| ---: | ---: |
| 337 (very high tier) | 10 MiB |
| 2,320 (unicorn) | 66 MiB |

- **It costs streams.** The buffer is live heap, which admission counts. On 1 CPU and 512 MB the Go memory limit is 461 MiB and admission refuses from 0.5 of it, 230 MiB. 66 MiB is then about 1,600 fewer streams of 42 KiB, 28% of the 5,770 that fit.
- **Replays grow with the buffer.** A resume looks through every change to its table since its position. After a 15-s cut at 2,000 changes/s that is 30,000 entries per resumed shape: about 6 ms today, about 1 ms with bytes compared.
- **A reconnect multiplies it.** With 5,770 streams of 5 shapes on that table: about 170 s of CPU today, about 29 s with bytes compared. Today most of those resumes are not covered at all: the client reads its state again over HTTP, which loads PostgreSQL instead.

### Design, in order

1. **Cheap replays (do first; it stands on its own).**
   - One comparison of a column's text with a constant, without allocating. It is used by `routedTo`, `registry.Candidates` and the hold index's candidate lookup.
   - Float, numeric, bool and json columns keep today's path.
   - The same change removes most of the reader's allocations (section 4).
2. **Age within a byte budget.**
   - **Drop the count per table.** `SLUICE_RING_MAX_AGE` becomes the span the buffer aims to cover. `SLUICE_RING_MAX_BYTES` bounds every table together, evicting the oldest entry first, so every busy table gets the same span.
   - **Keep each table's newest transaction under the byte bound too.** Today only the age sweep keeps it. With the count gone the byte bound is what binds, and a quiet table would otherwise lose the transaction its clients are up to date with.
   - **Do not buffer the revocation tables.** They cannot be subscribed to, so their entries can never be replayed. Free, and safe.
   - **Configuration.** `SLUICE_RING_EVENTS` goes, and disabling becomes `SLUICE_RING_MAX_BYTES=0`. That is a breaking configuration change.
3. **Coverage you can see.**
   - `sluice_resume_buffer_seconds`: the age of the oldest buffered entry, which is the span every busy table has.
   - `sluice_resumes_total{result}`: `resumed` or `not_resumed`.
   - Sizing is then read, not guessed.
4. **Only if a storm on a busy table still spends its CPU replaying after step 1:** an index by routing value inside each buffer, so a replay looks only at its own entries. Not designed further until measured.

**Decisions:**
- **The default budget:**
  - keep 64 MiB: 54 s at the unicorn's rate, and up to ~1,500 fewer streams per 512 MB under that load;
  - or a share of the Go memory limit: 10% is 46 MiB at 512 MB, 38 s at the unicorn's rate, 60 s up to ~1,600 changes/s;
  - recommended: the share, as admission already is one.
- **When to change the configuration:** in 0.10.0, already breaking for issuers, or in 0.11.

## 3. Holds once per request

### Today

- **Subscribe.** Each shape runs `installShape`: add its watch, register it, check the replica identity, then one `EXISTS` per hold. The shapes of a request go one after another.
- **`/token`.** Each shape replaces its watches, checks, runs its `EXISTS`, then rebinds.
- **In the load test** every shape of a stream is held by the same row, the member's `room_members` row:
  - 7 identical `EXISTS` per open and per `/token`, one after another;
  - about 40,000 in a reconnect of 5,770 streams;
  - the refill's goroutine profile had 2,391 subscriptions waiting for one of the pool's 8 connections to check their holds (`../refill/README.md`).

### Found: a shape receives changes while its hold check runs

A probe test makes the reader dispatch a change to the shape's table while `EXISTS` runs, then has `EXISTS` report the hold row missing. The refused shape got the change (`record map[id:7 project_id:42 title:secret]`).
- **Where it goes.** On a stream being opened, the change goes out right after `ready`. On `POST /subscribe`, at once.
- **The window** is the `EXISTS` round trip. In a storm that includes waiting for a pool connection, which took seconds in the refill.
- **Why it matters.** `EXISTS` is there to catch a hold row deleted between the issuer's verdict and the watch. Meanwhile the shape is already live.
- **Severity: low**, since the issuer must allow just as the row is deleted. Still a gap in authorization.

### Design

Per request (a stream opening, `POST /subscribe`):
1. **Prepare in request order, as now:** validate, one oracle call, then limits and warnings per shape.
2. **Add the watch of every shape that passed.**
3. **One round trip for the request's distinct holds.**
   - Deduplicated by relation, SQL and arguments, and sent as one `pgx.Batch` on one connection.
   - A missing row denies the shapes it holds; a batch that fails makes them `hold_check_unavailable`.
4. **Register each shape whose holds exist,** under the hold index's lock, and only if its watch is still current (not cut since step 2). This is a new `hold.Index` method: `CheckIdentity`'s checks plus the registration. A `DELETE` dispatched after the `EXISTS` snapshot then either:
   - finds the watch, and the cut removes the newly registered shape before the reader routes anything after it;
   - or finds it gone, and the shape is not registered.
5. **Resume or snapshot, as now.**

- **`/token`:** replace every watch, one batched `EXISTS`, then rebind or revoke each shape.
- **The contract does not change:** `wal_lsn` is still taken before installing, and `ready` is still written after.
- **Lock order:** hold index, then registry. Neither package calls the other, and the server holds neither lock across calls, so nothing takes them the other way.
- **Measure** with `../../plans/refill.plan` before and after; the refill report lists this as not done.

## 4. Work off the reader thread

### Measured

**The benchmark:** one message of the unicorn's mix, written as the load test writes it (one INSERT and one UPDATE per member, in one transaction). 380 rooms of 80 members with 28 streams, and 16 of 250 members with 85: 12,000 streams of 7 shapes, one hold each.
- **8.2–8.5 µs per change on one core:** about 120,000 changes/s if the reader did nothing else.
- **68 allocations, 3.9 KB per change.**
- **Share of the process.** The unicorn's 2,320 changes/s cost the reader about 0.02 cores. The whole process used 0.24 (103 ms per 1,000 changes, `../phase3/README.md`), so the reader is about a tenth of it. Most of the rest is delivery to clients and GC, already on the streams' goroutines (0.8.0 profiles; see the first run below).

Inside `OnChange` (CPU profile of the benchmark):

| Part | Share | Notes |
| --- | ---: | --- |
| Hold checks | 44% | `expr.Eval`; `strconv.ParseFloat` failing on uuid text, error allocation included, is 8% |
| Delivery | 25% | filter, projection, event |
| `primaryKeyOf` | 11% | a map of key values for every change, read only by Tier C probes |
| Routing (`registry.Candidates`) | 7% | |
| `strings.ToLower` in `ParseText` | 7% | uuids, already lowercase from PostgreSQL |
| Allocation, across the above | 36% | |

### Verdict

- **Moving delivery off the reader:** not now.
  - It would not add capacity on 1 CPU.
  - It needs per-stream shards, a watermark before acknowledging, and cut errors in the same queues as the events.
- **Cheap savings on the reader instead:**
  - build the primary-key map only for Tier C;
  - compare text with text without trying `ParseFloat`;
  - read columns without allocating (shared with section 2, step 1);
  - keep the Prometheus children per relation instead of calling `WithLabelValues` for every change;
  - expected (extrapolated): from ~8 to ~5 µs per change and far fewer allocations, a few percent of the process's CPU. Low priority, except the comparison shared with section 2.
- **Off-thread pays for database round trips on the reader,** which only happen in RLS mode:
  - **Tier B cross-checks** (the first `SLUICE_TIER_B_VERIFY` decisions per subscription) do not gate delivery: `crossCheck` only demotes later decisions. A small bounded pool can run them. The trade-off: a few more changes are decided in process before a demotion lands.
  - **Tier C probes** gate delivery. Moving them needs pending events in the stream queues (ROADMAP: move Tier C probes off the replication path). Large.
  - **Suspected, not measured:** a reconnect of RLS streams with Tier B shapes makes up to five synchronous probes per subscription on the reader. The load test runs issuer mode; an RLS storm would show it.
- **For efficiency, measure the whole process first.** A CPU profile of 0.10.0 at the unicorn load (`LOAD_PPROF_AT`) shows where the other nine tenths go now; the last such profile is of 0.8.0.

## 5. Guide and characterization

### The findings so far (`../phase2/notes.md`), on 0.10

| # | Finding | State |
| ---: | --- | --- |
| 1 | FULL against the primary key | Withdrawn (a bug voided the runs); re-measure |
| 2 | The cost follows the WAL's changes | Valid; its alternatives are not measured |
| 3 | Ephemeral state on channels | Valid; re-measure |
| 4 | `synchronous_commit=off` | Valid (PostgreSQL's behavior) |
| 5 | A slot's ceiling is one thread's | Outdated: hold checks are no longer the reader's work; measure the ceiling at 1, 2 and 4 CPUs |
| 6 | Big transactions | Valid; re-measure |
| 7 | Memory per stream | Outdated: ~42 KiB, ~5,770 streams per 512 MB (0.9) |
| 8 | Resume buffer coverage | Changes with section 2 |
| 9 | Hold column order | Obsolete since 0.9.0 |
| 10 | `/token` one call per shape | Obsolete since 0.10.0; one `EXISTS` per hold remains until section 3 |

**New from this evaluation:**
- An open write transaction anywhere in the database holds the slot (section 1).
- A resume's replay costs in proportion to its table's changes since the cut, times the shapes that resume (section 2).
- Shapes that share a hold will cost one check (after section 3).

**Missing for a guide that is generic:**
- **RLS mode.** The per-change cost of each tier (A; B with its cross-checks; C), and a reconnect storm. Nothing is measured under load.
- **Routing.** What an unindexed shape costs every change to its table.
- **Replica identity.** `DEFAULT`, `USING INDEX` and `FULL`: WAL bytes, Sluice CPU, and what each allows (deletes, transitions, holds), with TOAST.
- **Snapshots.** Cost per row, concurrency, and when reading over HTTP is better.
- **Write patterns.** 1 + N rows per action, against one row, against a channel.
- **Per-stream cost.** Shapes, channels, hook `ttl` and `/token` frequency.
- **More than one deployment.** What N slots cost PostgreSQL: each walsender decodes all of the WAL.
- **A sizing table.**

### Outline

1. **The cost model.** WAL changes are the unit of work; delivery is per matching stream.
2. **Tables:**
   - one row per fact, not per recipient;
   - counters and "last seen" columns off streamed tables;
   - ephemeral state on channels;
   - replica identity;
   - keys and indexes on filter columns.
3. **Shapes and filters:** an equality on an indexed column in the replica identity; deletes and transitions need the identity; unindexed shapes.
4. **Authorization:** RLS tiers and how to write policies for Tier A; issuer holds.
5. **Transactions:** size; long transactions and the slot.
6. **Clients:** upsert by key; `resumed: false`; reconnects.
7. **Sizing and operation:** streams per GB, changes per second per core, metrics and alerts.

Each rule with its reason and figure, measured apart from extrapolated, as planned in `../phase2/notes.md`.

### Runs, in order

1. A whole-process CPU profile at the unicorn on 0.10.0; it also feeds section 4.
2. The ceiling in changes per second at 1, 2 and 4 CPUs (rooms mix), profiled.
3. Memory per stream on 0.10 (idle ramp).
4. `FULL` against `DEFAULT` replica identity, very high tier and unicorn.
5. Write patterns at a fixed message rate: 1 + N rows, one row, a channel.
6. RLS: the per-change cost of each tier, and a reconnect storm; it feeds section 4.
7. Resume coverage and the refill with the buffer by time (after section 2).

Before and after where a run follows a change.

## Decisions for the user

1. Whether 0.10.0 is tagged before section 3's fix. The gap has been in every issuer-mode release, since issuer mode was added in 0.1.4 (`9917027`, 2026-09-12).
2. The resume buffer's default budget, and whether its configuration change goes into 0.10.0 or 0.11.
3. Whether clients get a delay event and an SDK status.
