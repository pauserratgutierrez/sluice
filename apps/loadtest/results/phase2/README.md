# Phase 2 — Baseline: Sluice 0.8.0 under production limits

Sluice 0.8.0 in issuer mode (RLS off), measured on one laptop with the production-shaped load test of phase 1. The baseline is what phase 3 is compared against. Every figure is **measured** unless it is marked **extrapolated**, and each one names the runs it comes from. The run reports (`raw/*.md`, `raw/*.json`) hold the full tables. `notes.md` is the log kept while the runs ran: incidents, method changes, and findings for the design guide.

## In short

- **At the production limits (1 CPU, 512 MB), 0.8.0 serves the high and very-high tiers with a wide margin, and cannot serve the unicorn.**
  - Very high: 1,500 streams, 337 changes/s. It used 0.13–0.14 cores, message p95 19 ms, 254 MiB of Go memory at peak.
  - Unicorn: 12,000 streams, 2,320 changes/s.
    - **At 1 CPU** it dispatched ~1,950 changes/s in its first 10 minutes, falling to ~1,200 by minute 60. Its backlog never stopped growing (435 MiB after an hour).
    - **At 2 CPUs** it kept up for 30 minutes, with message p95 0.44 s and 1.29 cores.
- **What fails first is CPU, on one thread.** The replication reader decodes and dispatches every change on one goroutine. In 0.8.0 most of that goroutine's work is **hold checks**: about half of all of Sluice's CPU in every tier. Each `room_members` UPDATE evaluates the filter of every hold indexed under the same room. Each message updates every member's row (80–250 rows per transaction).
- **CPU per change and live memory grow with time at constant load (a leak).** Very high, over 29 minutes:
  - live heap 75 → 104 MiB;
  - CPU per 1,000 changes 336 → 441 ms.

  The cause is a Sluice bug in the hold index: watches left behind by `/token` refreshes. It is fixed in the 0.8.1 candidate (`fix/hold-routing`), measured separately in `../phase3/v0.8.1.md`. With holds written member first, as 0.8.1's README advises:
  - CPU per change fell 75–80% (very high 95 against 365–387 ms per 1,000 changes; unicorn at 2 CPUs 105 against 535);
  - the unicorn's message p95 fell from 305 ms to 6 ms;
  - the live heap stayed flat over the 10 minutes measured.
- **A second Sluice bug, found while checking the primary-key runs.** With `REPLICA IDENTITY DEFAULT` or `USING INDEX`, an UPDATE that leaves the identity alone can revoke other rows' grants. This makes both **primary-key runs invalid**: their low CPU came from Sluice having cut nearly every grant. The FULL-against-primary-key comparison reported during the phase is withdrawn. A fix is prepared, pending a decision (see "Bugs found").
- **Memory per stream** (7 shapes and a hook channel per stream, issuer mode):
  - live heap ~50–70 KiB per stream;
  - Go memory (what GOMEMLIMIT bounds) ~95 KiB per stream when idle, and ~170 KiB at peak under the very-high load.
  - **Extrapolated:** a 512 MB container holds at most ~2,700 streams at the very-high activity per stream, and at most ~4,900 idle. That is fewer than `SLUICE_MAX_STREAMS=5000`.

## Setup

| | |
| --- | --- |
| Host | Intel i7-12700H laptop (6 P + 8 E cores, 20 threads), Windows 11, Docker Desktop on WSL2 (kernel 6.6.87.2), plugged in, power plan "Alto rendimiento" |
| Pinning | `env/pin-20cpu.env`: Sluice on vCPUs 2–3 (2–5 with 2 CPUs), PostgreSQL 6–9, issuer 10–11, proxy 12–13, runner 14–19 |
| Sluice | `ghcr.io/pauserratgutierrez/sluice:0.8.0`; `cpus` 1 (or 2), `memory` 512m (4g for the 12,000-stream runs, so memory would not decide whether CPU is enough); GOMAXPROCS 2 (Go rounds a quota below 2 up); GOMEMLIMIT 90% of the limit; `SLUICE_MAX_STREAMS` 5,000 (15,000–16,000 where needed) |
| PostgreSQL | 18.6, `synchronous_commit` on (phase 1 showed that `off` adds up to ~3 × `wal_writer_delay` to every change) |
| Issuer (and hook) | the load test's: 5–20 ms per call, 32 calls at once, hook `ttl` 0. Membership holds are written `room_id=eq.R,user_id=eq.U`, so 0.8.0 keys each one on either column at random |
| Schema | `room_messages`, `room_members` (`REPLICA IDENTITY FULL`; primary key `(room_id, user_id)`; index on `user_id`), `notifications`; publication `sluice_rooms` |
| Per stream | 7 shapes (messages of their room, their memberships, their notifications, …) plus a hook channel `room:<id>`; `/token` every 15 min |
| Per room | a message every 15 s, from a random phase. Each message is one transaction: INSERT the message, and UPDATE `last_message_at` on every member's row. Typing goes over the channel every 2 s for each typist; it never reaches the WAL |

The tiers (count × members / streams / typing share per room):

| Tier | Mix | Streams | Changes/s | Events/s |
| --- | --- | ---: | ---: | ---: |
| Realistic | 5 × 80 / 20 / 0.4 | 100 | 27 | 32 |
| High | 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 400 | 89 | 126 |
| Very high | 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 1,500 | 337 | 503 |
| Unicorn | 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 12,000 | 2,320 | 3,500 |

Changes are almost all `room_members` UPDATEs: 336 of the 337 per second at very high.

## Which runs count

| Status | Runs | Why |
| --- | --- | --- |
| Clean | `high-main-r1`, `high-main-r2`, `veryhigh-main-r1`, `veryhigh-main-r4`, `unicorn-main-r1` (1 CPU, 60 min), `unicorn-2cpu` (2 CPUs, 30 min, the re-run), `idle-ramp`, `bigtxn-high` | — |
| Contaminated (`contaminated/`) | `realistic-main-r2`, `realistic-main-r3`, `high-main-r3`, `veryhigh-main-r2`, `veryhigh-main-r3`, the first `unicorn-2cpu` | My work on the host (compiling, tests, image builds) overlapped them. Process CPU per change rises with it: by up to ~20% in a minute, and over a whole run in `veryhigh-main-r3`. They are left out of CPU medians and the capacity table. The realistic tier has no clean run: its figures carry that caveat (it is far from any limit) |
| Invalid (`invalid/`) | `realistic-main-r1`, the first `veryhigh-main-r3` | A harness fault: the previous run's 435 MiB of undispatched WAL, including a setup `TRUNCATE`, cut 196 shapes. Separately, a restart was counted that never happened. Both are fixed in `scripts/run.sh` and the sampler |
| Invalid (Sluice bug) | `veryhigh-pk`, `unicorn-pk` | See "Bugs found" |
| Diagnosis only | `pprof-high`, `pprof-veryhigh`, `pprof-unicorn-1cpu`, `pprof-unicorn-2cpu` | Built with pprof, with block and mutex sampling on, a 60-s CPU profile and a 5-s trace. Overhead on CPU per 1,000 changes (first 10 min): very high +10% (412 against 365/387 ms), high +15% (372 against 331/317), unicorn at 2 CPUs −11% (478 against 538), within the spread between runs. They locate the cost; none of their figures is used in a table here |

Between repetitions of the same load, CPU per 1,000 changes varied by ±6–10%.

## Results by tier (steady phase, 30 min; the unicorn at 1 CPU 60 min)

| Tier | Runs | Cores avg / peak | ms CPU / 1k changes | Message p50 / p95 / p99 ms | Member update p95 ms | Typing p95 ms | `/token` p50 / p95 ms | Go memory peak / RSS peak MiB | Verdict |
| --- | --- | --- | ---: | --- | ---: | ---: | --- | --- | --- |
| Realistic (contaminated) | r2 | 0.01 / 0.03 | 314 | 4.0 / 14.0 / 16.8 | 14 | 1.1 | 111 / 132 | 31 / 42 | pass |
| High | r1, r2 | 0.03 / 0.06 | 351–355 | 5–7 / 16–17 / 21–22 | 46–56 | 1.5–1.6 | 110 / 131 | 80 / 90 | pass |
| Very high | r1, r4 | 0.13–0.14 / 0.22–0.29 | 398–421 | 4.3–4.6 / 18–19 / 56–63 | 67–70 | 1.8–2.3 | 111 / 138–140 | 248–254 / 258–264 | pass |
| Unicorn, 1 CPU | r1 | 0.99 / 1.02 | 658 | 413 s / 1,166 s / 1,250 s | (same as messages) | 81 | 404 / 1,057 | 2,717 / 2,728 | **fail** (CPU) |
| Unicorn, 2 CPUs | `unicorn-2cpu` | 1.29 / 1.70 | 559 | 44 / 436 / 676 | 461 | 22 | 127 / 354 | 2,187 / 2,197 | pass |

All runs except the unicorn at 1 CPU delivered 100% of messages, member updates and typing events.

- **Member updates** reach a member's own `room_members` shape later than the message, at p95 46–70 ms against 16–19 ms. A message's transaction updates 80–250 member rows. The reader checks holds for each one in order, so the last row's delivery waits for all the earlier checks.
- **Unicorn at 1 CPU:** delivery and latency are those of a backlog that grows for the whole hour. The backlog reached 435 MiB, and a message arrived 7 minutes late at the median.

### Latency, stage by stage

The runner records each message's `commit_time` and reads, every second:
- PostgreSQL's WAL end;
- the walsender's `sent_lsn`;
- Sluice's confirmed position, which advances after a transaction is dispatched.

| Run | Write → commit p50 / p95 ms | Walsender behind the WAL end | Sluice (dispatched) behind the WAL end p50 / p95 / max s | Commit → client p50 / p95 ms |
| --- | --- | --- | --- | --- |
| Very high (r4) | 0.4 / 0.6 | 0 | — (under the sampling resolution) | 3.8 / 18.6 |
| Unicorn, 2 CPUs | 0 / 1 | 0 (max 0.06 s) | 0.09 / 0.45 / 1.27 | 44 / 436 |

At the unicorn the time a message spends between commit and client is the time it waits for the reader. PostgreSQL sends at once, and SSE writers add milliseconds; see the traces below. Per minute, the reader's p95 delay went from 0.30 s (minutes 2–5) to 0.5–0.63 s (minutes 26–31). It grew along with CPU per change, which is the leak.

## Where the CPU goes (pprof, 60-s CPU profiles 4 min into the steady phase)

Share of Sluice's total CPU in each profile:

| | High | Very high | Unicorn 1 CPU | Unicorn 2 CPUs |
| --- | ---: | ---: | ---: | ---: |
| Reader goroutine (`reader.(*Reader).stream`) | 52% | 53% | 55% | 56% |
| … of which hold checks (`hold.(*Index).OnChange`) | 48% | 49% | 52% | 52% |
| GC workers (`gcBgMarkWorker`) | 19% | 24% | 19% | 24% |
| HTTP handlers (SSE streams, `/token`, opens) | 18% | 13% | 17% | 13% |
| Delivery to streams (`deliver`) | — | — | — | 1.5% |

Inside the hold checks (unicorn at 2 CPUs, share of total CPU):
- `expr.Eval` evaluating hold filters: 37%;
- reading a column from the decoded tuple (`tupleRow.Column`): 15%, including a `strings.ToLower` per lookup (5%);
- `strconv.ParseFloat` failing on text values such as user ids, while comparing, and allocating an error each time: 6%;
- a map per evaluated filter (`hold.visible`) and per change (dedup of candidates): these feed the GC.

Why hold checks dominate:
- Every `room_members` UPDATE (99% of changes) finds its candidate watches by the row's `room_id` and `user_id`.
- In 0.8.0 each membership hold is keyed on one of the two at random, so about half the holds of every stream in the room are candidates for every member's row.
- With FULL each candidate is evaluated twice (old row and new row).
- Watches leaked by `/token` refreshes (see "Memory") are candidates as well, and their number grows with time.

### Is the reader thread the bottleneck?

Breakdown of the reader goroutine's state in each 5-s execution trace (taken ~5 min into the steady phase):

| | Running | Waiting for WAL (idle) | Waiting for a CPU | Longest unbroken busy span |
| --- | ---: | ---: | ---: | ---: |
| Very high, 1 CPU | 8% | 91% | 0.3% | 84 ms |
| Unicorn, 1 CPU | 93% | 0% | 7% | the whole trace |
| Unicorn, 2 CPUs | 50% | 46% | 3.5% | 907 ms (and one of 248 ms) |

- **At 1 CPU the reader is saturated.** It never waits for WAL. The process's one core is also shared with GC, HTTP handlers and SSE writers, which wait p95 31 ms (max 157 ms) for a CPU after the reader wakes them.
- **At 2 CPUs the reader has headroom on average but not in bursts.** About 66 of the ~80 preemption samples in its 907-ms busy span are in `hold.(*Index).OnChange`. Those bursts are the 0.45-s p95 delay. SSE writers wait p95 11 ms (max 40 ms) for a CPU, so the scheduler is not the problem.
- **The hold-index mutex.** The reader holds it while it evaluates candidates. That lock accounts for 86% (2 CPUs) and 93% (1 CPU) of all lock waiting in the process, so stream opens, `/token` refreshes and hook re-checks queue behind the reader.

So the hypothesis holds, with a qualifier.
- **Measured:** a slot's changes/s ceiling is the reader goroutine's, so CPUs beyond the second do not raise it.
- **Not yet known:** what that ceiling is once hold checks are cheap. In 0.8.0 the reader's work per change is set mostly by how many holds each change must evaluate, which this workload makes large. Phase 3 measures it again.

**Extrapolated** (0.8.0, this workload): at 2 CPUs the reader would saturate near ~4,600 changes/s, twice the unicorn's rate at 50% busy, measured at minute 5. The process would reach 2 cores near ~3,600 changes/s (1.29 cores at 2,320). The leak lowers both by ~20% per half hour. So the leak, not either figure, decides how long the 2-CPU unicorn keeps up.

## Memory: live heap against garbage

`/gc/heap/live:bytes` is not exported by 0.8.0. The runs were read three ways:
- the per-minute minimum of heap in use;
- `go_memstats_next_gc_bytes / 2`, which is the live heap at the last GC with GOGC=100, recorded from `pprof-unicorn-1cpu` on;
- heap profiles, whose in-use figure is the heap live at the last GC, at minutes 2 and 9.

| Run | Live heap, early → late | Heap in use (min per minute), early → late | CPU per 1k changes, same minutes |
| --- | --- | --- | --- |
| Very high r4 (`next_gc/2`) | 75 → 104 MiB (min 1 → 29) | 112 → 159 MiB | 336 → 441 ms |
| Unicorn 2 CPUs (heap profiles of `pprof-unicorn-2cpu`) | 658 → 764 MB (min 2 → 9) | 994 → 1,580 MiB (`unicorn-2cpu`, min 2 → 30) | 501 → 611 ms |
| Unicorn 1 CPU (heap profiles of `pprof-unicorn-1cpu`) | 685 → 742 MB (min 2 → 9) | 1,084 → 1,944 MiB (`unicorn-main-r1`, min 3 → 61) | 479 → 827 ms |

**The live heap grows: it is accumulation, not only garbage.** The diff of the minute-9 heap profile against minute 2 (unicorn at 2 CPUs):
- Shapes parsed for `/token` refreshes went up by 137 MB; the shapes parsed when streams opened went down by 83 MB, so refreshes add more than they free.
- Objects allocated by `hold.Add` and `hold.Replace` and still live rose from ~198k to ~255k, and those allocated by `registry.Rebind` from 40k to 127k (sampled counts).

These are watches left in the hold index by refreshes, together with the filters they keep alive.

Part of the growth is garbage that the GC still counts as live. The `hold.visible` maps and candidate slices are allocated while the GC is marking, and grow from ~43 to ~64 MB. Their share grows because GC cycles get longer as the heap grows.

The CPU per change rises for two reasons:
- more watches are candidates for every change;
- more marking work goes into a larger heap.

GC cycles per minute fall over the same period, which rules out garbage alone as the cause. (Larger, less frequent GC cycles cost about the same per second.)

**Per stream**, measured:
- live heap: very high 51 → 69 KiB (min 1 → 29); unicorn 55 → 64 KiB (min 2 → 9);
- heap in use after arrival (includes garbage and stacks): 74–90 KiB, as the reports' "KiB of live heap per stream" (a misnomer kept in the runner's output until phase 3);
- Go memory with 15,000 idle streams: 1,394 MiB, ~95 KiB per stream (`idle-ramp`, at 0.13 cores spent on token renewals and hook re-checks).

**Extrapolated:**
- At the production limit (GOMEMLIMIT 461 MiB), Go memory per stream at peak under the very-high load was ~170 KiB (254 MiB for 1,500 streams). That gives at most ~2,700 streams, and at most ~4,900 idle streams (461 MiB ÷ 95 KiB). Near those counts GC runs almost continuously, so the practical figure is lower.
- With 0.8.0's leak the live heap per stream keeps rising, so these numbers hold only for the first half hour.
- `SLUICE_MAX_STREAMS=5000` is therefore not safe at 512 MB with this many shapes per stream.

**Resume buffer:**
- **Memory:** ~0.5 KiB per buffered event (1.9–2.6 MiB for 3,900–5,700 events in the high, very-high and unicorn runs). **Computed:** `SLUICE_RING_EVENTS` costs ~2 MiB per busy table at 4,096, ~16 MiB at 32,768.
- **Coverage of `room_members`:** ~45 s at 64–89 changes/s, ~10 s at 337, ~1–2 s at 2,300. `room_messages` is always 60 s, the maximum age.
- **Probed against derived coverage:** the probed coverage is Sluice's own `resumed` answers at fixed ages. The derived coverage is `SLUICE_RING_EVENTS` ÷ the table's rate over the last minute. It was recomputed from every run's timeline:
  - Runs that derived it live over a minute (`veryhigh-main-r1` on) agree with the recomputation within 0.2 s.
  - The runs that derived it live from a 5-s rate are `realistic-main-r3` and `high-main-r1` to `r3`. That rate aliased with the 15-s message cycle, and their derived coverage changes from 60 s (live) to 46.5 s (recomputed).
  - The probed values (45 s, 10 s, 1–2 s) sit one probe age below the derived ones (46.5, 12.3, 1.8–2.8 s), as expected.

## Other measurements

- **Big transactions** (`bigtxn-high`, against the high tier):
  - A 100,000-row transaction committed in 341–365 ms. The walsender sent it 381–428 ms after commit, and Sluice dispatched it 469–494 ms after commit. The backlog peaked at 8–26 MiB.
  - Other streams' message p95 stayed at 14–17 ms before and during.
  - At 1,500 and 10,000 rows, dispatch followed commit by 22 ms and 33–65 ms.
- **Idle streams** (`idle-ramp`, 1 CPU, 4 GB): 15,000 streams passed, stepping up by 3,000.
  - Memory per stream: see above.
  - Opening a stream cost 3.2–4.3 ms of CPU at 400–1,500 streams (arrival in 30 s), 2.4–3.6 ms in the idle ramp, and 7–8 ms at 12,000 (arrival in 2 min). Each open makes 8 issuer calls (7 shapes plus the hook).
- **`/token`** re-resolves every shape with the issuer one after another: p50 ~110 ms with 7 shapes at 5–20 ms per call. At the unicorn it rose to 127 ms (2 CPUs) and 404 ms (1 CPU). Refreshes queue behind the reader on the hold-index mutex (mutex profile), and at 1 CPU also for the CPU.
- **Typing over channels** never touches the WAL. Its p95 was 1–2.3 ms up to 1,500 streams and 22 ms for the unicorn at 2 CPUs, while messages waited 0.44 s behind the reader.

## Capacity

This workload has 7 shapes and a hook channel per stream, membership holds on a two-column key, FULL identity on the busy table, and 1 + N rows per message. 0.8.0's hold-index leak affects every figure.

| | 1 CPU, 512 MB (production) | 2 CPUs |
| --- | --- | --- |
| **Measured**: highest load that passed | 337 changes/s, 1,500 streams (very high, 30 min): 0.13–0.14 cores, message p95 19 ms, Go memory 254 of 461 MiB | 2,320 changes/s, 12,000 streams (unicorn, 30 min, with 4 GB): 1.29 cores, message p95 0.44 s, Go memory 2.2 GiB |
| **Measured**: saturation | ~1,950 changes/s dispatched in the first 10 min, ~1,500 averaged over the hour, ~1,200 at minute 60, all at 0.99 cores (unicorn at 2,320 offered) | not reached in 30 min; reader 50% busy at minute 5, CPU per change +22% from minute 2 to minute 30 |
| **Extrapolated**: changes/s before saturation | ~1,950 falling with time; plan on ≤ ~1,200 for an hour of uptime | ~3,600 (process at 2 cores) or ~4,600 (reader saturated), whichever comes first, both falling ~20% per half hour |
| **Measured**: live heap per stream | 51 → 69 KiB (very high, min 1 → 29) | 55 → 64 KiB (unicorn, min 2 → 9) |
| **Measured**: Go memory per stream | ~95 KiB idle; ~170 KiB at peak under the very-high load | ~185 KiB at peak (2,187 MiB / 12,000) |
| **Extrapolated**: streams that fit | at most ~2,700 at the very-high activity per stream, ~4,900 idle | memory is not the limit with 4 GB |

## Bugs found

1. **Hold index: watches leaked and wrong grants could be revoked.** The fix is ready on branch `fix/hold-routing` (worktree `../sluice-0.8.1`) as 0.8.1, not committed.
   - **Cause:** `shape.Filter.RoutingKey` picks among equally good columns in map order, and `hold.Index` recomputed it when indexing and again when unindexing. About half of all `Replace` and `Remove` calls (every shape on every `/token`, every close) left the old watch in place.
   - **Effects:** stale watches pile up and cost CPU and memory on every change. One of them could also revoke the grant that replaced it. For example, after a refresh moved a grant from room 42 to room 43, deleting the room-42 row cut the new grant.
   - **The fix:**
     - each watch keeps where it was indexed;
     - `RoutingKey` takes the first equally good column in the filter;
     - only a stream's current watch can cut.
   - **Tests:** four new tests fail on 0.8.0 and pass with the fix. `go vet`, `go test -race ./...` (in a Linux container), the SDK's 32 tests, and the harness's smoke runs pass: 45 checks in `cmd/smoke` and 21 in `cmd/smoke-issuer`.
   - **Effect, measured:** see `../phase3/v0.8.1.md`.
2. **An UPDATE without an old tuple revokes holds on other rows.** The fix is prepared in the same worktree, uncommitted, and is outside 0.8.1's agreed scope: awaiting a decision.
   - **When it happens:** with `REPLICA IDENTITY DEFAULT` or `USING INDEX`, an UPDATE that leaves the identity columns alone carries no old tuple.
   - **Cause:** `hold.watchBroken` cuts every candidate the new row does not match. But a watch is a candidate when one routed column matches, so with a two-column identity the candidates include other rows' holds. Updating u2's membership of a room revokes u1's grant held by the same room. With member-first holds, updating u1's membership of one room revokes u1's grants held by their other rooms.
   - **Scope:** present in 0.8.0 and in the 0.8.1 candidate as it stands. FULL is not affected, because its old tuple is always present. Sluice's own remedy for `replica_identity_insufficient` suggests `USING INDEX`, which is exposed.
   - **Evidence:** this is what invalidated `veryhigh-pk` and `unicorn-pk`. `veryhigh-pk` delivered 301 of 502 events/s, all 10,500 subscriptions were forgotten, and 1.14 M cuts arrived as `shape_not_authorized`.
   - **The fix:** without an old tuple, the old row has the new row's identity columns and its other columns are unknown. A hold is cut only if that old row may have matched.
   - **Test:** `internal/hold/hold_identity_test.go` (four cases) fails without the fix and passes with it.
3. **Harness smoke flake (not Sluice behavior).** `cmd/smoke` "a snapshot row is encoded exactly as to_jsonb encodes it" failed once in three runs.
   - Sluice may deliver a row both from the snapshot and live: `snapshot.go` says duplicates are intended.
   - The test keeps only a row's last delivery, so a live duplicate overwrites `snapshot=true`.
   - Fix (test only): keep the snapshot delivery. Not done.

## Proposals for phase 3

### Order (as agreed, with what this phase adds)

1. **0.8.1:** the hold-index fix, measured in `../phase3/v0.8.1.md`. Whether bug 2's fix joins 0.8.1 is for you to decide.
2. **Cheaper work per change on the reader:**
   - candidates found by the most selective column, and only through it;
   - no map per evaluated filter or per change;
   - column lookup by position, not by lowercased name;
   - no `ParseFloat` attempt on text compared with text;
   - FULL's double evaluation only where the hold's filter reads a column that changed;
   - hold checks under a read lock, with a write lock only to remove a watch, so opens and refreshes stop queuing behind the reader;
   - per-change metrics with cached label children.

   Also part of this item:
   - the adaptive column choice (counting distinct values) and `pg_stats.n_distinct`, each measured before and after;
   - the real CPU of a no-destination UPDATE, separated from `DispatchSeconds` wall time.
3. **Work off the reader thread:** design only, no implementation without approval. The data here bounds what it can gain: the reader is ~55% of CPU, and at 2 CPUs half of its time is already idle.
4. **Degradation under saturation:** the proposal below.
5. **Then the rest:**
   - per-stream memory (B), using the heap figures above;
   - batched writer wake-ups (A);
   - the issuer and hook idle-connection timeout and `Idempotency-Key` (F);
   - the SDK's reconnect policy (E);
   - the grouped issuer call per stream, design only (G): `/token` and opens make 7–8 sequential calls per stream;
   - the README and ROADMAP fix about `resumed: false` after a restart.

   Deferred from this phase to after these changes: reconnect storms and resume coverage under cuts.

### Iteration method

- Go microbenchmarks for each change to the hot path:
  - `hold.Index.OnChange` with a realistic index: 12,000 streams, two-column holds, 80–250 members per room;
  - `expr.Eval` on hold filters;
  - tuple column lookup.
- Before and after each change: 10-min runs at very high (1 CPU) and the unicorn (2 CPUs). Compare CPU per 1,000 changes, live heap per minute and the reader's delay.
- At the end:
  - one 30–60 min run of the main combination;
  - the multi-hour stability run at very high;
  - the primary-key comparison again, valid once bug 2 is fixed.

### Degradation under saturation (proposal)

- **Measure it in seconds.**
  - Export the reader's delay in seconds (now minus the commit time of the transaction being dispatched; 0 when idle).
  - Export the share of time the reader is busy.
  - Export each busy table's resume-buffer coverage in seconds.
  - Today an operator sees only bytes, and only from PostgreSQL.
- **Tell the clients.** Past a configurable delay, open streams get a `warning` event (`lagging`, with the delay), and a second event when it recovers, so applications can show "updates delayed" instead of silence.
- **Protect the open streams first.**
  - Past a higher delay, refuse new streams with 503 and `Retry-After`, the way `SLUICE_MAX_STREAMS` already refuses. Opens add issuer and hold work that competes with the reader through the hold-index mutex.
  - Defer the catalog tick's hook re-checks and stagger `/token` refreshes while saturated.
  - `/readyz` reports degraded rather than failing, so a single-node deployment does not take itself out of its load balancer.
- **Before the slot is lost.**
  - Read `pg_replication_slots.safe_wal_size` and export it.
  - Warn in the log and to clients at 50% and 80% of `max_slot_wal_keep_size`.
  - Say plainly, in the warning and the docs, that past it every stream restarts with `resumed: false` and a snapshot.

## Files

- `raw/`: the clean, pprof and primary-key runs, as Markdown reports and gzipped JSON reports (`*.json.gz`; `loadtest summarize` reads them as they are).
  - Profiles are not in git, because they are large binaries: `*.cpu.pprof`, `*.heap*.pprof` (heap at 2 and 9 min from `pprof-unicorn-*`), `*.goroutine.pprof`, `*.block.pprof`, `*.mutex.pprof`, `*.trace.pprof`, 12 MB in all. They are kept with the run outputs, outside the repository's history, and can be attached to a release.
  - `queue.log`.
- `contaminated/`, `invalid/`: the runs set aside, with their reports.
- `notes.md`: what happened during the phase, method changes, and findings for the design guide (phase 4).
- Plans: `../../plans/phase2-tiers.plan`, `../../plans/phase2-baseline.plan`.
- Profiling build: `../../scripts/build-pprof.sh` and `cmd/sluice/pprof.go` (build tag `pprof`; see "Profiling" in the Sluice README).
