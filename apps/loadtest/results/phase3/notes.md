# Phase 3 — notes kept while working

## Priorities (set by the user, 2026-10-10)

Raised to the top of phase 3, after 0.8.1:

1. **The hold's index column must not depend on the issuer knowing the order.** 0.8.1 keys a hold on the first of its equally good columns, and the README asks issuers to put the most selective first. Measured cost of getting it wrong (0.8.1, very high, first 10 min): 365 ms of CPU per 1,000 changes room-first against 95 ms user-first. Candidates:
   - adaptive: count distinct values per column in the index;
   - `pg_stats.n_distinct`;
   - indexing a hold under every equally good column and evaluating only the shortest list.

   Each one is measured before and after.
2. **Hold checks must not block the lock that stream opens and `/token` take.** In 0.8.0 the reader held `hold.Index`'s mutex while it evaluated every candidate. That was 86–93% of all lock waiting in the process, and `/token` p50 rose to 127 ms at 2 CPUs and 404 ms at 1 CPU (phase 2, mutex profiles). Candidates:
   - evaluate under a read lock and take the write lock only to remove a watch;
   - a copy-on-write index per relation.

3. **Admission control by memory (asked 2026-10-10, not in 0.8.1).**
   - `SLUICE_MAX_STREAMS=0` becomes the default and means automatic: Sluice refuses new streams when its live memory nears GOMEMLIMIT. The threshold is configurable, for example 85%, with hysteresis so that admission does not flap.
   - A value above 0 is an extra fixed cap on top of that.
   - Document the margin a reconnect storm needs, and measure it with the `reconnect` profile.
   - Motivation: `v081f-unicorn-1cpu-512m`. 12,000 streams in 512 MB with the cap raised to 15,000 was killed 11 times for memory. Some kills came at only 144–382 MiB of Go memory five seconds earlier: thousands of streams reconnecting at once spike memory between two samples.

Then the order agreed before:
- cheaper work per change on the reader;
- work off the reader thread (design only);
- degradation under saturation;
- B, A, F, E, G;
- the README and ROADMAP fix about `resumed: false`.

## The subscription index and UPDATEs without an old tuple (reviewed for 0.8.1)

`server.deliver` treats an UPDATE without an old tuple as a row that was already in the shape (`matchOld, unknownOld = matchNew, unknownNew`). So:
- it never makes up an `enter` or a `leave`;
- it never drops a subscription;
- routing (`registry.Candidates`) uses the new row only.

What it cannot see is a row leaving a shape through a column outside the replica identity. That is the documented limitation:
- `replica_identity_insufficient` warns of it at subscribe, when the shape wants `DELETE` or transitions;
- `SLUICE_REPLICA_IDENTITY=strict` refuses such shapes.

No change needed for 0.8.1.

## 0.8.1, second fix: how the hold rule is kept

The rule: an UPDATE without an old tuple cuts no hold. It rests on every hold filter reading only replica-identity columns. Where that is enforced:
- **At grant:** `oracle.parseHolds` refuses the hold.
- **On each catalog refresh:** the `RefreshLeases` loop drops the shape.
- **New in 0.8.1:** `OnRelation` drops it at once, on the reader goroutine, before the next change is dispatched. It runs on every Relation message, including the first one after a restart, using the identity the WAL carries rather than the catalog's.

## Item 1: retention per `/token` (2026-10-10 ~23:30–00:30 UTC)

- **Go test** (`internal/server/token_retention_test.go`, branch `phase3`): 300 streams × 7 shapes renewed 30 times, 63,000 renewals in all. The live heap after GC went from 9,479 to 9,664 KiB, and both indexes kept their exact sizes (entries, constants, capacity).

  The test's oracle answered with a fresh grant and ignored the request's filter, so it could not see what follows.
- **Load run on the released 0.8.1** (`p3-token-unicorn-1cpu`): the unicorn at 1 CPU, `LOAD_TOKEN_EVERY=2m` (about 800 renewals a minute become 6,000).
  - The live heap after each GC rose from about 609 to about 712 MiB in 10 minutes, 13.7 MiB a minute.
  - Goroutines stayed flat at about 24,600, so no connection is left open.
- **Heap profiles** of the same run on a pprof build of v0.8.1 (`p3-token-unicorn-1cpu-pprof`): 593 → 619 → 695 → 707 MB at minutes 1, 4, 7 and 9.5.
  - The growth is parsed filters: `shape.Narrow` +116 MB cumulative, `shape.Parse` +85, `compileTerm` +30, `expr.And` +14.
  - It went on after every stream had renewed at least once (from minute 4).
- **Cause (Sluice bug, in 0.8.0 and 0.8.1).** `refreshIssuerShape` passed the subscription's effective filter to the issuer as the client's request, and `shape.Narrow` joins the grant's filter text to it. Every renewal therefore added the grant's terms once more (`project_id=eq.42,project_id=eq.42,…`). The consequences:
  - memory per shape grew with every renewal;
  - the cost of matching every change to the table grew with it;
  - the issuer's `requested.filter` grew too;
  - a grant that widened could not widen the shape back.

  At about 320 B per shape and renewal, with `/token` every 15 minutes: about 13 MB an hour at 1,500 streams of 7 shapes, about 100 MB an hour at 12,000.
- **Fix (0.8.2 candidate, branch `fix/token-filter`, `f9b9864`).** A subscription keeps the client's request (filter, columns, ops), and a refresh asks about that. Two tests (`token_refresh_filter_test.go`) use the real issuer client against an `httptest` issuer, and both fail without the fix: 50 renewals left 51 copies of `project_id=eq.42`, and a widened grant stayed narrow.

## Items 2–7 (2026-10-10 ~00:20–01:30 UTC), measured with microbenchmarks and short runs, as the user asked

- **Item 2.**
  - `BenchmarkMemberUpdate` (400 rooms × 28 streams × 7 holds; a member update with FULL's old tuple):
    - v0.8.1: room first 107 µs, member first 3.8 µs;
    - `phase3`: 4.8 and 4.3 µs.
  - The extra 0.5 µs member first is item 3's copy of the candidates.
- **Item 3.** `BenchmarkReplaceDuringHoldChecks`, in a Linux container: per-call times on the Windows host read 0 or ~1 ms, since its clock is too coarse. Item 2 alone against items 2+3:
  - mean 11–12 µs → 2.3–2.4 µs;
  - p99.9 ~2,100 µs → ~320–380 µs;
  - max 2.6–4.2 ms → 0.8–1.4 ms.
  - p50 (~1 µs) and p99 (~6–9 µs) barely moved.
- **Item 4, first version** (`p3i4-unicorn-1cpu-512m`, live heap ≥ 0.85 of `GOMEMLIMIT`):
  - 7,215 admitted, no kill;
  - RSS 510 of 512 MiB, the collector ran without pause;
  - backlog up to 25 MiB, message p95 62 s;
  - 985k refusals each verified a JWT and logged a warning.
- **Item 4, second version** (`p3i6-unicorn-1cpu-512m`, memory that cannot be freed ≥ 0.5):
  - 3,022 admitted, message p95 4.8 ms;
  - 0.36 cores while refusing about 1,640 opens a second; RSS peak 408 MiB;
  - 578 log lines.
  - The runner then did not honor `Retry-After`; it does now.
- **Items 5, 6, 7F, 7E.** Covered by tests, each failing without its change.
- **Item 7A+B** (`p3i6-unicorn-1cpu-5m` against `p3i7-unicorn-1cpu-5m`):
  - CPU per 1,000 changes 120 → 103 ms;
  - live heap 647 → 419 MiB;
  - Go memory 1,450 → 1,027 MiB;
  - message p95 8.0 → 9.3 ms.
  - The heap profile at 12,000 streams (0.8.1) showed parsed filters at 44% of the heap, and HTTP `bufio` buffers per connection at 22% (about 11 KiB per stream). The buffers are untouched.
- **Shared filters across catalog refreshes.** The catalog builds new `Relation` objects at every refresh, so the table keys on OID and columns, not on the pointer. Found when the `/token` retention test grew under `-race`: its oracle made new relations per call, so every renewal added table entries that cleanups removed only later.
- **The aborted 75-minute queue** (`phase3-holds.plan`): stopped at the user's request during its first run (`p3-v081-unicorn-1cpu-roomfirst`). The runner wrote a partial report on the way out: 74 s of steady phase on the released 0.8.1 with holds room first, at the unicorn on 1 CPU. It dispatched 1,503 changes/s of the 2,320 offered at 618 ms of CPU per 1,000 changes and 0.93 cores, falling behind. On the 0.9.0 candidate the order no longer matters (103 ms member first; equal in the microbenchmark).

## Review fixes and the storm at the limit (2026-10-10 ~04:30–07:10 UTC)

The user's review of `phase3` found two admission bugs, both reproduced with unit tests first:
- opens in progress were not counted, so a storm's simultaneous opens all passed on the same number of streams (fix: a reservation per open, released when the hub counts it);
- after a mass disconnect the floor still held the departed streams until the next collection (fix: closed streams are taken off the projection, down to 20% of the floor, and a stale floor forces a collection at most every 5 s).

The storm test the user made a condition for tagging: 1 CPU, 512 MB, default admission (`SLUICE_MAX_STREAMS=0`, `SLUICE_ADMIT_MEMORY=0.5`), the rooms profile offering 6,000 streams, a proxy cut 4 minutes into the steady phase (5 s down). Criteria: no kill for memory; refusals are clean `503`s with `Retry-After`; after the cut, every stream that had fit is back within 30 s; a reasonable message p95. Plan: `../../plans/phase3-storm.plan`.

- **`p3-storm-512m`** (image `p3-final2`, floor = everything the runtime could not free, fragmentation included):
  - 4,209 admitted, 263,109 refusals, every one with `Retry-After`; no kill, RSS peak 386 MiB; message p95 5.9 ms.
  - Failed the refill: after the cut, 2,647 streams were back at 30 s, 2,911 at 60 s, and 3,475 (82.5%) at 4 minutes, short of 4,209 until the end.
  - Cause: with fragmentation in it, the floor swung between 216 and 309 MiB at a steady 4,209 streams while the live heap stayed at 140–150 MiB.
  - Fix (`d12c6cb`): the floor is the live heap, the stack space goroutines use (`/gc/scan/stack`) and the runtime's own structures.
- **`p3-storm-512m-r2`** (image `p3-final3`):
  - 5,610 admitted, 78,059 refusals, every one with `Retry-After`; no kill, RSS peak 460 MiB, Go memory 452 of 461 MiB; message p95 8.4 ms.
  - Failed the refill: 4,177 back at 30 s, then stuck at 4,853 of 5,610 (86.5%) for more than 3 minutes, until the share dipped to 0.45 and it reached 5,402. At 4,853 streams `/metrics` read `sluice_admission_memory_share` 0.456.
  - Cause: the storm's replays and garbage lifted the floor past 0.5 for a few collections; it then settled at 0.456, above the 0.45 the hysteresis asks for, so admission never reopened.
  - Fix (`ff7d4ad`): admission also reopens once the projection has stayed below the threshold for 10 s.
  - New finding: 279 jumps in a room's message sequence after a resume that said `resumed: true` (0 in every earlier run). Cause, in the code since before 0.8: the replay runs on its own goroutine while the reader keeps delivering, so a change committed during the replay could reach the stream ahead of the replayed ones. A unit test reproduced it (the new change arrived before eight replayed ones). Fix in the same commit: a covered resume installs the shape with a `Backfill`; the reader leaves its changes to the replay, which catches up from the buffer by append order and hands over under the lock the reader checks.
  - Found while fixing it, both in the code before 0.8 too: a change was routed before it was buffered, so a shape installed between the two missed it; and a replay ignored the shape's `ops` for inserts and deletes.
- **`p3-storm-512m-r3`** (image `p3-final4`, `ff7d4ad`):
  - 5,767 admitted, 53,621 refusals, every one with `Retry-After`; no kill, RSS peak 452 MiB; message p95 8.9 ms over the run.
  - 0 jumps after a resume (279 in r2).
  - Refill after the cut: 3,854 at 20 s, 4,494 at 30 s, 5,096 at 60 s (88%), 5,351 at 2 min, 5,508 at 3 min, 5,528 at 4 min (95.9%). Admission no longer stuck: its share swung between 0.47 and 0.51 as it reopened every 10 s.
  - Why not all of them: in the minute after the cut the live heap after a collection reached 208 MiB with fewer streams (the storm's replays and garbage), and for minutes after it each stream held more memory: 85 MiB of goroutine stacks for 5,134 streams against 70 MiB for 5,767 before. Go starts new goroutines with stacks sized by the average use it measured, and the storm raised it.
  - New finding: 405 clients lost their room's hook channel. A join the hook gave no verdict on (the issuer service peaked at 4,733 calls a second) was refused with `channel_not_authorized`, which is not retryable. Fixed in `a692c6e` (`channel_hook_unavailable`, retryable; the SDK's channel recovers when the retried join is accepted).
- **`p3-storm-512m-cap4900`** (same image, `SLUICE_MAX_STREAMS=4900`, 85% of what r3 admitted): 4,900 open before the cut; 4,350 at 20 s, 4,729 at 30 s (96.5%), 4,853 at 45 s, all 4,900 at 55 s. RSS peak 455 MiB, message p95 7.6 ms.
- **Decision (user):** accept the refill at the admission threshold and document it, with a cap at about 85% of what admission lets in for operators who need every stream back within a minute.
- **`p3-storm-512m-r4`** (image `p3-final5`, `a692c6e`, the release candidate): 5,759 admitted, 55,087 refusals with `Retry-After`; no kill, RSS peak 458 MiB; message p95 9.5 ms; 0 jumps after a resume. Back after the cut: 4,545 at 30 s, 4,853 at 60 s (84%), 5,549 at 4 minutes (96%). 16 hook joins were refused with `channel_hook_unavailable` and retried; typing delivery 100.00% (97.68% in r3).
- **Checks on `d353ba3`:** `go vet`, `go test -race` (Linux), SDK 35 tests, `cmd/smoke` 45/0, `cmd/smoke-issuer` 21/0, `cmd/audit` 200/200, `live.mjs` 19/0.

## The user's second review (2026-10-11 ~01:00–02:00 UTC)

- Both admission fixes of the first review are in `95f1ea3`: the reservation per open (`server.go`, used for admission and `SLUICE_MAX_STREAMS`; tests `TestAdmissionCountsOpensInProgress`, `TestStreamOpensReleaseTheirReservations`, and now `TestMaxStreamsCountsOpensInProgress`), and closed streams taken off the projection with a debounced forced collection (`TestAdmissionAfterAMassDisconnect`, `TestAdmissionForcesACollectionWhenItsFloorIsStale`).
- New criterion: at least 99% back within 30 s at the threshold, no cap, no kill.
- The r4 timeline: right after the cut the floor was not stale (2,209 streams back within 5 s). At 4,740–4,853 streams it measured 206–212 MiB (0.45–0.46), and every few collections 236–252 MiB (0.51–0.55): the refill's own allocations, counted live because they were made during the collection. Each such reading started the 10 s hold.
- **`p3-storm-512m-r5`** (`239cbb8`, image `p3-final6`): up to the most streams held below the threshold in the last 5 minutes, admission asked only that the measured floor stay below 0.6, without the projection. Every client reopened at once: 30,386 goroutines and a live heap of 424 MB at 3,951 streams, Go memory 848 MiB against a limit of 461, reader lag 127 s, 43,311 `issuer_unavailable`, 1,812 streams back, message p95 200 s at the end. No kill. Reverted in `e76279f`.
- The same run showed where the refill's memory went: each resumed subscription copied every change to its table since its position (about 250 for a room's messages, ~100 bytes each) and held it while it waited for its queue.
- **`p3-storm-512m-r6`** (`e76279f`, image `p3-final7`): a resume copies only the changes routed to its shape, and one with nothing to replay hands over without a goroutine. 5,777 admitted; no kill, RSS peak 451 MiB; message p95 7.9 ms; 0 jumps after a resume. Back after the cut: 4,146 at 30 s (72%), 5,302 at 60 s (92%), 5,558 at 4 minutes (96%). Collections during the refill still read 0.51–0.56 (2,724 streams at 218 MiB, 4,129 at 228, 4,942 at 259), and at the end the node sat at the threshold with 96% of the streams.
- **Checks on `e76279f`** (with `main` merged in): `go vet`, `go test -race`, the `pprof` build, the load test's tests, SDK 35/0, `cmd/smoke` 45/0, `cmd/smoke-issuer` 21/0, `cmd/audit` 200/200, `live.mjs` 19/0.
- **`p3-stability-8h`** (image `p3-final7`, `e76279f`), 01:46–09:47 UTC: pass. Live heap with all streams open, median per hour: 53.6 MiB (hour 1), then 54.1–54.2; 10th percentile 52.9 → 53.4 MiB in 8 hours. Floor 69–70 MiB, RSS 166–170 MiB, goroutines ~3,025, indexes equal at start and end. No backlog; slot lag peak 2.15 MiB; message p95 9.5 ms, p99 17.8 ms. 6 `issuer_unavailable`, 2 `/token` calls cut by a reconnect. 7 cuts, each with all 1,500 streams back within 15.1 s. Collector pauses of 48–51 ms at cuts 1, 2, 6 and 7.
