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
