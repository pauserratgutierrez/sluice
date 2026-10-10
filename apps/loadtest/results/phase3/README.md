# Phase 3 — Improvements to Sluice

This phase is about the improvements that came out of phase 2 (`../phase2/README.md`):
- 0.8.1 fixed the hold index and was measured against 0.8.0 (`v0.8.1.md`).
- 0.8.2 fixed a retention on `POST /token`.
- Items 2–7 are on branch `phase3` and are proposed as 0.9.0.

Every figure is measured unless marked otherwise, and names the runs or benchmarks it comes from. The method agreed for this phase:
- Go tests and microbenchmarks to iterate.
- Short load runs before and after a change, on the same host and pinning as phase 2. The user asked for those over 10-minute runs halfway through the phase.
- One accelerated stability run at the end.

`notes.md` logs the work as it went.

## Results by item

| # | Item | Before | After | Measured with |
| --- | --- | --- | --- | --- |
| 1 | Retention per `POST /token` (released as 0.8.2) | live heap +13.7 MiB/min with tokens every 2 min at 12,000 streams; `/token` p95 528 ms | +0.9 MiB/min; `/token` p95 289 ms | `p3-token-unicorn-1cpu` (0.8.1) against `v082-token-unicorn-1cpu`; heap profiles of `p3-token-unicorn-1cpu-pprof` |
| 2 | Hold index column independent of the filter's order | a member update, holds room first: 107 µs (member first: 3.8 µs) | 4.8 µs room first, 4.3 µs member first | `BenchmarkMemberUpdate` (400 rooms × 28 streams × 7 holds), v0.8.1 against `phase3`; and, partial (74 s), 0.8.1 room first at the unicorn on 1 CPU fell behind at 1,503 of 2,320 changes/s, 618 ms per 1,000 changes (`p3-v081-unicorn-1cpu-roomfirst`) |
| 3 | Hold checks off the index lock that opens and `/token` take | `Replace` during back-to-back hold checks: mean 12 µs, p99.9 ~2.1 ms, max 2.6–4.2 ms | mean 2.4 µs, p99.9 ~0.35 ms, max 0.8–1.4 ms | `BenchmarkReplaceDuringHoldChecks` in a Linux container (the Windows host's clock is too coarse for per-call latency), item 2 alone against items 2+3 |
| 4 | Automatic admission by memory | 12,000 streams offered to 1 CPU and 512 MB with the cap at 15,000: killed for memory 11 times | 3,022 admitted, the rest refused with `503` and `Retry-After`; no kill; message p95 4.8 ms; 0.36 cores while refusing ~1,600 opens a second | `v081f-unicorn-1cpu-512m` against `p3i6-unicorn-1cpu-512m` |
| 5 | A hold check that could not be made is retryable | `shape_not_authorized`, not retried: a `/token` that timed out under load lost its shapes | `hold_check_unavailable`, retryable after `retry_after_ms`; a missing hold row still denies | `TestHoldCheckFailuresAreRetryableAndMissingHoldsDeny` (fails on the old classification) |
| 6 | Visible degradation: the reader's lag in seconds | lag only in bytes, from PostgreSQL | `sluice_reader_lag_seconds`; `/readyz` `reader_lag_seconds`, `"degraded": "reader_lag"` past `SLUICE_LAG_DEGRADED`, `503` past `SLUICE_READY_MAX_LAG` when set | `TestLagFollowsTheTransactionBeingDispatched`; `/readyz` read during `p3i6-unicorn-1cpu-512m` |
| 7A | Writer woken once per transaction | 120 ms of CPU per 1,000 changes (unicorn, 1 CPU) | 103 ms, together with 7B; message p95 8.0 → 9.3 ms (events wait up to 5 ms for the commit) | `p3i6-unicorn-1cpu-5m` against `p3i7-unicorn-1cpu-5m` |
| 7B | Identical filters shared | live heap 647 MiB (55 KiB per stream), Go memory 1,450 MiB | 419 MiB (36 KiB per stream), 1,027 MiB | same runs |
| 7F | Issuer and hook calls on a closed keep-alive | the call failed with `EOF` (`issuer_unavailable`) | re-sent on a new connection (`Idempotency-Key`, idle connections closed after 4 s) | `TestIssuerRetriesARequestOnAConnectionTheIssuerClosed` (fails without the key) |
| 7E | SDK `Retry-After` | a refused open was retried after 1 ms | after the `Retry-After` asked for, plus up to 10% | SDK test (fails without the change) |

All of 0.8.1 → 0.9.0 against the 0.8.0 baseline, the unicorn's 2,320 changes a second at 1 CPU:
- **0.8.0:** fell behind, about 1,950 changes/s dispatched and falling, at 499 ms of CPU per 1,000 changes.
- **0.9.0 candidate:** keeps up at 103 ms per 1,000 changes and 0.24 cores, with 36 KiB of live heap per stream against 46–48 KiB on 0.8.1 (55–64 KiB on 0.8.0).

## Item 1: the retention per `/token` (0.8.2)

- **The run.** With every token renewed every 2 minutes, about 6,000 renewals a minute, the live heap after each collection rose 13.7 MiB a minute on the released 0.8.1. Goroutines stayed flat.
- **The profiles.** Heap profiles at 1, 4, 7 and 9.5 minutes of a pprof build showed the growth in parsed filters: `shape.Narrow` +116 MB, `shape.Parse` +85.
- **The cause.** `refreshIssuerShape` passed the subscription's effective filter to the issuer as the client's request, and `shape.Narrow` joined the grant's terms to it again, so each renewal added them once more.
- **The fix.** A subscription keeps what the client requested, and a refresh asks about that.
- **The tests.** Two tests run against the real issuer client and an `httptest` issuer, and both fail without the fix: 50 renewals left 51 copies of `project_id=eq.42`, and a grant that widened stayed narrow.
- **Release.** Published as 0.8.2 (`ghcr.io/pauserratgutierrez/sluice:0.8.2`, `@pauserratgutierrez/sluice-js@0.8.2`).
- **What remains.** At 0.9 MiB a minute, the growth under the same load is not settled. The stability run below measures it again on the 0.9.0 candidate.

## Item 4: why 0.5 of the memory that cannot be freed

- **First version.** It measured the live heap alone against 0.85 of `GOMEMLIMIT`. With 12,000 streams offered to 512 MB, it admitted 7,215 and the process was never killed. But the two goroutine stacks per stream and the rest of the runtime's memory, which `GOMEMLIMIT` also counts, filled the limit: RSS stood at 510 of 512 MiB. The collector ran without pause, the reader fell 25 MiB behind, and messages arrived 30–60 s late (p95 62 s; `p3i4-unicorn-1cpu-512m`).
- **What admission measures now.** What a collection cannot give back: the live heap plus the runtime's memory outside it. The default threshold is 0.5 of the limit, so the heap can still double between collections.
- **Capacity decisions come first.** Refusals are decided before the token is verified, and logged once a second with their count.
- **Logged refusals.** 578 lines instead of about 985,000.
- **Per 512 MB.** About 3,000 streams of this workload, near the ~3,500 the phase 2 capacity table extrapolated.

## Stability

**The run** (`p3-stability-30m`, `../../plans/phase3-stability.plan`, image `sluice:p3-final` = branch `phase3` at `99c8662`):
- the very high tier (1,500 streams) at the production limits (1 CPU, 512 MB, automatic admission);
- tokens renewed every 2 minutes: 22,539 renewals;
- 2% of the streams ended and reopened every minute: 900 in all;
- one network cut through the proxy at minute 15: all 1,500 streams dropped and reconnected.

**Verdict:** pass. Every message, member update and typing event arrived (100–102%: changes replayed on resume count twice). Message p95 was 11 ms over the whole run, the cut included.

| | Start of steady (30 s) | End (1,835 s) |
| --- | ---: | ---: |
| Streams | 1,500 | 1,500 |
| Hold watches / routed / constants | 10,500 / 10,500 / 1,500 | 10,500 / 10,500 / 1,500 |
| Hold lists' capacity | 12,000 | 12,056 |
| Subscriptions / routed / constants | 10,500 / 10,500 / 3,055 | 10,500 / 10,500 / 3,055 |
| Live heap (the gauge at those samples) | 52.3 MiB | 55.7 MiB |

- **The indexes end the size they started.** Only a list's spare capacity grew, by 56 slots.
- **The live heap.** After each collection it stayed between 52.5 and 54.8 MiB, with single readings up to 79 MiB: garbage allocated during a collection, gone at the next. Its per-minute minimum went from 52.3 MiB (minute 1) to 53.3–53.9 MiB (minutes 26–30), 1–1.5 MiB in 30 minutes.
  - On 0.8.1, the same tier with tokens every 15 minutes grew 39% in 29 minutes (`../phase2/README.md`).
  - Whether the 1–1.5 MiB is warm-up or a small retention cannot be told from 30 minutes. The overnight run is prepared for that.
- **The memory a collection cannot free** went from 92–105 MiB (minutes 1–14) to 105–118 MiB (minutes 16–30). It is noisy, since it includes heap fragmentation, and it stepped up after the cut. Goroutines (~3,080) and RSS (175–230 MiB) stayed flat.
- **Counters:**
  - 900 `churned`;
  - 1,500 `stream_end:unexpected_eof` (the cut);
  - 5,651 `open_failed:connection_refused` (opens while the proxy was down);
  - 2,469 `msg_duplicate` (at-least-once delivery on resume).
- **Resume after the cut.** `room_members` resumed 3,884 times and could not resume 8,116 times. At 337 changes a second, its 4,096-event buffer covers about 12 s, less than the cut plus backoff. `room_messages` and `notifications` always resumed.

**Overnight run, prepared and not run** (about 8 h 10 min): `scripts/queue.sh plans/phase3-stability-night.plan results/phase3/raw`, after `docker build --build-arg VERSION=0.9.0-rc -t sluice:p3-final ../sluice-0.8.1` (branch `phase3`). It has tokens every 15 min, the same churn, and a cut every hour.


## Not done in this phase

- Work off the reader thread (design only): not started.
  - With hold checks cheap, the reader is far from saturated: 0.24 cores carry the unicorn's 2,320 changes a second on 1 CPU.
  - Its ceiling per slot is now an extrapolation: about 9,700 changes a second per core at 103 ms per 1,000.
- The README and ROADMAP fix about `resumed: false` after a restart.
- The design of a grouped issuer call per stream (G).
- Reconnect storms and resume coverage under cuts, deferred from phase 2. The stability run includes one cut. Storms at 12,000 streams with admission are still to measure, and so is the margin admission needs for them.

## Files

- `v0.8.1.md`: 0.8.1 against 0.8.0.
- `raw/`: the runs (Markdown reports, gzipped JSON). Profiles stay out of git.
- `notes.md`: what was done and found, in order.
- Plans in `../../plans/`:
  - `phase3-token.plan`, `v0.8.2.plan`;
  - `phase3-holds.plan` (stopped during its first run, whose partial report is kept);
  - `phase3-admission.plan`, `phase3-item7.plan`;
  - `phase3-stability.plan`, and `phase3-stability-night.plan` (prepared, not run).
- Sluice branches:
  - `fix/token-filter`: 0.8.2, merged and released.
  - `phase3`: the 0.9.0 candidate, on top of 0.8.2.
