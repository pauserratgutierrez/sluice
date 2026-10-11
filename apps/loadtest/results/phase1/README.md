# Phase 1 — a load test shaped like production

Goal of the phase: make `apps/loadtest` measure Sluice the way a product runs it — an issuer-mode Sluice in a 1-CPU, 512 MB container, chat rooms with hook channels, token renewals, big transactions and reconnect storms — before measuring anything. No Sluice code changed.

Raw results of every run below are in [`raw/`](raw/): one JSON (configuration, summary, verdict, whole timeline) and one Markdown summary per run.

## What changed in `apps/loadtest`

- **Compose.** Sluice runs with a production container's limits by default: `SLUICE_CPUS=1`, `SLUICE_MEM_LIMIT=512m`, `SLUICE_MAX_STREAMS=5000`, image `ghcr.io/pauserratgutierrez/sluice:0.8.0` (`scripts/run.sh -l` builds the working tree instead). PostgreSQL is `postgres:18.6-trixie` (`POSTGRES_IMAGE`). Every container can be pinned to its own cores (`SLUICE_CPUSET`, `DB_CPUSET`, `ISSUER_CPUSET`, `PROXY_CPUSET`, `RUNNER_CPUSET`; layouts in [`env/`](../../env/)).
- **Schema** [`sql/02-rooms.sql`](../../sql/02-rooms.sql): `room_messages`, `room_members` (replica identity `FULL` or the primary key's, switched per run), `notifications`, published together; a member's row is the issuer hold of every shape granted to them.
- **Issuer and hook** with latency (`5ms-20ms` uniform by default, or a value ± jitter), a concurrency limit with a queue (32 by default), and per-second call counts by kind.
- **Profiles** `rooms`, `bigtxn`, `reconnect` (restart of Sluice, or a cut at a TCP proxy in front of it) and `calibrate`. Streams are driven the way `packages/sluice-js` drives them: same backoff, `retry_after_ms`, resume map, retry of refused subscriptions, `/token` on a timer.
- **Measurements.** Latency per kind of event (messages, member updates, typing, notifications, replays apart) in an unbounded histogram; Sluice CPU per 1000 events and per 1000 decoded changes; RSS and Go memory against `GOMEMLIMIT`; GC pauses; four views of replication lag (Sluice's backlog from its live confirmed position, `sluice_wal_lag_bytes`, the slot's `confirmed_flush_lsn`, the walsender's `sent_lsn`); issuer calls per second; the runner's own CPU and RSS; every container's CPU and memory through the Docker socket.
- **Runner client** buffers 16 events per stream instead of 1 024 (57 KiB per connection before anything arrived); the new profiles parse on the reading goroutine with no channel at all.
- `scripts/run.sh` runs a profile detached and waits for the runner, because `--abort-on-container-exit` ends the whole run when the `reconnect` profile restarts Sluice.

The README documents every profile and variable.

## Validation runs (measured)

Laptop: Intel i7-12700H (6 P + 8 E cores, 20 threads), 32 GB, Windows 11, Docker Desktop 29.8.2 (WSL2 kernel 6.6.87.2, 20 vCPUs and 15.5 GiB visible). Plugged in, **Windows "Balanced" power plan** (Phase 2 needs the maximum-performance mode). Layout [`env/pin-20cpu.env`](../../env/pin-20cpu.env): Sluice on vCPUs 2–3 with a 1-CPU quota, which Go 1.26 turns into `GOMAXPROCS=2`; `GOMEMLIMIT` 461 MiB (90% of 512 MB). Issuer at `5ms-20ms`, concurrency 32. These runs check the harness; they are too small to say anything about capacity.

| Run | Streams | Load | Result |
| --- | ---: | --- | --- |
| `rooms`, `synchronous_commit=off` | 20 | 4 rooms × 20 members, a message every 5 s, typing | message p50/p95/p99 **177 / 375 / 402 ms**; typing 0.8 / 1.5 / 1.8 ms |
| `rooms`, `synchronous_commit=on` | 20 | same | message p50/p95/p99 **3.2 / 10.2 / 14.4 ms**; typing 0.9 / 1.8 / 2.2 ms |
| `bigtxn` | 200 | 10 rooms × 40, commits of 1 500 and 20 000 rows | all notifications delivered; 20 000 rows: 90 ms to commit, Sluice dispatched it 105 ms after the commit, backlog peaked at 5.7 MiB |
| `reconnect`, `cut` (×2) | 200 | 10 rooms × 40 | every stream back in 1.06–1.13 s, all 200 with every shape `resumed: true`, no message gap; issuer peak 965–1 018 calls/s |
| `reconnect`, `restart` | 200 | 10 rooms × 40 | every stream back in 10.1 s (`SLUICE_RECONNECT_SPREAD=10s`), issuer peak 649 calls/s, no message gap; see finding 2 for `resumed` |
| `calibrate` | 1 000 | fixed: 40 rooms × 100, 25 streams each, a message every 5 s, 2 typists | **183.6 ms of Sluice CPU per 1 000 events** (windows: 183.6, 183.2, 196.3), 2.44 ms per stream open; 1 325 events/s from 787 changes/s |
| `rls-a`, ladder 100 → 400 | 400 | capacity profile, unchanged | still works; p95 4 ms at 100 and 400 users (the README's 0.1.4 fan-out table, measured with `synchronous_commit=off`, shows 269–295 ms at 100–1 600 subscribers) |

All but the first `restart` attempt (aborted by Compose, which led to the detached runner in `scripts/run.sh`) passed their criteria.

## Findings so far

**Measured:**

1. **`synchronous_commit=off` in the old compose added 0–400 ms to every change's latency.** With it off, a commit returns before its WAL is flushed, and the walsender only sends flushed WAL, so changes wait for the WAL writer (`wal_writer_delay`, 200 ms). Typing broadcasts, which do not go through the WAL, were unaffected. The default is now PostgreSQL's (`on`); every report records the server's setting. The latencies in the load-test README's older tables were measured with it off.
2. **A restart does not always lose the resume buffer.** After `docker restart`, `room_messages` and `room_members` came back `resumed: true` on all 200 streams, and only `notifications`, which had seen no change, came back `false`. The new process starts replicating from the slot's `confirmed_flush_lsn`, which trails what the old one dispatched by up to `SLUICE_STATUS_INTERVAL` (10 s); PostgreSQL sends everything after it again, the buffer refills, and a client whose last position is newer than that can be covered. Sluice's README and ROADMAP say every subscription comes back `resumed: false` after a restart. The message-sequence check found no gap. To be measured at scale in Phase 2 (it depends on how far back each table's position is and on the 4 096-event buffer; see finding 6).
3. **Most of Sluice's CPU in this workload goes to changes nobody receives.** In `calibrate`, 787 changes/s are decoded to deliver 1 325 events/s: each message is 1 `INSERT` and 100 `UPDATE`s with `REPLICA IDENTITY FULL`, and only the 25 streamed members' updates are delivered. Dispatch averaged 167 µs per change. CPU per 1 000 events (184 ms) is far from the ~44 ms of the routed-events comparison because the denominators differ; reports now also give CPU per 1 000 decoded changes.
4. **GC pauses up to 33–52 ms** at 200–1 000 streams (`go_gc_duration_seconds` maximum). With `GOMAXPROCS=2` under a 1-CPU quota, a stop-the-world phase that meets CFS throttling lasts until the next quota period; this is the hypothesis to check in Phase 2, not a conclusion.

**From the code, not measured yet:**

5. **A reconnect storm costs the issuer 8 calls per stream** (7 shapes and the hook channel). At 12 000 streams that is ~96 000 calls. With the default issuer (32 at once, 12.5 ms average) that is ~2 560 calls/s, about 37 s of work, while Sluice gives the issuer `SLUICE_ISSUER_TIMEOUT=2s` per call: expect `issuer_unavailable` and retries. `reconnect` at 12 000 streams will measure it.
6. **The resume buffer holds every change, delivered or not, per table, up to `SLUICE_RING_EVENTS=4096`.** `room_members` receives `N` updates per message, so at the unicorn tier's ~2 300 changes/s its buffer covers under 2 s: a client that reconnects later than that cannot resume `room_members`. Phase 2 will measure the share of streams that resume after a cut, per tier.
7. **The hook is asked again for every joined stream on each catalog tick** (`ttl: 0`, `SLUICE_CATALOG_REFRESH=30s`), at most 16 calls at once: 12 000 streams at 12.5 ms each is ~9.4 s of every 30 s tick. `POST /token` asks the issuer once per shape, one after another (7 × ~12.5 ms plus a hold `EXISTS` each).

## Claims of the brief, checked against the code

| Claim | Holds | Where |
| --- | --- | --- |
| The SDK's first reconnect waits a random 0–500 ms | yes: `DEFAULT_BACKOFF = [500, 1000, 2000, 5000, 10_000]`, full jitter, and the step resets when a stream opens | `packages/sluice-js/src/client.ts` |
| `runStream` ignores `Retry-After` on `POST /stream`; only `retry_after_ms` from events | yes | `client.ts`, `runStream` |
| `SLUICE_MAX_STREAMS=0` refuses every stream | yes: `if s.hub.Count() >= s.cfg.MaxStreams` | `internal/server/server.go` |
| `writeEvents` calls `SetWriteDeadline` per frame | yes | `server.go`, `writeEvents` |
| `OnChange` does 4 `WithLabelValues` per change | yes: `WALMessages`, `Changes`, `DispatchSeconds`, `RoutingCandidates` | `internal/server/dispatch.go` |
| `writeFrame` runs `json.Marshal` per stream; the row is encoded once (`event.Row`) | yes | `server.go`, `internal/event` |
| The README says about 8 000 streams of five shapes and a channel fit in 512 MB | yes, in the `SLUICE_MAX_STREAMS` row; to be measured in Phase 2 | `README.md` |
| The resume buffer is in memory and a restart sends everyone back `resumed: false` | in memory, yes; but see finding 2 | `internal/hub/ring.go` |
| Every `UPDATE` is decoded and routed even when it reaches no stream | yes, and also buffered for resume and checked against holds | `dispatch.go`, `OnChange` |
| Caddy closes streams 5 s after a reload | not in this repository (production configuration); the `cut` mode reproduces a proxy that drops every stream at once | — |

## Assumptions to confirm before Phase 2

- Each member holds one stream, belongs to one room, and the issuer and hook answer from memory after the configured latency. The issuer serves **32 calls at once** by default; the real backend's limit changes every reconnect result.
- The 7 shapes of a stream are: the room's messages, the member's memberships, their notifications, and 4 fillers that cost a shape but carry no traffic.
- Typing is `K` members per room publishing every 2 s for the whole run (a fraction is an average across rooms), not bursts around each message.
- Messages are spread at random phases; the "burst" column of the tiers is what that produces, reported as the busiest second.
