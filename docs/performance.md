# Performance and sizing

What Sluice costs in CPU and memory, measured with [`apps/loadtest`](../apps/loadtest/README.md) on Sluice 0.11.0. Each figure names the run it comes from; the reports are in [`apps/loadtest/results/characterization/`](../apps/loadtest/results/characterization/) and the runs in [`plans/characterization.plan`](../apps/loadtest/plans/characterization.plan). Where a figure is an estimate rather than a measurement, it says so.

[Designing tables for realtime](realtime-design.md) turns these figures into rules for the schema and the shapes.

## Contents

- [How it was measured](#how-it-was-measured)
- [Summary](#summary)
- [CPU: changes per second](#cpu-changes-per-second)
- [Writes per action](#writes-per-action)
- [Replica identity](#replica-identity)
- [Memory per stream](#memory-per-stream)
- [The resume buffer](#the-resume-buffer)
- [Reconnects](#reconnects)
- [Big transactions](#big-transactions)
- [RLS mode](#rls-mode)
- [Sizing a node](#sizing-a-node)

## How it was measured

**The machine.** A laptop with an Intel Core i7-12700H (14 cores, 20 threads), running Docker Desktop's WSL2 VM, plugged in and in its maximum-performance power mode. Each container has its own cores ([`env/pin-20cpu.env`](../apps/loadtest/env/pin-20cpu.env)):
- Sluice gets two hardware threads with a quota of 1 CPU, so Go runs with `GOMAXPROCS=2` under a 1-CPU limit, as in a 1-CPU container on a larger server;
- PostgreSQL 18.6 gets four, with `synchronous_commit` on;
- the issuer two, the TCP proxy two, and the load generator six.

Nothing else ran on the host while a run measured: process CPU per change rises by 10–30% with unrelated work on the same machine.

**The workload** (the `rooms` profile, a generic chat; [`sql/02-rooms.sql`](../apps/loadtest/sql/02-rooms.sql)). Sluice runs in issuer mode, with a test issuer that answers in 5–20 ms.
- Rooms of 80 members (and a few of 200–250), some of whom have a stream open.
- Each stream has 7 shapes and the room's channel, joined through a hook:
  - the room's messages (`room_id=eq.R`);
  - the member's own memberships (`user_id=eq.U`);
  - their notifications;
  - four more shapes that cost what a shape costs (an issuer call, a hold, an index entry) but receive nothing.
- Every shape is held by the member's `room_members` row.
- Each room commits a message every 15 s: one `INSERT` into `room_messages` and an `UPDATE` of `last_message_at` on every member's row, in one transaction. A message is therefore 1 + 80 changes, of which one reaches each open stream as a message and one as a member update.
- Typing goes over the room's channel, every 2 s for some members, and never touches the WAL.
- Tokens are renewed every 15 minutes (`POST /token`). The hook answers `ttl: 0`, so Sluice asks it again about every joined channel on every catalog tick (30 s).

| Tier | Streams | Changes/s | Events/s delivered | Limits |
| --- | ---: | ---: | ---: | --- |
| high | 400 | 89 | ~130 | 1 CPU, 512 MB |
| very high | 1,500 | 337 | 502 | 1 CPU, 512 MB |
| unicorn | 12,000 | 2,320 | 3,470 | 1 CPU, 4 GB |

**The builds.** The steady runs (CPU, writes per action, replica identity, memory, big transactions, RLS) ran on `main` at `d38471e`, with the resume buffer keeping 60 s. The two commits after it, before 0.11.0, change what a resume costs, add 32 bytes to each buffered change, and set the buffer's default to 30 s; the reconnect runs are on the last of them (`320dff3`). Where a figure depends on the buffer, it says which age it ran with.

A run passes when, in its steady phase, no stream is closed for lagging, every kind of event has a p95 under 500 ms, at least 99% of the expected events arrive, Go's memory stays under `GOMEMLIMIT`, and the backlog of WAL received but not dispatched stays under 64 MiB and does not grow. Every run below passed.

## Summary

- **CPU follows the WAL's changes**, not the events delivered: 80 ms of CPU per 1,000 changes at the unicorn, falling to 32 ms at 11,400 changes a second as work batches up. One CPU carried 11,400 changes a second at 0.36 cores on average, with a message p95 of 24 ms; 2 CPUs brought that p95 down to 10 ms.
- **Memory follows the streams**: 55–65 KiB of live heap per open, quiet stream with 7 shapes and a channel, about 77 KiB of Go memory. A 512 MB container admitted 4,900 such streams with 30 s of a busy table in the resume buffer, and 5,770 when the buffer held 2 MiB.
- **The resume buffer follows the change rate**: about 520 bytes per buffered change with `REPLICA IDENTITY FULL`, 330 with the primary key's; 30 s of 1,150 changes a second is 17 MiB.
- **A reconnect of every stream** of a full 512 MB node had them all back within 10 s, every resume replayed from the buffer, at a message p99 of 63 ms for the streams already back.
- **The primary key's replica identity costs 9% less CPU** than `FULL` on the same load.
- **A 100,000-row transaction** reached its streams 0.5 s after it committed, and did not slow the other streams.

## CPU: changes per second

The unicorn's 12,000 streams on 1 CPU, with each room's message every 15 s (the unicorn itself), 7.5 s, 5 s and 3 s; the last two also on 2 CPUs. Steady phase of 6–10 minutes.

| Message every | Changes/s | CPUs | Cores avg / peak | CPU per 1,000 changes | Message p50 / p95 / p99 | Typing p95 | Run |
| ---: | ---: | ---: | --- | ---: | --- | ---: | --- |
| 15 s | 2,300 | 1 | 0.19 / 0.38 | 81 ms | 3.3 / 6.9 / 31 ms | 1.8 ms | `p4-unicorn-1cpu` |
| 7.5 s | 4,580 | 1 | 0.22 / 0.39 | 48 ms | 3.2 / 10 / 45 ms | 2.0 ms | `p4-rate-1cpu-x2` |
| 5 s | 6,860 | 1 | 0.27 / 0.45 | 39 ms | 3.0 / 7.5 / 48 ms | 1.9 ms | `p4-rate-1cpu-x3` |
| 3 s | 11,440 | 1 | 0.36 / 0.55 | 32 ms | 3.0 / 24 / 80 ms | 6.0 ms | `p4-rate-1cpu-x5` |
| 5 s | 6,860 | 2 | 0.27 / 0.46 | 40 ms | 3.1 / 6.8 / 12 ms | 1.6 ms | `p4-rate-2cpu-x3` |
| 3 s | 11,440 | 2 | 0.44 / 0.83 | 39 ms | 3.3 / 10 / 35 ms | 2.1 ms | `p4-rate-2cpu-x5` |

- **The cost per change falls as the rate rises.** A transaction wakes each stream's writer once, and a message's 81 changes are decoded, routed and buffered together.
- **The CPU limit shows in the tail before the average.** At 11,440 changes a second the process averaged 0.36 of its 1 CPU, yet the message p99 reached 80 ms: each message is a burst of 81 changes and up to 85 events, and under a 1-CPU quota a burst waits for the next period. With 2 CPUs the same load had a p99 of 35 ms.
- **The latency is Sluice's.** Of a message's 6.9 ms at p95 at the unicorn, 6.5 ms were from the commit to the client and the rest the write and commit.
- **The reader's own work** on one change of this load, on one core (hold checks, buffering, routing, and queueing its events, but not decoding), is about 3–4 µs (`BenchmarkRoomMessage` in `internal/server`): about 5% of the process's CPU at the unicorn. The rest is writing events to the streams, garbage collection, and each stream's background work: token renewals and hook re-checks.

## Writes per action

The very high tier with each message as its one row, without the `UPDATE` of every member's `last_message_at` (`p4-veryhigh-nomembers`), against the same tier with it (`p4-veryhigh-full`):

| | Changes/s | Cores avg | Go memory | Resume buffer | Message p95 |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 + 80 rows per message | 334 | 0.03 | 174 MiB | 9.4 MiB | 7.7 ms |
| 1 row per message | 4 | 0.02 | 141 MiB | 1.4 MiB | 6.4 ms |

The 80 updates per message reach one stream each, as a member update; the other 79 of each reach none. Sluice decodes, routes and buffers them all the same. Here they cost a third of the CPU and a fifth of the memory. At the unicorn the same pattern is 2,300 changes a second for 3,470 events.

## Replica identity

The very high tier with `REPLICA IDENTITY FULL` (`p4-veryhigh-full`) and with the primary key's (`p4-veryhigh-pk`):

| | CPU per 1,000 changes | Go memory | Resume buffer | Bytes per buffered change |
| --- | ---: | ---: | ---: | ---: |
| `FULL` | 86 ms | 174 MiB | 9.4 MiB | ~490 |
| primary key | 78 ms | 162 MiB | 5.8 MiB | ~300 |

With `FULL` every `UPDATE` carries the old row too, which is decoded, buffered and checked against each candidate hold. With the primary key's identity only the key columns of the old row are sent. A shape may then filter only on columns that cannot change, or on key columns; Sluice warns `replica_identity_insufficient` on a subscription that needs more.

## Memory per stream

**Quiet streams** (`p4-idle-ramp`): 600 rooms joining in steps of 3,000 streams, without messages or typing, on 1 CPU and 4 GB.

| Streams | Go memory | Live heap per stream | Cores |
| ---: | ---: | ---: | ---: |
| 3,000 | 269 MiB | 57 KiB | 0.02 |
| 6,000 | 500 MiB | 55 KiB | 0.04 |
| 9,000 | 693 MiB | 66 KiB | 0.06 |
| 12,000 | 916 MiB | 61 KiB | 0.08 |
| 15,000 | 1,122 MiB | 65 KiB | 0.10 |

- A stream with 7 shapes and a channel holds 55–65 KiB of live heap and two goroutines, about 77 KiB of Go memory with the collector's headroom.
- Quiet streams still cost CPU: 0.10 cores for 15,000, almost all of it hook re-checks. With `ttl: 0` Sluice asks the hook about every joined channel on every catalog tick, here 500 calls a second. A hook `ttl` of a few minutes removes most of it.

**Under load** the memory a collection cannot free, which is what admission measures, was 43–50 KiB per stream once the resume buffer is taken out: 575 MiB at the unicorn's 12,000 streams with a 65 MiB buffer, and 83 MiB at the very high tier's 1,500 with a 9 MiB buffer.

## The resume buffer

A client that reconnects asks to resume each table from the last position it saw. Sluice replays the changes since then from its buffer when it still holds them (`resumed: true`); otherwise the client reads its state again. The buffer keeps every table's changes for `SLUICE_RING_MAX_AGE` (30 s, removed on the catalog tick after it, so 30–60 s), within `SLUICE_RING_MEMORY` (a tenth) of the Go memory limit.

Its memory is the change rate times the age times the size of a change. The steady runs kept 60 s, so these hold twice what the default would; add 32 bytes per change for the routing keys of 0.11.0:

| Run | Changes/s | Buffer at 60 s |
| --- | ---: | ---: |
| very high, primary key | 334 | 5.8 MiB |
| very high, `FULL` | 334 | 9.4 MiB |
| unicorn | 2,300 | 65 MiB |
| unicorn's streams, message every 5 s | 6,860 | 194 MiB |
| unicorn's streams, message every 3 s | 11,440 | 323 MiB (budget 369 MiB) |

The buffer is live heap, so it counts toward admission: on a node full to its admission threshold, every MiB it holds is a MiB fewer for streams (see [Reconnects](#reconnects)).

A resume reads its table's changes from its position on, and passes over those that are not for it by a hash of their routing columns kept in the buffer: about 5 ns per change on one core (`BenchmarkReplay`), so a resume 20 s back on a table taking 1,150 changes a second reads 23,000 changes in about 0.1 ms.

## Reconnects

**The refill at the admission threshold** (`reconnect` profile): 6,000 streams offered to 1 CPU and 512 MB with no stream cap, so admission decides how many fit, then a cut of every connection at the proxy, which refuses connections for 5 s. The clients come back with the SDK's backoff and their resume positions. `room_members` takes 1,150 changes a second.

| Resume buffer | Buffer | Streams that fit | Back within | Opens p50 / p95 | Reconnect CPU | Message p95 / p99 during it | `room_members` resumed |
| --- | ---: | ---: | ---: | --- | ---: | --- | --- |
| 4,096 changes per table (0.10.0's) | 2.3 MiB | 5,768 | 10 s | 1.0 / 1.9 s | 33 s | 8 / 25 ms | none: they read their state |
| 30 s (default) | 17 MiB | 4,896 | 10 s | 0.7 / 1.8 s | 32 s | 9 / 63 ms | all |
| 60 s | 34 MiB | 4,448–4,472 | 10–15 s | 0.3–0.8 / 1.4–1.8 s | 28.5–29 s | 6–46 / 23–177 ms | all |

Runs `p4-refill` (0.10.0's buffer, on `bee4bc8`), `p4-keys-refill-age30`, and `p4-keys-refill-r1` and `-r2`. "Back within" is from the cut to the first sample, every 5 s, with every stream open again; 5 s of it the proxy refused connections. Go memory peaked at 438–442 of 461 MiB in every run, as before the cut.

- **A busy table resumes.** With the buffer of 0.10.0, `room_members` reached back about 4 s at this rate: 27,705 of its shapes came back `resumed: false`, each a client that had to read its state again. With a buffer by time, every one replayed.
- **The cost is memory.** The buffer is live heap, so a node full to its threshold admits fewer streams: 870 fewer at 30 s, 1,300 fewer at 60 s.
- **Replaying is cheap only with the routing keys.** Before them, each resume read the routing column of every change since its position from the tuples: at 60 s the reconnect took 41–58 s of CPU, opens 7–12 s at p50, and messages to the streams already back waited up to 8–27 s at p99. A profile put half of the process's CPU in that scan, and the reader waited 16 s in all for the scans to release the buffer (`p4-refill-r1`, `-r2`, `p4-fix-*`, `p4-fix-pprof-refill`).

**The cut at the very high tier**, 1,500 streams on 1 CPU and 512 MB (`p4-keys-resume-veryhigh`): every stream back within 15 s, every table resumed with positions up to 28 s old, in 2.1 s of CPU. The clients' backoff sets the 15 s.

## Big transactions

The high tier (400 streams) on 1 CPU and 512 MB, with transactions of 1,500, 10,000 and 100,000 notification rows, three of each, a quarter of the rows for members with a stream open (`p4-bigtxn-high`):

| Rows | Commit | Sent by PostgreSQL | Dispatched by Sluice | Backlog peak | Sluice CPU | Other streams' message p95, before → during |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1,500 | 7–8 ms | 22 ms | 22 ms | 0.5 MiB | 0.25–0.29 s | 8–12 → 6–8 ms |
| 10,000 | 35–47 ms | 47–64 ms | 54–69 ms | 2.7–3.2 MiB | 0.32–0.43 s | 7–11 → 6–10 ms |
| 100,000 | 368–377 ms | 414–484 ms | 460–505 ms | 2.5–21 MiB | 0.74–0.77 s | 6–14 → 7–11 ms |

Times are from the start of the transaction. Sluice delivers a transaction once PostgreSQL has sent all of it, and the other streams' latency did not move. A transaction is kept whole in the resume buffer as its table's newest: after the 100,000-row ones the live heap went from about 27 to 67–75 MiB, and stayed there while nothing newer was written to that table. The buffer's budget bounds it.

## RLS mode

The capacity profiles on RLS mode: concurrent users, one stream each, receiving bursts of writes to one table, with a policy of each tier; 1 CPU and 512 MB (`rls-a`, `rls-b`, `rls-c` in the reports):

| Tier | What Sluice does per change and subscriber | Users | Events/s | Event p95 | CPU per 1,000 events |
| --- | --- | ---: | ---: | ---: | ---: |
| A | nothing: the policy is a constant | 1,600 | 5,830 | 3 ms | 65 ms |
| B | evaluates the policy in process | 1,500 | 5,700 | 4 ms | 63 ms |
| C | asks PostgreSQL | 200 | 1,520 | 1.8 s | 410 ms |

- **Tiers A and B cost the same.** Evaluating a policy in process is not what costs; delivering is.
- **Tier C's latency grows with its subscribers**, since each change costs one query per subscriber: 244 ms at p95 with 25, 1.8 s with 200. `/diagnostics` names each Tier C policy, and `sluice_authz_tier_c_probes_total` counts the queries.
- These hunts open every stream of a step at once and do not retry a refused open, so where they stop says more about a burst of opens than about delivery. The figures above are the steps every tier passed.

## Sizing a node

**CPU.** At this workload's mix, about 0.1 ms of CPU per change at a few thousand changes a second, falling to 0.03–0.04 ms past 5,000, plus the background of the streams (0.10 cores per 15,000 quiet streams with a hook `ttl` of 0). One CPU keeps up with more than 11,000 changes a second, with a message p99 under 100 ms; for a tail under 50 ms at that rate, give it 2. The reader decodes and routes on one goroutine, so one slot's ceiling is one core's. Extrapolated from its 3–4 µs per change, which leave decoding out, that ceiling is far above the 11,400 changes a second measured here.

**Memory.** Admission (`SLUICE_ADMIT_MEMORY`, 0.5 of the Go memory limit) counts the memory a collection cannot free: about 45–50 KiB per stream of 7 shapes, plus the resume buffer, plus the process itself. On 512 MB (`GOMEMLIMIT` 461 MiB) that was 5,770 such streams with a buffer of 2 MiB, and 4,900 with the default 30 s of a table taking 1,150 changes a second (17 MiB; see [Reconnects](#reconnects)). A node sized so that its streams fit below that threshold also refills after a reconnect of all of them. Measure your own figure with your workload: `apps/loadtest` reports it as live heap per stream.

**WAL.** Bound `max_slot_wal_keep_size` and open transactions (README, WAL retention); alert on `sluice_slot_safe_wal_bytes`.

**Metrics to watch:**
- `sluice_reader_lag_seconds`: how late changes arrive; past `SLUICE_LAG_DEGRADED` every stream gets a `delay` event.
- `sluice_streams` and `sluice_streams_refused_total{reason="memory"}`: how full the node is.
- `sluice_resume_buffer_seconds` and `sluice_resumes_total`: how far back a client can resume, and how many could not.
- `rate(sluice_changes_total[1m])` by table: where the CPU goes.
- `sluice_slot_safe_wal_bytes`: how much WAL is left before the slot is lost.
