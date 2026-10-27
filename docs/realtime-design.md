# Designing tables for realtime

Sluice streams PostgreSQL's logical replication. Every change committed to a published table goes through one reader, which decodes it, routes it to the shapes it may match, and keeps it in the resume buffer. What Sluice costs therefore depends on the changes in the WAL, the shapes and streams open, and the resume buffer, much more than on the events it delivers.

Each rule below gives its reason and, where there is one, the measurement behind it, from [Performance and sizing](performance.md) (Sluice 0.11.0, 1 CPU, the load test's chat workload).

## Contents

1. [Publish only what clients subscribe to](#1-publish-only-what-clients-subscribe-to)
2. [Write one row per action, not one per recipient](#2-write-one-row-per-action-not-one-per-recipient)
3. [Keep ephemeral state on channels](#3-keep-ephemeral-state-on-channels)
4. [Choose the replica identity per table](#4-choose-the-replica-identity-per-table)
5. [Give every shape an equality on an indexed column](#5-give-every-shape-an-equality-on-an-indexed-column)
6. [Know what the resume buffer holds](#6-know-what-the-resume-buffer-holds)
7. [Big transactions are fine; long ones are not](#7-big-transactions-are-fine-long-ones-are-not)
8. [Keep `synchronous_commit` on](#8-keep-synchronous_commit-on)
9. [Keep each stream's background small](#9-keep-each-streams-background-small)
10. [Plan for every stream reconnecting at once](#10-plan-for-every-stream-reconnecting-at-once)
11. [In RLS mode, keep policies out of Tier C](#11-in-rls-mode-keep-policies-out-of-tier-c)

## 1. Publish only what clients subscribe to

**Why.** Every change to a published table is decoded, routed and buffered, whether or not a shape is on it. A table in the publication that no client reads costs as much per change as one they all read.

**How.** Keep the publication to the tables clients subscribe to, and keep write-heavy bookkeeping (counters, logs, queues, sessions) out of it. Sluice refuses a publication with a row filter, so split a table rather than filter it.

## 2. Write one row per action, not one per recipient

**Why.** An action that writes N rows is N changes, and Sluice handles each whether or not a stream receives it.

**Measured.** In the load test each message also updates `last_message_at` on every member's row: 1 + 80 changes, of which 79 of every 80 updates reach no stream. At 1,500 streams those updates were a third of Sluice's CPU and a fifth of its memory; at 12,000 streams they were 2,300 changes a second to deliver 3,470 events ([Writes per action](performance.md#writes-per-action)).

**How.**
- Keep per-recipient state where it is read. "Unread" can be computed when it is read, from the room's last message and the member's last-read position, which changes only when that member reads.
- Or write the shared fact once, on the parent: one `last_message_at` on the room, which every member's stream receives through a shape on `room_id`.
- Fan out in the client, from one change, rather than in the database.

## 3. Keep ephemeral state on channels

**Why.** Broadcast and presence go from client to clients through Sluice without touching the WAL: no write, no decoding, no slot, no resume buffer. Typing indicators, cursors and "online" do not need to survive a reconnect.

**Measured.** Typing over a channel had a p95 of 1.5–1.8 ms, against 6.9–7.7 ms for messages through the WAL, at 1,500 and 12,000 streams ([CPU](performance.md#cpu-changes-per-second)).

**How.** `POST /publish` and `POST /presence` from the client, on a channel whose namespace decides who may join (a hook for anything but public or per-user channels). For an event the server raises in a transaction, `pg_logical_emit_message` delivers a broadcast in commit order with the changes around it: it goes through the WAL, but writes no row and is not kept in the resume buffer.

## 4. Choose the replica identity per table

**Why.** The old row of an `UPDATE` or `DELETE` carries only the replica identity's columns. With `FULL` it carries every column, which Sluice decodes, keeps in the resume buffer and checks against each candidate hold. Without the columns a shape filters on, Sluice cannot tell that a row left the shape, nor evaluate a `DELETE`.

**Measured.** The same load with the primary key's identity cost 9% less CPU than with `FULL`, and each buffered change took about 300 bytes instead of 490 ([Replica identity](performance.md#replica-identity)).

**How.**
- The primary key's identity (`DEFAULT`) when shapes filter on the key, or on columns that never change, and holds name key columns.
- `USING INDEX` on a unique index over the filter columns and the key when a shape filters on a column that changes and needs deletes or transitions: what `FULL` would give for those columns, without the others.
- `FULL` only when nothing narrower serves. Sluice warns `replica_identity_insufficient` on a subscription that needs more than the identity carries, and `replica_identity_full_with_toast` where `FULL` carries large values.

## 5. Give every shape an equality on an indexed column

**Why.** Sluice routes a change by the value of one column: it finds the shapes whose `eq` term matches that value and evaluates only those. A shape with no `eq` term is evaluated on every change to its table. The same column, indexed in PostgreSQL, is what initial snapshots and hold checks query.

**How.**
- Give each filter an `eq` on a column that leads an index, for example `room_id=eq.R` or `user_id=eq.U`. Sluice routes by an indexed column in the replica identity when there is one.
- There is no `OR`: subscribe twice instead.
- `/diagnostics` warns `unindexed_shape`, and `sluice_routing_candidates` shows how many shapes each change is checked against.
- In issuer mode holds are routed the same way, and a hold may only read columns in its table's replica identity, so that the deletion of its row, or a change to one of its columns, is seen.

## 6. Know what the resume buffer holds

**Why.** A client that reconnects within the buffer's reach replays what it missed instead of reading its state again. The buffer keeps every table's changes for 30–60 s within a tenth of the memory limit, whoever they were for, so a busy table fills it with changes most clients never asked for. It is live memory, and admission counts it: on a full node, every MiB it holds is a MiB fewer for streams.

**Measured.** On 1 CPU and 512 MB with a table taking 1,150 changes a second, 30 s of buffer held 17 MiB, and admission let in 4,900 streams instead of 5,770. In a reconnect of all of them every resume replayed, where a buffer reaching back 4 s had sent each client of that table back to read its state ([Reconnects](performance.md#reconnects)).

**How.**
- Rule 2 shrinks the buffer as much as the CPU.
- Watch `sluice_resume_buffer_seconds` (how far back it reaches) and `sluice_resumes_total{result="not_resumed"}`.
- Set `SLUICE_RING_MAX_AGE` to the gaps your clients have: a proxy reload or a network blip is a few seconds plus their backoff. A restart of Sluice empties the buffer whatever its age.

## 7. Big transactions are fine; long ones are not

**Why.** A transaction is delivered as a whole once PostgreSQL has sent it, and the reader goes on with the next. A transaction that stays open, even one that wrote a single row to a table outside the publication, holds back the slot's restart point: the WAL piles up behind it, and with `max_slot_wal_keep_size` bounded the slot is eventually lost. Sluice then stops, and every client reads its state again.

**Measured.** A 100,000-row transaction reached its streams 0.5 s after it began; the other streams' latency did not move ([Big transactions](performance.md#big-transactions)). On PostgreSQL 18.6, an open transaction that had inserted one row lost a slot bounded at 64 MB after 60 MB of other writes (README, WAL retention).

**How.**
- Batch jobs can write in one transaction, at the cost of its size in the resume buffer while it is the table's newest.
- Bound open transactions with `idle_in_transaction_session_timeout` and `transaction_timeout`, and alert on `sluice_slot_safe_wal_bytes`. Sluice warns at half and 80% of `max_slot_wal_keep_size`, naming the oldest open transaction.

## 8. Keep `synchronous_commit` on

**Why.** The walsender sends only WAL that is flushed. With `synchronous_commit = off`, a commit waits for the WAL writer, every `wal_writer_delay` (200 ms), before Sluice can see it.

**Measured.** It adds 0–400 ms to every change. The load test runs with it on: a message's p95 from commit to client was 6.5 ms at 12,000 streams.

**How.** Leave it on for the transactions whose changes are streamed; it can be turned off per transaction where latency does not matter.

## 9. Keep each stream's background small

**Why.** Each shape is an issuer verdict, a hold, an index entry and part of every token renewal; each channel joined through a hook with `ttl: 0` is a hook call on every catalog tick (`SLUICE_CATALOG_REFRESH`, 30 s), for every stream that joined it.

**Measured.** 15,000 quiet streams of 7 shapes and a channel held 55–65 KiB of live heap each and used 0.10 cores, almost all of it hook re-checks, 500 calls a second ([Memory per stream](performance.md#memory-per-stream)).

**How.**
- Give the hook's verdicts a `ttl` of minutes: Sluice then asks once per channel and session per ttl, and a revoked member is cut within the ttl plus one tick instead of one tick. `POST /token` always asks again.
- One shape per table and filter a client needs; a shape that never receives anything still costs its share.
- Token lifetimes of minutes rather than seconds: each `/token` asks the issuer about every shape again (in one call).

## 10. Plan for every stream reconnecting at once

**Why.** A deploy of Sluice, a proxy reload or a network blip drops every stream at once, and they all come back within seconds. Each open costs an issuer call (one per stream), a hold check, and either a replay from the resume buffer or a fresh read of the client's state.

**Measured.** A node of 1 CPU and 512 MB, full to its admission threshold with 4,896 streams, had every one back within 10 s of a cut of all connections that held the proxy down for 5 s, every resume replayed. Opens took 0.7 s at p50, messages to the streams already back 63 ms at p99, and Go memory stayed where it was before the cut ([Reconnects](performance.md#reconnects)).

**How.**
- Use the SDK, or honour `Retry-After` and `retry_after_ms` with backoff and jitter as it does.
- On `SIGTERM` Sluice spreads its clients' reconnects over `SLUICE_RECONNECT_SPREAD`; give the container a stop timeout above `SLUICE_SHUTDOWN_GRACE`.
- Let admission refuse what does not fit (`503 too_many_streams` with `Retry-After`); size the node so that a full reconnect fits (see [Sizing a node](performance.md#sizing-a-node)).

## 11. In RLS mode, keep policies out of Tier C

**Why.** A Tier A policy is a constant and a Tier B policy is evaluated in process, on the change itself. A Tier C policy needs PostgreSQL: one query per subscriber per change.

**Measured.** Tiers A and B delivered about 5,700 events a second to 1,500 users at a p95 of 3–4 ms, at the same CPU per event. Tier C reached 1,520 events a second with 200 users, at a p95 of 1.8 s ([RLS mode](performance.md#rls-mode)).

**How.**
- Filter on the column the policy compares: `owner_id=eq.<me>` for `owner_id = auth.uid()` makes the subscription Tier A.
- Wrap per-query calls in a scalar subquery, `(select auth.uid())`, and avoid subqueries and functions Sluice cannot evaluate.
- `/diagnostics` names each Tier C policy (`tier_c_policy`) with a remedy; alert on `sluice_authz_tier_c_probes_total`.
