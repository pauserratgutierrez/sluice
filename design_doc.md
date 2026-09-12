# Sluice — Design Document

A sluice is a gate on a channel. It takes one flow — the PostgreSQL write-ahead log — and meters it out to each consumer, giving every subscriber exactly what it is entitled to and nothing more. That is the whole design in one word.

Sluice is a realtime data-streaming server for PostgreSQL. It replaces `supabase/realtime` in a self-hosted stack, is not protocol-compatible with it, and is built from zero with no legacy layers.

---

## Table of contents

1. [Goals and non-goals](#1-goals-and-non-goals)
2. [The one decision that determines everything](#2-the-one-decision-that-determines-everything)
3. [Terminology](#3-terminology)
4. [What Sluice requires from PostgreSQL](#4-what-sluice-requires-from-postgresql)
5. [Architecture](#5-architecture)
6. [Planes and primitives](#6-planes-and-primitives)
7. [The authorization model](#7-the-authorization-model)
8. [Shapes, filters and the routing index](#8-shapes-filters-and-the-routing-index)
9. [Replica identity requirements](#9-replica-identity-requirements)
10. [Wire protocol](#10-wire-protocol)
11. [Data flows](#11-data-flows)
12. [The replication reader](#12-the-replication-reader)
13. [Initial snapshots and the subscribe race](#13-initial-snapshots-and-the-subscribe-race)
14. [The signalling plane](#14-the-signalling-plane)
15. [Session revocation](#15-session-revocation)
16. [Connection pooling and impersonation hygiene](#16-connection-pooling-and-impersonation-hygiene)
17. [Backpressure and limits](#17-backpressure-and-limits)
18. [Observability](#18-observability)
19. [Startup validation](#19-startup-validation)
20. [Security model](#20-security-model)
21. [Failure modes](#21-failure-modes)
22. [Horizontal scale](#22-horizontal-scale)
23. [Configuration](#23-configuration)
24. [Go package layout](#24-go-package-layout)
25. [Roadmap](#25-roadmap)
26. [Appendix A — rejected alternatives](#appendix-a--rejected-alternatives)
27. [Appendix B — evidence index](#appendix-b--evidence-index)

---

## 1. Goals and non-goals

### Goals

- **Stream PostgreSQL row changes to authenticated web clients**, correctly filtered by the **shape oracle** this process is running: either the same row-level security and grants that govern ordinary reads (`rls`), or an application HTTP issuer that returns a concrete shape and holds (`issuer`).
- **Provide ephemeral messaging and presence** that never touch the database.
- **Require nothing installed in the database.** No extensions, no tables, no functions, no schemas. A stock PostgreSQL 15+ with `wal_level=logical` and a publication is sufficient.
- **Scale with the write rate, not with the subscriber count.** Authorization must not be on the per-message path.
- **Be correct where the incumbent is not**, specifically for `DELETE` under RLS.
- **Make expensive configurations loud**, not silent. If a policy or shape forces the slow path, the operator must be told, with the exact remedy.
- **Impose no schema conventions.** No mandatory `deleted_at`, no soft deletes, no `updated_at`, no naming rules, no ownership column.
- **Be operable by one person.** One static binary, one slot, one publication.

### Non-goals

- **Protocol compatibility with `@supabase/realtime-js`.** Explicitly out of scope. The Phoenix Channels wire format, the positional join-reply validation, and the dual v1/v2 framing would duplicate a large amount of surface area and tie Sluice to a protocol that evolves elsewhere. Sluice ships its own, smaller client.
- **Offline-first / local-first sync.** Sluice streams changes; it does not maintain a client-side replica, resolve conflicts, or provide a query engine. That is ElectricSQL's and Zero's problem space.
- **Multi-tenancy as a first-class concept.** One Sluice process serves one database. Run more processes for more databases.
- **Writing to the database.** Sluice reads. Application writes go through PostgREST or your own API.
- **A dashboard.** Metrics and a diagnostics endpoint, nothing more.

---

## 2. The one decision that determines everything

Authorization is resolved **once, at subscribe time**, into a filter. It is never evaluated per message.

This is not an optimisation. It is the difference between a system that works and one that does not, and it is the only reason Sluice can exist as an improvement rather than a rewrite.

Measured on PostgreSQL 18.4, cost of authorizing **one changed row** for N subscribers using per-subscriber impersonation (the `supabase/walrus` model):

| Subscribers | Impersonation model | Set-based / indexed model |
| --- | --- | --- |
| 100 | 739 changes/sec | 6,098 changes/sec |
| 1,000 | **108 changes/sec** | 4,405 changes/sec |
| 5,000 | **18.6 changes/sec** | 1,873 changes/sec |

The cost is linear at **~9–13 µs per subscriber per change**, splitting roughly 1:2 between the two `set_config` calls and the primary-key probe. Reimplementing the check as a compiled-predicate evaluation instead of a `SELECT EXISTS` probe produced the *same numbers*. The wall is the loop, not the query.

Two independent prior-art projects reached the same conclusion the hard way:

- **ElectricSQL** tried RLS-in-the-sync-engine and retreated. Their reasoning is worth quoting because it identifies a *correctness* problem, not just a performance one:

  > for ongoing replication the picture is a little more complex, because Electric uses a single replication stream and then fans out shapes to multiple clients... **the replication stream is behind the current state of the database. Postgres has MVCC internally but does not (natively) allow you to query the database at a particular snapshot. This means that by the time you have received a transaction in the replication stream, you can't query the database within the same snapshot.**

  Per-subscriber impersonation against the live table is therefore evaluating the policy against *the wrong database state*. Supabase Realtime has this bug latent.

- **rocicorp/zero** deprecated `definePermissions` in v0.25. Their docs now say: *"Zero does not have (or need) a first-class permission system like RLS."* Authorization is resolved server-side into a query; the sync engine executes a mechanical filter.

And Electric published the quantified fix:

> With non-optimized where clauses, throughput is inversely proportional to the number of shapes. If you have 10 shapes, Electric can process 1,400 changes per second. If you have 100 shapes, throughput drops to 140 changes per second.
>
> With optimized where clauses ... **~5,000 row changes per second no matter how many shapes you have.** ... We optimize this by **indexing shapes by their constant**, allowing a single lookup to retrieve all shapes for that constant instead of evaluating the where clause for each shape.

Centrifugo arrived at the same place from the pub/sub side with its `#`-suffixed user-limited channels: authorization by naming convention, checked once at subscribe against a JWT claim, zero work on the message path.

Sluice's contribution is to make this **sound and automatic** rather than a convention the application must uphold. See [§7](#7-the-authorization-model).

---

## 3. Terminology

| Term | Meaning |
| --- | --- |
| **Stream** | One long-lived SSE response. One per client connection. Identified by a `stream_id`. |
| **Subscription** | A registered interest attached to a Stream. Identified by a client-chosen `sub` label, unique within the Stream. |
| **Shape** | A replication-plane subscription target: `{schema, table, ops, filter, columns}`. Borrowed from Electric's vocabulary because it is well-understood. |
| **Oracle** | The process-wide judge of shapes. One of `rls` or `issuer`. Chosen at deploy time; there is no AND/OR of the two. |
| **Issuer** | The HTTP endpoint the `issuer` oracle calls at join and on `/token`. The application owns it. Sluice does not re-send the user's access token, only verified identity. |
| **Effective filter** | `authorized AND client`, AND-only. The client may omit or narrow; it cannot change an authorized equality or keep a whole table. |
| **Narrowing** | Combining the issuer's authorized filter with the client's. Conflicting equalities are a deny, not a silently empty shape. |
| **Hold** | A row that *keeps the permission alive*, named by the issuer. EXISTS at join is only of holds, never of the subscribed table. The first hold that stops matching (DELETE, or UPDATE leaving the filter) cuts **that** shape with `shape_not_authorized`. The stream stays open. |
| **Publication** | The PostgreSQL publication Sluice reads. Hold tables and shape tables must be in it. If a relation leaves it, those shapes and hold watches are dropped (`relation_unpublished`). |
| **Replica identity** | PostgreSQL `REPLICA IDENTITY` — which columns appear in WAL old tuples. Hold filter columns must be in it, or subscribe is denied: a DELETE cannot be cut without them. This is WAL, not a Sluice convention. |
| **Channel** | A signalling-plane subscription target: a free-form namespaced string, e.g. `room:42`. Independent of the shape oracle. |
| **Predicate** | The combined RLS `SELECT` policy expression for a relation: permissive policies OR'd, restrictive policies AND'ed. Used only by the `rls` oracle. |
| **Tier** | Which of three authorization strategies an **RLS** subscription resolved to. Issuer mode has no tiers and does not send `tier` on the wire. |
| **Routing key** | `(relation OID, column, constant)` — the index entry used to find interested subscriptions in O(1). |
| **Reader** | The single process/goroutine holding the replication connection. |
| **Hub** | The sharded in-process fan-out layer. |
| **`shape_not_authorized`** | That shape is not permitted (denied at join, issuer refresh deny, hold gone, admin drop). The stream and other subscriptions continue. |
| **`session_revoked`** | The identity backing the **stream** is gone (`auth.sessions` DELETE). The stream closes. Different axis from holds. |

---

## 4. What Sluice requires from PostgreSQL

This section is the contract. If your database satisfies it, Sluice works. Nothing else is needed and nothing is installed.

### Server settings

```
wal_level = logical                 # required, restart
max_replication_slots >= 1          # one slot for Sluice
max_wal_senders >= 1                # one walsender for Sluice
max_slot_wal_keep_size = <bounded>  # STRONGLY recommended; default -1 is unlimited
```

`supabase-headless` already sets all four, including `max_slot_wal_keep_size=4GB`.

### Roles

Two roles, deliberately separate:

```sql
-- 1. The replication role. Highly privileged; used ONLY for the slot.
CREATE ROLE sluice_repl WITH LOGIN REPLICATION PASSWORD '...';

-- 2. The authorization role. Assumes application roles to evaluate policies.
CREATE ROLE sluice_authz WITH LOGIN NOINHERIT PASSWORD '...';
GRANT anon, authenticated TO sluice_authz;
```

`sluice_repl` needs **no table privileges at all** — verified: a role with `REPLICATION` and zero grants streams every column of every published table. `REPLICATION` is documented as *"a very highly privileged role"*, which is exactly why it is not the role used for anything else.

`sluice_authz` mirrors PostgREST's `authenticator`: `NOINHERIT` so it can only *assume* application roles via `SET LOCAL ROLE`, never use their privileges implicitly. It needs `SELECT` on published tables **only if** initial snapshots are enabled — and then only via the assumed role, so no direct grant is required.

**Issuer mode does not change that LOGIN.** The pool still uses `SLUICE_DB_AUTHZ_URL`. What changes is how it *uses* the connection: no `SET ROLE`, no claims. Snapshots and hold EXISTS run as the pool role, so that role needs `GRANT SELECT` on published tables (including hold tables) and **`BYPASSRLS` or RLS off** on them. Startup refuses to run if a published table has RLS and the role does not bypass — otherwise RLS would become a second judge. This check exists only when `SLUICE_SHAPE_ORACLE=issuer`.

### Publication

```sql
CREATE PUBLICATION sluice;
ALTER PUBLICATION sluice ADD TABLE public.documents;
```

Explicit opt-in per table. `FOR ALL TABLES` is supported but discouraged.

**Do not use publication row filters or column lists.** Verified: a column used in a publication `WHERE` expression must be part of the replica identity, and if it is not, `UPDATE` and `DELETE` on that table **fail at the application**, not at replication:

```
ERROR:  cannot update table "docs"
DETAIL:  Column used in the publication WHERE expression is not part of the replica identity.
```

Sluice does its filtering in process, where it is per-subscriber and cannot break writes. Sluice validates the publication at startup and refuses to start if a row filter would be unsafe ([§19](#19-startup-validation)).

### Optional grants

```sql
-- only if the application publishes database-originated broadcasts
GRANT EXECUTE ON FUNCTION pg_logical_emit_message(boolean, text, text) TO app_role;
```

### Optional: session revocation

```sql
ALTER PUBLICATION sluice ADD TABLE auth.sessions;
ALTER PUBLICATION sluice ADD TABLE auth.users;
```

Off by default. See [§15](#15-session-revocation).

### That is the entire list

For comparison, `supabase/realtime` requires: a `_realtime` schema with encrypted tenant and extension tables, a `realtime` schema with 3 tables / 5 types / 15 functions created by 81 migrations, a `supabase_realtime_admin` role owning them, `CREATE` on the database, `SET` on `log_min_messages`, a day-partitioned `realtime.messages` table with a janitor process dropping partitions, and superuser to run its migrations.

---

## 5. Architecture

```
                          ┌──────────────────────────────────────────┐
                          │              PostgreSQL                  │
                          │                                          │
   app writes ───────────►│  tables ──► WAL ──► walsender (pgoutput) │
                          │                          │               │
                          │  pg_policy / pg_class ◄──┼── catalog     │
                          │  impersonated probes  ◄──┼── (Tier C)    │
                          └──────────────────────────┼───────────────┘
                                                     │
                          replication conn (1)       │  pgxpool (N)
                                                     ▼
   ┌─────────────────────────────────────────────────────────────────────┐
   │                              Sluice                                 │
   │                                                                     │
   │  ┌────────────┐   ┌──────────────┐   ┌───────────────────────────┐  │
   │  │  Reader    │──►│ Relation     │   │  Catalog cache            │  │
   │  │  (1 slot)  │   │ cache        │   │  policies, grants, RI,    │  │
   │  │  LSN ack   │   │ (OID→schema) │   │  columns, indexes         │  │
   │  └─────┬──────┘   └──────────────┘   └────────────┬──────────────┘  │
   │        │                                          │                 │
   │        │  Change / Message                        │                 │
   │        ▼                                          ▼                 │
   │  ┌──────────────────────────┐          ┌────────────────────────┐   │
   │  │  Routing index           │          │  Authorizer            │   │
   │  │  relOID → col → const    │◄─────────┤  Tier A / B / C        │   │
   │  │        → []subscription  │  register │  predicate compiler    │   │
   │  │  + unindexed []          │          │  lease manager         │   │
   │  └──────────┬───────────────┘          └───────────▲────────────┘   │
   │             │                                      │                │
   │             ▼                                      │                │
   │  ┌──────────────────────────┐          ┌───────────┴────────────┐   │
   │  │  Hub (sharded fan-out)   │          │  Control plane (POST)  │   │
   │  │  per-relation ring buf   │          │  subscribe/publish/... │   │
   │  │  bounded per-stream queue│          └───────────▲────────────┘   │
   │  └──────────┬───────────────┘                      │                │
   │             │                          ┌───────────┴────────────┐   │
   │             │                          │  Auth (ES256 / JWKS)   │   │
   │             │                          │  session registry      │   │
   │             │                          └────────────────────────┘   │
   │             ▼                                                       │
   │  ┌──────────────────────────┐   ┌──────────────────────────────┐    │
   │  │  SSE transport           │   │  Signalling: broadcast,      │    │
   │  │  flush, heartbeat, dl    │   │  presence (in-memory)        │    │
   │  └──────────┬───────────────┘   └──────────────────────────────┘    │
   └─────────────┼───────────────────────────────────────────────────────┘
                 │  text/event-stream                    ▲  POST (JSON)
                 ▼                                       │
        ┌────────────────────────────────────────────────┴───────┐
        │            Caddy  (HTTP/2 + HTTP/3, one connection)    │
        └────────────────────────────────────────────────────────┘
                                    │
                                 Client
```

### Component responsibilities

**Reader.** Owns the single replication connection. Issues `START_REPLICATION`, decodes `pgoutput`, maintains the relation cache, sends `Standby status update` feedback. Never touches authorization. Leader-elected by a PostgreSQL advisory lock so that multiple Sluice processes cannot fight over one slot.

**Catalog cache.** Reads `pg_policy`, `pg_class`, `pg_attribute`, `pg_index`, `has_column_privilege`. Invalidated on `Relation` messages (schema change signal) and on a periodic refresh. This is the only reason Sluice reads the catalog at all.

**Authorizer.** Runs at subscribe time. Compiles the relation's combined predicate, classifies the subscription into a tier, and produces an in-process evaluator. Manages leases for predicates whose truth can change over time.

**Routing index.** Maps `(relation OID, column, constant)` → subscription set, plus an `unindexed` list per relation for shapes with no equality filter.

**Hub.** Sharded fan-out. Per-relation bounded ring buffer for reconnect replay. Per-stream bounded queue with an explicit slow-consumer policy.

**Control plane.** `POST` endpoints for everything a client does upstream.

**SSE transport.** One long-lived `POST` response per stream, `text/event-stream`, heartbeats, write deadlines.

---

## 6. Planes and primitives

Sluice has **two planes** and **three primitives**. The plane distinction is architectural; the primitive distinction is about state semantics.

### Plane 1 — Replication (durable, database-derived)

The source of truth is PostgreSQL. Sluice stores nothing durable except the slot position, which PostgreSQL itself persists. If Sluice loses all memory, no application data is lost.

Primitive: **`change`**.

### Plane 2 — Signalling (ephemeral, never touches the database)

Primitive: **`broadcast`** — fire-and-forget. A message is an event. If you were not connected, you missed it. The server keeps nothing.

Primitive: **`presence`** — keyed state, last-write-wins per member, with automatic cleanup on disconnect. The server **must** keep state, because (a) a joining client needs the current full state, and (b) a disconnect must generate a `leave` event even though nobody sent a message.

Presence is therefore not "broadcast with extra steps": it is broadcast *plus* replicated keyed state *plus* liveness-driven tombstones. It is built on the broadcast plane but is a genuinely separate primitive. Presence state lives **in memory only, never in the database.**

### Where database-originated broadcast fits

A message emitted with `pg_logical_emit_message` arrives on the **replication** plane (same slot, same ordering) but is delivered as a **broadcast** primitive. This is deliberate and is the most valuable single mechanism in the design:

```sql
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid"}');
COMMIT;
```

Verified output on one slot, in transaction order:

```
BEGIN 810
message: transactional: 1 prefix: sluice:orders:1, sz: 25 content:{"event":"paid"}
table public.orders: UPDATE: id[bigint]:1 status[text]:'paid'
COMMIT 810
```

Properties:

- **Atomic with the DML.** Roll back the transaction and the message never existed. This solves the dual-write problem with **no outbox table**.
- **Globally ordered** with data changes on the same slot.
- **Replayable** within the slot's retained WAL — something `LISTEN/NOTIFY` cannot offer.
- `transactional = false` gives immediate, transaction-independent delivery.
- **Zero database objects.** Compare `supabase/realtime`, which needs a day-partitioned `realtime.messages` table, a second replication connection dedicated to reading it, and a janitor dropping partitions older than 72 hours.

Authorization for these messages is by **prefix**, not by row: the prefix is treated as a channel name and authorized exactly like any other signalling channel ([§14](#14-the-signalling-plane)).

### One stream, many subscriptions, declared interests

A client opens **one** Stream and registers **many** Subscriptions on it. Each Subscription declares exactly what it wants. A client receives only what it subscribed to; there is no "subscribe to one thing and receive everything".

---

## 7. The authorization model

Authorization is resolved **once, at subscribe time**, into a filter. It is never evaluated per message. That decision is the whole point of Sluice; which **oracle** produces the grant is a deploy-time choice.

`SLUICE_SHAPE_ORACLE=rls | issuer`. One per process. No AND/OR. Channels (`public | owner | hook`) combine with either.

- **`rls` (default).** GRANT + RLS, three tiers, JWT impersonation on snapshots, leases for volatile predicates. The rest of this section is that oracle.
- **`issuer`.** One HTTP call to the application at join and one per shape on `/token`. Fail closed. The issuer returns a **concrete** shape (≥1 non-negated equality) and ≥1 **hold**. Sluice computes `effective = authorized AND client`, installs the shape and the hold watches **then** EXISTS the holds, and cuts that shape when a hold row disappears from the WAL. No lease back to the issuer. No tiers on the wire (`oracle: "issuer"`, effective `filter`, no `tier`). Snapshots are privileged (no `SET ROLE`). The hot path is still `deliver`: a granted no-op Decision plus the effective filter.

Sluice does not judge the business. The issuer can be any API; a typical handler reuses the same `Authorize` as the resource GET. The client never presents a capability token. Admin `POST .../admin/shapes/drop` is ops, not the product kick path: it walks this node's subscriptions; it is not the hot path.

### 7.0 Preliminaries

For a relation `R` and a request with claims `C` (from the verified JWT) and role `ρ` (from the `role` claim), Sluice builds the **combined predicate**:

```
P(row, C) =  ( P_permissive_1 OR P_permissive_2 OR ... )
         AND ( P_restrictive_1 AND P_restrictive_2 AND ... )
```

from `pg_policy` where `polcmd IN ('r','*')`, `polqual IS NOT NULL`, and `polroles` includes `ρ` (or is `PUBLIC`). Verified working, including the RESTRICTIVE combination:

```sql
-- derived automatically from pg_policy
(((EXISTS ( SELECT 1
   FROM memberships m
  WHERE ((m.team_id = posts.team_id)
    AND (m.user_id = current_setting('request.jwt.claims', true))))))) AND (((id > 0)))
```

If RLS is not enabled on `R` (`relrowsecurity = false`), `P ≡ TRUE`. If the role has `BYPASSRLS` (e.g. `service_role`), `P ≡ TRUE`.

Column-level access is checked separately and always: `has_column_privilege(ρ, R, col, 'SELECT')` for every requested column. A column the role cannot select is never emitted, in any tier.

### 7.1 Tier A — constant reduction (the fast path)

**Criterion.** Let `E = {(col_i, k_i)}` be the set of equality constraints in the shape's filter, and `cols(P)` the set of `R`'s columns referenced by `P`.

> If `cols(P) ⊆ dom(E)`, then for **every** row in the shape, `P(row, C) = P(k, C)` — a constant.

Evaluate that constant **once**, at subscribe time, by rewriting `P` with each column reference replaced by its shape constant and evaluating the resulting expression under the subscriber's role and claims:

```sql
BEGIN;
  SELECT set_config('role', $role, true),
         set_config('request.jwt.claims', $claims, true);
  SELECT <P with column refs substituted by literals>;
ROLLBACK;
```

- Result `TRUE` → **accept**. Every row matching this shape is visible to this subscriber, and **no authorization work is ever done again for this subscription**.
- Result `FALSE` or `NULL` → **deny the subscription** with `403 shape_not_authorized`.

This is *sound*: it is not a heuristic. If the policy depends only on columns the shape pins to constants, the policy's truth value cannot vary across the shape's rows.

**This is the whole 200×.** The canonical Supabase policy shape lands here:

| Policy | Shape filter | `cols(P)` | Tier |
| --- | --- | --- | --- |
| `owner_id = auth.uid()` | `owner_id=eq.<me>` | `{owner_id}` | **A** |
| `tenant_id = (auth.jwt()->>'tenant')::uuid` | `tenant_id=eq.<mine>` | `{tenant_id}` | **A** |
| `is_public` | `is_public=eq.true` | `{is_public}` | **A** |
| RLS disabled | anything | `{}` | **A** |
| `owner_id = auth.uid()` | *(no filter)* | `{owner_id}` ⊄ `{}` | B |

Note the last row: **the same policy falls to Tier B if the client does not filter.** This is the right incentive — the client asking for a narrower shape is the client that gets the fast path — and Sluice reports it.

**Staleness.** `P(k, C)` is constant over the shape's *rows*, but is it constant over *time*? Two cases, classified from the parse tree:

- **Stable predicate** — references only `R`'s columns, `current_setting`, `auth.*()` helpers, and immutable/stable builtins. Its value cannot change unless the JWT changes. **No lease needed.** Re-evaluated on `access_token` refresh.
- **Volatile predicate** — references other tables (a subquery), `now()`, or a volatile function. Its value *can* change (a membership revoked, a time window closing). A **lease** applies: the decision is re-evaluated every `authz_lease` (default `60s`) in the background, and revoked immediately if it flips.

Supabase's equivalent is *"Client access policies are cached for the duration of the connection"* — an unbounded lease. Sluice's is bounded, configurable, and reported.

### 7.2 Tier B — compiled in-process predicate

**Criterion.** `cols(P) ⊄ dom(E)` (so `P` varies per row), **but** `P` is *pure over the row*: its parse tree references only

- columns of `R`,
- `current_setting('request.jwt.claims', true)` and the `auth.*()` helper functions,
- literals,
- a whitelist of immutable operators and functions (comparison, boolean, arithmetic, `IN`, `LIKE`/`ILIKE`, `IS NULL`, casts between whitelisted types, `COALESCE`, `jsonb` field extraction, array containment).

Then `P` is compiled to a Go evaluator and evaluated **against the tuple already delivered by the WAL**. Cost is nanoseconds and **zero database round trips**.

**Parsing is separated from capability.** Policy text is parsed by [`pgplex/pgparser`](https://github.com/pgplex/pgparser), a pure-Go port of PostgreSQL's own `gram.y` whose nodes map 1:1 onto `parsenodes.h`. It is not a subset: it accepts every expression PostgreSQL accepts, with PostgreSQL's operator precedence, and it needs no cgo, so the binary stays static.

What remains a whitelist is the *semantic* layer — which parsed nodes Sluice is willing to evaluate itself (the list above). That decision lives in one place, `internal/expr/convert.go`, and it **fails closed**: an unrecognised node becomes a non-evaluable marker, which `Analyze` reports as non-compilable, which routes the subscription to Tier C. Never to Tier A.

The distinction matters, because the two layers fail differently. A gap in a *grammar* subset can silently produce the wrong tree: with no production for `CURRENT_USER`, a hand-written parser falls through to its identifier rule and yields a column reference — the predicate looks compilable, the WAL tuple has no such column, and every row is withheld with nothing in `/diagnostics` to explain it. A gap in the *semantic* whitelist cannot do that: the construct is structurally unrepresentable and is reported by name.

Runtime cross-checks against PostgreSQL (`SLUICE_TIER_B_VERIFY`) cover what is left, which is no longer misparsing but *misevaluation*: a coercion difference, a collation-dependent comparison, a three-valued-logic corner.

Tier B is what makes `DELETE` correct ([§7.4](#74-delete-and-why-sluice-is-correct-where-supabase-is-not)).

### 7.3 Tier C — impersonated probe (compatibility fallback)

**Criterion.** `P` contains a subquery, a join, a volatile function, or anything else the compiler does not whitelist.

For each change, for each Tier C subscription on that relation:

```sql
BEGIN;
  SELECT set_config('role', $role, true),
         set_config('request.jwt.claims', $claims, true);
  EXECUTE sluice_probe;   -- prepared once per change, PK literals inlined
ROLLBACK;
```

This is exactly the WALRUS model, and it is exactly as slow: ~13 µs per subscriber per change, capping around 100 changes/sec at 1,000 subscribers.

Tier C exists so that Sluice is **never wrong** and never refuses a legitimate policy. But it is treated as a defect to be surfaced, not a normal mode:

- Counted in `sluice_authz_tier_c_probes_total{relation,policy}`.
- Subject to a separate, configurable rate budget (`tier_c_max_probes_per_second`, default `2000`). Over budget, the change is delivered with `"degraded": "authz_rate_limited"` **and withheld** from Tier C subscribers — never delivered unauthorized.
- Reported in the subscribe response and in `/v1/diagnostics` with the offending policy text and the reason it could not be compiled.
- Optionally refused outright (`tier_c: "deny"`), for operators who want a hard guarantee that nothing on their deployment runs the slow path.

**Known limitation, stated plainly.** Tier C evaluates the policy against the *live* database, not against the MVCC snapshot of the WAL record. This is the correctness problem ElectricSQL documented. Sluice cannot fix it — PostgreSQL does not expose snapshot-time querying — so Sluice's answer is to make Tier C avoidable, visible, and rare, and to document that Tier A and Tier B do not have this problem because they evaluate against the tuple the WAL delivered.

### 7.4 DELETE, and why Sluice is correct where Supabase is not

Supabase documents:

> RLS policies are not applied to `DELETE` statements, because there is no way for Postgres to verify that a user has access to a deleted record.

And `supabase/walrus` truncates `old_record` to primary keys whenever RLS is enabled:

```sql
and ( not is_rls_enabled or (c).is_pkey ) -- if RLS enabled, we can't secure deletes so filter to pkey
```

**PostgreSQL has no such limitation.** Verified with RLS enabled and `REPLICA IDENTITY FULL`:

```
table public.t_rls: DELETE: id[integer]:2 owner[text]:'bob' secret[text]:'BOB-TOP-SECRET'
```

The full old row is right there. The restriction is a consequence of *how walrus authorizes* (a `SELECT EXISTS` probe against a row that no longer exists), not of the WAL.

Sluice's Tier A and Tier B evaluate the predicate **against the old tuple**, which the WAL already delivered. There is nothing to look up, so `DELETE` is authorized exactly like `INSERT` and `UPDATE`, with the full old row, correctly, at zero cost.

Two fallback techniques were implemented and verified for Tier C, where the predicate cannot be evaluated in process:

- **Resurrect-and-rollback** — reinsert the old tuple in a subtransaction, `SELECT` it under the subscriber's role, force rollback. Correct (`alice: t`, `bob: f`, row stayed deleted). ~18 ms for 1,000 subscribers.
- **Synthetic-record evaluation** — evaluate the combined predicate against `jsonb_populate_record(null::R, old_tuple)`. Correct including a cross-table `EXISTS` and a RESTRICTIVE policy. ~34 ms for 1,000 subscribers.

**Neither is in v1.** Both fix correctness without fixing throughput, and Tier A/B already handle the cases that matter. They are documented here as the Tier C escape hatch and may be added behind a flag if a real workload demands it. For v1, a Tier C subscription receives `DELETE` events with `old` truncated to the replica identity columns and a `"degraded": "delete_authz_unavailable"` marker — the same behaviour as Supabase, but *labelled*, so the client knows.

### 7.5 Tier resolution summary

```
subscribe(shape, jwt)
  │
  ├─ verify JWT (ES256, alg pinned, JWKS)          → 401 on failure
  ├─ check session liveness (in-memory)            → 401 session_revoked
  ├─ resolve role ρ, claims C
  ├─ check has_column_privilege for every column   → 403 column_not_granted
  ├─ load combined predicate P from catalog cache
  │
  ├─ if P ≡ TRUE (no RLS, or BYPASSRLS)            → TIER A (accept)
  ├─ if cols(P) ⊆ dom(E)
  │     └─ evaluate P(k, C) once under ρ
  │           ├─ TRUE  → TIER A  (+ lease if volatile)
  │           └─ FALSE → 403 shape_not_authorized
  ├─ if P is pure over the row
  │     └─ compile to Go evaluator                 → TIER B
  └─ else                                          → TIER C (or 403 if tier_c=deny)
```

The response tells the client which tier it got and why, and Sluice records it.

### 7.6 How policies should be written, and why Sluice reads them that way

Two rules govern RLS performance inside PostgreSQL, and both interact with tier resolution. Sluice implements the first and reports on both.

**Wrap per-query functions in a scalar subquery.** `auth.uid()` is `STABLE`, not `IMMUTABLE`, so the planner will not hoist it out of a sequential scan on its own: it is re-invoked for every row examined. Wrapping it — `(select auth.uid())` — makes the planner treat it as an InitPlan and evaluate it once for the whole query. Supabase measured 179 ms against 9 ms over 100,000 rows, and worse for policies wrapping a `SECURITY DEFINER` function.

The catch is what that does to the stored predicate. PostgreSQL keeps `pg_get_expr(polqual, polrelid)` in its own normalised spelling, and the wrapper survives as a subquery node:

```
create policy p on documents for select to authenticated
  using (owner_id = (select auth.uid()));

pg_get_expr → (owner_id = ( SELECT auth.uid() AS uid))
```

A parser that treats every `SELECT` as opaque therefore classifies the *recommended* spelling as Tier C — the one path Sluice exists to avoid — and `/diagnostics` then advises denormalising a policy that is already optimal. So the parser unwraps a **FROM-less** select into the expression it contains, in all three positions PostgreSQL emits one: scalar, `IN (select …)`, and `= ANY (select …)`, the last two of which PostgreSQL normalises to the same `IN` form. Anything with a `FROM` is still a real subquery and still Tier C, because its value is not a function of the WAL tuple and the claim set. Wrapping therefore changes nothing about the tier: `(select auth.uid())` and `auth.uid()` resolve identically, and the fixtures assert exactly that.

**Name the roles with `TO`.** A policy with no `TO` clause applies to `PUBLIC`, so PostgreSQL evaluates the whole predicate for `anon` before discovering `anon` was never eligible. `TO authenticated` skips it outright. Sluice already honours `polroles` when it combines predicates (a policy that does not apply to the caller's role is not part of their predicate), so this costs nothing here — but it costs the application on every ordinary query, so it is reported.

Neither rule can be enforced from outside the database, so both are surfaced as diagnostics with a runnable `ALTER POLICY`, alongside a third that comes from the same guide: an index on every column a policy reads.

| Code | Fires when |
| --- | --- |
| `policy_function_not_wrapped` | a per-query call (`auth.*`, `current_setting`) is not inside `(select …)` |
| `policy_applies_to_public` | the policy has no `TO` clause |
| `unindexed_policy_column` | a column the predicate reads leads no index |

These are about the cost of the policy **inside PostgreSQL** — snapshots, Tier C probes, and every query the application itself makes. They never change a tier.

---

## 8. Shapes, filters and the routing index

### 8.1 Filter grammar

Deliberately narrow, PostgREST-shaped, and **AND-only**:

```
filter    := conj ( "," conj )*
conj      := [ "not." ] column "." op "." value
op        := eq | neq | lt | lte | gt | gte | in | like | ilike | is
```

Examples: `owner_id=eq.7f3a...`, `status=in.(open,pending),priority=gte.3`, `archived=is.false`, `title=not.like.*draft*`.

**No `OR`.** Not an oversight: `OR` destroys constant indexing and is also unsupported by Supabase. A client needing `OR` registers two subscriptions.

**Values are typed against `pg_attribute`** at subscribe time and rejected if they do not parse. The filter is never interpolated into SQL — it is compiled to a Go evaluator and, when a probe is needed, passed as bound parameters.

### 8.2 Narrowing only

When the **issuer** oracle authorizes a shape, any client-supplied refinement is combined as:

```
effective = authorized_filter AND client_filter
```

Rules, enforced in Sluice (the issuer can be wrong; Sluice does not trust it):

- The authorized filter uses the same grammar as [§8.1](#81-filter-grammar) and must contain **at least one non-negated equality**. Otherwise deny — that would be a whole-table grant. An *empty result* (zero rows currently matching) is not an error.
- For each authorized equality, the effective filter carries that constant. The client may omit it or add terms. A conflicting equality is a **deny**, not a silently empty shape.
- Columns are the intersection of the request and the issuer allowlist. Empty intersection is a deny. If the issuer omits `columns`, Sluice uses every column the **pool role** can `SELECT` (physical privilege, not the JWT ACL).

The `rls` oracle does not use a separate authorized filter: `Visible` applies the policy. The client still cannot widen *relative to the policy*.

This is Electric's rule and it is not negotiable.

### 8.3 The routing index

```
relations: map[relOID] → relationIndex

relationIndex:
    byColumn:  map[attnum] → map[constantKey] → subscriptionSet
    unindexed: subscriptionSet
```

On registration, Sluice picks a **routing key**: the equality constraint on the most selective indexed column (preferring columns covered by a unique index, then any indexed column, then any column). Remaining constraints become **residual predicates** evaluated in process.

On a change to relation `R`:

```
candidates := ∅
for each (attnum, constMap) in R.byColumn:
    v := tuple value at attnum          // from new tuple, or old tuple for DELETE
    candidates ∪= constMap[key(v)]
candidates ∪= R.unindexed

for each subscription in candidates:
    if !opMatches(subscription, change.op)      { continue }
    if !residualFilterMatches(subscription, t)  { continue }
    if !authorize(subscription, t)              { continue }   // Tier A: no-op
    emit(subscription, project(subscription.columns, t))
```

For a shape with an equality on an indexed column, `candidates` is exactly the interested set: **O(1) in the number of subscribers**.

Shapes landing in `unindexed` are scanned for every change to that relation. They are counted (`sluice_subscriptions{indexed="false"}`), rate-budgeted, and reported. This is the same trade-off Electric measured as 5,000 vs 140 changes/sec.

**For UPDATE, both tuples are routed.** A row moving out of a shape must produce a `leave`-style event, or the client's view goes stale — this is [supabase/walrus#64](https://github.com/supabase/walrus/issues/64), still open. Sluice emits:

| Old matches | New matches | Emitted |
| --- | --- | --- |
| no | yes | `change` with `op: "INSERT"` and `"transition": "enter"` |
| yes | yes | `change` with `op: "UPDATE"` |
| yes | no | `change` with `op: "DELETE"` and `"transition": "leave"` |
| no | no | nothing |

Evaluating "old matches" requires the old tuple's filter columns, which is what [§9](#9-replica-identity-requirements) is about.

### 8.4 Column projection

`columns` is optional; default is all columns the role may select. The replica identity columns are **always** included, so the client can always identify the row. Requested columns are intersected with `has_column_privilege`.

---

## 9. Replica identity requirements

The old tuple is needed for three things: authorizing `DELETE`, detecting shape transitions on `UPDATE`, and giving clients previous values. What arrives depends entirely on `REPLICA IDENTITY`.

Measured on a table with a 300 KB TOAST-able column:

| | `DEFAULT` | `USING INDEX (owner_id, id)` | `FULL` |
| --- | --- | --- | --- |
| DELETE old tuple | PK only | **PK + `owner_id`** | every column |
| DELETE message size | ~40 B | **93 B** | 300,127 B |
| DELETE WAL bytes | ~200 B | **248 B** | 3,704 B |
| UPDATE WAL bytes, untouched TOAST column | 200 B | **216 B** | 3,696 B |

`REPLICA IDENTITY FULL` calls `toast_flatten_tuple()`, inlining the entire out-of-line value into the old tuple on **every** UPDATE and DELETE, even though it was never touched. That is a **3,227× larger message** and **15–17× more WAL** for the same operation.

### The recommendation

**`REPLICA IDENTITY USING INDEX` on a unique index covering `(filter columns…, pk)`.**

```sql
CREATE UNIQUE INDEX documents_ri ON documents (owner_id, id);
ALTER TABLE documents REPLICA IDENTITY USING INDEX documents_ri;
```

Requirements: unique, not partial, not deferrable, no expressions, all columns `NOT NULL`. A composite `(filter_col, pk)` index satisfies this trivially and is usually useful for queries anyway.

**The trap Sluice must guard.** Dropping the index leaves `relreplident = 'i'` pointing at nothing, silently degrading to `NOTHING`, and then:

```
delete from docs where id = 4;
ERROR:  cannot delete from table "docs" because it does not have a replica identity and publishes deletes
```

**Application deletes fail.** Sluice validates this at startup and on every `Relation` message ([§19](#19-startup-validation)).

**Holds (issuer oracle).** Every column in a hold filter must be in that table's replica identity. If it is not, **subscribe is denied** with an actionable remedy — not a warning, not an optional strict flag. Without those columns in the old tuple, a DELETE cannot cut the grant. This is PostgreSQL `REPLICA IDENTITY`, not a Sluice convention.

### What Sluice does at subscribe time

If a shape's filter references a column not present in the relation's replica identity, **and** `ops` includes `DELETE` or transition detection is requested, Sluice returns the subscription with a warning and the exact remedy:

```json
{
  "sub": "docs",
  "tier": "A",
  "warnings": [{
    "code": "replica_identity_insufficient",
    "message": "DELETE events for this shape cannot be filtered or authorized: column \"owner_id\" is not in the replica identity of public.documents (currently DEFAULT).",
    "effect": "DELETE events will be delivered with old limited to primary keys and marked degraded.",
    "remedy": "CREATE UNIQUE INDEX documents_ri ON public.documents (owner_id, id); ALTER TABLE public.documents REPLICA IDENTITY USING INDEX documents_ri;"
  }]
}
```

Configurable to a hard `403` (`replica_identity: "strict"`) for operators who prefer to fail loudly.

### Unchanged TOAST

`pgoutput` marks unchanged TOASTed columns with the TupleData byte `'u'`. Sluice **never** treats these as `NULL` or as absent-meaning-empty. They are reported explicitly:

```json
{ "record": { "id": 1, "views": 42 }, "unchanged": ["body"] }
```

The client knows `body` is unchanged, not deleted. This is exactly the trap that makes `wal2json` unsuitable — it drops such columns from its JSON with no marker at all, so a consumer cannot distinguish "unchanged" from "not selected" from "dropped column", and a counter update silently blanks the article body on every subscriber.

`sluice_toast_unchanged_total{relation,column}` counts these so operators can find tables where `SET STORAGE MAIN` or a narrower shape would help.

---

## 10. Wire protocol

### 10.1 Design principles, borrowed from MCP

MCP built the POST-plus-SSE hybrid, then removed most of it in revision `2026-07-28`. The reasons are documented and directly applicable, so Sluice adopts the *later* shape:

- **POST-only.** The long-lived stream is a `POST` whose response is `text/event-stream`. Not `GET`, not `EventSource`. This is what makes `Authorization: Bearer` possible and removes every token-in-URL workaround.
- **Closing the stream is the signal.** No `unsubscribe`-on-disconnect message; a transport-level close is unambiguous.
- **`X-Accel-Buffering: no`** on every stream response.
- **Routing metadata mirrored into headers** so intermediaries never parse bodies — and **the server rejects any request whose header and body disagree**, because a load balancer routing on the header while the server acts on the body is a real vulnerability class.

Where Sluice deviates: MCP removed sessions entirely because its upstreams could be stateless. A fan-out server cannot be stateless — a Stream *is* server-side state. So Sluice keeps a `stream_id`, but makes routing explicit rather than requiring sticky sessions: `stream_id` is `<node_id>.<nonce>`, so a control POST landing on any node is forwarded to the owning node over the bus. In v1 the bus is in-process and this is a no-op; the seam exists so horizontal scale does not require load-balancer cooperation.

### 10.2 Endpoints

All under a configurable prefix, default `/sluice/v1`.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/stream` | Open the SSE stream. Long-lived. |
| `POST` | `/subscribe` | Add subscriptions to a stream. |
| `POST` | `/unsubscribe` | Remove subscriptions. |
| `POST` | `/publish` | Broadcast to a channel. |
| `POST` | `/presence` | `track` / `update` / `untrack`. |
| `POST` | `/token` | Rebind the stream to a refreshed access token. |
| `POST` | `/admin/jwks/refresh` | Reload JWKS. `service_role`. |
| `POST` | `/admin/shapes/drop` | Ops: drop matching issuer/rls shapes by identity + relation + equalities. Walks this node's subscriptions; not the hot path. `service_role` or the issuer bearer. |
| `GET` | `/healthz` | Liveness. Unauthenticated. |
| `GET` | `/readyz` | Readiness: slot connected, catalog loaded. |
| `GET` | `/metrics` | Prometheus. Optionally token-protected. |
| `GET` | `/diagnostics` | Human-readable configuration warnings. `service_role` only. |
| any | anything else | `404` JSON |

Authentication is `Authorization: Bearer <jwt>` on every endpoint except `/healthz`. No API key in the query string, ever.

### 10.3 Opening a stream

```http
POST /sluice/v1/stream HTTP/2
Authorization: Bearer eyJhbGciOiJFUzI1NiIs...
Content-Type: application/json
Accept: text/event-stream

{
  "subscriptions": [
    { "sub": "docs",
      "shape": { "schema": "public", "table": "documents",
                 "ops": ["INSERT","UPDATE","DELETE"],
                 "filter": "owner_id=eq.7f3a1c2e-...",
                 "columns": ["id","title","updated_at"],
                 "initial": "snapshot",
                 "transitions": true } },
    { "sub": "room",  "channel": "room:42", "presence": true },
    { "sub": "order", "channel": "sluice:orders:1" }
  ],
  "resume": { "public.documents": "1A2B/3C4D18" }
}
```

Response:

```http
HTTP/2 200
Content-Type: text/event-stream
Cache-Control: no-store
X-Accel-Buffering: no
Connection: keep-alive
```

Subscriptions may also be supplied later via `/subscribe`; both paths are equivalent. Supplying them here avoids one round trip on cold start.

### 10.4 Server → client events

Named SSE events, JSON `data`, one line. Heartbeat is a bare comment.

**`ready`** — always first.

```
event: ready
data: {"stream_id":"n1.k7Fq3xZm","server_time":"2026-08-08T15:04:05.123Z",
       "heartbeat_ms":20000,"wal_lsn":"1A2B/3C4D18",
       "subscriptions":[
         {"sub":"docs","tier":"A","indexed":true,"routing_key":"owner_id",
          "warnings":[]},
         {"sub":"inbox","ok":true,"oracle":"issuer","filter":"project_id=eq.42",
          "indexed":true,"routing_key":"project_id"},
         {"sub":"room","ok":true}]}
```

RLS subscriptions send `tier` A/B/C (`oracle` may be omitted). Issuer subscriptions send `oracle: "issuer"` and the effective `filter`, and **do not** send `tier`.

**`change`** — replication plane.

```
event: change
id: 1A2B/3C4D18:3
data: {"sub":"docs","op":"UPDATE","schema":"public","table":"documents",
       "commit_lsn":"1A2B/3C4D18","commit_time":"2026-08-08T15:04:06.001Z","seq":3,
       "record":{"id":91,"title":"Q3 plan","updated_at":"2026-08-08T15:04:06Z"},
       "old":{"id":91,"title":"Q3 draft"},
       "unchanged":["body"],
       "transition":null}
```

`id` is `<commit_lsn>:<seq>`, monotonic within a relation, usable for resume.

**`broadcast`** — signalling plane. No `id`; broadcasts are not resumable.

```
event: broadcast
data: {"sub":"room","channel":"room:42","event":"cursor",
       "payload":{"x":10,"y":42},"from":"7f3a1c2e-...","at":"2026-08-08T15:04:06.100Z"}
```

Database-originated broadcasts carry `"origin":"database"` and a `commit_lsn`.

**`presence`**

```
event: presence
data: {"sub":"room","channel":"room:42","type":"state",
       "members":{"7f3a...":{"meta":{"name":"Pau"},"since":"...","ref":"r1"}}}
```

```
event: presence
data: {"sub":"room","channel":"room:42","type":"diff",
       "joins":{"9b1e...":{...}},"leaves":{"4c7d...":{...}}}
```

**`error`** — subscription-scoped or stream-scoped.

```
event: error
data: {"sub":"docs","code":"shape_not_authorized","message":"...","retryable":false}
```

```
event: error
data: {"code":"session_revoked","message":"Session no longer exists.","retryable":false}
```

Stream-scoped errors are followed by stream closure. Subscription-scoped errors leave
the stream open and other subscriptions unaffected.

**`heartbeat`** — a bare comment, per the WHATWG recommendation:

```
: hb
```

Interval `heartbeat_ms`, default 20,000. This clears nginx's and AWS ALB's 60-second defaults with 2× margin and stays well under Cloudflare's 125-second Proxy Read Timeout. It is deliberately **not** below 15 s: frequent keepalives wake the cellular modem and reset the LTE RRC inactivity timer, which is a documented battery problem. Clients should close the stream on `document.hidden` and reopen with `resume` — the standard answer, and the one `@microsoft/fetch-event-source` implements.

### 10.5 Client → server

**`/subscribe`** and **`/unsubscribe`**

```json
{ "stream_id": "n1.k7Fq3xZm",
  "subscriptions": [ { "sub": "tasks", "shape": { ... } } ] }
```

Mirrored header: `Sluice-Stream-Id: n1.k7Fq3xZm`. If it disagrees with the body, `400`.

Response is per-subscription, and a partial failure is not a request failure:

```json
{ "results": [
    { "sub": "tasks", "ok": true, "tier": "B", "indexed": false,
      "warnings": [{ "code":"unindexed_shape", "message":"...", "remedy":"..." }] },
    { "sub": "inbox", "ok": true, "oracle": "issuer", "filter": "project_id=eq.42",
      "indexed": true, "routing_key": "project_id" } ] }
```

**`/publish`**

```json
{ "stream_id":"n1.k7Fq3xZm", "channel":"room:42",
  "event":"cursor", "payload":{"x":10,"y":42}, "self":false }
```

**`/presence`**

```json
{ "stream_id":"n1.k7Fq3xZm", "channel":"room:42",
  "action":"track", "key":"7f3a1c2e-...", "meta":{"name":"Pau"} }
```

`key` defaults to the JWT `sub`. A client may not track a key it is not authorized for (default: `key` must equal `sub` unless the channel policy says otherwise).

**`/token`**

```json
{ "stream_id":"n1.k7Fq3xZm", "access_token":"eyJ..." }
```

Re-verifies, rejects a `sub` change, and re-authorizes every shape on the stream. **RLS:** re-evaluates every tiered decision (claims may have changed). **Issuer:** one HTTP hop per shape (`action: "refresh"`); a deny drops that subscription, the stream survives. There is no periodic lease to the issuer.

An expired access token without `/token` closes the **stream** with `token_expired`. A hold that disappears cuts **that shape** with `shape_not_authorized` while the token is still valid.

**Issuer HTTP** (internal, `Authorization: Bearer <SLUICE_ISSUER_BEARER>`). One hop at join and one per shape on `/token`. Timeout / non-2xx / unreadable body → deny. Identity only (`role`, `sub`, `session_id`, `claims`); the user access token is not forwarded.

Request:

```json
{
  "action": "subscribe",
  "identity": { "role": "authenticated", "sub": "...", "session_id": "...", "claims": {} },
  "requested": {
    "schema": "public", "table": "documents",
    "filter": "project_id=eq.42,status=eq.open",
    "columns": ["id", "title"],
    "ops": ["INSERT", "UPDATE", "DELETE"]
  }
}
```

Response 200:

```json
{
  "allow": true,
  "shape": {
    "schema": "public", "table": "documents",
    "filter": "project_id=eq.42",
    "columns": ["id", "title", "body"]
  },
  "holds": [
    { "schema": "public", "table": "project_members",
      "filter": "project_id=eq.42,user_id=eq.<sub>" }
  ]
}
```

`holds` is required and non-empty. Each hold table must be in the publication; its filter must have equalities whose columns are in that table's replica identity. `shape.schema` / `shape.table` must match the catalog (`relname`, not an alias): `Documents` ≠ `documents`, and a grant for another name is denied. PostgreSQL folds unquoted identifiers to lowercase; quoted names that differ are distinct relations.

### 10.6 Resume semantics

`resume` maps relation → last received `commit_lsn`. On reconnect, Sluice replays from the per-relation ring buffer, re-filtering and re-authorizing every replayed change against the *current* subscription — replay is not trusted to have been authorized before.

If the requested LSN is older than the ring buffer's floor:

```
event: error
data: {"sub":"docs","code":"resume_too_old","message":"...",
       "retryable":true,"action":"resnapshot"}
```

The ring is bounded per relation (`ring_events` default 4,096, `ring_max_age` default `60s`), giving `O(relations × ring)` memory rather than `O(shapes × ring)`. Replay cost is `O(ring)` per reconnecting client, which is acceptable because reconnections are rare relative to changes.

This is more than Supabase offers for Postgres Changes (nothing) and honest about its bound, which is the position Electric also takes.

---

## 11. Data flows

### 11.1 Subscribe — all the expensive work happens once

```
client ──POST /subscribe {shape, filter} + Bearer──► Sluice
   │
   ├─ verify JWT: ES256, alg pinned, kid → JWKS cache            [~µs, no I/O]
   ├─ session liveness check                                     [in-memory set]
   ├─ catalog lookup: policies, grants, columns, RI, indexes      [cached]
   ├─ authorize:
   │    Tier A → one impersonated expression evaluation          [1 query, once]
   │    Tier B → compile predicate to Go evaluator               [0 queries]
   │    Tier C → mark, budget, report                            [0 queries now]
   ├─ (optional) initial snapshot                                [1 query, paged]
   └─ index by routing key                                       [O(1) insert]
   ◄── per-subscription result with tier, warnings, remedies
```

### 11.2 Change — no authorization work on this path

```
app ──INSERT/UPDATE/DELETE──► PostgreSQL
        │ validates grants + RLS, executes, writes WAL
        ▼
   walsender ── pgoutput ──► Reader (one permanent slot, START_REPLICATION)
        │
        ├─ Relation msg → update relation cache, revalidate RI, invalidate policy cache
        ├─ Begin/Commit → carry commit_lsn and commit_time
        └─ Insert/Update/Delete
              ├─ decode tuple (text format; 'u' → unchanged, 'n' → null)
              ├─ route: value at routing column → map lookup       [O(1)]
              ├─ per candidate: op match, residual filter, authz   [Tier A: no-op]
              ├─ project columns, append to relation ring buffer
              └─ enqueue to each stream's bounded queue → SSE flush
        ▼
   SendStandbyStatusUpdate(confirmed_lsn) once delivered/queued
```

### 11.3 Database-originated broadcast — atomic, no tables

```
BEGIN;
  UPDATE orders SET status='paid' WHERE id=1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid"}');
COMMIT;
        │  same slot, same ordering, same transaction
        ▼
   Reader → Message msg (prefix, transactional, content)
        ├─ strip the configured prefix → channel name
        ├─ authorize channel per subscription (cached at subscribe)
        └─ emit as `broadcast` with origin:"database" and commit_lsn
```

Roll the transaction back and the message never existed.

### 11.4 Client broadcast

```
client A ──POST /publish {channel, event, payload}──► Sluice
             ├─ channel authorization (cached from subscribe)
             ├─ payload size check
             └─ hub fan-out → clients B..N (SSE), and A if self:true
```

### 11.5 Presence

```
client ──POST /presence {action:"track", key, meta}──► Sluice
             ├─ key authorization (default: key == jwt.sub)
             ├─ update in-memory channel state
             └─ coalesce into the next diff tick (presence_broadcast_ms, default 1500)

stream closes (any reason)
             └─ remove all keys owned by that stream → `leave` in the next diff
```

No database involvement at any point. New joiners get a full `state` event; everyone else gets `diff`.

### 11.6 Revocation

```
user signs out ──► GoTrue DELETEs auth.sessions row
        │
        ▼
   same slot ──► Reader sees DELETE on auth.sessions
        ├─ extract session id from the old tuple (PK, always present)
        ├─ look up streams holding that session_id
        └─ emit error{code:"session_revoked"} and close them
```

Milliseconds, zero polling, zero extra queries.

---

## 12. The replication reader

### 12.1 Connection and options

A dedicated `pgconn.PgConn` with `replication=database`. Not part of `pgxpool`.

```
CREATE_REPLICATION_SLOT sluice LOGICAL pgoutput          -- permanent, once
START_REPLICATION SLOT sluice LOGICAL 0/0 (
    proto_version   '4',
    publication_names 'sluice',
    messages        'true',
    binary          'false',
    streaming       'off',
    origin          'any'
)
```

Every option here is a deliberate, measured choice.

**`proto_version '4'`.** Verified: the negotiated version **alone changes nothing**. Requesting 1, 4, or 4 with `streaming=parallel` produced byte-identical output — same MD5, same 528 bytes, same message sequence. The version declares *capability*; the **options** determine the message set. `Stream*` messages appear only with `streaming` on/parallel, two-phase messages only with `two_phase` on, and the two extra `Stream Abort` fields only with `streaming=parallel`. Requesting 4 costs nothing, is safe, and means enabling `streaming` later is a config flag rather than a protocol change. The version is a per-connection option, not slot state, so it can be changed at any time by reconnecting — there is no migration.

The server enforces the rules with clear errors, so a misconfiguration is loud:

```
ERROR:  requested proto_version=2 does not support parallel streaming, need 4 or higher
ERROR:  requested proto_version=1 does not support streaming, need 2 or higher
ERROR:  client sent proto_version=5 but server only supports protocol 4 or lower
```

**`streaming 'off'`.** This is the important one. With streaming off, **everything the reader receives is already committed and durable**, so it can forward immediately with zero buffering and the reader is effectively stateless. With `streaming on` you receive *uncommitted* changes and **must** buffer them in the Go heap until `Stream Commit`, because emitting a change from a transaction that later aborts is unacceptable. Either way the transaction is buffered somewhere; server-side is disk-backed and bounded by `logical_decoding_work_mem`, client-side is a heap that OOMs and kills every connection on the node. `pg_stat_replication_slots.spill_txns`/`spill_bytes` makes the server-side cost observable, so this is not a blind choice.

Sluice supports `streaming = on` behind a flag, and its decoder implements the full v4 message set including `Stream Abort`'s parallel-mode fields, so the option is real rather than aspirational. The reader computes the minimum sufficient `proto_version` from the enabled options and refuses combinations its decoder cannot parse — capability, not aspiration.

**`binary 'false'`.** Measured: the same INSERT was **88 bytes in text format and 112 bytes in binary** — binary is 27% *larger* here, because text is compact for small values (`"1"` is one byte; binary `int4` is always four) while arrays and numerics carry heavy headers. And binary would require implementing `numeric`'s four-word struct, `timestamptz` as int64 microseconds since 2000-01-01, the full array header, and `jsonb`'s leading version byte — plus the text path anyway, because `pgoutput` falls back to `'t'` per column for types with no `typsend`. Since Sluice emits JSON, text format is both smaller and closer to the output. This corrects an earlier assumption that binary would be faster.

**`messages 'true'`** enables `pg_logical_emit_message` delivery. Verified that `messages=false` removes the `M` message and its surrounding `B`/`C`.

**`origin 'any'`** for now; `none` becomes relevant only in bidirectional replication topologies.

### 12.2 Feedback loop

```go
for {
    msg := recv(ctx)            // with deadline < wal_sender_timeout/2
    switch msg {
    case PrimaryKeepalive:
        if msg.ReplyRequested { sendStandbyStatusUpdate(confirmed) }
    case XLogData:
        m := pgoutput.Parse(msg.WALData)
        dispatch(m)
    }
    if time.Since(lastFeedback) > statusInterval { sendStandbyStatusUpdate(confirmed) }
}
```

`statusInterval` default 10 s, matching `pg_recvlogical`. `wal_sender_timeout` defaults to 60 s, so the deadline is set to 20 s to guarantee a reply well inside it.

**`confirmed` advances only after a change has been queued to every interested stream or deliberately dropped.** This is the backpressure mechanism: a stalled Sluice retains WAL rather than losing data. Bounded by `max_slot_wal_keep_size`; if the slot is invalidated (`wal_removed`), Sluice logs a fatal diagnostic and requires an explicit operator action, because silently resuming would mean silent data loss.

### 12.3 At-least-once and idempotency

PostgreSQL documents:

> The current position of each slot is persisted only at checkpoint, so in the case of a crash the slot might return to an earlier LSN... **Logical decoding clients are responsible for avoiding ill effects from handling the same message more than once.**

Sluice therefore guarantees **at-least-once**, not exactly-once, and says so in the protocol docs. Every `change` carries `commit_lsn` and `seq`, which together are a total order within a relation. Clients upsert by primary key and ignore `(commit_lsn, seq)` pairs they have already applied.

Sluice additionally drops any change whose `(commit_lsn, seq)` is `<=` the last one it emitted for that relation, which suppresses the common post-crash replay window without claiming exactly-once.

### 12.4 Leader election

```sql
SELECT pg_try_advisory_lock(hashtext('sluice:' || $slot_name));
```

Held on a dedicated connection for the reader's lifetime. Only the leader opens the replication connection. Followers serve streams and forward changes received over the bus. This is Electric's approach and it prevents the "N slots each retaining WAL" failure mode.

### 12.5 Schema changes

`pgoutput` re-sends `Relation` when a relation's definition changes. On receipt Sluice:

1. Diffs the column list. Columns that disappeared are removed from projections; subscriptions requesting them get a `warning` event.
2. Revalidates the replica identity and emits/clears warnings.
3. In **rls** mode: invalidates the cached policy set for that relation and re-resolves the tier for every subscription on it. **A policy change must be able to revoke access**, so re-resolution can downgrade Tier A to a denial. In **issuer** mode the catalog tick does **not** call the issuer and does not re-evaluate RLS; it drops subscriptions and hold watches whose relation left the publication (`relation_unpublished`) and those whose hold replica identity no longer covers the hold filter (`shape_not_authorized`, same reason as join).
4. Emits `event: warning` with `code: "schema_changed"` to affected subscriptions.

Because DDL is not replicated, `Relation` is the only in-band signal available. A periodic catalog poll (`catalog_refresh`, default `30s`) covers policy changes that do not alter a relation's shape and therefore produce no `Relation` message.

---

## 13. Initial snapshots and the subscribe race

The classic race: fetch initial state over HTTP, then subscribe, and lose everything that changed in between. Supabase Realtime does not solve this. Sluice does.

`"initial": "snapshot"` on a shape makes Sluice serve the initial read itself, so the snapshot and the stream position are consistent:

```
1. floor := reader.confirmedLSN          // reader is always at or behind the WAL head
2. BEGIN ISOLATION LEVEL REPEATABLE READ;
     -- rls: SET LOCAL role + claims so PostgreSQL applies RLS
     -- issuer: no SET ROLE; SELECT as the pool role (BYPASSRLS or RLS off)
     SELECT <columns> FROM <table> WHERE <effective filter> ORDER BY <pk> LIMIT <page>;
     ... paged ...
   COMMIT;
3. replay the relation ring buffer from `floor`, re-filtered and re-authorized
4. continue live
```

Zero rows is success: `snapshot_end` with `rows: 0` is a valid empty shape. The permission lives in the hold, not in “the table already has rows”.

Taking `floor` from the **reader's confirmed LSN before the snapshot transaction begins** guarantees no gaps: the reader cannot be ahead of any transaction the snapshot can see, so replaying from `floor` covers every commit the snapshot might have missed. Duplicates are possible and expected — clients upsert by primary key. This is deliberately the safe direction; using `pg_current_wal_lsn()` instead would leave a narrow window where a commit record written before the snapshot but not yet visible could be skipped.

The snapshot is emitted as ordinary `change` events with `op: "INSERT"` and `"snapshot": true`, terminated by:

```
event: snapshot_end
data: {"sub":"docs","rows":1204,"floor_lsn":"1A2B/3C4D18"}
```

Costs, stated plainly: a real query per subscribing client, executed under RLS, which is the one place Sluice deliberately puts load on the database. It is rate-limited (`snapshot_max_concurrent`, default 4; `snapshot_max_rows`, default 50,000) and metered. `"initial": "none"` is the default.

---

## 14. The signalling plane

### 14.1 Channels and authorization

A channel is a string with a namespace prefix: `<namespace>:<name>`, e.g. `room:42`.

Sluice does **not** create a `realtime.messages` table, and it does **not** use insert-and-rollback against one. Channel authorization has three modes, configured per namespace:

| Mode | Behaviour |
| --- | --- |
| `public` | Any authenticated stream may subscribe and publish. |
| `owner` | The channel name must contain the caller's `sub`. Centrifugo's `#user_id` idea: `notify:7f3a1c2e-...` is subscribable only by that user. O(1), no I/O. |
| `hook` | Sluice `POST`s the subscribe/publish request to a configured URL and honours the verdict, with a cache TTL. Your application decides; Sluice does not learn your auth model. |

`hook` is Electric's gatekeeper pattern and Centrifugo's proxy pattern. It is the escape hatch for arbitrary business rules, and the docs will recommend colocating the hook (Centrifugo's sidecar advice) because it is on the subscribe path.

Namespaces are declared in config with their mode, payload limits and rate budgets. An unknown namespace is rejected — never implicitly public.

### 14.2 Broadcast

Fire-and-forget. Nothing persisted, no replay. `self` controls echo to the sender.

**No silent failures.** Supabase's protocol docs admit that with `ack: false` (the default) *"all push failures — including size violations and RLS write denials — are silently dropped. RLS denials are always silent regardless of `ack`."* Sluice's `/publish` is a request with a response: an oversized payload or an unauthorized channel returns `413` or `403` synchronously. There is no mode in which a publish silently vanishes.

### 14.3 Presence

In-memory, per channel: `map[key] → {meta, ref, since, streamID}`.

- Last-write-wins per key.
- `state` on join, `diff` thereafter, coalesced on a `presence_broadcast_ms` tick (default 1500, matching Phoenix.Tracker's period) so cursor spam cannot amplify into N² messages.
- Automatic `leave` on stream close — the transport-level close *is* the signal.
- `presence_max_keys_per_channel` (default 10, matching the documented Supabase limit) and `presence_max_calls_per_window` to stop `track()`-per-mousemove.

The documented Supabase behaviour of receiving simultaneous spurious `join`/`leave` during a `sync` is a consequence of Phoenix.Tracker's CRDT reconciliation. Single-node Sluice has no such reconciliation and therefore does not have that artefact. When multi-node arrives ([§22](#22-horizontal-scale)) it will, and the docs will say so.

---

## 15. Session revocation

A JWT is valid until `exp`, default one hour. A signed-out user would otherwise keep receiving data for up to an hour on an already-open stream. For a server whose entire job is holding long-lived connections, that is a real security property, not a nicety.

Verified in `supabase/auth` v2.195.0:

- The `session_id` claim **is** `auth.sessions.id` — `GenerateAccessToken` stringifies the same UUID it uses to `SELECT` the session, and refuses to issue a token without one.
- Sign-out **`DELETE`s** the row (`LogoutSession`, `Logout`, `LogoutAllExceptMe` are all `DELETE`). It does **not** set `refresh_tokens.revoked` — those rows vanish by cascade — so watching `revoked` would miss every sign-out.
- `auth.sessions` has `sessions_pkey (id)`, so it is replication-eligible with UPDATE and DELETE old-tuple keys.
- Supabase's own docs recommend exactly this check: *"You can check that the `session_id` claim in the JWT corresponds to a row in the `auth.sessions` table. If such a row does not exist, it means that the user has logged out."*

### Mechanism

```sql
ALTER PUBLICATION sluice ADD TABLE auth.sessions;   -- optional
ALTER PUBLICATION sluice ADD TABLE auth.users;      -- optional, for bans
```

Sluice consumes these on the **same slot it already reads**:

- `DELETE` on `auth.sessions` → close every stream holding that `session_id`.
- `UPDATE` on `auth.users` setting `banned_until` in the future → close every stream for that `user_id`. (v2.195.0 added *"reject AT from banned users"* to GoTrue itself; this mirrors it.)

At subscribe time, a stream's `session_id` is validated once against `auth.sessions` (one indexed lookup) if `revocation.verify_on_subscribe` is on. Thereafter revocation is push-based. Sessions expired by `not_after` are **not** deleted proactively — GoTrue prunes them 72 hours later — so Sluice also compares `not_after` when it does verify.

### Constraints this design respects

- **Entirely optional.** `revocation.enabled = false` by default. Sluice must run against any PostgreSQL with no GoTrue present. Nothing else in the design depends on it.
- **No `auth` schema knowledge leaks elsewhere.** It is one isolated module reading two table names from config.
- **Off the documented path.** Supabase does not document `auth.*` replication as supported, and GoTrue applies its own migrations on startup without regard for your publication. The docs will say so.

### JWKS cache busting

Supabase's docs warn that third-party components can trust a revoked signing key for up to ~20 minutes because of multi-level caching:

> Supabase products (Auth, Data API, Storage, Realtime) **do not rely on this cache and revocation is instantaneous.** Should this be an issue for you, ensure you've built a cache busting mechanism as part of your app's backend infrastructure.

Sluice is one of those components, so it exposes `POST /admin/jwks/refresh` (`service_role` only) which forces a JWKS refetch, and refetches automatically on an unknown `kid`.

---

## 16. Connection pooling and impersonation hygiene

Two separate connection resources:

1. **The replication connection.** Exactly one, outside any pool, owned by the leader.
2. **`pgxpool`** as `sluice_authz`, for Tier A evaluations, Tier C probes, catalog reads and snapshots. Default `pool_max_conns = 8`.

Because authorization is at subscribe time, this pool sees traffic proportional to **join rate**, not change rate. That is the difference between thousands of queries per second and tens.

### Impersonation must be transaction-scoped

Always:

```sql
BEGIN;
  SELECT set_config('role', $1, true),                 -- true = local
         set_config('request.jwt.claims', $2, true);
  ...
ROLLBACK;   -- or COMMIT; either way Postgres unwinds the settings
```

Never session-level `SET ROLE`. PostgreSQL unwinds transaction-scoped settings at COMMIT/ROLLBACK regardless of pool behaviour, which eliminates an entire class of bug: a connection returned to the pool with a stale `role` being handed to the next tenant.

`pgxpool` hook note: `BeforeAcquire` is **deprecated** in favour of `PrepareConn func(context.Context, *pgx.Conn) (bool, error)` (v5.7.6+). The error return matters — `BeforeAcquire` returning `false` cannot report *why*, so a failure silently destroys connections until you hit *"too many failed attempts acquiring connection"*. Sluice uses `PrepareConn` and also installs `AfterRelease` doing a defensive `DISCARD ALL` when `paranoid_pool_reset` is enabled.

The role name is validated against `pg_roles` and never string-interpolated. `pgx` `>= 5.9.2` is a hard floor (CVE-2026-41889, SQL injection via dollar-quoted literals in simple protocol), and Sluice uses the default extended protocol throughout.

### PgBouncer

Not used, not needed, and not planned for v1. If it is added later:

- Transaction pooling is compatible with everything Sluice does through the pool, because all state is transaction-scoped by design.
- The replication connection must **bypass** the pooler entirely. Config keeps `SLUICE_DB_REPL_URL` and `SLUICE_DB_AUTHZ_URL` separate precisely so this is a configuration change, not a code change.

---

## 17. Backpressure and limits

Three independent queues, each bounded, each with an explicit policy.

**PostgreSQL → Reader.** Bounded by the slot. If Sluice stalls, WAL accumulates on disk up to `max_slot_wal_keep_size`, then the slot is invalidated. Monitored via `sluice_slot_retained_bytes` with a warning threshold at 50% of the configured cap.

**Reader → Hub.** A bounded channel (`dispatch_queue`, default 8,192). Full means the reader blocks, which propagates backpressure to the slot. **Never drops.**

**Hub → Stream.** Per-stream bounded queue (`stream_queue`, default 256). On overflow, the policy is per-plane and this is the one place Sluice drops data:

| Plane | Policy on overflow |
| --- | --- |
| `change` | Emit `error{code:"stream_lagging", action:"resnapshot"}` and close the stream. A silently truncated change stream is worse than a closed one. |
| `broadcast` | Drop oldest, increment `sluice_stream_dropped_events_total{plane="broadcast"}`. Broadcast is explicitly best-effort. |
| `presence` | Coalesce — a newer diff supersedes an older one for the same channel. |

Slow consumers are additionally bounded by `http.ResponseController.SetWriteDeadline`, which avoids a watchdog goroutine per connection.

### Per-stream and per-tenant budgets

| Limit | Default | Notes |
| --- | --- | --- |
| `max_streams` | 50,000 | per process |
| `max_subscriptions_per_stream` | 100 | matches Supabase's channels-per-connection |
| `max_shape_subscriptions_per_stream` | 20 | shapes are more expensive than channels |
| `subscribe_rate` | 20/s per stream | |
| `publish_rate` | 100/s per stream | |
| `presence_rate` | 5 per 30 s per stream | matches the documented Supabase limit |
| `max_payload_bytes` | 256 KiB | broadcast |
| `max_change_bytes` | 1 MiB | beyond this, large values are elided and the change is marked `"degraded":"payload_too_large"` with the field names listed |
| `tier_c_max_probes_per_second` | 2,000 | global |
| `unindexed_shapes_max` | 200 | global; further unindexed shapes are refused |

### No timers per connection

A single shared timer wheel with jitter drives heartbeats and lease refreshes. A `time.Ticker` per connection is the documented way to make 100k connections expensive, and Go's own stdlib hit the equivalent battery bug (golang/go#48622).

---

## 18. Observability

The design principle is: **make the expensive path loud.** Supabase's central failure of ergonomics is that you can write a policy that costs you 100× and never be told.

### Metrics (Prometheus, `/metrics`)

Replication:

```
sluice_wal_lsn{kind="received|flushed|confirmed"}          gauge
sluice_wal_lag_bytes                                       gauge
sluice_slot_retained_bytes                                 gauge
sluice_slot_active                                         gauge
sluice_wal_messages_total{type="insert|update|delete|truncate|message|relation|begin|commit"} counter
sluice_reader_reconnects_total{reason}                      counter
sluice_reader_is_leader                                     gauge
```

Dispatch:

```
sluice_changes_total{schema,table,op}                       counter
sluice_change_dispatch_seconds{schema,table}                histogram
sluice_change_fanout_subscribers{schema,table}              histogram
sluice_routing_candidates{schema,table}                     histogram   # index effectiveness
sluice_toast_unchanged_total{schema,table,column}            counter
```

Authorization — the important ones:

```
sluice_subscriptions{schema,table,tier,indexed}             gauge
sluice_authz_resolutions_total{tier,result}                  counter
sluice_authz_resolve_seconds{tier}                           histogram
sluice_authz_tier_c_probes_total{schema,table}                counter   # ALERT ON THIS
sluice_authz_tier_c_seconds{schema,table}                     histogram
sluice_authz_lease_refreshes_total{result="held|revoked"}     counter
sluice_authz_compile_failures_total{schema,table,reason}      counter   # why Tier B was impossible
```

Streams:

```
sluice_streams                                               gauge
sluice_stream_queue_depth                                    histogram
sluice_stream_dropped_events_total{plane,reason}              counter
sluice_stream_closed_total{reason}                            counter
sluice_stream_lifetime_seconds                                histogram
```

Signalling and snapshots:

```
sluice_broadcast_published_total{namespace,origin="client|database"}  counter
sluice_presence_members{channel}                                      gauge
sluice_snapshot_seconds{schema,table}                                 histogram
sluice_snapshot_rows_total{schema,table}                              counter
sluice_revocations_total{source="session|ban"}                         counter
```

Config health:

```
sluice_config_warnings{code}                                 gauge
```

### `/diagnostics`

A human-readable JSON report, `service_role` only, answering "what is wrong with my setup?" — the endpoint Supabase does not have:

```json
{
  "oracle": "rls",
  "slot": { "name":"sluice", "active":true, "retained_bytes":1048576,
            "retained_pct_of_cap":0.02, "wal_status":"reserved" },
  "publication": { "name":"sluice", "tables":12, "has_row_filters":false },
  "warnings": [
    { "code":"tier_c_policy", "severity":"high",
      "relation":"public.invoices",
      "policy":"invoice_team_member",
      "predicate":"EXISTS (SELECT 1 FROM memberships m WHERE ...)",
      "reason":"predicate contains a subquery and cannot be compiled in process",
      "impact":"38 subscriptions on this relation authorize per change; measured 12.4 µs/subscriber",
      "remedy":"denormalise team_id onto public.invoices and use a policy of the form team_id = ... so shapes filtering on team_id resolve to Tier A" },

    { "code":"replica_identity_insufficient", "severity":"medium",
      "relation":"public.documents",
      "reason":"shapes filter on owner_id but replica identity is DEFAULT (primary key only)",
      "impact":"DELETE events cannot be filtered or authorized; 4 subscriptions affected",
      "remedy":"CREATE UNIQUE INDEX documents_ri ON public.documents (owner_id, id); ALTER TABLE public.documents REPLICA IDENTITY USING INDEX documents_ri;" },

    { "code":"replica_identity_full_with_toast", "severity":"medium",
      "relation":"public.articles",
      "reason":"REPLICA IDENTITY FULL and column \"body\" has storage 'x' (TOAST-able)",
      "impact":"every UPDATE/DELETE inlines the full column into the old tuple; measured 15x WAL amplification",
      "remedy":"switch to REPLICA IDENTITY USING INDEX over the columns actually needed" },

    { "code":"unindexed_shape", "severity":"low",
      "relation":"public.events",
      "reason":"7 subscriptions have no equality filter on an indexed column",
      "impact":"these are scanned for every change to the relation",
      "remedy":"add an equality filter on an indexed column, or index the filtered column" }
  ]
}
```

Every warning has a `remedy` that is a runnable statement or a concrete instruction. In issuer mode the report sets `"oracle":"issuer"` (URL, hold count) and does not treat RLS policies as the judge.

### Logging

Structured (`log/slog`), JSON in production. **Never** log JWTs, claims bodies, filter values, or row data. Log LSNs, relation names, subscription ids, tiers and codes. Sampled at high volume.

---

## 19. Startup validation

Sluice refuses to start, or starts with loud warnings, rather than failing subtly later. This section exists because two of the empirically-verified failure modes **break application writes**, not replication.

Checked at startup and re-checked on every `Relation` message:

| Check | On failure |
| --- | --- |
| `wal_level = logical` | **fatal** |
| `max_replication_slots` has headroom | **fatal** |
| replication role can connect and has `rolreplication` | **fatal** |
| publication exists | **fatal** |
| **publication has no row filters or column lists whose columns are absent from the replica identity** | **fatal** — this configuration makes application `UPDATE`/`DELETE` fail |
| **every published table's replica identity is adequate for the ops it publishes** | **fatal** — `relreplident = 'i'` with a dropped index makes application `DELETE` fail |
| `max_slot_wal_keep_size` is bounded (`!= -1`) | **warn**, prominently |
| `idle_replication_slot_timeout` is `0`, or comfortably larger than any expected outage | **warn** — non-zero means a long outage destroys the slot and the change stream |
| published tables have primary keys | **warn** per table |
| tables with `REPLICA IDENTITY FULL` have TOAST-able columns | **warn** with measured amplification |
| `sluice_authz` can `SET ROLE` to each configured application role | **fatal** if `SLUICE_SHAPE_ORACLE=rls` |
| published tables: pool role has `SELECT`, and `BYPASSRLS` or RLS off | **fatal** if `SLUICE_SHAPE_ORACLE=issuer` |
| JWKS is fetchable and contains at least one key of the pinned algorithm | **fatal** |
| JWKS does not contain a symmetric `oct` key | **warn** — `supabase-headless`'s own smoke test asserts this |
| `pg_logical_emit_message` is executable by the app role | **warn** if DB broadcast is enabled |

The replica-identity check is worth spelling out because it is the one most likely to be missed:

```sql
SELECT c.oid::regclass AS rel,
       c.relreplident,
       CASE c.relreplident
         WHEN 'd' THEN (SELECT count(*) FROM pg_index i
                         WHERE i.indrelid = c.oid AND i.indisprimary) > 0
         WHEN 'i' THEN (SELECT count(*) FROM pg_index i
                         WHERE i.indrelid = c.oid AND i.indisreplident) > 0
         WHEN 'f' THEN true
         ELSE false
       END AS adequate
FROM pg_publication_rel pr
JOIN pg_class c ON c.oid = pr.prrelid
JOIN pg_publication p ON p.oid = pr.prpubid
WHERE p.pubname = $1;
```

A `relreplident = 'i'` row with `adequate = false` means somebody dropped the index and deletes are now failing. Verified reproducible.

---

## 20. Security model

### Trust boundaries

- **The shape oracle is the authority on what a user may read.** In `rls` mode that is PostgreSQL: `pg_policy`, `relrowsecurity`, `has_column_privilege`. In `issuer` mode that is the application endpoint; Sluice only enforces concreteness, narrowing, publication, replica identity on holds, and physical `SELECT` for the pool role. RLS is not a second judge on that path.
- **The JWT is the only client identity.** Verified with a pinned algorithm against a JWKS. Streams are bound to `sub` and `session_id` at open time and cannot change identity via `/token`. The issuer sees that identity, not the access token.
- **`sluice_repl` sees everything.** Confirmed: a `REPLICATION` role with zero grants reads every column of every published table. This is inherent to logical decoding. Consequences: its credential is as sensitive as a superuser's, it is used for nothing else, and Sluice never logs decoded row data.

### Specific measures

- **`alg` is pinned** (default `ES256`). A `kid` pointing at an HMAC key must not turn a public JWKS into a signing oracle, so anything but the configured algorithm is rejected before key lookup.
- **No tokens in URLs, ever.** No query-string `apikey`, no token in the SSE URL. This is the single biggest security win over the WebSocket-based incumbent and the reason `supabase-headless`'s Caddyfile can drop `apikey_from_query`, `translate_apikey_query` and `mirror_x_api_key` for the Sluice route.
- **Header/body agreement.** `Sluice-Stream-Id` must match the body. Mirroring routing metadata without this check is a documented vulnerability class.
- **Narrowing-only filters.** Client refinements are AND-combined with the authorized shape.
- **Column projection is intersected with grants**, never trusted from the client.
- **`Vary: Authorization`** on every response, so no intermediary can serve one user's stream to another. Electric hit this exact bug.
- **CORS without credentials.** `Access-Control-Allow-Credentials` is never sent, so a wildcard origin is safe; the token is in a header, not a cookie.
- **`Cache-Control: no-store`** on every stream and control response.
- **Fail closed everywhere.** An unrecognised parse node means Tier C, not Tier A. A missing catalog entry means denial. A rate-limited Tier C probe means the change is withheld, not delivered.
- **Timing.** Subscribe denials return a uniform error and are not distinguishable by timing from "shape empty".

### Known residual risks, stated

1. **Tier C evaluates against live data, not the WAL snapshot.** Inherent to PostgreSQL; mitigated by making Tier C avoidable and visible.
2. **A `DELETE` under Tier C cannot be authorized per-row.** Delivered with old tuple truncated to the replica identity and marked degraded.
3. **`REPLICA IDENTITY FULL` broadcasts the whole old row to anyone authorized for the shape.** The walrus README's warning applies to any system: *"ensure that each table's replica identity only contains information that is safe to expose"*. Sluice's `USING INDEX` recommendation exists partly for this reason — a narrow replica identity is a smaller blast radius.
4. **A leaked `sluice_repl` credential is equivalent to full read access** to every published table.

---

## 21. Failure modes

| Failure | Behaviour |
| --- | --- |
| PostgreSQL restarts | Reader reconnects with backoff, resumes from `confirmed_flush_lsn`. Streams stay open; a `warning` event notes the gap. |
| Reader crashes, Sluice survives | Leader lock released, re-acquired, reader restarts from the slot. |
| Sluice crashes | Slot retains WAL. On restart, changes since `confirmed_flush_lsn` are replayed. At-least-once; clients upsert by PK. |
| Slot invalidated (`wal_removed`) | **Fatal, deliberate.** Log, mark `/readyz` unhealthy, refuse to serve `change` subscriptions until an operator acts. Silently resuming would be silent data loss. |
| Slot invalidated (`idle_timeout`) | Same. Startup validation warns about a non-zero `idle_replication_slot_timeout` for this reason. |
| Client network drops | Stream closes; presence keys removed; client reconnects with `resume`. |
| Client too slow | `change`: closed with `stream_lagging`. `broadcast`: dropped oldest. `presence`: coalesced. |
| JWT expires without refresh | Stream closed with `token_expired`. |
| Session revoked | Stream closed with `session_revoked` within milliseconds. |
| Policy changed to deny | Detected on the catalog tick; affected subscriptions receive `error{code:"shape_not_authorized"}` and are removed. Bounded by `SLUICE_CATALOG_REFRESH` (default `30s`), not instant — see [§21.1](#211-revocation-is-bounded-not-instant). |
| Role loses `BYPASSRLS` | Same path: the bypass set is part of the authorization fingerprint, so the grant it produced is withdrawn on the next tick. |
| Column privilege revoked | **Not detected.** Checked once at subscribe time; see [§21.1](#211-revocation-is-bounded-not-instant). |
| Table dropped from publication | Subscriptions receive `error{code:"relation_unpublished"}`. |
| Column dropped | Projection updated; `warning{code:"schema_changed"}`. |
| Replica-identity index dropped | Startup/`Relation` validation turns fatal — because application deletes are already failing. |
| Caddy config reload | `stream_close_delay` keeps streams alive; clients that do reconnect use `resume`. |

### 21.1 Revocation is bounded, not instant

Sluice authorizes a subscription once, at subscribe time. That is the decision the whole design rests on — it is what makes cost scale with the write rate instead of the subscriber count — and it is only sound if the authorization can also be *withdrawn*.

Withdrawal cannot be pushed. PostgreSQL emits no notification when a policy is created or dropped, and none when a role attribute changes; `DROP POLICY` does not even produce a `Relation` message in the WAL. So Sluice polls: on every `SLUICE_CATALOG_REFRESH` tick (default `30s`) it reloads the catalog and compares `catalog.AuthzVersion()`, a counter over a hash of exactly what an authorization decision reads — each relation's `RLSEnabled`, every SELECT policy's name, permissiveness, roles and `USING` text, and the set of roles holding `BYPASSRLS` or superuser. When that moves, every subscription is re-resolved and the ones that lost access are dropped with `shape_not_authorized`.

Three consequences worth stating plainly rather than discovering in production:

- **A dropped policy stays enforceable-but-unenforced for up to one tick.** Lower `SLUICE_CATALOG_REFRESH` to shorten the window; the check itself is a hash comparison, so the cost of a shorter tick is the catalog query, not the re-resolution.
- **Column privileges are outside the fingerprint.** `has_column_privilege` is not part of the catalog snapshot, so `REVOKE SELECT (col)` never reaches an open stream at all. Including it would cost a round trip per subscription per tick rather than a hash comparison, which is a different order of expense; it is deferred deliberately and reported by the audit as `column_grants_not_revocable`.
- **Re-resolution cannot fail open.** `Resolve` does no database I/O, so it can only return `ErrDenied` or `ErrTierCDisabled`. A database blip cannot manufacture a spurious revocation, which matters because the failure path drops the subscription.

This is the same shape of residual as the Tier C snapshot skew ([§7.3](#73-tier-c--impersonated-probe-compatibility-fallback)), and for the same underlying reason: there is no push notification for a catalog change any more than there is an as-of-LSN snapshot. The two differ in direction, though, and the difference matters. Revocation lag errs toward over-delivering to someone who *was* authorized moments ago, and its window is a knob you control. Skew can deliver commit-time content to someone who was *not* authorized at commit time, and its window is replication lag.

---

## 22. Horizontal scale

v1 is single-process. The seams that make N processes possible are in the design from the start, because retrofitting them is expensive:

1. **One reader, N servers.** The advisory lock already elects a single reader. Adding nodes must not add slots — *"Multiple replication slots increase WAL retention on Postgres since each slot independently prevents WAL from being cleaned up."*
2. **The `Bus` interface.** `Publish(topic, payload)` / `Subscribe(topic)`. In v1 an in-process implementation; later NATS (embeddable, single-binary-friendly) or Redis. The reader publishes changes to the bus; every node's Hub consumes.
3. **`stream_id` carries the node.** `<node_id>.<nonce>`, so control POSTs are forwarded to the owning node rather than requiring sticky load balancing. This is the direct lesson from MCP SEP-2575.
4. **Presence needs a CRDT when multi-node.** Deferred deliberately. Single-node presence is exact; multi-node presence will need Phoenix.Tracker-style reconciliation, with the spurious-`join`/`leave`-during-`sync` artefact that entails. Documented when it lands.
5. **Authorization is already node-local**, because it is resolved at subscribe time on the node holding the stream. No shared authorization state is needed.

Per-node target: **100k–200k streams**, the figure Centrifugo operates at in production with ordinary goroutines. At ~4 KiB RSS per idle connection plus per-subscription state, 100k streams is a few GiB — comfortable. `gnet`-style event loops are explicitly rejected: callbacks must never block, and one 10 ms database call inside `OnTraffic()` stalls every connection on that loop.

---

## 23. Configuration

Environment variables, prefix `SLUICE_`. Every default is chosen to be safe rather than fast.

```bash
# ── server ────────────────────────────────────────────────────────────────
SLUICE_LISTEN_ADDR=0.0.0.0:4000
SLUICE_PATH_PREFIX=/sluice/v1
SLUICE_LOG_LEVEL=info
SLUICE_LOG_FORMAT=json
SLUICE_NODE_ID=                       # default: hostname
SLUICE_SHUTDOWN_GRACE=15s

# ── database ──────────────────────────────────────────────────────────────
SLUICE_DB_REPL_URL=postgres://sluice_repl:...@db:5432/postgres?replication=database
SLUICE_DB_AUTHZ_URL=postgres://sluice_authz:...@db:5432/postgres
SLUICE_DB_POOL_MAX_CONNS=8
SLUICE_DB_POOL_MIN_CONNS=2
SLUICE_PARANOID_POOL_RESET=false

# ── replication ───────────────────────────────────────────────────────────
SLUICE_SLOT_NAME=sluice
SLUICE_PUBLICATION=sluice
SLUICE_PROTO_VERSION=4
SLUICE_STREAMING=off                  # off | on | parallel
SLUICE_BINARY=false
SLUICE_MESSAGES=true
SLUICE_STATUS_INTERVAL=10s
SLUICE_DISPATCH_QUEUE=8192
SLUICE_MESSAGE_PREFIX=sluice:         # pg_logical_emit_message prefix → channel

# ── ring buffer / resume ──────────────────────────────────────────────────
SLUICE_RING_EVENTS=4096
SLUICE_RING_MAX_AGE=60s

# ── auth ──────────────────────────────────────────────────────────────────
SLUICE_JWKS_URL=http://auth:9999/.well-known/jwks.json
SLUICE_JWKS_REFRESH=5m
SLUICE_JWT_ALG=ES256                  # pinned; anything else is rejected
SLUICE_JWT_ISSUER=
SLUICE_JWT_AUDIENCE=authenticated
SLUICE_JWT_LEEWAY=10s
SLUICE_ANON_ROLE=anon
SLUICE_ALLOWED_ROLES=anon,authenticated,service_role

# ── authorization ─────────────────────────────────────────────────────────
SLUICE_SHAPE_ORACLE=rls              # rls | issuer
SLUICE_ISSUER_URL=                   # required when oracle=issuer
SLUICE_ISSUER_BEARER=                # required when oracle=issuer; not the user JWT
SLUICE_ISSUER_TIMEOUT=2s
SLUICE_AUTHZ_LEASE=60s                # for volatile RLS predicates only
SLUICE_CATALOG_REFRESH=30s
SLUICE_TIER_C=allow                   # allow | deny
SLUICE_TIER_C_MAX_PROBES_PER_SECOND=2000
SLUICE_TIER_B_VERIFY=5                # cross-checks per Tier B subscription; 0 disables
SLUICE_UNINDEXED_SHAPES_MAX=200
SLUICE_REPLICA_IDENTITY=warn          # warn | strict
SLUICE_DEGRADED_DELETES=withhold      # withhold | deliver — incomplete DELETE tuples

# ── snapshots ─────────────────────────────────────────────────────────────
SLUICE_SNAPSHOT_ENABLED=true
SLUICE_SNAPSHOT_MAX_CONCURRENT=4
SLUICE_SNAPSHOT_MAX_ROWS=50000
SLUICE_SNAPSHOT_PAGE_SIZE=1000

# ── streams / limits ──────────────────────────────────────────────────────
SLUICE_HEARTBEAT=20s
SLUICE_STREAM_QUEUE=256
SLUICE_WRITE_TIMEOUT=10s
SLUICE_MAX_STREAMS=50000
SLUICE_MAX_SUBS_PER_STREAM=100
SLUICE_MAX_SHAPES_PER_STREAM=20
SLUICE_MAX_PAYLOAD_BYTES=262144
SLUICE_MAX_CHANGE_BYTES=1048576
SLUICE_SUBSCRIBE_RATE=20
SLUICE_PUBLISH_RATE=100
SLUICE_PRESENCE_RATE=5
SLUICE_PRESENCE_WINDOW=30s
SLUICE_PRESENCE_BROADCAST=1500ms
SLUICE_PRESENCE_MAX_KEYS=10

# ── channels ──────────────────────────────────────────────────────────────
# namespace:mode[:hook_url]
SLUICE_CHANNELS=room:public,notify:owner,billing:hook:http://api:8080/sluice/authz
SLUICE_CHANNEL_HOOK_TTL=60s
SLUICE_CHANNEL_HOOK_TIMEOUT=2s

# ── revocation (optional) ─────────────────────────────────────────────────
SLUICE_REVOCATION_ENABLED=false
SLUICE_REVOCATION_SESSIONS_TABLE=auth.sessions
SLUICE_REVOCATION_USERS_TABLE=auth.users
SLUICE_REVOCATION_VERIFY_ON_SUBSCRIBE=true

# ── observability ─────────────────────────────────────────────────────────
SLUICE_METRICS_ENABLED=true
SLUICE_DIAGNOSTICS_ENABLED=true
```

---

## 24. Go package layout

```
sluice/
├── cmd/sluice/                     server wiring, startup validation, graceful shutdown
├── cmd/keygen/                     harness secrets and ES256 API keys
├── cmd/smoke/                      end-to-end harness validation (~36 assertions)
├── cmd/smoke-issuer/               issuer overlay smoke (does not replace the 36)
├── cmd/issuer-stub/                harness HTTP issuer for cmd/smoke-issuer
├── cmd/audit/                      production-readiness battery (RLS spectrum, WAL edges, diagnostics)
├── cmd/load/                       realtime stress probe against the harness
├── internal/
│   ├── config/                     env parsing, validation, defaults
│   ├── auth/                       JWT/JWKS verification, alg pinning, revocation
│   ├── catalog/                    policies, grants, columns, RI, indexes (cached)
│   ├── authz/                      three-tier authorizer, probes, leases, Tier B verify
│   ├── oracle/                     shape oracle: rls wrapper, issuer HTTP client
│   ├── hold/                       issuer hold index, EXISTS, WAL cut
│   ├── expr/                       convert (pgparser AST) / analyze / fold / evaluate
│   ├── shape/                      filter grammar, narrowing, routing-key selection
│   ├── registry/                   constant-indexed subscription index
│   ├── pgoutput/                   logical replication decoder (owns the 'u' marker)
│   ├── reader/                     single replication connection, LSN feedback, leader lock
│   ├── hub/                        streams, fan-out, ring buffers, presence
│   ├── server/                     HTTP surface, SSE, dispatch, snapshots, hooks, diagnostics
│   ├── event/                      shared event types
│   ├── timer/                      shared jittered wheel (one timer for every stream)
│   └── metrics/                    Prometheus collectors
├── deploy/                         compose harness: db bootstrap, fixtures, Caddy
├── packages/sluice-js/             typed TypeScript client
└── design_doc.md
```

Rules the code follows:

- **Per-connection state is pointer-light** where it matters for fan-out (ring buffers, registry indexing), because Go's GC marking cost scales with live pointer-bearing objects.
- **`sync.Pool` for encode buffers.**
- **No `time.Ticker` per connection** — the shared wheel.
- **`authz.Decision` is immutable; `authz.Handle` publishes it with `atomic.Pointer`.** Re-resolution replaces the decision wholesale and dispatch loads it once per change, so every field read belongs to one generation. Editing in place raced the dispatch path on two levels: the scalar fields tore (`Predicate` is an interface, `PredicateSQL` a string — two words each), and assigning `Predicate` published a pointer to a freshly built expression tree with no release barrier, letting a reader follow it into nodes whose writes it had no guarantee of seeing. An `RWMutex` would also close both, but it would put shared-cache-line traffic on the per-change × per-subscriber path; an atomic load is free by comparison. Cross-check state (`verifyLeft`, `downgraded`) lives outside `Decision` and is carried across generations only when `PredicateSQL` is unchanged, so a lease renewal cannot forget an earned downgrade and a genuinely new predicate cannot inherit a verdict about the old one.
- **`pgoutput` decoding is owned in-tree.** `jackc/pglogrepl` is used for the replication protocol handshake; message decoding for the versions Sluice needs lives in `internal/pgoutput`.
- **Pure Go, no cgo.** Policy text is parsed by `pgplex/pgparser`, a Go port of PostgreSQL's grammar; there is no `libpg_query` binding and no cgo, so the binary is static. Anything `internal/expr` declines to *evaluate* fails closed to Tier C.
- **Horizontal scale seam (`Bus`) is designed (§22) but not built.** Single-node `hub` only.

---

## 25. Roadmap

### v0.1 — critical path (done)

The goal was to prove the design against a real database, not to ship.

- [x] Design document
- [x] `pgoutput` decoder, full v1–v4 message set, `'u'` handling
- [x] Replication reader: slot, `START_REPLICATION`, LSN feedback, leader lock
- [x] Catalog cache: policies, grants, columns, replica identity, indexes
- [x] Startup validation with fatal/warn classification
- [x] Filter grammar, parse, type-check
- [x] Routing index with constant indexing
- [x] Authorizer: Tier A reduction, Tier B compilation, Tier C probe
- [x] SSE transport + control plane
- [x] JWT/JWKS with pinned `alg`
- [x] Broadcast (client + database via `pg_logical_emit_message`)
- [x] Presence, single-node
- [x] Session revocation via `auth.sessions`
- [x] Metrics + `/diagnostics`
- [x] Test harness: PostgreSQL 18.4, GoTrue, PostgREST, Caddy, Sluice
- [x] End-to-end validation of the critical path (later expanded to 36 assertions; see v0.2)

Validated end-to-end against the live harness (`cmd/smoke`):

```
subscription           tier  indexed  routing key   note
docs                   A     yes      owner_id
posts                  B     no                     warn:unindexed_shape warn:replica_identity_full_with_toast
invoices               C     yes      team_id
metrics                A     no                     warn:unindexed_shape
articles               A     yes      owner_id      warn:replica_identity_insufficient
```

with the load-bearing assertions passing: own-rows-only delivery under Tier A, per-row evaluation under Tier B, **`DELETE` authorized correctly with the full old row**, another user's `DELETE` withheld, `unchanged: ["body"]` on an untouched TOASTed column, a transactional `pg_logical_emit_message` broadcast delivered and a rolled-back one never delivered, agreement with PostgREST as the authorization oracle, and an unauthorized shape refused at subscribe time. The current suite also covers snapshots, differential evaluation vs PostgreSQL, security negatives, and a small fan-out load — **36 checks, 0 failures** on a healthy harness.

Two bugs were found by the tests rather than in production, which is the point of writing them: the expression lexer omitted `:` so no `::` cast ever tokenized, and `ParseText` turned `numeric 'NaN'` into a float that is not valid JSON. Startup validation also caught a bug in itself — `pg_has_role` needs `MEMBER`, not `USAGE`, to test whether a `NOINHERIT` role can `SET ROLE`.

### v0.2 — gaps closed (done)

Everything listed as a v0.1 gap has been resolved, and the resolutions changed the design in two places worth recording.

- [x] **Tier C `DELETE` is now correct.** Rather than marking it degraded, the predicate is evaluated against the tuple the WAL delivered, using `jsonb_populate_record` to reconstitute the row. This works even for a policy containing a subquery against another table. It requires a complete tuple, so it applies when `REPLICA IDENTITY FULL` is set; otherwise the event is **withheld** by default (`SLUICE_DEGRADED_DELETES=withhold`), not delivered, because telling a subscriber that a row they may not see was deleted is the leak walrus avoided by truncating to primary keys.
- [x] **Tier B is now verified against PostgreSQL at runtime.** The first `SLUICE_TIER_B_VERIFY` (default 5) decisions on each subscription are also evaluated by PostgreSQL on the same tuple and the verdicts compared. A disagreement downgrades the subscription to Tier C, logs at ERROR and increments `sluice_authz_downgrades_total`. This converts "trust the compiler" into "verify it against PostgreSQL, per policy, at runtime", at a cost bounded per subscription rather than per change.
- [x] `initial: "snapshot"` implemented (§13), including the gapless replay floor.
- [x] Channel `hook` mode implemented, fail-closed, with a bounded TTL cache and a short negative TTL so a blip does not become an outage.
- [x] Shared jittered timer wheel replaces the per-stream ticker (§17).
- [x] Lease refresh implemented: volatile predicates are re-evaluated on a timer and revoked access drops the subscription. Without this, resolving once would have meant never revoking.
- [x] Typed TypeScript client (`packages/sluice-js`): shapes, channels, presence, reconnect/resume, zero runtime deps.
- [x] Expanded smoke suite: differential expr vs PostgreSQL, security negatives, load — **36 assertions**.

**Remaining gaps**, deliberate:

- The `Bus` seam of §22 exists conceptually but there is no interface type yet.
- Resurrect-and-rollback (§7.4) is still unimplemented; the **synthetic-record** path (via `jsonb_populate_record`) covers Tier C `DELETE` at lower cost when the WAL tuple is complete.

### v0.3 — PostgreSQL's grammar replaces Sluice's (done)

The hand-written lexer and recursive-descent parser (840 lines) are gone. Policy text is now parsed by [`pgplex/pgparser`](https://github.com/pgplex/pgparser) and converted to Sluice's evaluable AST by `internal/expr/convert.go` (525 lines, 391 of them code). Still pure Go, still no cgo, still a static binary — verified with `CGO_ENABLED=0`.

The motivation was not line count. It was that a hand-maintained *grammar* subset has a failure mode a semantic whitelist does not: **a missing production does not fail, it misparses.** With no rule for `CURRENT_USER`, the old parser fell through to its identifier rule and produced a column reference. `Analyze` saw a predicate over a plain column, called it compilable, and put the subscription in Tier B — where the WAL tuple has no `current_user` column, every evaluation returns unknown, and every row is withheld silently, with `/diagnostics` reporting nothing wrong. The same shape of bug had already been found twice by tests (`::` never tokenizing because the lexer omitted `:`; `(select auth.uid())` classified as an opaque subquery, sending the single most common Supabase policy idiom to the slowest tier).

Now the grammar is PostgreSQL's, so precedence, quoting (`E''`, `$$…$$`, `U&''`, quoted identifiers), and every operator form come for free, and the only thing Sluice maintains is the list of nodes it will evaluate — where a gap is structurally unrepresentable and reported by name.

Two real gaps surfaced immediately and were fixed in the process:

- **`NOT IN` was never being compiled.** `pg_get_expr` does not preserve `IN`/`NOT IN`; it deparses them as `= ANY (ARRAY[…])` and `<> ALL (ARRAY[…])`. Only the first was handled, so every `NOT IN` policy silently fell to Tier C.
- **Array casts were evaluated as scalar casts.** `normalizeType` strips `[]`, so `(tags)::text[]` was treated as `::text`. Now an array cast is either pushed down into an `ARRAY[…]` literal (which is what PostgreSQL does) or declined.

One duplication was removed rather than added to: `convert.go` initially carried its own operator whitelist, which already disagreed with `analyze.go`'s `evaluableBinaryOps` (the list documented as matching `evalBinary`). A mutation test — whitelisting `~` and expecting the suite to fail — exposed that the converter's copy was dead weight, since `Analyze` is what actually enforces. Deleting it left one list, and improved diagnostics: `describeOp` names "POSIX regular-expression operator ~" where the converter had said `operator "~"`.

Test coverage grew from the swap: **106 new subtests** across grammar breadth (evaluated, not merely parsed, including three-valued-logic cases), fail-closed behaviour for 17 declined constructs, n-ary boolean flattening, and hostile input. The declined-construct suite asserts three things per case — not compilable, a non-empty reason for `/diagnostics`, and that `Eval` also refuses — so a caller ignoring `Compilable()` still cannot obtain a bogus visibility decision.

The cost is binary size: the goyacc tables take the `sluice` binary to ~25 MB. For a server that is not a consideration.

PostgREST is in the harness deliberately: it is the **authorization oracle** for **RLS** mode. The correctness invariant Sluice must satisfy in that mode is *"a client receives a change for row R if and only if that client could `SELECT` row R through PostgREST"*, and having both in the harness makes that invariant directly testable.

### v0.4 — issuer shape oracle

A second, process-wide oracle: `SLUICE_SHAPE_ORACLE=issuer`. Same tube (SSE, WAL, registry, snapshot, resume, channels). The application authorizes at join over HTTP, Sluice narrows AND-only, and a hold row on the same slot is the kick. RLS LOGIN, tiers, and the harness default are unchanged. The harness issuer smoke (`cmd/smoke-issuer`) is an overlay on the same compose up — a second process, publication, and stub — and does not replace the 36 RLS assertions.

### Next — hardening (not done)

- CI: build, vet, `-race` tests, and harness smoke on every push.
- Property-based authorization tests: generate policies and shapes, assert Sluice's tier decision agrees with PostgREST's visibility for every row.
- Resume/ring-buffer correctness under reader restarts.
- Larger load harness: 10k streams, measured changes/sec by tier.
- `two_phase` and `streaming=on` exercised.
- Operational burn-in and backup/restore / slot-lifecycle runbooks.

### Later — scale seams

- `Bus` backed by NATS.
- Multi-node with forwarded control messages.
- Presence CRDT.

### Deliberately deferred

- Resurrect-and-rollback for Tier C `DELETE` (synthetic-record already ships). Neither path fixes Tier C throughput; Tier A/B covers the cases that matter.
- WebTransport transport. Revisit when the draft is an RFC, `webtransport-go` has flow control, and a browser implements the h2 fallback.
- Batch authorization helper functions in the database. Verified to be worth only 2–3× against a 100× gap, and it would violate the zero-database-objects goal.

---

## Appendix A — rejected alternatives

| Alternative | Why rejected |
| --- | --- |
| **`wal2json`** | Drops unchanged-TOAST columns from its JSON with **no marker**, so a consumer cannot distinguish "unchanged" from "not selected" from "dropped column". A counter update silently blanks an article body on every subscriber. Also: repeats schema and type names per row, text encoding in a single-threaded walsender, out-of-tree `.so` per major version, last tagged release predates PG 18. |
| **Triggers + `LISTEN/NOTIFY`** | Three independent disqualifiers. (1) Identical payloads in one transaction are **silently deduplicated** — verified: 3 `pg_notify` calls, 2 notifications. A change stream cannot collapse events. (2) `SignalBackends` is O(all listeners): **32× throughput collapse at 100 idle listeners** on PG 18; the fix is PG 19. (3) No persistence and no replay — a disconnected listener misses everything, permanently. Plus a 7,999-byte payload ceiling, no `LISTEN` under transaction pooling, and trigger work inside the writer's critical path. |
| **SQL polling (`pg_logical_slot_get_changes`)** | What Supabase does, at 100 ms. Adds a latency floor, runs decoding inside a SQL transaction that materialises the whole result, gets no keepalives or feedback, has no backpressure, and cannot use synchronous replication. The streaming protocol has none of these problems. |
| **`binary = true`** | Measured **112 bytes vs 88** for the same row — larger, not smaller — and requires implementing `numeric`, `timestamptz`, array headers and the `jsonb` version byte, *plus* the text path anyway because `pgoutput` falls back per column for types without `typsend`. Sluice emits JSON; text is closer. |
| **`streaming = on` by default** | Delivers uncommitted changes, forcing the reader to buffer whole transactions in the Go heap. With `off`, everything received is already durable and can be forwarded with zero buffering; the buffering happens in PostgreSQL, disk-backed and observable via `pg_stat_replication_slots`. |
| **Publication row filters / column lists** | Verified to make application `UPDATE`/`DELETE` **fail** when a filter column is absent from the replica identity. A filtering mechanism that can break writes is not acceptable. Filtering belongs in process, per subscriber. |
| **Per-role replication slots** | Does not filter anything — logical decoding ignores RLS entirely, verified with a zero-grant role reading secrets. And N slots each independently retain WAL, multiplying the disk-exhaustion risk. |
| **Per-subscriber impersonation (WALRUS)** | Measured 108 changes/sec at 1,000 subscribers, 18.6 at 5,000, and — per ElectricSQL's documented analysis — **subtly incorrect**, because it evaluates the policy against the live database rather than the WAL record's snapshot. Retained only as Tier C, budgeted and reported. |
| **`REPLICA IDENTITY FULL` as the default recommendation** | Measured 3,227× larger DELETE messages and 15–17× more WAL on a table with a 300 KB TOAST-able column, because `toast_flatten_tuple()` inlines untouched out-of-line values. `USING INDEX` gets the same filter columns at ~1/15 the cost. |
| **Batch authorization function in the database** | Removes network round trips (2–3×) but not the per-subscriber work. Does not close a 100× gap, and requires creating database objects, violating a primary goal. |
| **Protocol compatibility with `@supabase/realtime-js`** | Positional join-reply validation that tears down the channel on mismatch, two simultaneous framing versions (v1 JSON-object, v2 JSON-array plus 7-byte binary headers), Phoenix Channels lifecycle semantics, and a client that now lives in the `supabase-js` monorepo (the `realtime-js` repo is archived). Large surface area, no control over its evolution, and it would force Sluice's model to match Supabase's. |
| **WebSocket transport** | The browser API cannot set headers, so the token must go in the URL — the exact hack `supabase-headless`'s Caddyfile carries today. RFC 8441 over h2 is implemented everywhere and broken in practice (1006 closes, malformed Safari handshakes under Private Relay, nginx refusing to implement it). SSE over h2 is one stream of the same connection, with `Authorization` intact. |
| **`EventSource`** | Cannot set headers (so, tokens in URLs), cannot POST, cannot change its subscription set without reconnecting, and consumes 1 of the browser's 6 per-origin HTTP/1.1 connections. A `fetch()`-based SSE client fixes all four. |
| **WebTransport** | Baseline since March 2026 and genuinely attractive, but the IETF draft is only in WG Last Call, `webtransport-go` has **no flow control**, it needs UDP/443 end-to-end, and no browser is confirmed to implement the h2 fallback. Revisit later; the transport layer is an interface so it is additive. |
| **`gnet` / event loops** | Callbacks must never block, so one database call stalls every connection on the loop; you end up handing work to a goroutine pool anyway. Benchmarks show goroutine-per-connection *wins* on throughput and loses only on tail latency, and Centrifugo runs 1M connections in production on ordinary goroutines. |
| **Rust** | Real advantage (no GC marking cost proportional to live set), but the measured spread between good and bad *architecture* on the same runtime is ~200×, while Rust buys perhaps 3× on an I/O-bound fan-out. Supabase itself kept Realtime in Elixir while writing its ETL engine in Rust — they chose per workload. Revisit only if profiling shows GC is the binding constraint. |

---

## Appendix B — evidence index

Every non-obvious claim in this document traces to a verified source. The full research archive, including raw command output, lives in the `supabase-headless` repository under `docs/` (gitignored).

| Claim | Evidence |
| --- | --- |
| Logical decoding ignores RLS; a zero-grant `REPLICATION` role reads everything | `07` §1 — `pg_recvlogical` output as role `cdc` |
| `REPLICA IDENTITY FULL` + RLS gives the full old row on DELETE | `07` §2 |
| The PK truncation is a walrus decision from Jan 2023 | `02` §3 — walrus PR #65, exact source line |
| `REPLICA IDENTITY FULL` TOAST amplification | `07` §3, §9 — 200,158-byte message, 12.6×/15×/17× WAL |
| Impersonation costs ~9–13 µs/subscriber/change | `07` §4, §5 |
| Fixing DELETE correctness does not fix throughput | `07` §5 — both techniques, same numbers |
| `proto_version` alone does not change the wire format | `07` §6 — identical MD5 for 1, 4, 4+parallel |
| `binary=true` is larger | `07` §7 — 112 vs 88 bytes |
| `REPLICA IDENTITY USING INDEX` is the middle path | `07` §9 |
| Publication row filters break application writes | `07` §10 |
| `LISTEN/NOTIFY` deduplicates identical payloads | `07` §11 |
| `pg_logical_emit_message` is transactional and rides the same slot | `07` §6 |
| NOTIFY 32× collapse at 100 idle listeners | `03` §8 — pgsql-hackers measurements |
| Streaming protocol keepalives, feedback, backpressure | `03` §1 |
| `pgoutput` `'u'` marker vs `wal2json` omission | `03` §6 — both sources quoted |
| Supabase RLS throughput ceiling (~4,000 msgs/sec) | `02` §4 — published benchmarks |
| ElectricSQL's MVCC-snapshot argument | `05` §3 — maintainer quote |
| Electric's constant-indexing numbers | `05` §3 |
| Zero deprecated `definePermissions` | `05` §3 |
| Centrifugo's 1M-connection result and `#` channels | `05` §7 |
| `session_id` is `auth.sessions.id`; sign-out DELETEs | `06` §2, §3 — GoTrue v2.195.0 source |
| Supabase recommends the `auth.sessions` existence check | `06` §7 |
| SSE 6-connection limit; 256-stream Chromium cap | `04` §1 — Chromium source |
| MCP removed sessions and resumability, and why | `04` §7 — SEP-2575, SEP-2567 |
| WebTransport Baseline March 2026; `webtransport-go` has no flow control | `04` §4 |
| `pglogrepl` parses only v1/v2, no tagged release | `05` §2 |
| `pgx` ≥ 5.9.2 required (CVE-2026-41889) | `05` §2 |
| `pgxpool.BeforeAcquire` deprecated for `PrepareConn` | `05` §2 |
| Go 1.26 Green Tea GC default, 10–40% | `05` §1 |
| The stack's Caddyfile carries the apikey-in-URL hack | `01` §"Gateway" |