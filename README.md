# Sluice

A realtime data-streaming server for PostgreSQL. It reads the write-ahead log through one logical replication slot and streams row changes, broadcasts and presence to authenticated clients over Server-Sent Events, giving every subscriber exactly the rows it may read.

It is built to replace `supabase/realtime` in a self-hosted stack. It is not protocol-compatible with it, and it installs **nothing in the database**: no extensions, tables, functions or schemas.

> **Status: working prototype.** Everything below is implemented and exercised by the suites in this repository against PostgreSQL 18.4, GoTrue, PostgREST and Caddy. It has not been burned in under production traffic. Planned work and known gaps are in [ROADMAP.md](ROADMAP.md).

## Contents

- [How it works](#how-it-works)
- [What PostgreSQL must provide](#what-postgresql-must-provide)
- [Running it](#running-it)
- [Authorization](#authorization)
- [Shapes](#shapes)
- [Wire protocol](#wire-protocol)
- [Channels: broadcast and presence](#channels-broadcast-and-presence)
- [Session revocation](#session-revocation)
- [Delivery guarantees and backpressure](#delivery-guarantees-and-backpressure)
- [Operating it](#operating-it)
- [Security model](#security-model)
- [Configuration reference](#configuration-reference)
- [Clients](#clients)
- [Development and testing](#development-and-testing)
- [Repository layout](#repository-layout)

## How it works

Authorization is decided **once per subscription, at subscribe time**, never once per change per subscriber. The decision is either a constant (Tier A), an in-process predicate evaluated against the tuple the WAL already carries (Tier B), or, only for policies that cannot be evaluated in process, one impersonated query per change (Tier C, reported as a defect).

Subscriptions are indexed by the constant their filter pins a column to (`owner_id=eq.<uuid>`), so a change is routed to its subscribers with a map lookup instead of a scan.

One process reads one permanent replication slot (`pgoutput`, text format, streaming off: everything it receives is already committed). Each client holds one long-lived `POST` whose response is `text/event-stream`, plus short control `POST`s. The access token travels in `Authorization: Bearer`, never in a URL.

Two process-wide **shape oracles** decide what a shape may see:

- **`rls`** (default): the same row-level security policies and column grants that govern a `SELECT` by the caller's role.
- **`issuer`**: your API decides over HTTP when a client joins, and names a **hold** row whose deletion revokes the grant through the WAL.

## What PostgreSQL must provide

Tested with PostgreSQL 18.4. The default `SLUICE_PROTO_VERSION=4` needs PostgreSQL 16 or newer; older versions are untested.

```sql
-- server settings (wal_level needs a restart)
--   wal_level = logical
--   max_replication_slots >= 1          -- one slot per Sluice deployment
--   max_wal_senders >= 1
--   max_slot_wal_keep_size = <bounded>  -- the default -1 is unlimited

CREATE ROLE sluice_repl  WITH LOGIN REPLICATION PASSWORD '...';
CREATE ROLE sluice_authz WITH LOGIN NOINHERIT   PASSWORD '...';
GRANT anon, authenticated TO sluice_authz;   -- every role in SLUICE_ALLOWED_ROLES

CREATE PUBLICATION sluice;
ALTER PUBLICATION sluice ADD TABLE public.documents;
```

- **`sluice_repl`** is used only for the replication connection (`SLUICE_DB_REPL_URL`, with `replication=database`). It needs no table privileges: logical decoding is not subject to RLS or grants, so it reads every column of every published table. Treat its credential like a superuser's.
- **`sluice_authz`** is the pool role (`SLUICE_DB_AUTHZ_URL`). `NOINHERIT` means it can only act as an application role deliberately, with a transaction-scoped `SET LOCAL ROLE`. In `rls` mode it must be able to assume every non-`BYPASSRLS` role in `SLUICE_ALLOWED_ROLES`. It needs no other grant.
- **Snapshots for a `BYPASSRLS` role** such as `service_role` also `SET ROLE` to it, so they fail unless you `GRANT service_role TO sluice_authz`, which lets `sluice_authz` bypass RLS itself. Sluice only does that for a verified token of that role; startup warns (`role_not_assumable`) when the grant is missing.
- **Issuer mode** reads with the pool role itself (no `SET ROLE`): it needs `SELECT` on every published table and `BYPASSRLS` (or RLS off on those tables). Startup refuses otherwise.
- **Publications:** tables can be added explicitly, `FOR TABLES IN SCHEMA` or `FOR ALL TABLES`. **Do not use row filters**: a column in a publication `WHERE` must be in the replica identity or the application's own `UPDATE`/`DELETE` fail, so Sluice refuses to start with one. Column lists are not checked; columns they leave out are simply absent from events.
- **Replica identity** decides which old columns reach Sluice on `UPDATE`/`DELETE`; see [Replica identity](#replica-identity).
- **`pg_logical_emit_message`** (database broadcasts) is executable by every role by default.

## Running it

### Harness

`deploy/compose.yml` runs the plain `postgres:18.4-trixie` image, GoTrue (real ES256 tokens), PostgREST (the oracle the tests compare against), Caddy, the RLS-mode Sluice, and an issuer-mode Sluice with its stub issuer.

```bash
cp .env.example .env
sh deploy/keygen.sh >> .env        # runs in a container; then delete the empty duplicates
docker compose -f deploy/compose.yml --env-file .env up -d --build
```

The gateway listens on `GATEWAY_PORT` (default 8000): `/auth/v1`, `/rest/v1`, `/sluice/v1` and `/sluice-issuer/v1`.

### Published image

`ghcr.io/pauserratgutierrez/sluice` contains only the static `sluice` binary and CA certificates. Required variables are `SLUICE_DB_REPL_URL`, `SLUICE_DB_AUTHZ_URL` and `SLUICE_JWKS_URL`; [`deploy/sluice.env.example`](deploy/sluice.env.example) lists every variable with its default.

```bash
docker run --rm -p 4000:4000 --env-file deploy/sluice.env.example \
  -e SLUICE_DB_REPL_URL='postgres://sluice_repl:...@db:5432/postgres?replication=database' \
  -e SLUICE_DB_AUTHZ_URL='postgres://sluice_authz:...@db:5432/postgres' \
  -e SLUICE_JWKS_URL='http://auth:9999/.well-known/jwks.json' \
  ghcr.io/pauserratgutierrez/sluice:latest
```

`sluice -healthcheck` probes `GET /healthz` (the image's `HEALTHCHECK`); `sluice -version` prints the version.

Behind a proxy, disable response buffering for the stream route (Caddy: `flush_interval -1`; nginx honours the `X-Accel-Buffering: no` header Sluice sends) and do not time out long-lived responses. CORS is the gateway's job; Sluice sends no CORS headers.

## Authorization

### RLS oracle

For a relation and the caller's role, Sluice combines the relation's `SELECT` policies the way PostgreSQL does: permissive policies `OR`ed, restrictive policies `AND`ed. A policy applies when its `TO` list names the role, or a role whose privileges the caller's role has (`pg_has_role(..., 'USAGE')`), or when it has no `TO` (`PUBLIC`). No RLS on the table, or a role with `BYPASSRLS` or superuser, means every row. The owner bypass (a table's owner without `FORCE ROW LEVEL SECURITY`) is not modeled, so an owner's subscription is judged by the policies: fail-closed. Claim-derived parts (`auth.uid()`, `current_setting('request.jwt.claims', true)::jsonb ->> 'sub'`) are folded to constants for the subscription.

Policy text is parsed with [`pgplex/pgparser`](https://github.com/pgplex/pgparser), a pure-Go port of PostgreSQL's grammar. What Sluice evaluates in process is a whitelist of node types (`internal/expr/convert.go`); anything else makes the predicate non-compilable, which can only move a subscription to Tier C, never grant it.

| Tier | When | Per-change cost |
| --- | --- | --- |
| **A** | The predicate only reads columns the shape pins with `eq` filters (or there is no predicate). Sluice substitutes the constants and evaluates it once; `false` refuses the subscription with `shape_not_authorized`. | none |
| **B** | The predicate is compilable but reads other columns. It is evaluated in process against the WAL tuple, `DELETE` included (against the old tuple). The first `SLUICE_TIER_B_VERIFY` decisions per subscription made on a complete tuple are also asked of PostgreSQL (`jsonb_populate_record` under the caller's role); any disagreement demotes the subscription to Tier C for good, logs at `ERROR` and increments `sluice_authz_downgrades_total`. | in-process evaluation |
| **C** | The predicate has a subquery, a function outside the whitelist, or anything else not compilable. | one query per subscriber per change |

Tier C evaluates the policy under the caller's role and claims against the WAL tuple rebuilt with `jsonb_populate_record` when the tuple carries every column (a new tuple unless it has unchanged TOASTed columns; an old tuple only with `REPLICA IDENTITY FULL`). Otherwise an `INSERT`/`UPDATE` is checked by probing the live row by primary key, and a `DELETE` cannot be decided. Probes run on the replication path, so they slow every subscriber. They are capped globally by `SLUICE_TIER_C_MAX_PROBES_PER_SECOND`; over the cap the change is withheld from Tier C subscribers, never delivered unauthorized. Each probe (and each Tier B cross-check) is bounded by `SLUICE_TIER_C_TIMEOUT`, and a probe that runs out of time withholds the change the same way. `SLUICE_TIER_C=deny` refuses such subscriptions with `policy_requires_impersonation`. Tier C reads live tables, so a policy reading another table can see a different state than the one at commit time.

**Undecidable changes are withheld.** When the filter or the policy needs a value the WAL did not carry — an unchanged TOASTed column, or a column outside a narrow replica identity on `DELETE` — the change is not delivered and `sluice_authz_unknown_total` counts it. With `SLUICE_DEGRADED_DELETES=deliver`, such a `DELETE` is delivered with `"degraded": "delete_authz_unavailable"`.

**Revocation.** Every `SLUICE_CATALOG_REFRESH` tick reloads the catalog. When anything a decision reads changed — policies, RLS flags, roles with `BYPASSRLS`, role memberships — every RLS subscription is re-resolved and the ones that lost access are dropped with `shape_not_authorized`. Time-dependent predicates (`now()`) and Tier C decisions are also re-resolved when their `SLUICE_AUTHZ_LEASE` has expired, checked on the same tick; `POST /token` re-resolves every subscription of the stream. So a policy change reaches open streams within one tick, not instantly. Hook channel joins are re-checked the same way when their verdict expires (see [Channels](#channels-broadcast-and-presence)). `REVOKE SELECT (column)` does not reach open streams: column grants are checked only at subscribe time, against answers kept until the next `SLUICE_CATALOG_REFRESH` tick.

**Columns.** The projection is the requested columns (all columns when none are requested) plus the table's key columns (primary key, or the replica identity index), intersected with the caller's `SELECT` privileges. Denied columns are reported with `columns_not_granted`; nothing outside the projection is ever emitted.

**Writing policies Sluice (and PostgreSQL) handle well.** Wrap per-query calls in a scalar subquery, `(select auth.uid())`: PostgreSQL then evaluates them once per query, and Sluice unwraps the `FROM`-less select, so both spellings resolve to the same tier. Give every policy a `TO` clause. Index every column a policy reads. `/diagnostics` reports `policy_function_not_wrapped`, `policy_applies_to_public` and `unindexed_policy_column` with a statement to run. Filtering on the column a policy compares (`owner_id = auth.uid()` with `.eq('owner_id', me)`) is what makes a subscription Tier A.

### Issuer oracle

`SLUICE_SHAPE_ORACLE=issuer`. When a client subscribes to a shape, Sluice POSTs to `SLUICE_ISSUER_URL` with `Authorization: Bearer <SLUICE_ISSUER_BEARER>` (the user's access token is never forwarded). Redirects are not followed; a timeout (`SLUICE_ISSUER_TIMEOUT`), a non-2xx or an unreadable body denies.

```json
{
  "action": "subscribe",
  "identity": { "role": "authenticated", "sub": "…", "session_id": "…", "claims": { } },
  "requested": { "schema": "public", "table": "documents",
                 "filter": "project_id=eq.42", "columns": ["id", "title"], "ops": ["INSERT"] }
}
```

`action` is `refresh` when the stream presents a new token (`POST /token`). A grant:

```json
{
  "allow": true,
  "shape": { "schema": "public", "table": "documents",
             "filter": "project_id=eq.42", "columns": ["id", "title", "body"] },
  "holds": [ { "schema": "public", "table": "project_members",
               "filter": "project_id=eq.42,user_id=eq.<sub>" } ]
}
```

Sluice checks the grant before using it:

- `shape` names the requested table (catalog name, case-sensitive).
- `shape.filter` uses the [filter grammar](#filters) and has at least one non-negated equality, so it can never mean the whole table.
- The effective filter is `authorized AND client`. The client may omit or repeat an authorized equality; a different constant is a deny.
- Columns are the requested ones (or all) plus the key columns, intersected with `shape.columns` when present (an empty list denies), and with the pool role's physical `SELECT`.
- `holds` is non-empty. Every hold table is published, and every hold filter has an equality and only reads columns in that table's replica identity (otherwise its `DELETE` could not be detected).

Sluice then installs the hold watches and the shape, and only after that checks that every hold row exists, so a `DELETE` racing the join is still caught. When a hold row is deleted, or updated out of its filter, the shape is dropped with `shape_not_authorized`; the stream and its other subscriptions continue. Zero rows in the subscribed table is a valid, empty shape. The ready result carries `oracle: "issuer"` and the effective `filter`, and no `tier`. Snapshots and `EXISTS` run as the pool role.

`POST /admin/shapes/drop` (service_role token, or the issuer bearer) drops this process's subscriptions matching a table, equalities and optionally an identity:

```json
{ "schema": "public", "table": "documents", "equalities": { "project_id": "42" },
  "identity": { "sub": "…", "role": "authenticated" } }
```

## Shapes

```json
{ "sub": "docs", "shape": {
    "schema": "public", "table": "documents",
    "filter": "owner_id=eq.7f3a…,status=in.(open,pending)",
    "columns": ["id", "title"], "ops": ["INSERT", "UPDATE", "DELETE"],
    "transitions": true, "initial": "snapshot" } }
```

`sub` is a label you choose, unique within the stream. `schema` defaults to `public`. `ops` defaults to `INSERT`, `UPDATE`, `DELETE` (`*` or `ALL` means the same three); `TRUNCATE` is opt-in and is delivered to every subscription on the table that asked for it, without filtering or authorization. `initial` is `none` (default) or `snapshot`.

### Filters

PostgREST spelling, AND-only: `column=op.value`, joined with commas.

| Operator | Meaning |
| --- | --- |
| `eq`, `neq`, `lt`, `lte`, `gt`, `gte` | comparison; the value is typed by the column |
| `in.(a,b,…)` | membership, 1 to 100 values |
| `like`, `ilike` | pattern, `*` as the wildcard |
| `is.null`, `is.true`, `is.false` | null and boolean tests |
| `not.` prefix | negation, e.g. `status=not.eq.draft` |

Values may be double-quoted (`title=eq."a,b"`). A filter naming an unknown column is refused; values are never interpolated into SQL. There is no `OR`: it would defeat constant routing. Subscribe twice instead.

**Routing.** Among the non-negated `eq` terms, Sluice prefers a column that leads an index and is in the replica identity, then any indexed column, then any `eq` column. A shape with none is **unindexed**: it is scanned on every change to its table, gets the `unindexed_shape` warning, and at most `SLUICE_UNINDEXED_SHAPES_MAX` of them are admitted per process.

### Replica identity

The old tuple of an `UPDATE`/`DELETE` carries only the replica identity columns (`DEFAULT`: primary key; `USING INDEX`: that index; `FULL`: every column). An `UPDATE` that does not change the replica identity key carries no old tuple at all. So:

- A `DELETE` is delivered only if the filter and the policy can be evaluated on the old tuple. A filter on a column outside the replica identity withholds deletes.
- With `transitions: true`, an `UPDATE` that moves a row into the shape arrives as `op: "INSERT"` with `transition: "enter"`, and one that moves it out arrives as `op: "DELETE"` with `transition: "leave"` (the `record` then holds the new values, `old` the previous ones). The same entry is also delivered as `INSERT`/`enter` to subscriptions that receive `UPDATE`s without asking for transitions. Detecting either requires the old tuple to carry the filter columns. Without an old tuple a matching row is an `UPDATE`, and a row leaving the shape is not detected.
- Subscribing with `DELETE` or `transitions` on a filter column outside the replica identity returns a `replica_identity_insufficient` warning with the `CREATE UNIQUE INDEX … ; ALTER TABLE … REPLICA IDENTITY USING INDEX …` to run; `SLUICE_REPLICA_IDENTITY=strict` refuses it instead.

`REPLICA IDENTITY USING INDEX` on a unique index over `(filter columns…, primary key)` gives deletes and transitions what they need without `FULL`, which carries every old column — including unchanged TOASTed values — in every `UPDATE` and `DELETE` (warned as `replica_identity_full_with_toast`). Dropping the index a `USING INDEX` identity names breaks the application's own deletes; Sluice refuses to start in that state.

### Values

`record` and `old` hold the projected columns, encoded as `to_jsonb` encodes them (which is what PostgREST returns), identically in snapshot rows and live changes:

- booleans as JSON booleans; integers, floats and `numeric` as JSON numbers with PostgreSQL's exact digits (`NaN` and `Infinity` as strings);
- `json` and `jsonb` embedded;
- `timestamp` and `timestamptz` in ISO 8601, `timestamptz` in UTC (`2026-09-29T20:24:42.39+00:00`);
- arrays as JSON arrays (lower bounds dropped), composite types as objects, domains as their base type;
- everything else (`date`, `time`, `interval`, `uuid`, `bytea`, `money`, enums, …) as PostgreSQL's text output.

Sluice sets `DateStyle` and `IntervalStyle` on its own sessions, so the database's settings don't change the encoding. The few cases where it departs from `to_jsonb` are listed in [ROADMAP.md](ROADMAP.md#known-limitations).

An unchanged TOASTed column is not sent: it is listed in `unchanged` and absent from `record`, and the client keeps its previous value. When `record` + `old` exceed `SLUICE_MAX_CHANGE_BYTES`, both are trimmed to the key columns and the event carries `"degraded": "change_too_large"`.

### Initial snapshots

With `initial: "snapshot"`, Sluice reads the rows itself in the same projection and filter, then keeps streaming, with no gap between the two:

1. The subscription is registered, so every change dispatched from then on reaches it live.
2. The replay floor is the reader's confirmed LSN, taken before the snapshot query: everything dispatched up to it is in the snapshot.
3. One read-only query (under the caller's role and claims in RLS mode, so RLS applies) returns at most `SLUICE_SNAPSHOT_MAX_ROWS` rows, ordered by the key. Rows arrive as `change` events with `op: "INSERT"`, `snapshot: true` and `commit_lsn` = the floor, then `snapshot_end` (with `truncated: true` if more rows matched). They are encoded exactly like live changes (see [Values](#values)).
4. Buffered changes from the floor on are replayed, so a live change delivered before an older snapshot row arrives again after it.

Duplicates are possible; clients upsert by primary key. At most `SLUICE_SNAPSHOT_MAX_CONCURRENT` snapshots run at once. A failure sends `snapshot_failed` (retryable).

## Wire protocol

All paths are under `SLUICE_PATH_PREFIX` (default `/sluice/v1`). Every endpoint except health checks and `/metrics` requires `Authorization: Bearer <jwt>`. JSON bodies are limited to 1 MiB (publish: `SLUICE_MAX_PAYLOAD_BYTES` plus envelope). Errors are `{"error": "<code>", "message": "…"}`.

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/stream` | open the SSE stream, optionally with subscriptions and a resume map |
| `POST` | `/subscribe` | add subscriptions to a stream |
| `POST` | `/unsubscribe` | remove subscriptions by label |
| `POST` | `/publish` | broadcast to a joined channel |
| `POST` | `/presence` | track, update or untrack presence on a joined channel |
| `POST` | `/token` | rebind the stream to a refreshed token |
| `POST` | `/admin/jwks/refresh` | refetch the JWKS (service_role) |
| `POST` | `/admin/shapes/drop` | drop matching shapes (service_role or the issuer bearer) |
| `GET` | `/healthz` | liveness; also served without the prefix |
| `GET` | `/readyz` | readiness; also served without the prefix |
| `GET` | `/metrics` | Prometheus, unauthenticated; also without the prefix (`SLUICE_METRICS_ENABLED`) |
| `GET` | `/diagnostics` | configuration findings (service_role, `SLUICE_DIAGNOSTICS_ENABLED`) |

The stream and every JSON response carry `Cache-Control: no-store` and `Vary: Authorization`.

### Opening a stream

```http
POST /sluice/v1/stream
Authorization: Bearer eyJ…
Content-Type: application/json
Accept: text/event-stream

{ "subscriptions": [ { "sub": "docs", "shape": { … } },
                     { "sub": "room", "channel": "room:42", "presence": true } ],
  "resume": { "public.documents": "0/1A2B3C4" } }
```

The response is `200` with `Content-Type: text/event-stream`, `X-Accel-Buffering: no` and `Sluice-Stream-Id: <node>.<n>-<n>`. It fails with `401 unauthorized`, `400 bad_request` (malformed JSON), `503 too_many_streams` (`SLUICE_MAX_STREAMS`) or `503 server_shutdown` (the process is stopping). Closing the response is how a client leaves: everything the stream held, presence included, is released.

Control requests name the stream with `stream_id` in the body, the `Sluice-Stream-Id` header, or both; if both are present and differ the request is rejected (`400 stream_id_mismatch`). The token must name the same `sub` and `role` as the stream (`403 forbidden`); stream ids are not secrets. A stream id from another process is `404 unknown_stream`.

### Events

Each event is an SSE frame `event: <name>` + `data: <one JSON line>`. A heartbeat comment `: hb` is written every `SLUICE_HEARTBEAT`.

**`ready`** — first, always:

```json
{ "stream_id": "n1.1790…-7", "server_time": "…", "heartbeat_ms": 20000, "wal_lsn": "0/1A2B3C4",
  "subscriptions": [
    { "sub": "docs", "ok": true, "tier": "A", "indexed": true, "routing_key": "owner_id",
      "reason": "predicate reduces to a constant over the shape and is stable", "warnings": [] },
    { "sub": "room", "ok": true } ] }
```

A subscription result has `sub`, `ok`, and then either an `error` (`{code, message}`) or: `tier` (RLS) or `oracle: "issuer"` and `filter` (issuer), `indexed`, `routing_key`, `reason`, `warnings`. The same warnings are also sent as `warning` events.

**`change`** — `id: <commit_lsn>:<seq>` (not on snapshot rows or `TRUNCATE`):

```json
{ "sub": "docs", "op": "UPDATE", "schema": "public", "table": "documents",
  "commit_lsn": "0/1A2B3C4", "commit_time": "…", "seq": 3,
  "record": { "id": 91, "title": "Q3 plan" }, "old": { "id": 91 },
  "unchanged": ["body"], "transition": "enter", "degraded": "change_too_large", "snapshot": true }
```

`op` is `INSERT`, `UPDATE`, `DELETE` or `TRUNCATE`; `record` is absent for `DELETE`; `old` is present when the WAL carried an old tuple; the last five fields appear only when they apply. `seq` increases per subscription.

**`snapshot_end`** — `{ "sub", "rows", "floor_lsn", "truncated"? }`.

**`broadcast`** — `{ "sub", "channel", "event", "payload", "from"?, "origin": "client" | "database", "commit_lsn"?, "at" }`. `from` is the publisher's `sub`; `commit_lsn` is set for transactional database messages.

**`presence`** — `{ "sub", "channel", "type": "state", "members": { key: member } }` on join, then `{ …, "type": "diff", "joins"?: {…}, "leaves"?: {…} }`. A member is `{ "meta", "since", "ref" }`.

**`warning`** — `{ "sub"?, "code", "message", "effect"?, "remedy"? }`.

**`error`** — `{ "sub"?, "code", "message", "retryable", "action"?, "retry_after_ms"? }`. With `sub` it ends that subscription (or reports a retryable problem with it) and the stream continues; without `sub` the stream is closing. `retry_after_ms` is how long to wait before reconnecting.

| Code | Where | Meaning |
| --- | --- | --- |
| `shape_not_authorized` | subscribe, later | denied, revoked by a policy change or `/token`, hold removed, or dropped by an operator |
| `column_not_granted` | subscribe | the role may read none of the requested columns |
| `policy_requires_impersonation` | subscribe | Tier C with `SLUICE_TIER_C=deny` |
| `relation_not_published`, `relation_unpublished` | subscribe, later | the table (or a hold table) is not, or no longer, in the publication |
| `invalid_filter`, `invalid_columns`, `invalid_ops`, `invalid_subscription` | subscribe | malformed request |
| `duplicate_sub`, `too_many_subscriptions`, `too_many_shapes`, `too_many_unindexed_shapes` | subscribe | limits |
| `replica_identity_insufficient` | subscribe | `SLUICE_REPLICA_IDENTITY=strict` |
| `unknown_namespace` | subscribe | channel refused |
| `channel_not_authorized` | subscribe, later | channel refused, or a hook channel's join revoked on re-check or `/token` |
| `resume_too_old` | subscribe, after a snapshot | the position is no longer buffered; `action: "resnapshot"` |
| `invalid_resume`, `snapshot_failed`, `internal` | subscribe | as named |
| `stream_lagging` | stream | the client did not keep up; `action: "resnapshot"` |
| `server_shutdown` | stream | the process is stopping; reconnect after `retry_after_ms` (spread over `SLUICE_RECONNECT_SPREAD`), with resume |
| `token_expired`, `session_revoked`, `user_banned` | stream | the identity behind the stream is gone |

Warning codes: `columns_not_granted`, `unindexed_shape`, `replica_identity_insufficient`, `replica_identity_full_with_toast`, `replica_identity_broken`, `snapshot_disabled`, `schema_changed`.

### Control requests

| Request body | Response |
| --- | --- |
| `/subscribe` `{ "stream_id", "subscriptions": [ … ] }` | `{ "results": [ … ] }`; `429 rate_limited` past `SLUICE_SUBSCRIBE_RATE`/s |
| `/unsubscribe` `{ "stream_id", "subs": ["docs"] }` | `{ "removed": n }` |
| `/publish` `{ "stream_id", "channel", "event", "payload", "self" }` | `{ "delivered": n }`; `403 channel_not_subscribed`, `413 payload_too_large`, `429 rate_limited` |
| `/presence` `{ "stream_id", "channel", "action": "track" \| "update" \| "untrack", "meta" }` | `{ "ok": true }`; `403 presence_key_not_allowed`, `403 channel_not_subscribed`, `429 rate_limited`, `429 presence_too_many_keys` |
| `/token` `{ "stream_id", "access_token" }` | `{ "ok": true, "revoked_subscriptions": n }`; `403 subject_mismatch` |

`/token` needs no `Authorization` header: the body's token is verified. It may not change the stream's `sub` (a stream opened without one may gain one). Every shape is re-resolved with the new claims (issuer: one `refresh` call per shape), and every hook channel is asked about again; `revoked_subscriptions` counts both.

### Resume

A client that reconnects sends `resume: { "schema.table": "<commit_lsn>" }` with the last commit LSN it saw per table. For each shape on that table Sluice replays, from its in-memory buffer, every change whose commit LSN is **at or after** that position, re-filtered and re-authorized; the snapshot is skipped. The buffer holds up to `SLUICE_RING_EVENTS` changes per table since this process started replicating; entries older than `SLUICE_RING_MAX_AGE` are swept, except each table's newest transaction. A position the buffer cannot cover — older than what it holds, or from before this process started — returns `resume_too_old` (`action: "resnapshot"`); the subscription is live, but the client must discard its state and resubscribe (for example with a snapshot). Broadcasts, presence and `TRUNCATE` are not replayed.

## Channels: broadcast and presence

A channel is `namespace:name`; the namespace is everything before the first `:`. Each namespace is declared in `SLUICE_CHANNELS` as `namespace:mode[:hook_url]`, comma-separated (default `room:public`). An undeclared namespace is refused, never public. The mode decides who may **join**; publishing and presence only require that the stream joined.

| Mode | Who may join |
| --- | --- |
| `public` | any valid token |
| `owner` | a token whose `sub` the channel name ends with, after a `:` (`notify:<sub>`) |
| `hook` | whoever your endpoint allows |

**Hook.** Sluice POSTs `{ "action": "subscribe", "channel", "namespace", "role", "sub", "session_id", "claims" }` to the namespace's URL, with `Authorization: Bearer <SLUICE_CHANNEL_HOOK_BEARER>` when that variable is set. Redirects are not followed. The verdict is cached per URL, channel, role, sub and session, so while it is valid it also answers new joins by the same session, on any of its streams.

| Response | Verdict | Valid for |
| --- | --- | --- |
| `2xx` `{ "allow": true \| false, "reason"?, "ttl"? }` | as returned | `ttl` seconds, else `SLUICE_CHANNEL_HOOK_TTL`; `ttl: 0` is not cached |
| `403` | deny | `SLUICE_CHANNEL_HOOK_TTL` |
| `401` (Sluice's credential rejected), other status, unreadable body, timeout (`SLUICE_CHANNEL_HOOK_TIMEOUT`) | none: joins are refused | 2 s |

A join is a lease on its verdict. Validity is shortened at random by up to a fifth, so joins made together do not all expire together. On the first `SLUICE_CATALOG_REFRESH` tick after it expires, Sluice asks again, once per verdict however many of the session's streams joined. An allow extends the join; a denial removes it with a `channel_not_authorized` error for that subscription, and the stream receives nothing more from the channel. A re-check that gets no verdict keeps the join and asks again on the next tick, so an endpoint outage does not eject everyone. A revocation therefore reaches an open stream within the verdict's validity plus one tick. `POST /token` asks again at once for every hook channel of the stream, ignoring cached verdicts. The cost is one endpoint call per joined verdict (channel and session) per `max(ttl, tick)`; with `ttl: 0`, one per tick.

**Broadcast from clients.** `POST /publish` delivers `payload` to every stream that joined the channel (the sender too with `self: true`) and returns how many streams it was queued on. At most `SLUICE_PUBLISH_RATE` per second per stream, `SLUICE_MAX_PAYLOAD_BYTES` per payload.

**Broadcast from the database.** A logical message whose prefix starts with `SLUICE_MESSAGE_PREFIX` is delivered to the channel named by the rest of the prefix, in commit order with the changes around it:

```sql
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid","payload":{"id":1}}');
COMMIT;
```

A transactional message (`true`) exists only if the transaction commits. Content that is JSON is the payload; if it is an object with an `event` field it is an envelope (`event` names the broadcast, `payload` is the payload); anything else is sent as a JSON string. The default event name is `message`.

**Presence.** Keyed membership held in memory, last write wins. The key is always the token's `sub`, so presence needs a token with one; `key` in the request may only repeat it. `track`/`update` set the key's `meta`; `untrack` removes it; closing the stream or unsubscribing from the channel removes every key it tracked. Joins and leaves are coalesced into one `diff` per channel every `SLUICE_PRESENCE_BROADCAST` and sent to every stream that joined the channel; a stream that joined with `presence: true` first receives the full `state`. At most `SLUICE_PRESENCE_RATE` requests per `SLUICE_PRESENCE_WINDOW` per stream and `SLUICE_PRESENCE_MAX_KEYS` keys per channel. Two streams of the same user share one key.

## Session revocation

Optional (`SLUICE_REVOCATION_ENABLED`), for any identity service that deletes a session row on sign-out and names that row in the token. Add the tables to the publication, each with its primary key as replica identity. For GoTrue:

```sql
ALTER PUBLICATION sluice ADD TABLE auth.sessions, auth.users;
```

with `SLUICE_REVOCATION_USERS_TABLE=auth.users`. A service without bans publishes only its sessions table and leaves `SLUICE_REVOCATION_USERS_TABLE` unset.

A `DELETE` on `SLUICE_REVOCATION_SESSIONS_TABLE` (sign-out deletes the session row whose `id` is the token's `SLUICE_JWT_SESSION_CLAIM` claim, `session_id` for GoTrue) closes every stream holding that session with `session_revoked`. Both ids are compared lowercased. Deleting an account whose sessions cascade revokes each of them the same way. When `SLUICE_REVOCATION_USERS_TABLE` is set, an `UPDATE` on it that leaves `SLUICE_REVOCATION_USERS_BAN_COLUMN` (`banned_until`) in the future closes the user's streams with `user_banned`; a ban time already past, as GoTrue leaves it when a timed ban runs out, is not a ban, and an `UPDATE` that clears it or moves it into the past lifts the ban. Revoked sessions are remembered for two hours, and bans until their time or for two hours, whichever is sooner: their tokens are refused on every endpoint, and open streams are also checked on every heartbeat. Only revocations the slot delivered since the process started are known.

Streams are also closed with `token_expired` at the first heartbeat after the token's `exp`.

## Delivery guarantees and backpressure

- **At least once.** PostgreSQL may re-send transactions after a restart, resumes replay whole transactions, and snapshots overlap the live stream. Clients upsert by primary key.
- **Order.** Changes reach a stream in commit order; the queue is never reordered.
- **Per-stream queue** (up to `SLUICE_STREAM_QUEUE` events; it grows as events arrive, so an idle stream holds none). When it is full, a live `change` closes the stream with `stream_lagging` (the client resumes or resnapshots); any other event is dropped and counted in `sluice_stream_dropped_events_total`. Snapshot rows, `snapshot_end` and resume replays are read faster than any client drains them, so instead of overflowing they wait while the queue is half full, leaving the other half to live events. Everything queued is written together and flushed once. Each write is bounded by `SLUICE_WRITE_TIMEOUT`; a stream that cannot be written is closed.
- **Replication backpressure.** The reader dispatches every change before it acknowledges the transaction, and acknowledges only committed, dispatched positions (every `SLUICE_STATUS_INTERVAL` and when the server asks). Between transactions it acknowledges the server's WAL end, so a quiet publication does not hold WAL back. A slow dispatch — Tier C probes, Tier B cross-checks, a full queue closing streams — delays the slot, and PostgreSQL retains WAL up to `max_slot_wal_keep_size`.

## Operating it

**Health.** `/healthz` is `200` while the process runs. `/readyz` is `200` when the catalog is loaded, this process is streaming from the slot and it is not shutting down; its body reports `catalog_loaded`, `replicating`, `streams`, `confirmed_lsn`, and `draining` once shutdown has begun.

**Shutdown.** On `SIGTERM` or `SIGINT` Sluice stops accepting connections, refuses new streams with `503 server_shutdown`, and ends every open stream with a `server_shutdown` error whose `retry_after_ms` is spread over `SLUICE_RECONNECT_SPREAD`, so clients come back over that window rather than all at once. Requests in flight get up to `SLUICE_SHUTDOWN_GRACE`, after which the process exits; without any it exits at once. Give the container a stop timeout above that grace.

**Memory.** Unless `GOMEMLIMIT` is set, Sluice sets the Go heap's soft limit to 90% of the container's cgroup memory limit at startup, so garbage is collected before the container is OOM-killed.

**One reader per slot.** At startup a process takes a session advisory lock keyed by the slot name (on a pool connection it keeps for its lifetime). A second process with the same slot stands by: it retries every 5 seconds, reports `503` on `/readyz`, and starts replicating when the lock is released. The slot itself allows only one streaming connection. The standby does not share the load; a takeover is a reconnect of every client.

**Startup validation.** Sluice refuses to start when:

- `wal_level` is not `logical`;
- no replication slot is free for a slot that does not exist yet;
- the publication is missing or has a row filter;
- a published table that publishes `UPDATE`/`DELETE` has an inadequate replica identity (`USING INDEX` on a dropped index, `DEFAULT` without a primary key, `NOTHING`), which is already breaking the application's writes;
- (RLS mode) a role in `SLUICE_ALLOWED_ROLES` does not exist, or `sluice_authz` cannot assume one that does not bypass RLS;
- (issuer mode) the pool role lacks `SELECT` or the RLS bypass on a published table;
- the JWKS cannot be fetched or has no usable key for the pinned algorithm.

It starts with a warning, kept in `/diagnostics`, for: `max_slot_wal_keep_size = -1` (`unbounded_wal_retention`), a non-zero `idle_replication_slot_timeout` (`idle_slot_timeout`), published tables without a primary key (`no_primary_key`), a replication role without `REPLICATION` (`replication_role_attribute`), and a `BYPASSRLS` allowed role `sluice_authz` cannot assume (`role_not_assumable`). A JWKS that contains symmetric keys is logged; they are never loaded.

**`GET /diagnostics`** (service_role):

```json
{ "oracle": "rls",
  "slot": { "name": "sluice", "active": true, "wal_status": "reserved", "retained_bytes": 8048,
            "confirmed_lsn": "0/80551F8", "received_lsn": "0/80551F8" },
  "subscriptions": { "total": 3, "streams": 2, "relations": 2, "unindexed": 1,
                     "by_tier": { "A": 2, "B": 1, "C": 0 } },
  "publication": { "name": "sluice", "tables": 12 },
  "replication_options": { "proto_version": 4, "messages": true },
  "warnings": [ { "code": "tier_c_policy", "severity": "high", "relation": "public.invoices",
                  "policy": "invoices_team_member", "reason": "…", "impact": "…", "remedy": "…" } ] }
```

Issuer mode adds `"issuer": { "url", "timeout", "holds" }` and reports no tiers. Warning codes, besides the startup ones: `replica_identity_broken`, `replica_identity_full_with_toast`, `policy_unparseable`, `policy_applies_to_public`, `policy_function_not_wrapped`, `unindexed_policy_column`, `tier_c_policy`, `tier_c_subscriptions` (RLS), `hold_replica_identity` (issuer), `unindexed_shape`.

**Metrics** (`/metrics`, Prometheus). The one to alert on is `sluice_authz_tier_c_probes_total`; `sluice_authz_downgrades_total` should always be 0.

| Metric | Labels |
| --- | --- |
| `sluice_wal_lsn` (gauge), `sluice_wal_lag_bytes`, `sluice_slot_retained_bytes` | `kind` = received, confirmed |
| `sluice_wal_messages_total` | `type` |
| `sluice_reader_reconnects_total`, `sluice_reader_is_leader` | |
| `sluice_changes_total` | `schema`, `table`, `op` |
| `sluice_change_dispatch_seconds`, `sluice_routing_candidates` (histograms) | `schema`, `table` |
| `sluice_toast_unchanged_total` | `schema`, `table`, `column` |
| `sluice_changes_truncated_total` | `schema`, `table` |
| `sluice_subscriptions` (gauge) | `schema`, `table`, `tier`, `indexed` |
| `sluice_authz_resolutions_total` | `tier`, `result` = granted, denied, refused |
| `sluice_authz_resolve_seconds` | `tier` |
| `sluice_authz_tier_c_probes_total`, `sluice_authz_tier_c_withheld_total`, `sluice_authz_downgrades_total` | `schema`, `table` |
| `sluice_authz_compile_failures_total` | `schema`, `table`, `reason` |
| `sluice_authz_unknown_total` | `schema`, `table`, `op` |
| `sluice_authz_lease_refreshes_total` | `result` = held, revoked |
| `sluice_channel_hook_rechecks_total` (per joined channel) | `result` = held, revoked, unavailable |
| `sluice_streams` (gauge) | |
| `sluice_stream_dropped_events_total` | `kind` |
| `sluice_stream_closed_total` | `reason` |
| `sluice_broadcast_published_total` | `namespace`, `origin` |
| `sluice_presence_updates_total` | `namespace`, `action` |
| `sluice_snapshot_seconds`, `sluice_snapshot_rows_total` | `schema`, `table` |
| `sluice_revocations_total` | `source` = session, ban |
| `sluice_config_warnings` (gauge, 1 per active `/diagnostics` warning) | `code` |

The slot and warning metrics refresh every `SLUICE_CATALOG_REFRESH`.

**Failures.**

- **PostgreSQL restart or dropped replication connection:** the reader reconnects with backoff (1 s, doubling to about 30 s) from the slot's confirmed position; streams stay open.
- **Slot invalidated:** the reader stops and the process exits, because resuming would hide a gap. Recreate the slot; clients must resnapshot.
- **Sluice restart:** streams are closed; clients reconnect, and their resume positions are no longer buffered, so they resnapshot.
- **A table leaves the publication:** its shapes are dropped with `relation_unpublished`.
- **A table's definition changes:** its subscriptions get `schema_changed`; dropped columns disappear from events, and new columns are not added until the client resubscribes.

**Logs** are `log/slog`, JSON by default (`SLUICE_LOG_FORMAT=text` for text). Sluice does not log tokens, claims or row data.

## Security model

- **Tokens.** The algorithm is pinned (`SLUICE_JWT_ALG`, ES256 or RS256; HS256 is not offered), `exp` is required, the `role` must be in `SLUICE_ALLOWED_ROLES` (see below for tokens without one), `iss` is checked when `SLUICE_JWT_ISSUER` is set, and `aud` for tokens that have a `sub`. `sb_*` opaque keys are refused. The JWKS is refetched every `SLUICE_JWKS_REFRESH` (checked on the catalog tick), on `POST /admin/jwks/refresh`, and when a token names an unknown `kid`, at most once every 10 seconds.
- **Tokens without a role.** With `SLUICE_JWT_REQUIRE_ROLE=false`, accepted only with `SLUICE_SHAPE_ORACLE=issuer` (RLS mode needs a role for `SET LOCAL ROLE`), the `role` claim is ignored, present or not, and the identity has an empty role: `SLUICE_ALLOWED_ROLES` is not used, and the issuer and hooks receive `"role": ""`. No token can then be `service_role`, so `POST /admin/jwks/refresh` and `GET /diagnostics` are unreachable with a JWT; `POST /admin/shapes/drop` still takes the issuer bearer.
- **No tokens in URLs.** The stream is a `POST`, so the token is always a header.
- **Stream ownership.** A control request must carry a token with the stream's `sub` and `role`; a header/body disagreement on the stream id is rejected.
- **Least data.** Filters only narrow what the oracle grants; projections are intersected with grants; undecidable changes are withheld; a failed catalog refresh keeps the previous state rather than widening it.
- **Shared secrets.** `SLUICE_ISSUER_BEARER` authenticates Sluice to the issuer, `SLUICE_CHANNEL_HOOK_BEARER` to hook endpoints. Neither is a user credential, and neither client follows redirects, so neither secret is resent elsewhere.
- **What `sluice_repl` sees.** Everything published, regardless of RLS. `REPLICA IDENTITY FULL` also puts every old column in the stream Sluice reads, so keep replica identities narrow where you can.
- **Unauthenticated endpoints.** `/healthz`, `/readyz` and `/metrics` (which exposes table and namespace names); restrict them at the gateway if that matters.

## Configuration reference

Environment variables. Durations use Go syntax (`30s`, `5m`). The runnable template is [`deploy/sluice.env.example`](deploy/sluice.env.example).

| Variable | Default | |
| --- | --- | --- |
| `SLUICE_LISTEN_ADDR` | `0.0.0.0:4000` | |
| `SLUICE_PATH_PREFIX` | `/sluice/v1` | |
| `SLUICE_LOG_LEVEL` | `info` | `debug`, `info`, `warn`, `error` |
| `SLUICE_LOG_FORMAT` | `json` | or `text` |
| `SLUICE_NODE_ID` | hostname | prefixes stream ids |
| `SLUICE_SHUTDOWN_GRACE` | `15s` | for requests in flight; streams end at once |
| `SLUICE_RECONNECT_SPREAD` | `10s` | window the `retry_after_ms` of `server_shutdown` is drawn from; `0` sends none |
| `SLUICE_DB_REPL_URL` | required | must include `replication=database` |
| `SLUICE_DB_AUTHZ_URL` | required | must not |
| `SLUICE_DB_POOL_MAX_CONNS` / `_MIN_CONNS` | `8` / `2` | the reading process keeps one for its lock |
| `SLUICE_PARANOID_POOL_RESET` | `false` | `DISCARD ALL` on every returned connection |
| `SLUICE_SLOT_NAME` / `SLUICE_PUBLICATION` | `sluice` / `sluice` | |
| `SLUICE_PROTO_VERSION` | `4` | 1–4 |
| `SLUICE_MESSAGES` | `true` | deliver `pg_logical_emit_message` |
| `SLUICE_STATUS_INTERVAL` | `10s` | slot acknowledgement interval |
| `SLUICE_MESSAGE_PREFIX` | `sluice:` | must not be empty |
| `SLUICE_RING_EVENTS` / `SLUICE_RING_MAX_AGE` | `4096` / `60s` | resume buffer per table |
| `SLUICE_JWKS_URL` | required | |
| `SLUICE_JWKS_REFRESH` | `5m` | |
| `SLUICE_JWT_ALG` | `ES256` | or `RS256` |
| `SLUICE_JWT_ISSUER` | empty | not checked when empty |
| `SLUICE_JWT_AUDIENCE` | `authenticated` | |
| `SLUICE_JWT_LEEWAY` | `10s` | |
| `SLUICE_ALLOWED_ROLES` | `anon,authenticated,service_role` | ignored when the role is not required |
| `SLUICE_JWT_REQUIRE_ROLE` | `true` | `false` accepts tokens without `role`; issuer mode only |
| `SLUICE_JWT_SESSION_CLAIM` | `session_id` | claim holding the session row's `id` (e.g. `sid`) |
| `SLUICE_SHAPE_ORACLE` | `rls` | or `issuer` |
| `SLUICE_ISSUER_URL` / `SLUICE_ISSUER_BEARER` | empty | required in issuer mode |
| `SLUICE_ISSUER_TIMEOUT` | `2s` | |
| `SLUICE_AUTHZ_LEASE` | `60s` | re-check of time-dependent and Tier C decisions |
| `SLUICE_CATALOG_REFRESH` | `30s` | catalog, lease, JWKS and health tick |
| `SLUICE_TIER_C` | `allow` | or `deny` |
| `SLUICE_TIER_C_MAX_PROBES_PER_SECOND` | `2000` | per process |
| `SLUICE_TIER_C_TIMEOUT` | `1s` | per Tier C probe and Tier B cross-check |
| `SLUICE_TIER_B_VERIFY` | `5` | cross-checks per Tier B subscription; 0 disables |
| `SLUICE_UNINDEXED_SHAPES_MAX` | `200` | per process |
| `SLUICE_REPLICA_IDENTITY` | `warn` | or `strict` |
| `SLUICE_DEGRADED_DELETES` | `withhold` | or `deliver` |
| `SLUICE_SNAPSHOT_ENABLED` | `true` | |
| `SLUICE_SNAPSHOT_MAX_CONCURRENT` / `_MAX_ROWS` | `4` / `50000` | |
| `SLUICE_HEARTBEAT` | `20s` | at least 5 s |
| `SLUICE_STREAM_QUEUE` | `256` | most events queued per stream |
| `SLUICE_WRITE_TIMEOUT` | `10s` | per write |
| `SLUICE_MAX_STREAMS` | `50000` | per process |
| `SLUICE_MAX_SUBS_PER_STREAM` / `SLUICE_MAX_SHAPES_PER_STREAM` | `100` / `20` | shapes and channels / shapes |
| `SLUICE_MAX_PAYLOAD_BYTES` / `SLUICE_MAX_CHANGE_BYTES` | `262144` / `1048576` | |
| `SLUICE_SUBSCRIBE_RATE` / `SLUICE_PUBLISH_RATE` | `20` / `100` | per second per stream |
| `SLUICE_PRESENCE_RATE` / `SLUICE_PRESENCE_WINDOW` | `5` / `30s` | per stream |
| `SLUICE_PRESENCE_BROADCAST` | `1500ms` | diff interval |
| `SLUICE_PRESENCE_MAX_KEYS` | `10` | per channel |
| `SLUICE_CHANNELS` | `room:public` | `namespace:mode[:hook_url]`, comma-separated |
| `SLUICE_CHANNEL_HOOK_TTL` / `_TIMEOUT` | `60s` / `2s` | |
| `SLUICE_CHANNEL_HOOK_BEARER` | empty | sent only when set |
| `SLUICE_REVOCATION_ENABLED` | `false` | |
| `SLUICE_REVOCATION_SESSIONS_TABLE` | `auth.sessions` | |
| `SLUICE_REVOCATION_USERS_TABLE` | unset | no user-level revocation when unset; `auth.users` for GoTrue bans |
| `SLUICE_REVOCATION_USERS_BAN_COLUMN` | `banned_until` | |
| `SLUICE_METRICS_ENABLED` / `SLUICE_DIAGNOSTICS_ENABLED` | `true` / `true` | |

## Clients

- **JavaScript/TypeScript:** [`@pauserratgutierrez/sluice-js`](packages/sluice-js/README.md), typed against the same generated `Database` types as a PostgREST client, with reconnect and resume built in.
- **Anything else:** the [wire protocol](#wire-protocol) is plain HTTP. Read the stream with a streaming `fetch` (or equivalent), not `EventSource`, which cannot send an `Authorization` header or a request body.

## Development and testing

```bash
go vet ./... && go test ./...        # unit tests (CI on release tags also runs -race)
cd packages/sluice-js && npm test    # SDK unit and type-level tests
```

End-to-end suites run on the harness network. Build a tool, then run it with the database password:

```bash
docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 golang:1.26-alpine go build -o .bin/smoke ./cmd/smoke
docker run --rm --network deploy_private_net -v "$PWD/.bin:/b:ro" \
  -e POSTGRES_PASSWORD="$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2)" alpine:3.22 /b/smoke
```

| Tool | What it checks |
| --- | --- |
| `cmd/smoke` | the RLS critical path: tiers, delivery and withholding, `DELETE`, TOAST, broadcasts, PostgREST agreement, snapshots, resume, value encoding vs `to_jsonb`, evaluator vs PostgreSQL, security negatives, a small fan-out |
| `cmd/smoke-issuer` | the issuer process: grants, narrowing, hold cut, `/token` refresh (run after `cmd/smoke`) |
| `cmd/audit` | a broad policy spectrum, DML and WAL edge cases, revocation, `/diagnostics`; needs `SERVICE_ROLE_KEY` and writes a JSON report to `/out` |
| `cmd/load` | a fan-out soak on the harness (`LOAD_SCENARIO`, `LOAD_STREAMS`, `LOAD_CHANGES`, `LOAD_USERS`) |
| `node packages/sluice-js/test/live.mjs [baseUrl]` | the built SDK through the gateway |
| [`apps/loadtest`](apps/loadtest/README.md) | capacity hunts against a published image, with its own compose stack |

The harness applies `deploy/db/fixtures.sql`, `issuer_fixtures.sql` and `audit_fixtures.sql` on every `up`. Releases (image and SDK, from `v*.*.*` tags) are described in [MAINTENANCE.md](MAINTENANCE.md).

## Repository layout

```
cmd/sluice          the server: wiring, startup validation, reader lock
cmd/smoke           end-to-end RLS checks
cmd/smoke-issuer    end-to-end issuer checks
cmd/issuer-stub     the harness issuer
cmd/audit           broad audit battery
cmd/load            fan-out soak
cmd/keygen          harness secrets and ES256 keys
internal/config     environment parsing and validation
internal/reader     the replication connection and slot acknowledgement
internal/pgoutput   pgoutput decoding (including the unchanged-TOAST marker)
internal/catalog    policies, roles, grants, replica identity, indexes
internal/expr       policy and filter expressions: convert, analyze, fold, evaluate
internal/authz      the RLS tiers, probes, cross-checks and leases
internal/oracle     the rls and issuer oracles
internal/hold       issuer hold watches
internal/shape      filter grammar, narrowing, routing key
internal/registry   the subscription routing index
internal/hub        streams, channels, presence, resume buffer, rate limits
internal/server     HTTP surface, dispatch, snapshots, hooks, diagnostics
internal/auth       JWT verification and revocation
internal/timer      the shared heartbeat wheel
internal/metrics    Prometheus collectors
internal/event      wire event types
deploy/             compose harness, database bootstrap and fixtures, Caddy, env template
packages/sluice-js  TypeScript client
apps/loadtest       independent load test of a published image
```
