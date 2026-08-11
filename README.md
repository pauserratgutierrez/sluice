# Sluice

A realtime data-streaming server for PostgreSQL.

A sluice is a gate on a channel. It takes one flow — the write-ahead log — and meters it out to each consumer, giving every subscriber exactly what it is entitled to and nothing more.

Sluice replaces `supabase/realtime` in a self-hosted stack. It is not protocol-compatible with it, and it requires **nothing installed in the database**: no extensions, no tables, no functions, no schemas. Four server settings, two roles, and a publication.

> **Status: working prototype, not yet production.** The critical path is implemented and validated end-to-end against a live PostgreSQL 18.4, GoTrue, PostgREST and Caddy: 36 server assertions, 15 SDK unit tests, 16 SDK live checks, and a race-clean Go suite, all passing.

---

## Why

`supabase/realtime` authorizes **every change against every subscriber**. Its own documentation says so:

> Postgres Changes authorizes every event against each subscriber. When you make a single change to a table with 100 subscribed users, Realtime performs 100 authorization checks — one per user — so throughput scales with the number of subscribers, not the write rate. Changes are also processed on a single thread to preserve their order, which means larger compute add-ons don't meaningfully increase Postgres Changes throughput.

Measured on PostgreSQL 18.4, that model costs **~9–13 µs per subscriber per change**:

| Subscribers on one change | Per-subscriber impersonation | Indexed / constant-reduced |
| --- | --- | --- |
| 100 | 739 changes/sec | 6,098 changes/sec |
| 1,000 | **108 changes/sec** | 4,405 changes/sec |
| 5,000 | **18.6 changes/sec** | 1,873 changes/sec |

Sluice resolves authorization **once, at subscribe time**, into something that costs nothing per change. That single decision is the whole point; everything else follows from it.

## What it does differently

| | `supabase/realtime` | Sluice |
| --- | --- | --- |
| Database objects required | `_realtime` schema, `realtime` schema, 3 tables, 5 types, 15 functions, 81 migrations, a dedicated owner role, a partition janitor | **none** |
| Change source | `pg_logical_slot_get_changes` polled every 100 ms, on a **temporary** slot | `START_REPLICATION` streaming protocol, permanent slot, LSN feedback and real backpressure |
| Output plugin | `wal2json` | `pgoutput` (in-core) |
| Unchanged TOASTed column | dropped from the JSON with **no marker** | explicit `unchanged: ["body"]` |
| Authorization | one impersonated probe per subscriber per change | resolved once, in three tiers |
| `DELETE` under RLS | `old_record` truncated to primary keys | full old row, correctly authorized |
| Database broadcast | day-partitioned `realtime.messages` + a second replication connection + a janitor | `pg_logical_emit_message`, atomic with your transaction, zero tables |
| Session revocation | none; a signed-out user streams until the JWT expires | pushed on the same slot, milliseconds |
| Transport | WebSocket, so the token travels in the URL | SSE over POST, `Authorization: Bearer` |
| Expensive configuration | silent | reported at `/diagnostics` with a runnable remedy |

## The three authorization tiers

Chosen automatically at subscribe time, reported back to the client, and visible in metrics.

**Tier A — constant reduction.** If the RLS predicate reads only columns the shape's filter pins to equality constants, then its truth value is the same for every row in the shape. Evaluate it once; never again. A policy of `owner_id = auth.uid()` with a shape filtered on `owner_id` lands here — the canonical Supabase case, and the reason the numbers above are 40× apart.

**Tier B — compiled predicate.** Row-dependent but pure over the row, so it is compiled to an in-process evaluator and run against the tuple the WAL already delivered. Zero database round trips, full RLS semantics — **including `DELETE`**, which Supabase documents as impossible precisely because it probes a live table instead of the old tuple.

**Tier C — impersonated probe.** Subqueries, joins, volatile functions. Correct, but ~13 µs per subscriber per change. Treated as a defect to surface, not a normal mode: rate-budgeted, counted in `sluice_authz_tier_c_probes_total`, and reported at `/diagnostics` with the offending policy and a concrete rewrite. Set `SLUICE_TIER_C=deny` to refuse such subscriptions outright.

## How to write policies

Two rules, both from Supabase's own RLS performance guide, both of which Sluice understands and checks.

**Wrap per-query calls in a scalar subquery.** `auth.uid()` is `STABLE`, so PostgreSQL re-invokes it for every row it scans. `(select auth.uid())` becomes an InitPlan evaluated once per query — 9 ms instead of 179 ms over 100,000 rows.

**Give every policy a `TO` clause**, so an ineligible role is rejected before the predicate runs rather than after.

```sql
CREATE POLICY documents_own ON public.documents
  FOR SELECT TO authenticated
  USING (owner_id = (select auth.uid()));

CREATE INDEX ON public.documents (owner_id);   -- every column a policy reads
```

PostgreSQL stores that wrapper as a subquery — `(owner_id = ( SELECT auth.uid() AS uid))` — so a naive reader would call the recommended spelling uncompilable and drop it to Tier C. Sluice unwraps FROM-less selects, in all the positions PostgreSQL emits them, and the two spellings resolve to **exactly the same tier**. A select with a `FROM` is a real subquery and still Tier C.

What you have not done is reported, with a runnable statement:
`policy_function_not_wrapped`, `policy_applies_to_public`, `unindexed_policy_column`.

## What PostgreSQL must provide

That's the whole contract:

```sql
-- server settings (restart for wal_level)
--   wal_level = logical
--   max_replication_slots >= 1
--   max_wal_senders >= 1
--   max_slot_wal_keep_size = <bounded>     -- the default -1 is unlimited

CREATE ROLE sluice_repl  WITH LOGIN REPLICATION PASSWORD '...';
CREATE ROLE sluice_authz WITH LOGIN NOINHERIT   PASSWORD '...';
GRANT anon, authenticated TO sluice_authz;

CREATE PUBLICATION sluice;
ALTER PUBLICATION sluice ADD TABLE public.documents;
```

`sluice_repl` needs **no table privileges at all** — logical decoding is not subject to RLS or grants, which is exactly why it is used for nothing else and why its credential must be treated as a superuser's.

Do not add publication row filters. Verified: a column used in a publication `WHERE` expression must be part of the replica identity, and when it is not, the **application's own** `UPDATE` and `DELETE` statements fail. Sluice validates for this at startup and refuses to run.

## Try it

The harness is a full stack: PostgreSQL 18.4 (the **plain** upstream image — no custom build, no `wal2json`), GoTrue for real ES256 tokens, PostgREST as the authorization oracle, Caddy, and Sluice.

```bash
git clone https://github.com/pauserratgutierrez/sluice && cd sluice

cp .env.example .env
sh deploy/keygen.sh >> .env      # runs in a container; no local Go needed
# then delete the empty duplicates the example left behind

docker compose -f deploy/compose.yml --env-file .env up -d --build
docker compose -f deploy/compose.yml --env-file .env ps
```

## Run the published image

The runtime image is only the `sluice` binary (plus CA certs). It does not include Compose, Postgres, GoTrue, or the harness.

```bash
docker pull ghcr.io/pauserratgutierrez/sluice:latest

docker run --rm -p 4000:4000 \
  --env-file deploy/sluice.env.example \
  -e SLUICE_DB_REPL_URL='postgres://sluice_repl:...@db:5432/postgres?replication=database' \
  -e SLUICE_DB_AUTHZ_URL='postgres://sluice_authz:...@db:5432/postgres' \
  -e SLUICE_JWKS_URL='http://auth:9999/.well-known/jwks.json' \
  ghcr.io/pauserratgutierrez/sluice:latest
```

Required env vars are `SLUICE_DB_REPL_URL`, `SLUICE_DB_AUTHZ_URL`, and `SLUICE_JWKS_URL`. Every other knob and its default is listed in [`deploy/sluice.env.example`](deploy/sluice.env.example). The image healthcheck runs `/sluice -healthcheck` against `GET /healthz`.

Run the end-to-end validation:

```bash
docker run --rm -v "$PWD:/src" -w /src -e CGO_ENABLED=0 \
  golang:1.26-alpine go build -o .bin/smoke ./cmd/smoke

docker run --rm --network deploy_private_net -v "$PWD/.bin:/b:ro" \
  -e POSTGRES_PASSWORD="$(grep '^POSTGRES_PASSWORD=' .env | cut -d= -f2)" \
  alpine:3.22 /b/smoke
```

It signs a user up through GoTrue, opens a stream, and asserts the design's claims — tier selection, RLS delivery including `DELETE`, TOAST `unchanged` markers, transactional broadcast, PostgREST agreement, snapshots, differential evaluation against PostgreSQL, security negatives, and a small fan-out load:

```
subscription           tier  indexed  routing key   note
---------------------  ----  -------  ------------  ----
docs                   A     yes      owner_id
posts                  B     no                     warn:unindexed_shape warn:replica_identity_full_with_toast
invoices               C     yes      team_id
metrics                A     no                     warn:unindexed_shape
articles               A     yes      owner_id      warn:replica_identity_insufficient

  PASS  only the caller's own INSERT is delivered
  PASS  per-row evaluation delivers own+public and withholds others' private
  PASS  the caller's own DELETE is delivered with the full old row
  PASS  another user's DELETE is withheld
  PASS  an untouched TOASTed column arrives as unchanged:["body"]
  PASS  a transactional WAL message is delivered as a broadcast
  PASS  a message emitted in a rolled-back transaction never arrives
  PASS  PostgREST sees exactly the pre-existing rows plus the ones Sluice streamed
  PASS  a shape whose predicate reduces to FALSE is refused at subscribe time
  …
  PASS  compiled evaluator agrees with PostgreSQL on N evaluations
  PASS  another user's token cannot drive someone else's stream
  …

== 36 checks, 0 failures ==
```

## Using it from a client

### JavaScript / TypeScript

Use [`@pauserratgutierrez/sluice-js`](https://www.npmjs.com/package/@pauserratgutierrez/sluice-js) — one client, one SSE connection, typed against the same generated `Database` types as PostgREST:

```bash
npm install @pauserratgutierrez/sluice-js
```

```ts
import { createClient } from '@pauserratgutierrez/sluice-js'
import type { Database } from './database.types'

const sluice = createClient<Database>('https://api.example.com/sluice/v1', {
  accessToken: async () => (await supabase.auth.getSession()).data.session?.access_token,
})

const docs = await sluice
  .from('documents')
  .eq('owner_id', userId)               // pins the policy column -> Tier A
  .select('id', 'title', 'updated_at')
  .withInitialSnapshot()
  .on('*', ({ op, record }) => console.log(op, record?.title))
  .subscribe()
```

Full API, filters, broadcast, presence, and reconnection: [`packages/sluice-js/README.md`](packages/sluice-js/README.md).

### Wire protocol (any language)

One long-lived `POST` whose response is `text/event-stream`, plus short control POSTs. Over HTTP/2 that is **one connection**, not two: the stream is one multiplexed stream and each POST is another.

```js
const res = await fetch('/sluice/v1/stream', {
  method: 'POST',
  headers: {
    Authorization: `Bearer ${accessToken}`,   // a header, not a query parameter
    'Content-Type': 'application/json',
    Accept: 'text/event-stream',
  },
  body: JSON.stringify({
    subscriptions: [
      { sub: 'docs', shape: {
          schema: 'public', table: 'documents',
          filter: `owner_id=eq.${userId}`,
          columns: ['id', 'title', 'updated_at'],
          transitions: true,
      }},
      { sub: 'room', channel: 'room:42', presence: true },
    ],
  }),
})
```

Events arrive as named SSE frames:

```
event: ready
data: {"stream_id":"n1.…","subscriptions":[{"sub":"docs","tier":"A","indexed":true,…}]}

event: change
id: 1A2B/3C4D18:3
data: {"sub":"docs","op":"UPDATE","record":{…},"old":{…},"unchanged":["body"]}
```

`EventSource` is deliberately not used: it cannot set headers, cannot POST, and cannot change its subscription set without reconnecting. Those three limitations are the only reason realtime tokens ever travelled in URLs.

## Broadcast from the database, atomically

```sql
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid"}');
COMMIT;
```

Both arrive on the same slot, in transaction order. Roll back and the message never existed — the dual-write problem solved with no outbox table, no retention policy, and no janitor. Needs one grant:

```sql
GRANT EXECUTE ON FUNCTION pg_logical_emit_message(boolean, text, text) TO your_app_role;
```

## Operating it

```bash
curl -H "Authorization: Bearer $SERVICE_ROLE_KEY" localhost:4000/sluice/v1/diagnostics
```

```json
{
  "slot": { "active": true, "retained_bytes": 1280, "wal_status": "reserved" },
  "replication_options": { "proto_version": 4, "streaming": "off", "binary": false },
  "warnings": [
    { "code": "tier_c_policy", "severity": "high",
      "relation": "public.invoices", "policy": "invoices_team_member",
      "reason": "contains a subquery (EXISTS (SELECT ...)), which cannot be evaluated against the WAL tuple",
      "impact": "measured at roughly 13 microseconds per subscriber per change, which caps throughput near 100 changes/sec at 1,000 subscribers",
      "remedy": "denormalise the joined column onto public.invoices so the policy becomes a direct comparison …" }
  ]
}
```

Every warning carries a remedy that is a runnable statement. The metric to alert on is `sluice_authz_tier_c_probes_total`.

## Measured behaviour

The design claim is that dispatch cost is flat in subscriber count. Measured on the harness, delivering the same 20 changes:

| Subscribers | Events delivered | Wall time | Events/s |
| --- | --- | --- | --- |
| 100 | 2,000 | 349 ms | 5,727 |
| 400 | 8,000 | 359 ms | 22,297 |
| 1,000 | 20,000 | 359 ms | 55,722 |

Wall time is constant; only the event count scales. For comparison, `supabase/realtime`'s published figure for the RLS path is 5 database changes per second at 4,000 subscribers, because it authorizes every change against every subscriber.

End-to-end latency from `INSERT` to a browser event, through Caddy: **48 ms**.

## What's left before production

Sluice is a working prototype with good test coverage, not production software. In rough order of importance:

1. **Run it against a copy of your real schema and traffic.** Everything measured so far uses fixtures designed to exercise each tier. Your policies are the variable that matters; `/diagnostics` will tell you which ones fall to Tier C.
2. **Operational burn-in.** Kill the database mid-stream, fill the slot, restart under load, run for a week. The failure paths are implemented and reasoned about, but they have not been exercised for days at a time.
3. **A CI pipeline.** Build, vet, `-race` tests, and the harness smoke suite on every push. None of that exists yet.
4. **An open-source license** (SDK is still `UNLICENSED`).
5. **Horizontal scale**, if you need more than one node: the `Bus` seam is designed ([design doc](design_doc.md) §22) but not built.
6. **Backup/restore and slot lifecycle runbooks.** An invalidated slot is a deliberate hard stop; the recovery procedure should be written down before you need it.

Published artifacts are already cut from `v*.*.*` tags: the runtime image on GHCR (`ghcr.io/pauserratgutierrez/sluice`) and the SDK on npm. How to release is in [`MAINTENANCE.md`](MAINTENANCE.md).

## Layout

```
cmd/sluice          the server
cmd/keygen          harness secrets and ES256 API keys
cmd/smoke           end-to-end validation (~36 assertions)
internal/expr       the expression engine: parse, analyze, fold, reduce, evaluate
internal/authz      the three-tier authorization model
internal/auth       JWT/JWKS verification and session revocation
internal/pgoutput   the logical replication decoder (owns the 'u' marker)
internal/reader     the single replication connection and LSN feedback
internal/catalog    cached policies, grants, replica identity, index coverage
internal/shape      filter grammar and routing-key selection
internal/registry   the constant-indexed subscription index
internal/hub        streams, fan-out, ring buffers, presence
internal/server     HTTP surface, SSE, dispatch, snapshots, hooks, diagnostics
internal/timer      shared jittered wheel: one timer for every stream on the node
internal/metrics    Prometheus collectors
internal/config     env parsing and defaults
internal/event      shared event types
deploy/             compose harness: db bootstrap, fixtures, Caddy; sluice.env.example lists every runtime SLUICE_* knob
packages/sluice-js  the typed TypeScript client
design_doc.md       full design: protocol, authz tiers, config, failure modes
MAINTENANCE.md      how to cut image and SDK releases
```

## Design decisions worth knowing before changing anything

The full rationale lives in [`design_doc.md`](design_doc.md). The short version:

- **`proto_version = 4`, `streaming = off`, `binary = false`.** All three look arbitrary and are not. The negotiated protocol version alone changes nothing on the wire (verified: 1, 4 and 4+parallel produce byte-identical output); the *options* determine the message set. `streaming = off` means everything received is already committed, so the reader forwards immediately and holds no buffer. And `binary = true` was measured **larger** than text (112 vs 88 bytes) while requiring per-type decoders.
- **`REPLICA IDENTITY USING INDEX`, not `FULL`.** A unique index on `(filter columns…, pk)` puts the columns you filter on into old tuples at ~1/15 the WAL cost and ~1/3200 the message size of `FULL`, which inlines entire TOASTed values on every update.
- **No `LISTEN/NOTIFY`.** Identical payloads in one transaction are silently deduplicated, throughput collapses 32× at 100 idle listeners on PostgreSQL 18, and a disconnected listener misses everything permanently.
- **PostgreSQL's grammar, Sluice's semantics.** Policy text is parsed by [`pgplex/pgparser`](https://github.com/pgplex/pgparser), a pure-Go port of PostgreSQL's `gram.y` — no cgo, no `libpg_query`, still a static binary. Sluice does *not* maintain a grammar subset, because a missing production does not fail, it misparses: with no rule for `CURRENT_USER` a hand-written parser falls through to its identifier rule, and the resulting "column" the WAL can never supply withholds every row in silence. What Sluice does maintain is the set of parsed nodes it will evaluate, in `internal/expr/convert.go`, where a gap is structurally unrepresentable and reported by name.
- **Fail closed everywhere.** An unrecognised expression node means Tier C, never Tier A. A value the WAL did not carry means *unknown*, never *visible*. Every operator the parser accepts is checked against the set the evaluator implements, because an operator that parses but cannot be evaluated would compile to a predicate that returns *unknown* for every row — withholding everything, silently, with nothing in `/diagnostics` to explain it.