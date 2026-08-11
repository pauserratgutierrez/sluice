# @pauserratgutierrez/sluice-js

Typed realtime client for [Sluice](../../README.md). PostgreSQL row changes, ephemeral broadcast and presence over **one** SSE connection, with the access token in an `Authorization` header rather than a query parameter.

Zero runtime dependencies. ESM. Works in browsers, Node ≥ 20, Deno and workers.

For the raw HTTP/SSE contract (any language, no client library), see [Wire protocol](../../README.md#wire-protocol-any-language) in the main README.

```bash
npm install @pauserratgutierrez/sluice-js
```

Install from [npm](https://www.npmjs.com/package/@pauserratgutierrez/sluice-js). Versions are published on the same `v*.*.*` tags as the server image (see [`MAINTENANCE.md`](../../MAINTENANCE.md)). In git, `package.json` stays at `0.0.0`; CI sets the published version from the tag.

## Quick start

```ts
import { createClient } from '@pauserratgutierrez/sluice-js'
import type { Database } from './database.types' // your generated PostgREST types

const sluice = createClient<Database>('https://api.example.com/sluice/v1', {
  accessToken: async () => (await supabase.auth.getSession()).data.session?.access_token,
})

const docs = await sluice
  .from('documents')                    // ← autocompleted from Database
  .eq('owner_id', userId)               // ← column and value type checked
  .select('id', 'title', 'updated_at')  // ← narrows the row type
  .withInitialSnapshot()
  .on('*', ({ op, record, old }) => {
    // record is Pick<Row, 'id' | 'title' | 'updated_at'>
    console.log(op, record?.title)
  })
  .subscribe()
```

The `Database` generic is the **same generated types file** a PostgREST client uses. The SDK is hand-written because it has a public API and semver matters; the types it consumes are generated, because a schema has no API to break.

## Check the tier

The single most useful thing this client tells you:

```ts
if (docs.tier === 'C') {
  console.warn('this subscription authorizes on every change:', docs.reason)
  for (const w of docs.warnings) console.warn(w.code, '→', w.remedy)
}
```

| Tier | What it costs per change |
| --- | --- |
| `A` | nothing — the policy reduced to a constant over your shape |
| `B` | an in-process evaluation, no database work |
| `C` | **one impersonated query per change, per subscriber** |

Tier C is correct but does not scale. If you see it, the server's `/diagnostics` endpoint names the offending policy and suggests a rewrite. Usually the fix is to add an `.eq()` on the column the policy compares, or to denormalise a joined column onto the table.

`indexed: false` is the other one to watch: it means your shape has no equality filter on an indexed column, so the server scans it for every change to that table.

## Changes

```ts
sluice.from('documents')
  .eq('owner_id', userId)
  .withTransitions()
  .on('INSERT', c => cache.set(c.record.id, c.record))
  .on('UPDATE', c => {
    // `unchanged` lists columns the WAL did not carry because they hold an
    // unchanged TOASTed value. They are NOT null and NOT deleted -- keep your
    // existing value for them. This is the distinction wal2json throws away.
    const prev = cache.get(c.record.id)
    cache.set(c.record.id, { ...prev, ...c.record })
  })
  .on('DELETE', c => cache.delete(c.old.id))
  .subscribe()
```

**`withTransitions()`** is worth understanding. Without it, a row that stops matching your filter simply stops producing events, and your local copy keeps a row that no longer belongs there. With it, you get a synthetic `INSERT` when a row enters the shape (`transition: 'enter'`) and a synthetic `DELETE` when it leaves (`transition: 'leave'`).

**`withInitialSnapshot()`** closes the race between fetching initial state over HTTP and subscribing. The server takes its replay floor *before* opening the snapshot transaction, so nothing slips through the gap. Rows arrive as `INSERT` with `snapshot: true`, terminated by `onSnapshotEnd`. Duplicates between the snapshot and the live stream are possible and intended — upsert by primary key.

### Filters

AND-only, PostgREST spelling. `*` is the wildcard for `like`/`ilike`.

```ts
.eq('status', 'open').gte('priority', 3).in('kind', ['a', 'b']).notEq('archived', true)
.like('title', 'draft*').is('deleted_at', null)
```

There is no `or()`, and that is deliberate: `OR` destroys the constant indexing that makes dispatch O(1) in subscriber count. Register two subscriptions.

## Broadcast and presence

```ts
const room = sluice.channel<{ name: string }>('room:42')

room.on<{ x: number; y: number }>('cursor', (p, meta) => draw(p, meta.from))
room.onJoin(joins => console.log('joined', Object.keys(joins)))
room.onLeave(leaves => console.log('left', Object.keys(leaves)))

await room.subscribe()
await room.track({ name: 'Pau' })

const delivered = await room.send('cursor', { x, y })
```

`send` is a request with a response: an oversized payload or an unauthorized channel **throws**, rather than being silently dropped. `track` is withdrawn automatically when the connection closes — there is no leave message to send.

Database-originated broadcasts arrive on the same handler with `meta.origin === 'database'` and a `commit_lsn`:

```sql
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid"}');
COMMIT;
```

Roll the transaction back and the message never existed.

## Connection lifecycle

One client holds **one** stream and multiplexes every subscription over it. Over HTTP/2 that stream and the control POSTs share a single connection.

```ts
const sluice = createClient<Database>(url, {
  accessToken: () => token,
  onStatusChange: s => setOnline(s === 'open'),
  onError:   e => Sentry.captureException(e),
  onWarning: w => console.warn('[sluice]', w.code, w.remedy),
  backoff: [500, 1000, 2000, 5000, 10_000],   // full jitter is applied
  pauseWhenHidden: true,                       // default
})
```

**Reconnection is automatic and gapless within the server's retention window.** The client tracks the last commit LSN per table and resumes from it, so a dropped connection does not silently lose changes. If the requested position has aged out of the server's buffer you get `resume_too_old` with `action: 'resnapshot'`.

**Refresh the token when your auth library does:**

```ts
supabase.auth.onAuthStateChange((_e, session) => {
  if (session) void sluice.setAuth(session.access_token)
})
```

Every authorization decision is re-resolved server-side on refresh, so a subscription that is no longer permitted is dropped with an error instead of quietly continuing. If the token expires without a refresh, the stream closes with `token_expired`. If the user signs out, the server sees the `auth.sessions` delete on its replication stream and closes the stream within milliseconds.

**`pauseWhenHidden`** closes the stream on `document.hidden` and reopens with a resume when the tab returns. This is the standard mitigation for the conflict between proxies wanting frequent keepalives and mobile radios wanting silence.

## Errors

```ts
import { SluiceError } from '@pauserratgutierrez/sluice-js'

.onError(e => {
  if (e.code === 'shape_not_authorized') showPermissionDenied()
  else if (e.action === 'resnapshot') void refetchEverything()
  else if (e.retryable) { /* the client is already retrying */ }
})
```

| Code | Meaning |
| --- | --- |
| `shape_not_authorized` | the policy denies this shape for this caller |
| `relation_not_published` | add the table to the publication |
| `invalid_filter` | the filter references an unknown column or a bad value |
| `replica_identity_insufficient` | DELETE cannot be filtered; the remedy is a runnable `ALTER TABLE` |
| `resume_too_old` | the buffer aged out; resnapshot |
| `token_expired` / `session_revoked` | reauthenticate |
| `stream_lagging` | the client could not keep up and was disconnected |

## API

| | |
| --- | --- |
| `createClient<DB>(url, options)` | create a client |
| `.from(table)` | typed shape builder on `public` |
| `.schema(s).from(table)` | typed shape builder on another schema |
| `.channel<M>(name)` | signalling channel |
| `.setAuth(token)` | rebind to a refreshed token |
| `.close()` | close the stream and forget everything |
| `.connectionStatus` | `connecting` \| `open` \| `reconnecting` \| `closed` |

`parseSSE(body, signal)` is exported too, if you need the framing for a custom
transport.

## Development

```bash
npm install
npm run build        # tsc → dist (ESM + .d.ts + source maps)
npm test             # builds, then runs the unit and type-level tests
npm run typecheck:test
node test/live.mjs   # drives the built SDK against a running harness via Caddy
```

The unit tests import from `dist/`, so what is verified is what ships. The type-level test uses `@ts-expect-error` to assert that a wrong table name, a wrong column, or a wrong value type **fails to compile** — that is the whole point of the typing, so it is tested rather than assumed.