# @pauserratgutierrez/sluice-js

Typed realtime client for [Sluice](../../README.md): PostgreSQL row changes, broadcast and presence over **one** SSE connection, with the access token in an `Authorization` header rather than a query parameter.

Zero runtime dependencies. ESM. Needs `fetch` with streaming response bodies (browsers, Node ≥ 20, Deno, workers).

For the raw HTTP/SSE contract, see [Wire protocol](../../README.md#wire-protocol) in the main README.

```bash
npm install @pauserratgutierrez/sluice-js
```

Versions are published on the same `v*.*.*` tags as the server image (see [`MAINTENANCE.md`](../../MAINTENANCE.md)). In git, `package.json` stays at `0.0.0`; CI sets the published version from the tag.

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

The `Database` generic is the **same generated types file** a PostgREST client uses.

## Check the oracle

What comes back from `subscribe()` depends on which shape oracle the server runs.

**RLS** (`SLUICE_SHAPE_ORACLE=rls`, the default): `tier` is `A`, `B` or `C`.

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

Tier C is correct but does not scale; the server's `/diagnostics` names the policy and suggests a rewrite. Usually the fix is an `.eq()` on the column the policy compares, or denormalising a joined column onto the table.

**Issuer** (`SLUICE_SHAPE_ORACLE=issuer`): there are no tiers. `oracle` is `'issuer'` and `filter` is the effective filter after narrowing.

```ts
if (docs.oracle === 'issuer') console.log('effective filter', docs.filter)
```

`indexed: false` means the shape has no equality filter on an indexed column, so the server scans it for every change to that table.

A subscription the server refuses resolves with `ok: false` and `error`, calls `onError`, and is not resent on reconnect.

## Changes

```ts
sluice.from('documents')
  .eq('owner_id', userId)
  .withTransitions()
  .on('INSERT', c => cache.set(c.record.id, c.record))
  .on('UPDATE', c => {
    // `unchanged` lists columns the WAL did not carry because they hold an
    // unchanged TOASTed value. They are NOT null and NOT deleted -- keep your
    // existing value for them.
    const prev = cache.get(c.record.id)
    cache.set(c.record.id, { ...prev, ...c.record })
  })
  .on('DELETE', c => cache.delete(c.old.id))
  .subscribe()
```

**`select()`** restricts the projection and narrows the row type. The server adds the table's key columns (primary key, or the replica identity index) when you may read them, so rows can always be identified.

**`withTransitions()`**: an UPDATE that moves a row out of the shape arrives as a `DELETE` with `transition: 'leave'`, and one that moves it in as an `INSERT` with `transition: 'enter'`. Both need the table's replica identity to carry the filter columns; the server warns (`replica_identity_insufficient`) when it does not. Without transitions, a row that stops matching simply stops producing events.

**`withInitialSnapshot()`** makes the server read the current rows itself and then continue live, with no gap between the two. Rows arrive as `INSERT` with `snapshot: true`, followed by `onSnapshotEnd({ rows, truncated })` (`truncated` when more rows matched than the server's cap). Duplicates around the boundary are possible and intended — upsert by primary key. Snapshot rows use PostgreSQL's JSON encoding (ISO 8601 timestamps, JSON arrays) while live changes use its text output, so parse timestamps with a parser that accepts both.

`ops('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE')` restricts operations; the default is the first three.

### Filters

AND-only, PostgREST spelling. `*` is the wildcard for `like`/`ilike`.

```ts
.eq('status', 'open').gte('priority', 3).in('kind', ['a', 'b']).notEq('archived', true)
.like('title', 'draft*').is('deleted_at', null)
```

Also `neq`, `gt`, `lt`, `lte`, `ilike`, `notIn`, `notLike`. There is no `or()`: `OR` would stop the server routing a change with a map lookup. Register two subscriptions.

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

The channel's namespace (`room`) must be configured on the server. `send` is a request with a response: an oversized payload or a channel you have not joined **throws**. `on('*', …)` receives every event. Presence is keyed by your token's `sub` (a token without one cannot track), and your entry is withdrawn automatically when the connection closes. Registering `onPresence`, `onJoin` or `onLeave` before `subscribe()` requests the full roster on join; `channel.presence` holds the current roster.

Database-originated broadcasts arrive on the same handlers with `meta.origin === 'database'` and, for transactional messages, `meta.commit_lsn`:

```sql
BEGIN;
  UPDATE orders SET status = 'paid' WHERE id = 1;
  SELECT pg_logical_emit_message(true, 'sluice:orders:1', '{"event":"paid","payload":{"id":1}}');
COMMIT;
```

Roll the transaction back and the message never existed.

## Connection lifecycle

One client holds **one** stream and multiplexes every subscription over it.

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

**Reconnection is automatic.** The client remembers the last commit LSN it saw per table and resumes from it, so changes missed while disconnected are replayed (the last transaction you saw may arrive again). If the server no longer has that position (it restarted, or you were away too long), the shape gets `resume_too_old` with `action: 'resnapshot'` and the client forgets the stale position: discard what you hold for that shape and subscribe again, for example with `withInitialSnapshot()`.

**Tokens.** `accessToken` is called on every (re)connect and control request, so a function returning the current token is enough in most apps. Call `setAuth(token)` when your auth library refreshes: from then on the client uses that token, and an open stream is rebound to it — every authorization decision is re-resolved server-side, and a subscription no longer permitted is dropped with an error. If the stream closed with `token_expired` and the reconnect was refused, `setAuth` reconnects it.

```ts
supabase.auth.onAuthStateChange((_e, session) => {
  if (session) void sluice.setAuth(session.access_token)
})
```

If a **hold** row is deleted (issuer mode), that shape is cut with `shape_not_authorized` and the stream stays open. If the user signs out and the server has session revocation enabled, the stream closes with `session_revoked` and reconnecting with that token is refused.

**`pauseWhenHidden`** closes the stream while `document.hidden` and reopens it, with a resume, when the tab returns.

## Errors

```ts
import { SluiceError } from '@pauserratgutierrez/sluice-js'

.onError(e => {
  if (e.code === 'shape_not_authorized') showPermissionDenied()
  else if (e.action === 'resnapshot') void refetchEverything()
})
```

Subscription errors go to that subscription's `onError`; stream errors go to the client's `onError`.

| Code | Meaning |
| --- | --- |
| `shape_not_authorized` | not permitted: RLS policy, issuer deny, revoked on refresh, hold removed, or dropped by an operator |
| `relation_not_published` / `relation_unpublished` | the table is not (or no longer) in the publication |
| `invalid_filter`, `invalid_columns` | unknown column or bad value |
| `unknown_namespace`, `channel_not_authorized` | the channel was refused |
| `resume_too_old` | the resume position is gone; resnapshot |
| `snapshot_failed` | the initial snapshot failed (retryable) |
| `stream_lagging` | the client did not keep up; the stream reconnects with a resume |
| `token_expired` | the token expired; the stream closed |
| `session_revoked`, `user_banned` | the identity was revoked; the stream closed |
| `http_401`, `http_403` | the stream request was refused; the client stops retrying |

The full list is in the main README's [event reference](../../README.md#events).

## API

| | |
| --- | --- |
| `createClient<DB>(url, options)` | create a client |
| `.from(table)` | typed shape builder on `public` |
| `.schema(s).from(table)` | typed shape builder on another schema |
| `.channel<M>(name)` | signalling channel |
| `.setAuth(token)` | use a refreshed token |
| `.close()` | close the stream and forget everything |
| `.connectionStatus` | `connecting` \| `open` \| `reconnecting` \| `closed` |

`parseSSE(body, signal)` is exported too, if you need the framing for a custom transport.

## Development

```bash
npm install
npm run build        # tsc → dist (ESM + .d.ts + source maps)
npm test             # builds, then runs the unit and type-level tests
npm run typecheck:test
node test/live.mjs   # drives the built SDK against a running harness through the gateway
```

The unit tests import from `dist/`, so what is verified is what ships. The type-level test uses `@ts-expect-error` to assert that a wrong table name, a wrong column, or a wrong value type **fails to compile**.
