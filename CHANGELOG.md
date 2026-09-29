# Changelog

One version number covers the server image (`ghcr.io/pauserratgutierrez/sluice`) and the SDK (`@pauserratgutierrez/sluice-js`).

## 0.3.0

Hook channel joins are now leases on their verdict.

### Changed

- A stream that joined a `hook` channel is asked about again once the verdict expires, on the next `SLUICE_CATALOG_REFRESH` tick, and on `POST /token`. A denial removes the join with a `channel_not_authorized` error for that subscription; before, a join lasted as long as the stream. A re-check that gets no verdict (timeout, 5xx, unreadable body, 401) keeps the join and retries on the next tick.
- `ttl: 0` in a hook response means the verdict is not cached, for allows and denials alike: every join asks, and a joined stream is asked again on every tick. Before, `0` meant "use `SLUICE_CHANNEL_HOOK_TTL`". Omit `ttl` to get that default.
- A verdict's validity is shortened at random by up to a fifth, so joins made together do not all expire on the same tick.
- `POST /token` counts revoked hook channels in `revoked_subscriptions`.
- SDK: when the server removes a channel join, or refuses it when a reconnect resends it, `Channel.onError` receives the error, `send` throws, `channel.presence` is emptied, and `subscribe()` may be called again, including from inside `onError`. Before, the channel kept reporting itself subscribed.
- SDK: a subscription ended by a non-retryable error is forgotten before its error handler runs.

### Added

- Metric `sluice_channel_hook_rechecks_total{result="held|revoked|unavailable"}`.

### For hook endpoints

- Each joined verdict (channel and session) now costs one call per `max(ttl, SLUICE_CATALOG_REFRESH)` while the join lasts; with `ttl: 0`, one per tick. A session's streams on the same channel share one call.
- Return a short `ttl` for grants that can be withdrawn (blocks, removals). A revocation reaches an open stream within the verdict's validity plus one tick.

## 0.2.2

### Fixed

- The catalog refresh ran with PostgreSQL's JIT enabled, and the type query's cost estimate made it compile on every refresh: 0.4 to 1.2 s per refresh, now about 10 ms. The refresh runs in one read-only transaction with `jit = off`.

## 0.2.1

### Changed

- Live changes are encoded as `to_jsonb` encodes them, the same as snapshot rows and PostgREST: ISO 8601 timestamps with `timestamptz` in UTC, arrays as JSON arrays, composite types as objects, domains as their base type. Before, live changes carried PostgreSQL's text output (`2026-09-29 20:24:42+00`, `{a,b}`). The exceptions are listed in [ROADMAP.md](ROADMAP.md#known-limitations).
- Snapshots read columns in text format and share the live encoder, so both paths cannot drift. Snapshots of tables with a column named `t` no longer fail.

## 0.2.0

Published by mistake from the same commit as 0.1.7. Identical to 0.1.7.
