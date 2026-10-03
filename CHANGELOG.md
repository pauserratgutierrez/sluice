# Changelog

One version number covers the server image (`ghcr.io/pauserratgutierrez/sluice`) and the SDK (`@pauserratgutierrez/sluice-js`).

## 0.4.0

### Fixed

- A ban that had run out still banned. GoTrue leaves `banned_until` set when a timed ban ends, and any later `UPDATE` of that user (a sign-in sets `last_sign_in_at`) closed their streams with `user_banned` and refused their tokens for two hours. A ban is now in force only while `banned_until` is in the future, is enforced until then (at most two hours), and an `UPDATE` that clears it lifts it.
- A snapshot or resume larger than the stream queue closed the stream with `stream_lagging`: its rows were queued faster than any client reads them. Snapshot rows, `snapshot_end` and replays now wait for room, filling at most half the queue so live changes keep the other half.
- Sluice never exited on `SIGTERM`: closing the database pool waited forever for the connection holding the reader lock, so every stop ended in a `SIGKILL`. It now exits in well under a second.
- Shutdown left every stream open until `SLUICE_SHUTDOWN_GRACE` ran out, so a container stop with a shorter timeout killed the process with no event sent. Streams now end at once with `server_shutdown` (see Added), and `/readyz` reports `503` while draining.
- A resume replay that outlived its subscription kept delivering to it; it now stops, and follows a `/token` rebind of filter and columns.

### Changed

- The stream queue grows as events arrive, up to `SLUICE_STREAM_QUEUE`, so an idle stream holds no buffer (it reserved about 12 KiB before). Everything queued is written together and flushed once, instead of one write and flush per event.
- A change is projected and encoded to JSON once per distinct column list, and shared by every subscriber with that projection, instead of once per subscriber.
- Shapes in one `/stream` or `/subscribe` request are resolved concurrently (up to four at once) and installed in request order, with the same limits.
- Column privileges are cached until the next `SLUICE_CATALOG_REFRESH` tick, so a reconnect wave costs one query per table and role instead of one per shape. A `GRANT` or `REVOKE` reaches new subscriptions within one tick.
- The issuer and hook HTTP clients keep up to 64 idle connections per host (Go's default is 2).
- A Tier C probe takes two round trips instead of four, and every probe and Tier B cross-check is bounded by `SLUICE_TIER_C_TIMEOUT`; one that runs out of time withholds the change.
- Unless `GOMEMLIMIT` is set, the Go heap's soft limit is 90% of the container's cgroup memory limit.
- Keep-alive connections idle between requests are closed after five minutes.
- SDK: `subscribe()` calls made in the same tick are sent in one `/subscribe`.

### Added

- `server_shutdown` stream error, with `retry_after_ms` drawn from `SLUICE_RECONNECT_SPREAD` (default `10s`) so clients reconnect spread out. `POST /stream` answers `503 server_shutdown` while shutting down.
- `SLUICE_TIER_C_TIMEOUT` (default `1s`).
- SDK: a stream-scoped error's `retry_after_ms` sets the delay before the next reconnect, and is exposed as `SluiceError.retryAfterMs`.

### For clients

- `error` events may carry `retry_after_ms`. The SDK honors it; other clients should wait that long before reconnecting.

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
