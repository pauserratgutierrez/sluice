# Changelog

One version number covers the server image (`ghcr.io/pauserratgutierrez/sluice`) and the SDK (`@pauserratgutierrez/sluice-js`).

## 0.7.0

### Fixed

- A replication slot invalidated on PostgreSQL 18 (`max_slot_wal_keep_size` exceeded, `idle_replication_slot_timeout`), or dropped while Sluice ran (a database restore), left the reader retrying forever: streams stayed open with heartbeats only and no error. PostgreSQL 18 reports an invalidated slot with a message Sluice did not recognise. After a failed stream the reader now reads the slot from `pg_replication_slots`, and when it can no longer stream every change the process exits with an error, ending every stream with `server_shutdown`.
- SDK: a shape that had received no change before its stream dropped reconnected without a resume position, so the changes made while it was disconnected were lost without notice, on every server restart among other times. Shapes now resume from the position the server gives when they go live.
- An issuer timeout, network error or `5xx` refused the shape as `shape_not_authorized`, which the SDK forgets: a brief issuer outage during a reconnect wave or a `/token` ejected live subscriptions for good. It is now the retryable `issuer_unavailable`.
- `TRUNCATE` of a hold table did not cut the shapes it held. They are now dropped with `shape_not_authorized`.

### Changed

- **Breaking (protocol):** whether a resume is covered is in the subscription result, as `resumed: true | false`, instead of an asynchronous `resume_too_old` error. `resume_too_old` is now only sent when the buffer no longer reaches an initial snapshot's floor. Clients other than the SDK that send `resume` must read `resumed`.
- `wal_lsn` in `ready` is taken before the subscriptions are installed, and `/subscribe` returns one too: resuming from it replays whatever those subscriptions missed.
- An issuer `4xx` other than `408` and `429`, a redirect and `"allow": false` still deny. A `5xx`, `408`, `429`, timeout, network error or unreadable body is `issuer_unavailable`, with the issuer's `Retry-After` (at most 60 s) or a delay spread between 1 and 5 s as `retry_after_ms`.
- `POST /token` in issuer mode drops a shape the issuer gives no verdict on with `issuer_unavailable` (it was `shape_not_authorized`), and counts it in `revoked_subscriptions`. A new token never keeps a grant made for the previous one.
- The resume buffer grows with each table's traffic up to `SLUICE_RING_EVENTS`, instead of reserving every slot on the first change, and is bounded in bytes by `SLUICE_RING_MAX_BYTES` across every table, evicting the oldest change first.
- The replication slot is created by the process that takes the reader lock, after taking it; a process standing by no longer creates it.
- SDK: a subscription refused with a retryable error stays registered and is asked for again after `retry_after_ms`. A subscription error with `retry_after_ms` means the server removed it and the client subscribes again. The client no longer forgets a position on `resume_too_old`.
- Harness: the Sluice containers' healthcheck is `-readycheck`.

### Added

- SDK: `onLive` on shapes, called each time the server installs the subscription, with `{ sub, reason: 'subscribed' | 'resubscribed', resumed, walLsn }`. It fires once the shape is live, so a read made from it cannot miss a change. `resumed` is true only when the server replays everything since the previous stream. Type `LiveEvent`.
- `issuer_unavailable` subscription error: retryable, with `retry_after_ms`.
- `SLUICE_RING_EVENTS=0` disables the resume buffer; it needs `SLUICE_SNAPSHOT_ENABLED=false`. Every resume is then `resumed: false`.
- `SLUICE_RING_MAX_BYTES` (default `67108864`, 64 MiB).
- `SLUICE_SLOT_RECREATE` (default `false`). With `true`, an invalidated slot is dropped and recreated at startup instead of stopping the process, never while another process holds it; it logs a warning and counts `sluice_slot_recreated_total`.
- `SLUICE_REVOCATION_SESSION_LOOKUP` (default `false`; needs `SLUICE_REVOCATION_ENABLED=true`). `POST /stream` and `POST /token` check that the token's session row still exists, so a session signed out before a restart cannot reconnect: `401` when it is gone, `503 session_check_unavailable` when the lookup fails. Found sessions are trusted for 30 s.
- `sluice -readycheck` probes `/readyz`.
- Metrics `sluice_slot_recreated_total` and `sluice_resume_buffer_bytes`; `result="unavailable"` on `sluice_authz_resolutions_total` and `sluice_authz_lease_refreshes_total`.

### For clients

- Register `onLive` before `subscribe()` and read the current state there unless `e.resumed`. The first `onLive` (`reason: 'subscribed'`) replaces a read made after subscribing; it runs before `subscribe()` resolves.
- `issuer_unavailable` is retryable and handled by the SDK: `subscribe()` resolves with `ok: false` and `onError` receives it with `retryable: true`, but the subscription stays registered and `onLive` follows once it is live. Treat only `retryable: false` errors as the end of a subscription.
- `POST /stream` and `POST /token` may answer `503 session_check_unavailable` with the session lookup on; the SDK retries the stream (`http_503`), and `setAuth` throws a retryable error.

### For issuers

- Deny with `200 {"allow": false}` or a `4xx`. A `5xx`, `408` or `429` is an outage: the client is told to try again, after your `Retry-After` when you send one.

### For operators

- With `SLUICE_REVOCATION_SESSION_LOOKUP=true`: `GRANT SELECT (id) ON <sessions table> TO <authz role>;`. Startup refuses without it. The `id` column must be `uuid` or text holding lowercase ids.
- Health: use `sluice -readycheck` (`/readyz`) for a single-process deployment. Docker Compose does not restart unhealthy containers; Sluice exits on its own when its slot is gone, and needs a restart policy.
- An invalidated slot now stops the process at startup until it is dropped, or until `SLUICE_SLOT_RECREATE=true` replaces it.
- Size `SLUICE_MAX_STREAMS` to the memory limit (about 8 000 five-shape streams in 512 MB, measured with 0.4.0).

## 0.6.0

### Added

- `SLUICE_JWT_SESSION_CLAIM` (default `session_id`) names the claim that carries the session id, so session revocation works with identity services other than GoTrue (Better Auth: `sid`). The session id is lowercased on both sides of the comparison.
- `SLUICE_JWT_REQUIRE_ROLE` (default `true`). With `false`, tokens without a `role` are accepted and the claim is ignored even when present; the identity's role is empty, `SLUICE_ALLOWED_ROLES` is unused, and startup checks no roles. Only accepted with `SLUICE_SHAPE_ORACLE=issuer`. In that mode no JWT is `service_role`, so `/admin/jwks/refresh` and `/diagnostics` are unreachable with a token; `/admin/shapes/drop` still takes the issuer bearer.
- `SLUICE_REVOCATION_USERS_BAN_COLUMN` (default `banned_until`).

### Changed

- **Breaking:** `SLUICE_REVOCATION_USERS_TABLE` has no default. Unset, there is no user-level revocation. A GoTrue deployment that relies on bans closing streams must now set `SLUICE_REVOCATION_USERS_TABLE=auth.users`.

### For operators

- Upgrading with `SLUICE_REVOCATION_ENABLED=true` and GoTrue bans: set `SLUICE_REVOCATION_USERS_TABLE=auth.users`. Nothing else changes for GoTrue tokens.
- The issuer and hook payloads now carry `"role": ""` for tokens accepted without a role.

## 0.5.0

### Fixed

- SDK: a client stopped by a refused stream (`401`, `403`) reported `reconnecting` although it no longer retried. It now reports `closed`; `setAuth` reopens it.
- SDK: after the server ended a stream, the status stayed `open` while the client waited to reconnect, for up to `retry_after_ms` after `server_shutdown`.

### Changed

- SDK: `reconnecting` is reported only when the stream drops or an attempt to open it fails. Replacing a stream the server ended (`token_expired`, `server_shutdown`) is `connecting`.

### Added

- SDK: `idle` connection status, for a client with nothing subscribed or a hidden tab. It is also the status of a new client, which reported `closed` before.

### For clients

- A `switch` over `ConnectionStatus` needs an `idle` case. A "connection lost" notice can show `reconnecting` as is, without a delay to hide planned reconnects.

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
