# Roadmap

Nothing in this file is implemented. [README.md](README.md) describes what Sluice does today; this is what it does not do yet, and what could be done about it.

## Known limitations

Current behavior that is deliberate or accepted for now, with its consequence.

- **One process serves every stream.** A second process on the same slot only stands by. There is no bus to share fan-out across processes, so capacity is one process's, and a takeover or restart reconnects every client.
- **Resume does not survive a Sluice restart.** The resume buffer is in memory, so after a restart every client resnapshots.
- **Revocation of a policy is bounded, not instant.** Policy, RLS, role and membership changes reach open streams on the next `SLUICE_CATALOG_REFRESH` tick; PostgreSQL emits no notification for them.
- **Column grants are not re-checked.** `REVOKE SELECT (column)` does not reach open streams; privileges are checked at subscribe time only, against answers cached until the next catalog refresh.
- **Tier C reads live tables.** A policy with a subquery is evaluated against the database's current state, not the state at commit time, and the probe runs on the replication path (bounded by `SLUICE_TIER_C_TIMEOUT`).
- **Table-owner RLS bypass is not modeled.** An owner's subscription is judged by the policies (fail-closed).
- **Tier B compares in Go.** Text comparisons use byte order rather than the column's collation, and `numeric` comparisons use float64. The PostgreSQL cross-check on the first decisions per subscription is the safeguard.
- **Session revocation only knows what it saw.** Sessions deleted before the process started are not known, and a revoked session is remembered for two hours.
- **The value encoding follows `to_jsonb` with three exceptions.** A `json` column is embedded as stored, not normalized the way `jsonb` would be. A float may arrive in exponent form (`1e+30`) where `to_jsonb` writes every digit. A type that `to_jsonb` converts through a cast to `json` (for example `hstore`) arrives as its text output.
- **Snapshots are one query.** At most `SLUICE_SNAPSHOT_MAX_ROWS` rows, with `truncated` set when more matched; there is no paging.
- **Snapshots for `BYPASSRLS` roles** need `sluice_authz` to be granted that role.
- **Publication column lists are not validated.**
- **Only PostgreSQL 18.4 is tested.** The default `SLUICE_PROTO_VERSION=4` needs 16 or newer.
- **`/metrics` is unauthenticated.**
- **`/token` may change a stream's role** (the `sub` may not change, except from none).

## Candidate work

### Hardening

- CI on every push: build, vet, `-race`, SDK tests and the harness suites. Today the release workflows only run on tags.
- Property-based authorization tests: generate policies, shapes and rows, and assert that Sluice's decision matches what PostgREST returns.
- Operational burn-in: kill the database mid-stream, fill the slot, restart under load, run for days.
- Runbooks for backup/restore and the slot lifecycle, including recovery from an invalidated slot.
- Test against PostgreSQL 16 and 17, and lower `SLUICE_PROTO_VERSION` against older servers.
- An open-source license (the SDK is `UNLICENSED`).

### Authorization

- Model the table-owner bypass using the role memberships the catalog already loads.
- Detect column-grant revocation (it costs a query per subscription per tick rather than a hash comparison).
- Batch hook re-checks: one endpoint call for several channels whose verdicts expire on the same tick (needs a multi-channel hook request).
- Per-namespace hook secrets, or signed hook requests (HMAC over the body with a timestamp) instead of a static bearer.
- Cap the `ttl` a hook may return.
- Move Tier C probes off the replication path, so one slow policy does not delay every subscriber. Today each probe is bounded by `SLUICE_TIER_C_TIMEOUT`, which caps the delay but does not remove it.

### Delivery

- Paged snapshots for large shapes.
- A durable or shared resume buffer, so a restart or takeover does not force a resnapshot.
- A subscribe-time `auth.sessions` lookup, to refuse tokens whose session was deleted before the process started.

### Scale

- A bus between processes (for example NATS), with control requests forwarded to the process that owns a stream (the stream id already carries the node id).
- Presence across processes, which needs reconciliation between nodes.

### Deferred on purpose

- **Streaming large transactions (`streaming = on`).** It would deliver uncommitted changes that must be buffered in Sluice until commit; the reader does not do that, so the option is not offered.
- **A second Tier C `DELETE` technique** (re-inserting the old row in a subtransaction and selecting it under the caller's role). It would decide deletes without `REPLICA IDENTITY FULL`, but not make Tier C faster.
- **WebTransport** as an alternative transport.
