# Load-test app

An independent Docker app that load-tests a **published** Sluice image. It is not the `deploy/` harness and it does not import Sluice's Go module. It speaks the public protocol the way a product would: mint JWTs, `POST /stream`, insert rows, count SSE events, and (for signalling) `POST /publish`, `POST /presence`, and `pg_logical_emit_message`.

Default target: [`ghcr.io/pauserratgutierrez/sluice:0.8.0`](https://github.com/pauserratgutierrez/sluice/pkgs/container/sluice) against `postgres:18.6-trixie`. `SLUICE_IMAGE` and `POSTGRES_IMAGE` point it at other tags, and `scripts/run.sh -l <profile>` builds the working tree and runs a profile against that.

**Sluice runs with a production container's limits by default:** 1 CPU (`SLUICE_CPUS`), 512 MB (`SLUICE_MEM_LIMIT`) and `SLUICE_MAX_STREAMS=5000`. The capacity hunts below were measured without those limits; to reproduce them, raise all three (for example `SLUICE_CPUS=16 SLUICE_MEM_LIMIT=6g SLUICE_MAX_STREAMS=50000`).

## Profiles

Two families. The **capacity profiles** search for a ceiling with bursts of writes; the **production-shaped profiles** hold a realistic load for minutes or hours, with the reconnects, token renewals and hook re-checks a product causes, and report whether it meets a set of criteria.

### Capacity profiles

| Profile | Oracle / plane | What runs | Question it answers |
| --- | --- | --- | --- |
| `rls-a` | RLS tier A | geometric hunt + binary search on two axes | How many concurrent **users** (1 conn each), and how many **connections per user** (1 user), still deliver ≥90% of change events? |
| `rls-b` | RLS tier B | calibrated ladder 100 → 1500 | What does compiled per-row eval look like as fan-out grows? |
| `rls-c` | RLS tier C | calibrated ladder 25 → 200 | Where do impersonated probes fall over on this machine? |
| `issuer` | issuer + holds | same hunt as `rls-a` | Same two axes, through join-time HTTP grants and WAL holds. |
| `broadcast` | signalling | ladder; client `POST /publish` then `pg_logical_emit_message` | Does one room fan out, from the client and from the WAL, without a messages table? |
| `presence` | signalling | ladder on **users** only | Do `track()` diffs converge, and do disconnects produce leaves? (Presence keys are the JWT subject, so extra connections of the same user overwrite.) |
| `mixed` | tier A + room | ladder; INSERT then publish on the same stream | Do changes and broadcasts share a stream without starving each other? |
| `kick` | issuer hold | ladder on **users**; one INSERT then `DELETE` the hold | Does a membership delete cut the shape with `shape_not_authorized` while the stream stays up? |
| `idle` | tier A + room | ladder 1000 → 20000 on **users**; streams with `LOAD_IDLE_SHAPES` shapes and a channel, held open `LOAD_IDLE_HOLD` with no traffic | What does an open, quiet stream cost in memory, goroutines and CPU? Most of a product's streams look like this. |

Every step also reports **Sluice's own resource use**, from the Go and process metrics on its `/metrics`, sampled every `LOAD_SAMPLE_EVERY`: memory per stream (growth of heap in use plus goroutine stacks from the step's start to every stream open, before any traffic), goroutines per stream, peak live memory and RSS, CPU seconds and CPU per 1000 delivered events, and the longest recent GC pause. They are in the JSON and in a second table of the Markdown report.

A step **passes** when ≥95% of streams open, ≥90% of expected events (or presence members / kicks) arrive, and Sluice is not mass-closing streams as `stream_lagging`. The runner sizes the hunt ceiling from the container cgroup (CPU / memory) and caps at 40 000 streams so a laptop Docker VM is not frozen; `LOAD_MAX_STREAMS` overrides both. Memory limits on the compose services are the other guardrail.

### Production-shaped profiles

They run against `sluice-rooms`, an issuer-mode Sluice with RLS off and Sluice's own defaults (`SLUICE_CATALOG_REFRESH=30s`, `SLUICE_HEARTBEAT=20s`, `SLUICE_DB_POOL_MAX_CONNS=8`, …), every one overridable with its usual name. The schema is a generic chat, [`sql/02-rooms.sql`](sql/02-rooms.sql):

- `room_messages (room_id, seq, author_id, body, created_at)`;
- `room_members (room_id, user_id, last_message_at)`, primary key `(room_id, user_id)`, replica identity `FULL` or the primary key's (`LOAD_REPLICA_IDENTITY=full|pk`, applied by the runner before each run);
- `notifications (user_id, kind, body, created_at)`, for big transactions;
- all three in publication `sluice_rooms`. A member's `room_members` row is the issuer **hold** of every shape the issuer grants them.

| Profile | What runs | What it reports |
| --- | --- | --- |
| `rooms` | `R` rooms of `N` members, `M` of them with a stream. Each stream has `S` shapes and the room's channel, joined through a **hook**. Each room commits a message every `LOAD_MSG_EVERY`: one `INSERT` and an `UPDATE` of `last_message_at` on all `N` members, in one transaction. `K` members per room publish typing to the channel every `LOAD_TYPING_EVERY`. Every stream renews its token every `LOAD_TOKEN_EVERY` (`POST /token`, which asks the issuer about every shape again, in one call, and the hook about the channel). | Per phase (arrival, steady, run): events/s and its 1 s peak, latency p50/p95/p99 for **messages**, **member updates** and **typing** separately, Sluice CPU per 1000 events and per stream open, RSS and Go memory against `GOMEMLIMIT`, GC pauses, slot lag, issuer calls/s and their peak, runner CPU and RSS, every container's CPU and memory. Then a verdict. |
| `bigtxn` | `rooms`, plus commits of `LOAD_BIGTXN_ROWS` notification rows (default 1 500, 10 000 and 100 000, each `LOAD_BIGTXN_REPEAT` times), of which a fraction `LOAD_BIGTXN_STREAMED` goes to members with a stream open (capped at the streams there are). | Per commit: time for the walsender to send it, for Sluice to dispatch it and for the last notification to arrive, the largest backlog, Sluice's CPU, and the other streams' latency in the 30 s before and while it is processed. |
| `reconnect` | `rooms` through a TCP proxy, plus waves that drop every stream at once. `LOAD_RECONNECT_MODE=restart` restarts the Sluice container (streams end with `server_shutdown` and come back over `SLUICE_RECONNECT_SPREAD`; the new process replays the slot from its `confirmed_flush_lsn`, so a table whose position is newer than that can still come back `resumed: true`). `cut` drops every connection at the proxy, as a reverse proxy does on reload (`LOAD_PROXY_DOWN=0`) or restart (`LOAD_PROXY_DOWN=2s`): no `server_shutdown`, and Sluice keeps its resume buffer. | Per wave: how long until 50/90/99/100% of the streams are back, each stream's downtime, open attempts, the issuer's busiest second, `issuer_unavailable` refusals, how many came back with every shape `resumed: true`, and message-sequence gaps after a resumed reconnect (which would be lost changes). |
| `calibrate` | A fixed load: 40 rooms × 100 members, 25 streams each (1 000), 7 shapes, a message every 5 s, 2 typists per room. Only `LOAD_CALIBRATE_WINDOWS` (3) and `LOAD_CALIBRATE_WINDOW` (60 s) change. | **ms of Sluice CPU per 1000 events** (median of the windows) and per stream open. Run it on two machines to get the factor between them. |
| `ramp` | The rooms of `LOAD_ROOMS`/`LOAD_ROOM_MIX` join in steps of about `LOAD_RAMP_STEP` streams, each with its writer and typists, so streams and traffic grow together (with `LOAD_MSG_EVERY=0` and typing off, a ramp of idle streams). Each step settles for `LOAD_RAMP_SETTLE` and is judged on a window of `LOAD_RAMP_HOLD`; the ramp stops at the first step that fails, plus `LOAD_RAMP_AFTER_FAIL` more. | Per step: streams, changes/s, events/s, Sluice's cores, Go memory against `GOMEMLIMIT`, RSS, live heap per stream, CPU per stream open, GC, latency, why it failed, and what was near its limit (Sluice's CPU quota, `GOMEMLIMIT`, the runner's cores, the issuer's queue). The largest passing step is the ceiling. |

**Resume-buffer probe.** Every `LOAD_PROBE_EVERY` (2 min; 0 turns it off), the runner opens a short stream per age in `LOAD_PROBE_AGES` (0.5 s to 90 s) with the identity of a live member, asking to resume each table from the position the run saw that long ago, and reads Sluice's `resumed` answer: how far back each table can be resumed right now. The report also derives it from each table's change rate over the last minute (`SLUICE_RING_EVENTS` changes, at most `SLUICE_RING_MAX_AGE`).

**Profiles.** With `LOAD_PPROF_AT`, the runner takes a CPU profile of `LOAD_PPROF_SECONDS` that far into the steady phase, then heap and goroutine snapshots, from a Sluice built with the `pprof` tag (`scripts/build-pprof.sh` builds `sluice:pprof`; run with `SLUICE_IMAGE=sluice:pprof SLUICE_PULL_POLICY=never SLUICE_PPROF_ADDR=0.0.0.0:6060`). They are saved next to the report: `go tool pprof -top results/<run>.cpu.pprof`.

How a stream behaves, in all four: it is driven the way [`packages/sluice-js`](../../packages/sluice-js/src/client.ts) drives one. One `POST /stream` carries every subscription and a resume map (the last `commit_lsn` seen per table, or the `wal_lsn` of the `ready` that installed it). When it ends, the client waits `retry_after_ms` if the server sent one, or the `Retry-After` of a refused open (`429`, `503`) plus up to a tenth, otherwise a random delay up to the backoff step (`LOAD_BACKOFF`, the SDK's `500ms,1s,2s,5s,10s`), and the step resets once a stream opens. A subscription refused with a retryable error (`issuer_unavailable`) is asked for again with `POST /subscribe` after its `retry_after_ms`.

The shapes of a stream, for `S = LOAD_SHAPES`: the room's messages (`room_id=eq.R`), the member's own memberships (`user_id=eq.U`, which receives their `last_message_at` updates), their notifications (`INSERT` only), and `S − 3` filler shapes. A filler costs what a shape costs (an issuer call, a hold, a registry entry, a refresh on `/token`) but asks for `DELETE` on the member's own row, which never happens, so it carries no traffic. Most of the `N` updates of a message reach no stream; Sluice decodes, routes and buffers them all the same.

The **issuer** stands in for an application's backend: it answers after `LOAD_ISSUER_LATENCY` per call (`10ms` is 10 ms ± `LOAD_ISSUER_JITTER`, `5ms-20ms` is uniform in the range) plus `LOAD_ISSUER_SHAPE_LATENCY` (same forms; default `0`) for each shape the call asks about, serves at most `LOAD_ISSUER_CONCURRENCY` calls at once and queues the rest (`LOAD_ISSUER_QUEUE_MAX` bounds the queue; past it, `503` with `Retry-After: 1`). The hook answers `ttl: LOAD_HOOK_TTL` (0: not cached, so Sluice asks again about every joined stream on every catalog tick). Membership holds name the user first (`LOAD_HOLD_ORDER`), the order the Sluice README advises. It counts its calls per second by kind (`subscribe`, `refresh`, `hook`) at `GET /stats`.

**Pass criteria** (the verdict, on the steady phase unless noted): no stream closed with `stream_lagging` during the run; p95 below `LOAD_P95_MAX` (500 ms) for every kind of delivered event; Go's memory (`sys − released`, what `GOMEMLIMIT` bounds) below `GOMEMLIMIT` and no Sluice restart nobody asked for; the backlog — WAL Sluice has received but not dispatched, `pg_current_wal_lsn()` minus its live confirmed position — below `LOAD_SLOT_LAG_MAX_MB` (64) and not growing (the last quarter's mean under twice the first's plus 4 MiB); at least `LOAD_DELIVERY_MIN` (99%) of the expected events delivered; no failed stream opens.

The runner of these profiles mounts the Docker socket (`DOCKER_SOCKET`, default `/var/run/docker.sock`). It reads every container's CPU and memory through it, and `reconnect` in `restart` mode restarts Sluice through it. The socket is root on the host: run this on a machine you control.

## Run

Docker Compose v2. No local Go toolchain required.

```bash
cd apps/loadtest
cp .env.example .env          # optional; compose has the same defaults

docker compose --profile rooms up --build --abort-on-container-exit --exit-code-from runner-rooms
```

Every profile has the same shape: `--profile <p>` and `--exit-code-from runner-<p>`, except `reconnect` with `LOAD_RECONNECT_MODE=restart`: Compose's `--abort-on-container-exit` ends the whole run when Sluice restarts, even under a restart policy. `scripts/run.sh` starts the stack detached instead, follows the runner, waits for its exit code (`docker compose wait`), saves Sluice's log and takes the stack down; it runs in Git Bash on Windows and in any Linux shell:

```bash
scripts/run.sh [-e env/pin-20cpu.env] [-l] [-f] <profile> [VAR=value ...]
#   -e  load a file of variables first (a core layout, a load)
#   -l  build the working tree as sluice:local and test that
#   -f  start from an empty database volume (needed after a schema change)

scripts/run.sh -e env/pin-20cpu.env rooms LOAD_LABEL=alto LOAD_ROOMS=20 LOAD_DURATION=30m
```

Run **one profile at a time**. Each starts PostgreSQL, a JWKS server, the matching Sluice, and one runner; the issuer family also starts the issuer, and `reconnect` the proxy. Compose inlines every default; an optional `.env` next to `compose.yml` overrides them. `scripts/run.sh` also drops the load test's replication slots before starting Sluice, so a run never inherits WAL a previous one left undispatched.

A series of runs is a plan, one run per line (`<label> <profile> [-e FILE] [VAR=value ...]`); `scripts/queue.sh` runs it in order into a results directory, skipping labels that already have a report, so an interrupted plan resumes where it stopped. [`plans/`](plans/) holds the ones the published results came from:

```bash
scripts/queue.sh plans/phase2-tiers.plan results/phase2/raw
docker compose run --rm --no-deps runner-rooms summarize /results/phase2/raw   # results/ is /results
```

`summarize` (also `go run ./cmd/loadtest summarize DIR...`) prints Markdown tables of the production-shaped reports in a directory, with the median of each label's repetitions (`-r1`, `-r2`, …): the steady phase, resume-buffer coverage, reconnect waves, big transactions and ramps.

Tear down, including the database volume (needed after schema changes): `docker compose --profile <p> down -v` (or `make nuke` where `make` exists).

### Limits and core pinning

| Variable | Default | |
| --- | --- | --- |
| `SLUICE_CPUS` | `1` | CPU quota of the Sluice container |
| `SLUICE_MEM_LIMIT` | `512m` | its memory limit; Sluice sets `GOMEMLIMIT` to 90% of it unless `SLUICE_GOMEMLIMIT` is set |
| `SLUICE_MAX_STREAMS` | `5000` | Sluice's stream cap, in every profile |
| `SLUICE_CPUSET`, `DB_CPUSET`, `ISSUER_CPUSET`, `PROXY_CPUSET`, `RUNNER_CPUSET` | unset | the cores each container may run on (`ISSUER_CPUSET` also holds the JWKS server) |
| `RUNNER_MEM_LIMIT`, `DB_MEM_LIMIT` | `6g`, `2500m` | |
| `POSTGRES_IMAGE` | `postgres:18.6-trixie` | |
| `POSTGRES_SYNCHRONOUS_COMMIT` | `on` | PostgreSQL's default. With `off` the walsender only sees a commit once the WAL writer flushes it (every `wal_writer_delay`, 200 ms), which adds 0–400 ms to every change's latency: the capacity results below were measured that way. |

Without pinning, the runner, PostgreSQL and Sluice compete for the same cores and a ceiling can be the runner's. [`env/pin-20cpu.env`](env/pin-20cpu.env) is a layout for a 20-vCPU Docker VM and [`env/pin-4cpu.env`](env/pin-4cpu.env) one for a 4-vCPU VPS. Give Sluice **two** cores with a quota of one: Go sets `GOMAXPROCS` to the quota rounded up, with a minimum of 2, which is what a 1-CPU container on a larger VPS runs with; a single pinned core would run it with one. The report records `GOMAXPROCS` and `GOMEMLIMIT` as Sluice reported them. Under Docker Desktop the VM's vCPUs are not tied to physical cores, so pinning separates the containers from each other but not from the host's scheduling; on a laptop, plug it in and use the maximum-performance power mode.

## Knobs

Capacity profiles:

| Env | Default | Meaning |
| --- | --- | --- |
| `LOAD_AXIS` | `all` | `users`, `conns`, or `all`. Presence and kick default to `users` only. |
| `LOAD_HUNT` | auto (on for A and issuer) | `true` / `false` |
| `LOAD_MAX_STREAMS` | cgroup-derived, ≤40000 | hunt ceiling; overrides the 40 000 cap |
| `LOAD_START` | `100` | first probe |
| `LOAD_CHANGES` | `20` | inserts or publishes per user after streams are ready |
| `LOAD_LADDER` | see profile | used when hunt is off |
| `LOAD_WAVE` | `256` | concurrent stream opens |
| `LOAD_OPEN_MIN` | `0.95` | pass bar for opens |
| `LOAD_DELIVERY_MIN` | `0.90` | pass bar for events / presence / kicks (`0.99` in the production-shaped profiles) |
| `LOAD_CHANNEL` | `room:load` | signalling channel (`room` must be in `SLUICE_CHANNELS`) |
| `LOAD_PRESENCE_WAIT` | `8s` | how long to wait for coalesced presence diffs |
| `LOAD_IDLE_HOLD` | `60s` | how long `idle` holds its streams open |
| `LOAD_IDLE_SHAPES` | `5` | shapes per `idle` stream, besides its channel |
| `LOAD_SAMPLE_EVERY` | `1s` (`5s` in the production-shaped profiles) | resource sampling interval |
| `SLUICE_IMAGE` | `ghcr.io/pauserratgutierrez/sluice:0.8.0` | the target |
| `SLUICE_PULL_POLICY` | `missing` | `never` for a locally built image |

Production-shaped profiles:

| Env | Default | Meaning |
| --- | --- | --- |
| `LOAD_ROOMS`, `LOAD_ROOM_MEMBERS`, `LOAD_ROOM_STREAMS`, `LOAD_TYPING` | `10`, `80`, `20`, `1` | `R`, `N`, `M`, `K`: rooms, members per room, members with a stream, typists per room (a fraction is an average: `0.5` is one typist in every other room) |
| `LOAD_ROOM_MIX` | unset | several kinds of room at once, `count:members:streams[:typing],…`, e.g. `40:80:20:0.5,4:250:60:1` |
| `LOAD_STREAMS` | unset | sizes `LOAD_ROOMS` for this many streams when `LOAD_ROOMS` is not set |
| `LOAD_SHAPES` | `7` | `S`, shapes per stream (besides the channel) |
| `LOAD_MSG_EVERY` | `15s` | a message per room this often; `0` sends none |
| `LOAD_TYPING_EVERY`, `LOAD_TYPING_ENABLED` | `2s`, `true` | |
| `LOAD_DURATION` | `10m` | the steady phase, after arrival (`bigtxn` and `reconnect` size it to their occurrences when unset) |
| `LOAD_ARRIVAL` | `0` | spread the stream opens at random over this long; `0` opens them all, `LOAD_WAVE` at a time |
| `LOAD_ARRIVAL_TRAFFIC` | `true` | write messages and typing while streams arrive |
| `LOAD_OPEN_TIMEOUT` | `3m` | how long after the arrival window to wait for the last opens |
| `LOAD_TOKEN_EVERY`, `LOAD_TOKEN_TTL` | `15m`, `1h` | `POST /token` per stream, from a random first offset; lifetime of each token |
| `LOAD_CHURN` | `0` | share of the open streams ended and reopened every minute, chosen at random; each reopens as after any disconnect, with its resume positions |
| `LOAD_REPLICA_IDENTITY` | `full` | `room_members`' replica identity: `full` or `pk` |
| `LOAD_BACKOFF` | `500ms,1s,2s,5s,10s` | the reconnect schedule (the SDK's default) |
| `LOAD_SSE_BUFFER` | `16` | (capacity profiles) events a stream buffers in the runner |
| `LOAD_STATS_EVERY` | `1m` | latency window of the timeline |
| `LOAD_DRAIN` | `5s` | wait for in-flight events after the writers stop |
| `LOAD_P95_MAX`, `LOAD_SLOT_LAG_MAX_MB` | `500ms`, `64` | pass criteria |
| `LOAD_SEED` | random | the rooms' users; the report records it |
| `LOAD_LABEL` | unset | names the report |
| `LOAD_ISSUER_LATENCY`, `LOAD_ISSUER_JITTER` | `5ms-20ms`, `0.2` | the issuer's (and hook's) answer time |
| `LOAD_ISSUER_CONCURRENCY`, `LOAD_ISSUER_QUEUE_MAX` | `32`, `0` | calls served at once (0: no limit); queued calls before `503` (0: no bound) |
| `LOAD_HOOK_TTL` | `0` | the hook's `ttl` |
| `LOAD_HOLD_ORDER` | `user-first` | how membership holds are written: `user_id=eq.U,room_id=eq.R` (`user-first`) or `room_id=eq.R,user_id=eq.U` (`room-first`). From 0.8.1 Sluice indexes a hold by the first of its equally good columns, so `room-first` puts every member of a room in one list; 0.8.0 picks either column at random. Runs before this knob existed were `room-first` |
| `LOAD_BIGTXN_ROWS` | `1500,10000,100000` | rows per commit |
| `LOAD_BIGTXN_STREAMED` | `0.25` | fraction of the rows whose recipient has a stream |
| `LOAD_BIGTXN_AFTER`, `LOAD_BIGTXN_EVERY`, `LOAD_BIGTXN_REPEAT` | `1m`, `2m`, `3` | first commit, spacing, commits per size |
| `LOAD_BIGTXN_BASELINE`, `LOAD_BIGTXN_TIMEOUT` | `30s`, `5m` | latency window before each commit; longest wait for one |
| `LOAD_RECONNECT_MODE` | `cut` | `restart` or `cut` |
| `LOAD_RECONNECT_AFTER`, `LOAD_RECONNECT_EVERY`, `LOAD_RECONNECT_COUNT` | `1m`, `2m`, `3` | first wave, spacing, waves |
| `LOAD_RECONNECT_TIMEOUT` | `3m` | longest wait for a wave to come back |
| `LOAD_PROXY_DOWN` | `0` | in `cut` mode, how long the proxy refuses connections after dropping them |
| `LOAD_CALIBRATE_WINDOWS`, `LOAD_CALIBRATE_WINDOW` | `3`, `1m` | |
| `LOAD_RAMP_STEP`, `LOAD_RAMP_ARRIVAL`, `LOAD_RAMP_SETTLE`, `LOAD_RAMP_HOLD`, `LOAD_RAMP_AFTER_FAIL` | `1000`, `20s`, `15s`, `90s`, `0` | streams per step, spread of their opens, wait before judging, judged window, steps after the first failure |
| `LOAD_PROBE_EVERY`, `LOAD_PROBE_AGES` | `2m` (`0` in `ramp` and `calibrate`), `500ms` … `90s` | the resume-buffer probe |
| `LOAD_PPROF_AT`, `LOAD_PPROF_SECONDS`, `LOAD_PPROF_URL` | unset, `60s`, `http://sluice-rooms:6060/debug/pprof` | profiles of a `pprof` build |
| `LOAD_PPROF_HEAP_AT` | `2m,9m` | with `LOAD_PPROF_AT`, also heap profiles at these times into the steady phase |
| `LOAD_ISSUER_NAME` | unset | names the issuer's configuration in the report |
| `SLUICE_GOMAXPROCS`, `SLUICE_PPROF_ADDR` | unset | Sluice's `GOMAXPROCS` (else Go's default for the quota) and pprof address (`pprof` builds) |
| `SLUICE_PPROF_BLOCK_RATE`, `SLUICE_PPROF_MUTEX_FRACTION` | unset (`100000`, `100`) | block and mutex profile sampling of a `pprof` build; `0` turns either off, for runs that only need heap profiles |
| `RESULTS_DIR` | `./results` | where reports go (`scripts/queue.sh` sets it) |
| `HOST_POWER_PLAN` | detected | recorded in each report; `scripts/run.sh` reads the Windows power plan or the Linux CPU governor |
| `SLUICE_RECONNECT_SPREAD`, `SLUICE_ISSUER_TIMEOUT`, `SLUICE_CATALOG_REFRESH`, `SLUICE_HEARTBEAT`, `SLUICE_DB_POOL_MAX_CONNS`, `SLUICE_RING_*`, `SLUICE_STREAM_QUEUE`, `SLUICE_LOG_LEVEL`, `SLUICE_GOMEMLIMIT`, `SLUICE_GOGC` | Sluice's defaults | passed to `sluice-rooms` |

Pass them in `.env`, on the compose command, or after the profile in `scripts/run.sh`:

```bash
LOAD_AXIS=users LOAD_MAX_STREAMS=2000 docker compose --profile rls-a up --build --abort-on-container-exit --exit-code-from runner-rls-a
```

## Results

Results are written to `results/` (`*.json` and `*.md`): the capacity profiles' steps, and for the production-shaped ones a summary, the verdict, and the whole timeline (one point every `LOAD_SAMPLE_EVERY`, one latency window every `LOAD_STATS_EVERY`, delivered events and issuer calls per second). Runs straight under `results/` stay out of git, and `scripts/run.sh` also keeps Sluice's log in `results/logs/`, which stays out too. A phase's runs that are kept (`results/phaseN/`) go in with their Markdown reports in clear and their JSON reports gzipped (`gzip -9n`, `*.json.gz`; `summarize` and `scripts/queue.sh` read them as they are). Profiles (`*.pprof`) stay out of git.

### Resources: 0.3.0 against 0.4.0 (this machine, 2026-10-03)

Same machine and runner, one profile at a time, fresh database each run, no container limits on Sluice. Memory per stream is measured from the fresh process (see [Capacity profiles](#capacity-profiles)).

| Run | 0.3.0 | 0.4.0 |
| --- | ---: | ---: |
| `idle`, 1 shape + channel: memory per stream | 52 KiB | 37–41 KiB |
| `idle`, 20 000 streams: peak RSS | 980 MiB | 783 MiB |
| `idle`, 20 000 streams: CPU over the step | 14.1 s | 11.5 s |
| `idle`, longest GC pause | 26 ms | 2.6 ms |
| `idle`, 5 shapes + channel, 512 MB container: memory per stream | 66 KiB | 48 KiB |
| … streams that fit without swap or GC pressure | about 6 000 | about 8 000 |
| … opening 8 000 streams | 3.1 s (swapping) | 0.74 s |
| `issuer`, 8 000 users: opening every stream | 1.12 s | 0.87 s |
| `issuer`, 8 000 users: CPU per 1000 events | 150 ms | 135 ms |
| `rls-a` fan-out to 16 000 connections, mean of 6: events/s | 152k | 157k |
| … CPU per 1000 events | 83 ms | 77 ms |

Fan-out throughput at 16 000 connections varies by ±20% between runs on this VM; the two are equal within that noise. Past about 10 000 streams in 512 MB 0.4.0 reaches its `GOMEMLIMIT` and spends CPU on collection instead of being OOM-killed, so a stream cap below that is the brake to set.

### Results (0.1.4, this machine)

Measured 2026-09-12 against `ghcr.io/pauserratgutierrez/sluice:0.1.4` in Docker Desktop on a Windows laptop (20 CPUs visible to the VM, ~6 GiB cgroup), without limits on Sluice. Bursts of 20 `INSERT`s, not a multi-minute soak. Delivery of **opened** streams was 100% on every step below; none were closed as `stream_lagging`.

The numbers are **Sluice on this host**, not a product SLA. The hunt bar is ≥95% of streams *open*, so it can report a higher N than the last 100%-open point.

#### Fan-out (1 user, N connections — one change, N subscribers)

This is the design claim. Wall time stays flat until the machine is busy shipping SSE frames:

| Subscribers | Wall (20 changes) | Events/s | p95 | Opened |
| ---: | ---: | ---: | ---: | --- |
| 100 | 406 ms | 4.9k | 269 ms | 100% |
| 800 | 407 ms | 39k | 294 ms | 100% |
| 1 600 | 408 ms | 78k | 284 ms | 100% |
| 3 200 | 606 ms | 106k | 579 ms | 100% |
| 12 000 | 2.0 s | 120k | 762 ms | 100% |
| 16 000 | 2.6 s | 122k | 895 ms | 100% |
| 28 000 | 5.0 s | 112k | 1.0 s | 100% |

100–1 600: the wall time stays flat (~410 ms) and events/s scales with N. Past ~3 000 the cost is writing and flushing N sockets, not authorizing.

#### Concurrent users (1 connection each — routed, not fan-out)

| Profile | 100% open | Hunt (≥95% open) | Notes |
| --- | ---: | ---: | --- |
| RLS A | 28 000 | 29 700 | Opens stuck at **28 231** — Linux default ephemeral ports (`32768–60999`), a limit of the **runner**, not of Sluice. Those 28 231 still delivered 100%. |
| RLS B | 1 500 (ladder top) | — | Not a max hunt. Fan-out 1 500: 74k events/s, p95 256 ms, wall 405 ms. |
| RLS C | 200 (ladder top) | — | p95 1.75 s, ~1.9k events/s. Impersonated probes; this is the slow path on purpose. |
| Issuer | 16 000 users | 16 350 | Failures above that are **join denies** (issuer HTTP + hold `EXISTS`), not lost events. Fan-out 16 000 conns: 100% delivery, ~114k events/s. Hunt on conns: 17 100. |

#### Signalling (same machine, same image, 2026-09-13)

Ladders, not hunts. 100% open and 100% delivery at every step.

| Profile | Top of ladder | What 100% means | Wall / p95 at the top |
| --- | --- | --- | --- |
| Broadcast | 1 500 users and 1 500 conns | 20 client publishes **and** 20 `pg_logical_emit_message` each reach every stream (60k + 60k events at 1 500) | ~0.7 s / p95 286–346 ms; ~81–83k events/s combined |
| Presence | 800 users (1 conn) | Every roster converges after `track()`, then half disconnect and the rest see the leaves | ~1.5 s (one coalescing tick). Events/s here is members / wait, not a flood. |
| Mixed | 1 500 users and 1 500 conns | 20 `INSERT`s **and** 20 publishes on the **same** SSE stream, no `stream_lagging` | Fan-out 1 500: 81k events/s, p95 326 ms |
| Kick | 200 issuer users | One change, then `DELETE` the hold → `shape_not_authorized` on every stream | ~0.8 s / p95 378 ms |

Broadcast from the database matches the client path at these sizes: no outbox table, same fan-out. Presence is deliberately not a throughput race — diffs are coalesced (~1.5 s), which is why 800 members settle in one tick rather than 800×800 messages.

## Stack

```
runner  ──POST /stream──►  [proxy]  ──►  sluice  ──logical slot──►  postgres 18.6
   │                                       ▲  │
   │                                       │  └── POST /shapes, /hook ──►  issuer
   │                                       └── GET JWKS ──  jwks (public ES256)
   └── INSERT / UPDATE / emit_message ────────────────────────────────────┘
   └── POST /publish, /presence, /token, /subscribe ──► sluice
   └── /var/run/docker.sock (production-shaped profiles): container stats, restart
```

There is no GoTrue and no PostgREST. The JWKS service mints an ES256 key pair on first start and serves the public set; the runner signs JWTs from the private key on the same volume. The proxy only runs in the `reconnect` profile.

The capacity profiles' schema ([`sql/01-schema.sql`](sql/01-schema.sql)) is a small SaaS, not a copy of `deploy/db/fixtures.sql`:

- `notes` — `owner_id = auth.uid()`, shape pins `owner_id` → **tier A** (also `mixed`)
- `posts` — `visibility = public OR owner_id = auth.uid()` → **tier B**
- `invoices` + `team_members` — `EXISTS` subquery → **tier C**
- `project_docs` + `project_members` — issuer grant + hold, RLS off (`issuer`, `kick`)
- channel `room:load` — public namespace `room` (`broadcast`, `presence`, `mixed`)

## Layout

```
compose.yml          profiles + shared YAML anchors
Dockerfile           one binary: jwks | issuer | proxy | run | summarize
env/                 core layouts (cpusets) for a 20-vCPU and a 4-vCPU machine
plans/               the series of runs behind the published results (scripts/queue.sh)
scripts/run.sh       run one profile, keep Sluice's log, take the stack down
scripts/queue.sh     run a plan in order, resumable
scripts/build-pprof.sh  build sluice:pprof from the working tree
sql/                 Postgres init: roles, publications, both schemas
cmd/loadtest         subcommand entrypoint
internal/runner      capacity hunts; rooms, bigtxn, reconnect, calibrate, ramp; reports; summarize
internal/issuer      the shape issuer and channel hook (latency, concurrency, stats)
internal/proxy       the TCP proxy that drops connections on command
internal/dockerapi   container stats and restart over the Docker socket
internal/sse         SSE parsing
internal/authn       ES256 minting
results/             run reports (local)
```

`go test ./...` covers the SSE parser, JWT mint, Prometheus scrape, presence diffs, the hunt search, the latency histogram, the room configuration and the issuer's rules. It does not start Sluice; the compose profiles are the real tests.
