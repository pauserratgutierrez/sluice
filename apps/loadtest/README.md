# Load-test app

An independent Docker app that load-tests a **published** Sluice image. It is not the `deploy/` harness and it does not import Sluice's Go module. It speaks the public protocol the way a product would: mint JWTs, `POST /stream`, insert rows, count SSE events, and (for signalling) `POST /publish`, `POST /presence`, and `pg_logical_emit_message`.

Pinned target: [`ghcr.io/pauserratgutierrez/sluice:0.1.4`](https://github.com/pauserratgutierrez/sluice/pkgs/container/sluice) (label `org.opencontainers.image.version=0.1.4`, healthcheck `/sluice -healthcheck`).

## What it measures

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

A step **passes** when ≥95% of streams open, ≥90% of expected events (or presence members / kicks) arrive, and Sluice is not mass-closing streams as `stream_lagging`. The runner sizes the hunt ceiling from the container cgroup (CPU / memory) and caps at 40 000 streams so a laptop Docker VM is not frozen. Memory limits on the compose services are the other guardrail.

Results are written to `results/` (`*.json` and `*.md`). That directory is gitignored except for `.gitkeep`.

## Results (0.1.4, this machine)

Measured 2026-09-12 against `ghcr.io/pauserratgutierrez/sluice:0.1.4` in Docker Desktop on a Windows laptop (20 CPUs visible to the VM, ~6 GiB cgroup). Bursts of 20 `INSERT`s, not a multi-minute soak. Delivery of **opened** streams was 100% on every step below; none were closed as `stream_lagging`.

The numbers are **Sluice on this host**, not a product SLA. The hunt bar is ≥95% of streams *open*, so it can report a higher N than the last 100%-open point.

### Fan-out (1 user, N connections — one change, N subscribers)

This is the design claim. Wall time stays flat until the machine is busy shipping SSE frames:

| Subscribers | Wall (20 changes) | Events/s | p95 | Opened |
| ---: | ---: | ---: | ---: | --- |
| 100 | 406 ms | 4.9k | 269 ms | 100% |
| 800 | 407 ms | 39k | 294 ms | 100% |
| 1 600 | 408 ms | 78k | 284 ms | 100% |
| 3 200 | 606 ms | 106k | 579 ms | 100% |
| 12 000 | 2.0 s | 120k | 762 ms | 100% |
| 16 000 | 2.6 s | 122k | 895 ms | 100% |
| 28 000 | 5.0 s | 112k | 1.0 s | 100% |

100–1 600 is the same shape as the small harness table in the main README (~410 ms, events/s scaling with N). Past ~3 000 the cost is writing and flushing N sockets, not authorizing.

### Concurrent users (1 connection each — routed, not fan-out)

| Profile | 100% open | Hunt (≥95% open) | Notes |
| --- | ---: | ---: | --- |
| RLS A | 28 000 | 29 700 | Opens stuck at **28 231** — Linux default ephemeral ports (`32768–60999`), a limit of the **runner**, not of Sluice. Those 28 231 still delivered 100%. |
| RLS B | 1 500 (ladder top) | — | Not a max hunt. Fan-out 1 500: 74k events/s, p95 256 ms, wall 405 ms. |
| RLS C | 200 (ladder top) | — | p95 1.75 s, ~1.9k events/s. Impersonated probes; this is the slow path on purpose. |
| Issuer | 16 000 users | 16 350 | Failures above that are **join denies** (issuer HTTP + hold `EXISTS`), not lost events. Fan-out 16 000 conns: 100% delivery, ~114k events/s. Hunt on conns: 17 100. |

### Signalling (same machine, same image, 2026-09-13)

Ladders, not hunts. 100% open and 100% delivery at every step.

| Profile | Top of ladder | What 100% means | Wall / p95 at the top |
| --- | --- | --- | --- |
| Broadcast | 1 500 users and 1 500 conns | 20 client publishes **and** 20 `pg_logical_emit_message` each reach every stream (60k + 60k events at 1 500) | ~0.7 s / p95 286–346 ms; ~81–83k events/s combined |
| Presence | 800 users (1 conn) | Every roster converges after `track()`, then half disconnect and the rest see the leaves | ~1.5 s (one coalescing tick). Events/s here is members / wait, not a flood. |
| Mixed | 1 500 users and 1 500 conns | 20 `INSERT`s **and** 20 publishes on the **same** SSE stream, no `stream_lagging` | Fan-out 1 500: 81k events/s, p95 326 ms |
| Kick | 200 issuer users | One change, then `DELETE` the hold → `shape_not_authorized` on every stream | ~0.8 s / p95 378 ms |

Broadcast from the database matches the client path at these sizes: no outbox table, same fan-out. Presence is deliberately not a throughput race — diffs are coalesced (~1.5 s), which is why 800 members settle in one tick rather than 800×800 messages.

## Run

Docker Compose v2. No local Go toolchain required.

```bash
cd apps/loadtest
cp .env.example .env          # optional; compose has the same defaults

docker compose --profile rls-a     up --build --abort-on-container-exit --exit-code-from runner-rls-a
docker compose --profile rls-b     up --build --abort-on-container-exit --exit-code-from runner-rls-b
docker compose --profile rls-c     up --build --abort-on-container-exit --exit-code-from runner-rls-c
docker compose --profile issuer    up --build --abort-on-container-exit --exit-code-from runner-issuer
docker compose --profile broadcast up --build --abort-on-container-exit --exit-code-from runner-broadcast
docker compose --profile presence  up --build --abort-on-container-exit --exit-code-from runner-presence
docker compose --profile mixed     up --build --abort-on-container-exit --exit-code-from runner-mixed
docker compose --profile kick      up --build --abort-on-container-exit --exit-code-from runner-kick
```

Or `make rls-a` / `make broadcast` / … 

Run **one profile at a time**. Each profile starts Postgres, a JWKS server, the matching Sluice process, and one runner. The issuer and kick profiles also start the shape-issuer.

Compose inlines every default. An optional `.env` next to `compose.yml` overrides them. There is no `runner.env` / `sluice.env` for this app.

Tear down, including the database volume (needed after schema changes):

```bash
make nuke
```

## Stack

```
runner  ──POST /stream──►  sluice:0.1.4  ──logical slot──►  postgres 18.4
   │                         ▲
   │                         └── GET JWKS ──  jwks (public ES256)
   └── INSERT / emit_message ────────────────┘
   └── POST /publish, /presence ─────────────► sluice
   └── (issuer, kick) POST /shapes ──  issuer (in-memory membership)
```

There is no GoTrue and no PostgREST. The JWKS service mints an ES256 key pair on first start and serves the public set; the runner signs JWTs from the private key on the same volume.

The schema is a small SaaS, not a copy of `deploy/db/fixtures.sql`:

- `notes` — `owner_id = auth.uid()`, shape pins `owner_id` → **tier A** (also `mixed`)
- `posts` — `visibility = public OR owner_id = auth.uid()` → **tier B**
- `invoices` + `team_members` — `EXISTS` subquery → **tier C**
- `project_docs` + `project_members` — issuer grant + hold, RLS off (`issuer`, `kick`)
- channel `room:load` — public namespace `room` (`broadcast`, `presence`, `mixed`)

## Knobs

| Env | Default | Meaning |
| --- | --- | --- |
| `LOAD_AXIS` | `all` | `users`, `conns`, or `all`. Presence and kick default to `users` only. |
| `LOAD_HUNT` | auto (on for A and issuer) | `true` / `false` |
| `LOAD_MAX_STREAMS` | cgroup-derived, ≤40000 | hunt ceiling |
| `LOAD_START` | `100` | first probe |
| `LOAD_CHANGES` | `20` | inserts or publishes per user after streams are ready |
| `LOAD_LADDER` | see profile | used when hunt is off |
| `LOAD_WAVE` | `256` | concurrent stream opens |
| `LOAD_OPEN_MIN` | `0.95` | pass bar for opens |
| `LOAD_DELIVERY_MIN` | `0.90` | pass bar for events / presence / kicks |
| `LOAD_CHANNEL` | `room:load` | signalling channel (`room` must be in `SLUICE_CHANNELS`) |
| `LOAD_PRESENCE_WAIT` | `8s` | how long to wait for coalesced presence diffs |
| `SLUICE_IMAGE` | `ghcr.io/pauserratgutierrez/sluice:0.1.4` | override the target |

Pass them in `.env` or on the compose command:

```bash
LOAD_AXIS=users LOAD_MAX_STREAMS=2000 docker compose --profile rls-a up --build --abort-on-container-exit --exit-code-from runner-rls-a
```

## Layout

```
compose.yml          profiles + shared YAML anchors (one Sluice definition, two processes)
Dockerfile           one binary: jwks | issuer | run
                     (jwks mints the ES256 pair, then serves the public JWKS)
sql/                 Postgres init: roles, publications, app schema
cmd/loadtest         subcommand entrypoint
internal/runner      client, hunt orchestration, change/signalling drivers, reports
internal/issuer      standalone shape issuer (not cmd/issuer-stub)
internal/authn       ES256 minting
results/             last run JSON/Markdown (local)
```

`go test ./...` covers the SSE parser, JWT mint, Prometheus scrape, presence diffs, and the hunt search. It does not start Sluice; the compose profiles are the real tests.
