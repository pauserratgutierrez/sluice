# Sluice load test — rooms — smoke-final

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Started 2026-10-09T10:43:36Z, finished 2026-10-09T10:44:11Z, seed 1689786705944174395
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 4 × 20 / 5 / 1 | 7 | 5s | every 2s | full | 0s | 30s |

Expected with every stream open: 20 streams, 17 changes/s, 16 events/s (4 messages, 4 member updates, 8 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 0 | 0 (0) | 0 | 0.07 | 0.0 | 0.0 | 0.33 / 0.24 | 24 | 15 / 461 | 0.12 | 0.00 | 3.44 | 0 | 759.5 (160) | 0.03 |
| steady | 35 | 14 (36) | 14 | 0.16 | 333.3 | 317.5 | 0.00 / 0.01 | 28 | 18 / 461 | 2.55 | 0.00 | 3.44 | 0 | 0.8 (20) | 0.01 |
| run | 35 | 14 (36) | 14 | 0.23 | 479.2 | 456.3 | 0.01 / 0.24 | 28 | 18 / 461 | 2.55 | 0.00 | 3.44 | 0 | 0.8 (20) | 0.03 |

Arrival: 0.2 s to open every stream (complete: true); 3.50 ms of Sluice CPU per open; 334.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | open | 20 | 110.1 | 124.4 | 124.4 | 126.8 |
| steady | member | 120 | 3.3 | 6.2 | 6.4 | 6.5 |
| steady | message | 120 | 3.1 | 6.1 | 6.4 | 6.4 |
| steady | publish | 60 | 0.9 | 2.0 | 2.2 | 2.3 |
| steady | token | 1 | 105.0 | 105.0 | 105.0 | 105.3 |
| steady | txn | 24 | 2.4 | 4.5 | 5.1 | 5.7 |
| steady | typing | 240 | 0.9 | 1.9 | 2.2 | 2.4 |
| run | member | 120 | 3.3 | 6.2 | 6.4 | 6.5 |
| run | message | 120 | 3.1 | 6.1 | 6.4 | 6.4 |
| run | open | 20 | 110.1 | 124.4 | 124.4 | 126.8 |
| run | publish | 60 | 0.9 | 2.0 | 2.2 | 2.3 |
| run | token | 1 | 105.0 | 105.0 | 105.0 | 105.3 |
| run | txn | 24 | 2.4 | 4.5 | 5.1 | 5.7 |
| run | typing | 240 | 0.9 | 1.9 | 2.2 | 2.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 120 | 120 | 100.00 |
| member | 120 | 120 | 100.00 |
| typing | 240 | 240 | 100.00 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 160 |
| token_ok | 1 |
| warning:replica_identity_insufficient | 20 |
