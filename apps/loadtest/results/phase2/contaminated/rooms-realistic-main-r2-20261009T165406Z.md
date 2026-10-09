# Sluice load test — rooms — realistic-main-r2

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T16:23:32Z, finished 2026-10-09T16:54:06Z, seed 4254368877818743029
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 5 × 80 / 20 / 0.4 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 100 streams, 27 changes/s, 32 events/s (7 messages, 7 member updates, 19 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 14 (112) | 27 | 0.38 | 938.3 | 469.1 | 0.01 / 0.02 | 29 | 19 / 461 | 0.41 | 0.00 | 0.17 | 0 | 29.6 (94) | 0.01 |
| steady | 1805 | 32 (158) | 27 | 15.28 | 262.5 | 314.4 | 0.01 / 0.03 | 42 | 31 / 461 | 4.79 | 0.00 | 0.12 | 0 | 4.4 (116) | 0.01 |
| run | 1835 | 32 (158) | 27 | 15.66 | 267.2 | 316.9 | 0.01 / 0.03 | 42 | 31 / 461 | 4.79 | 0.00 | 0.17 | 0 | 4.8 (116) | 0.01 |

Arrival: 29.6 s to open every stream (complete: true); 3.80 ms of Sluice CPU per open; 75.8 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 85 | 6.1 | 10.0 | 10.1 | 10.1 |
| arrival | message | 85 | 5.0 | 10.0 | 10.1 | 10.1 |
| arrival | open | 100 | 46.3 | 56.1 | 58.6 | 80.3 |
| arrival | publish | 16 | 0.9 | 1.0 | 1.0 | 1.3 |
| arrival | token | 1 | 91.6 | 91.6 | 91.6 | 92.1 |
| arrival | txn | 10 | 2.5 | 3.5 | 3.5 | 4.3 |
| arrival | typing | 235 | 0.8 | 1.0 | 1.3 | 1.3 |
| steady | member | 12000 | 6.4 | 14.3 | 17.8 | 23.4 |
| steady | message | 12000 | 4.0 | 14.0 | 16.8 | 20.3 |
| steady | publish | 1800 | 0.9 | 1.1 | 1.6 | 8.4 |
| steady | token | 201 | 111.1 | 132.1 | 140.3 | 146.3 |
| steady | txn | 600 | 2.7 | 3.9 | 12.4 | 19.4 |
| steady | typing | 34200 | 0.9 | 1.1 | 1.5 | 8.5 |
| run | member | 12085 | 6.4 | 14.1 | 17.8 | 23.4 |
| run | message | 12085 | 4.0 | 14.0 | 16.8 | 20.3 |
| run | open | 100 | 46.3 | 56.1 | 58.6 | 80.3 |
| run | publish | 1816 | 0.9 | 1.1 | 1.5 | 8.4 |
| run | token | 202 | 111.1 | 132.1 | 140.3 | 146.3 |
| run | txn | 610 | 2.7 | 3.9 | 11.1 | 19.4 |
| run | typing | 34435 | 0.9 | 1.1 | 1.5 | 8.5 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 12085 | 12085 | 100.00 |
| member | 12085 | 12085 | 100.00 |
| typing | 34435 | 34435 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 27 | 45.0 | 45.0 | 60.0 |
| public.room_messages | 0 | 45.0 | 45.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 800 |
| token_ok | 202 |
| warning:replica_identity_insufficient | 100 |
