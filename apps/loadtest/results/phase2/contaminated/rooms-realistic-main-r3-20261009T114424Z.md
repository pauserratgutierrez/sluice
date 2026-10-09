# Sluice load test — rooms — realistic-main-r3

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T11:13:49Z, finished 2026-10-09T11:44:24Z, seed 4692051883814337821
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 5 × 80 / 20 / 0.4 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 100 streams, 27 changes/s, 32 events/s (7 messages, 7 member updates, 19 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 29 | 14 (93) | 28 | 0.57 | 1353.9 | 703.7 | 0.02 / 0.02 | 29 | 19 / 461 | 0.11 | 0.00 | 0.16 | 0 | 29.8 (86) | 0.01 |
| steady | 1805 | 32 (118) | 27 | 15.99 | 274.7 | 329.0 | 0.01 / 0.02 | 41 | 31 / 461 | 9.07 | 0.00 | 0.12 | 0 | 4.4 (108) | 0.02 |
| run | 1834 | 32 (118) | 27 | 16.56 | 282.5 | 335.2 | 0.01 / 0.02 | 41 | 31 / 461 | 9.07 | 0.00 | 0.16 | 0 | 4.8 (108) | 0.02 |

Arrival: 29.4 s to open every stream (complete: true); 5.70 ms of Sluice CPU per open; 90.8 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 89 | 4.4 | 17.3 | 17.5 | 17.7 |
| arrival | message | 89 | 3.4 | 17.3 | 17.3 | 17.3 |
| arrival | open | 100 | 47.4 | 58.6 | 63.2 | 69.7 |
| arrival | publish | 19 | 0.9 | 1.2 | 1.2 | 1.3 |
| arrival | token | 3 | 130.6 | 130.6 | 130.6 | 134.1 |
| arrival | txn | 10 | 2.6 | 3.5 | 3.5 | 4.0 |
| arrival | typing | 243 | 0.9 | 1.3 | 1.4 | 1.4 |
| steady | member | 12000 | 6.6 | 14.9 | 21.9 | 27.5 |
| steady | message | 12000 | 3.8 | 14.4 | 19.8 | 27.5 |
| steady | publish | 1800 | 0.9 | 1.1 | 1.9 | 5.7 |
| steady | token | 201 | 109.1 | 130.6 | 140.3 | 146.2 |
| steady | txn | 600 | 2.7 | 4.3 | 16.5 | 21.6 |
| steady | typing | 34200 | 0.9 | 1.1 | 1.9 | 5.6 |
| run | member | 12089 | 6.6 | 15.0 | 21.9 | 27.5 |
| run | message | 12089 | 3.8 | 14.5 | 19.6 | 27.5 |
| run | open | 100 | 47.4 | 58.6 | 63.2 | 69.7 |
| run | publish | 1819 | 0.9 | 1.2 | 1.8 | 5.7 |
| run | token | 204 | 109.1 | 130.6 | 138.2 | 146.2 |
| run | txn | 610 | 2.7 | 4.3 | 16.1 | 21.6 |
| run | typing | 34443 | 0.9 | 1.1 | 1.9 | 5.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 12088 | 12089 | 100.01 |
| member | 12088 | 12089 | 100.01 |
| typing | 34443 | 34443 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 32 | 45.0 | 45.0 | 60.0 |
| public.room_messages | 0 | 45.0 | 45.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 800 |
| token_ok | 204 |
| warning:replica_identity_insufficient | 100 |
