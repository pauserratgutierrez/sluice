# Sluice load test — rooms — high-main-r2

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T12:15:38Z, finished 2026-10-09T12:46:13Z, seed 16105021168769287874
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 43 (138) | 89 | 1.30 | 996.2 | 486.9 | 0.04 / 0.07 | 56 | 46 / 461 | 0.26 | 0.00 | 0.37 | 0 | 116.6 (455) | 0.19 |
| steady | 1805 | 125 (345) | 89 | 56.26 | 248.4 | 351.2 | 0.03 / 0.07 | 90 | 80 / 461 | 10.46 | 0.00 | 0.31 | 0 | 17.1 (416) | 0.01 |
| run | 1835 | 124 (345) | 89 | 57.56 | 252.6 | 353.4 | 0.03 / 0.07 | 90 | 80 / 461 | 10.46 | 0.00 | 0.37 | 0 | 18.7 (455) | 0.19 |

Arrival: 30.0 s to open every stream (complete: true); 3.25 ms of Sluice CPU per open; 74.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 364 | 6.2 | 33.5 | 37.1 | 38.0 |
| arrival | message | 364 | 5.5 | 11.7 | 11.8 | 12.1 |
| arrival | open | 400 | 44.8 | 55.6 | 60.7 | 67.3 |
| arrival | publish | 29 | 0.7 | 1.6 | 2.2 | 2.9 |
| arrival | token | 2 | 98.8 | 98.8 | 98.8 | 140.8 |
| arrival | txn | 30 | 2.9 | 4.6 | 10.3 | 10.7 |
| arrival | typing | 577 | 0.8 | 1.6 | 2.9 | 2.9 |
| steady | member | 48000 | 11.3 | 45.8 | 58.1 | 95.3 |
| steady | message | 48000 | 7.4 | 16.8 | 21.6 | 41.3 |
| steady | publish | 4501 | 0.9 | 1.6 | 1.7 | 12.1 |
| steady | token | 803 | 110.1 | 132.1 | 140.3 | 162.1 |
| steady | txn | 1800 | 2.9 | 6.9 | 17.0 | 24.9 |
| steady | typing | 130524 | 0.9 | 1.5 | 1.7 | 12.1 |
| run | member | 48364 | 11.3 | 45.8 | 58.1 | 95.3 |
| run | message | 48364 | 7.4 | 16.8 | 21.6 | 41.3 |
| run | open | 400 | 44.8 | 55.6 | 60.7 | 67.3 |
| run | publish | 4530 | 0.9 | 1.6 | 1.7 | 12.1 |
| run | token | 805 | 110.1 | 132.1 | 140.3 | 162.1 |
| run | txn | 1830 | 2.9 | 6.9 | 16.8 | 24.9 |
| run | typing | 131101 | 0.9 | 1.5 | 1.7 | 12.1 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48364 | 48364 | 100.00 |
| member | 48364 | 48364 | 100.00 |
| typing | 131101 | 131101 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 16 | 45.0 | 45.0 | 60.0 |
| public.room_messages | 0 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 805 |
| warning:replica_identity_insufficient | 400 |
