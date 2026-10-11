# Sluice load test — rooms — v081f-veryhigh-full

- Target: `sluice:0.8.1-rc2`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T22:16:41Z, finished 2026-10-09T22:27:16Z, seed 11727768109821421286
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 211 (626) | 336 | 3.65 | 575.4 | 361.0 | 0.12 / 0.14 | 143 | 133 / 461 | 0.13 | 0.00 | 1.09 | 0 | 442.1 (1132) | 0.06 |
| steady | 605 | 498 (1037) | 334 | 21.17 | 70.2 | 104.7 | 0.03 / 0.08 | 187 | 177 / 461 | 2.39 | 0.00 | 0.79 | 0 | 63.4 (852) | 0.03 |
| run | 635 | 485 (1037) | 334 | 24.82 | 80.6 | 116.9 | 0.04 / 0.14 | 187 | 177 / 461 | 2.39 | 0.00 | 1.09 | 0 | 81.4 (1132) | 0.06 |

Arrival: 30.1 s to open every stream (complete: true); 2.43 ms of Sluice CPU per open; 79.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1442 | 3.3 | 6.2 | 9.9 | 22.8 |
| arrival | message | 1442 | 3.3 | 6.2 | 9.9 | 22.8 |
| arrival | message_commit_to_client | 1442 | 2.8 | 5.8 | 6.9 | 22.1 |
| arrival | message_write_to_commit | 1442 | 0.4 | 1.0 | 1.6 | 3.6 |
| arrival | open | 1500 | 45.3 | 54.5 | 60.7 | 83.5 |
| arrival | publish | 181 | 0.5 | 1.0 | 1.5 | 1.6 |
| arrival | token | 16 | 103.9 | 124.4 | 124.4 | 129.6 |
| arrival | txn | 110 | 2.3 | 5.3 | 5.8 | 9.6 |
| arrival | typing | 3459 | 0.6 | 1.0 | 1.5 | 1.7 |
| steady | member | 60000 | 4.0 | 8.4 | 13.9 | 22.2 |
| steady | message | 60000 | 3.9 | 7.8 | 13.4 | 22.2 |
| steady | message_commit_to_client | 60000 | 3.5 | 7.3 | 13.0 | 21.9 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.6 | 0.8 | 1.8 |
| steady | publish | 6000 | 0.9 | 1.6 | 2.0 | 9.4 |
| steady | token | 1034 | 110.1 | 130.6 | 138.2 | 145.7 |
| steady | txn | 2200 | 2.5 | 5.4 | 11.3 | 21.0 |
| steady | typing | 181500 | 0.9 | 1.6 | 1.9 | 20.6 |
| run | member | 61442 | 4.0 | 8.3 | 13.9 | 22.8 |
| run | message | 61442 | 3.8 | 7.6 | 13.4 | 22.8 |
| run | message_commit_to_client | 61442 | 3.4 | 7.2 | 13.0 | 22.1 |
| run | message_write_to_commit | 61442 | 0.4 | 0.6 | 0.9 | 3.6 |
| run | open | 1500 | 45.3 | 54.5 | 60.7 | 83.5 |
| run | publish | 6181 | 0.9 | 1.6 | 2.0 | 9.4 |
| run | token | 1050 | 110.1 | 130.6 | 138.2 | 145.7 |
| run | txn | 2310 | 2.5 | 5.4 | 11.1 | 21.0 |
| run | typing | 184959 | 0.9 | 1.6 | 1.9 | 20.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61441 | 61442 | 100.00 |
| member | 61441 | 61442 | 100.00 |
| typing | 184958 | 184959 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 10.0 | 10.0 | 12.3 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1050 |
| warning:replica_identity_insufficient | 1500 |
