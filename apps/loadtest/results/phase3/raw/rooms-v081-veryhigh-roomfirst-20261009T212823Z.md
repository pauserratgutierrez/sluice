# Sluice load test — rooms — v081-veryhigh-roomfirst

- Target: `sluice:0.8.1-rc`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds room-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T21:17:48Z, finished 2026-10-09T21:28:23Z, seed 16068586066682590502
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 178 (548) | 336 | 4.74 | 886.3 | 468.8 | 0.16 / 0.21 | 163 | 154 / 461 | 0.37 | 0.00 | 1.07 | 0 | 438.7 (1513) | 0.07 |
| steady | 605 | 498 (938) | 334 | 73.84 | 244.9 | 365.2 | 0.12 / 0.16 | 216 | 206 / 461 | 5.79 | 0.00 | 0.76 | 0 | 63.3 (1194) | 0.18 |
| run | 635 | 483 (938) | 334 | 78.58 | 256.1 | 370.1 | 0.12 / 0.21 | 216 | 206 / 461 | 5.79 | 0.00 | 1.07 | 0 | 81.1 (1513) | 0.18 |

Arrival: 30.1 s to open every stream (complete: true); 3.16 ms of Sluice CPU per open; 92.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1350 | 5.3 | 13.5 | 18.3 | 24.2 |
| arrival | message | 1350 | 3.3 | 10.8 | 11.7 | 13.5 |
| arrival | message_commit_to_client | 1350 | 2.8 | 10.3 | 11.5 | 13.2 |
| arrival | message_write_to_commit | 1350 | 0.4 | 1.0 | 1.2 | 1.4 |
| arrival | open | 1500 | 44.8 | 56.1 | 62.7 | 80.0 |
| arrival | publish | 134 | 0.6 | 0.9 | 1.2 | 6.5 |
| arrival | token | 21 | 103.9 | 125.4 | 125.4 | 129.2 |
| arrival | txn | 110 | 2.4 | 3.3 | 4.3 | 9.7 |
| arrival | typing | 2648 | 0.6 | 1.1 | 3.2 | 6.7 |
| steady | member | 60000 | 10.2 | 55.0 | 78.3 | 114.3 |
| steady | message | 60000 | 4.2 | 15.4 | 21.6 | 59.9 |
| steady | message_commit_to_client | 60000 | 3.8 | 14.9 | 21.4 | 59.1 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.5 | 0.7 | 3.9 |
| steady | publish | 6000 | 0.9 | 1.7 | 4.9 | 37.8 |
| steady | token | 1023 | 111.1 | 134.1 | 152.6 | 166.3 |
| steady | txn | 2200 | 2.5 | 4.6 | 15.6 | 28.2 |
| steady | typing | 181500 | 0.9 | 1.6 | 5.1 | 39.2 |
| run | member | 61350 | 10.2 | 54.5 | 77.3 | 114.3 |
| run | message | 61350 | 4.2 | 15.3 | 21.6 | 59.9 |
| run | message_commit_to_client | 61350 | 3.8 | 14.9 | 21.1 | 59.1 |
| run | message_write_to_commit | 61350 | 0.4 | 0.5 | 0.8 | 3.9 |
| run | open | 1500 | 44.8 | 56.1 | 62.7 | 80.0 |
| run | publish | 6134 | 0.9 | 1.7 | 4.8 | 37.8 |
| run | token | 1044 | 111.1 | 134.1 | 152.6 | 166.3 |
| run | txn | 2310 | 2.5 | 4.5 | 14.5 | 28.2 |
| run | typing | 184148 | 0.9 | 1.6 | 5.1 | 39.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61347 | 61350 | 100.00 |
| member | 61347 | 61350 | 100.00 |
| typing | 184148 | 184148 | 100.00 |

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
| token_ok | 1044 |
| warning:replica_identity_insufficient | 1500 |
