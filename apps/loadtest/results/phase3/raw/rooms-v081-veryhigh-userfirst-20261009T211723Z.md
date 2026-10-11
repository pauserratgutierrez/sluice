# Sluice load test — rooms — v081-veryhigh-userfirst

- Target: `sluice:0.8.1-rc`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T21:06:48Z, finished 2026-10-09T21:17:23Z, seed 3308535685223300828
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 208 (615) | 336 | 3.91 | 623.2 | 386.7 | 0.13 / 0.15 | 159 | 149 / 461 | 0.54 | 0.00 | 1.11 | 0 | 446.7 (1231) | 0.13 |
| steady | 605 | 498 (788) | 334 | 19.17 | 63.6 | 94.8 | 0.03 / 0.06 | 189 | 180 / 461 | 2.62 | 0.00 | 0.75 | 0 | 63.2 (1059) | 0.02 |
| run | 635 | 485 (788) | 334 | 23.08 | 75.0 | 108.7 | 0.04 / 0.15 | 189 | 180 / 461 | 2.62 | 0.00 | 1.11 | 0 | 81.4 (1231) | 0.13 |

Arrival: 30.1 s to open every stream (complete: true); 2.61 ms of Sluice CPU per open; 81.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1497 | 3.1 | 5.6 | 7.1 | 8.6 |
| arrival | message | 1497 | 3.0 | 5.3 | 6.6 | 7.0 |
| arrival | message_commit_to_client | 1497 | 2.5 | 4.7 | 6.2 | 6.6 |
| arrival | message_write_to_commit | 1497 | 0.4 | 0.8 | 0.9 | 2.0 |
| arrival | open | 1500 | 44.8 | 55.6 | 59.6 | 74.6 |
| arrival | publish | 168 | 0.5 | 0.9 | 1.3 | 2.3 |
| arrival | token | 30 | 109.1 | 128.5 | 128.5 | 129.3 |
| arrival | txn | 110 | 2.2 | 3.2 | 4.3 | 5.4 |
| arrival | typing | 3231 | 0.6 | 1.1 | 1.3 | 2.2 |
| steady | member | 60000 | 4.1 | 7.9 | 14.8 | 38.7 |
| steady | message | 60000 | 4.0 | 7.7 | 14.7 | 38.7 |
| steady | message_commit_to_client | 60000 | 3.6 | 7.2 | 14.3 | 38.4 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.6 | 0.7 | 2.4 |
| steady | publish | 6003 | 0.9 | 1.5 | 1.8 | 18.7 |
| steady | token | 1015 | 108.0 | 129.5 | 134.1 | 150.3 |
| steady | txn | 2200 | 2.5 | 4.6 | 12.7 | 36.9 |
| steady | typing | 181597 | 0.9 | 1.5 | 1.8 | 18.7 |
| run | member | 61497 | 4.0 | 7.8 | 14.7 | 38.7 |
| run | message | 61497 | 3.9 | 7.6 | 14.7 | 38.7 |
| run | message_commit_to_client | 61497 | 3.6 | 7.1 | 14.1 | 38.4 |
| run | message_write_to_commit | 61497 | 0.4 | 0.6 | 0.8 | 2.4 |
| run | open | 1500 | 44.8 | 55.6 | 59.6 | 74.6 |
| run | publish | 6171 | 0.9 | 1.5 | 1.8 | 18.7 |
| run | token | 1045 | 108.0 | 129.5 | 134.1 | 150.3 |
| run | txn | 2310 | 2.5 | 4.4 | 12.6 | 36.9 |
| run | typing | 184828 | 0.9 | 1.5 | 1.8 | 18.7 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61494 | 61497 | 100.00 |
| member | 61494 | 61497 | 100.00 |
| typing | 184828 | 184828 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 334 | 10.0 | 10.0 | 12.3 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1045 |
| warning:replica_identity_insufficient | 1500 |
