# Sluice load test — rooms — v081f-veryhigh-default

- Target: `sluice:0.8.1-rc2`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T22:05:41Z, finished 2026-10-09T22:16:17Z, seed 969989105247914288
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | pk | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 196 (688) | 336 | 4.08 | 692.6 | 403.6 | 0.14 / 0.16 | 157 | 147 / 461 | 0.52 | 1.02 | 1.10 | 0 | 447.7 (1269) | 0.06 |
| steady | 605 | 498 (765) | 334 | 18.36 | 60.9 | 90.8 | 0.03 / 0.07 | 186 | 176 / 461 | 3.34 | 0.01 | 0.72 | 0 | 62.8 (1149) | 0.03 |
| run | 635 | 484 (765) | 334 | 22.44 | 73.0 | 105.7 | 0.04 / 0.16 | 186 | 176 / 461 | 3.34 | 1.02 | 1.10 | 0 | 81.0 (1269) | 0.06 |

Arrival: 30.1 s to open every stream (complete: true); 2.72 ms of Sluice CPU per open; 80.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1411 | 3.1 | 5.2 | 11.6 | 11.6 |
| arrival | message | 1411 | 3.0 | 5.2 | 11.6 | 11.6 |
| arrival | message_commit_to_client | 1411 | 2.6 | 4.4 | 10.6 | 10.7 |
| arrival | message_write_to_commit | 1411 | 0.4 | 1.1 | 1.5 | 2.1 |
| arrival | open | 1500 | 45.3 | 55.6 | 59.6 | 73.9 |
| arrival | publish | 152 | 0.5 | 1.5 | 2.0 | 4.5 |
| arrival | token | 30 | 105.0 | 124.4 | 127.5 | 138.4 |
| arrival | txn | 110 | 2.3 | 3.4 | 4.1 | 4.5 |
| arrival | typing | 3069 | 0.6 | 1.4 | 2.8 | 4.8 |
| steady | member | 60000 | 3.8 | 9.3 | 17.3 | 25.6 |
| steady | message | 60000 | 3.8 | 8.5 | 17.0 | 25.6 |
| steady | message_commit_to_client | 60000 | 3.4 | 8.0 | 16.8 | 25.2 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.6 | 1.1 | 2.4 |
| steady | publish | 6000 | 0.9 | 1.6 | 1.9 | 13.6 |
| steady | token | 985 | 110.1 | 130.6 | 136.2 | 152.4 |
| steady | txn | 2200 | 2.5 | 5.2 | 15.4 | 22.7 |
| steady | typing | 181500 | 0.9 | 1.6 | 1.9 | 17.2 |
| run | member | 61411 | 3.8 | 9.2 | 17.0 | 25.6 |
| run | message | 61411 | 3.8 | 8.5 | 17.0 | 25.6 |
| run | message_commit_to_client | 61411 | 3.4 | 8.0 | 16.5 | 25.2 |
| run | message_write_to_commit | 61411 | 0.4 | 0.6 | 1.2 | 2.4 |
| run | open | 1500 | 45.3 | 55.6 | 59.6 | 73.9 |
| run | publish | 6152 | 0.9 | 1.6 | 1.9 | 13.6 |
| run | token | 1015 | 110.1 | 130.6 | 136.2 | 152.4 |
| run | txn | 2310 | 2.5 | 5.2 | 15.2 | 22.7 |
| run | typing | 184569 | 0.9 | 1.6 | 1.9 | 17.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61409 | 61411 | 100.00 |
| member | 61409 | 61411 | 100.00 |
| typing | 184568 | 184569 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 328 | 10.0 | 10.0 | 12.5 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1015 |
| warning:replica_identity_insufficient | 1500 |
