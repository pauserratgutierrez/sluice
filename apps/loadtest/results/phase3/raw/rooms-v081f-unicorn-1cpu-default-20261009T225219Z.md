# Sluice load test — rooms — v081f-unicorn-1cpu-default

- Target: `sluice:0.8.1-rc2`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T22:40:13Z, finished 2026-10-09T22:52:19Z, seed 8533767219777268432
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | pk | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 121 | 1430 (4033) | 2319 | 44.47 | 258.1 | 159.1 | 0.37 / 0.79 | 1110 | 1101 / 3686 | 0.81 | 0.10 | 6.94 | 0 | 1020.2 (2132) | 0.29 |
| steady | 605 | 3472 (4297) | 2301 | 151.19 | 72.0 | 108.6 | 0.25 / 0.55 | 1298 | 1287 / 3686 | 0.81 | 0.04 | 5.14 | 0 | 513.4 (1398) | 0.23 |
| run | 726 | 3132 (4297) | 2304 | 195.66 | 86.1 | 117.1 | 0.27 / 0.79 | 1298 | 1287 / 3686 | 0.81 | 0.10 | 6.94 | 0 | 597.6 (2132) | 0.29 |

Arrival: 120.5 s to open every stream (complete: true); 3.71 ms of Sluice CPU per open; 81.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 47905 | 3.2 | 40.7 | 127.5 | 624.3 |
| arrival | message | 47908 | 3.0 | 37.6 | 125.4 | 624.3 |
| arrival | message_commit_to_client | 47908 | 2.6 | 37.6 | 124.4 | 623.9 |
| arrival | message_write_to_commit | 47908 | 0.4 | 0.7 | 1.3 | 2.5 |
| arrival | open | 12000 | 45.8 | 89.6 | 1036.3 | 1853.3 |
| arrival | publish | 3295 | 0.5 | 18.8 | 74.2 | 212.2 |
| arrival | token | 807 | 111.1 | 301.1 | 1368.1 | 1906.5 |
| arrival | txn | 3182 | 2.2 | 3.5 | 4.8 | 17.1 |
| arrival | typing | 76509 | 0.7 | 47.4 | 128.5 | 299.0 |
| steady | member | 480028 | 3.3 | 7.4 | 45.8 | 551.5 |
| steady | message | 480028 | 3.2 | 7.1 | 42.8 | 523.3 |
| steady | message_commit_to_client | 480028 | 2.8 | 6.5 | 42.2 | 522.8 |
| steady | message_write_to_commit | 480028 | 0.3 | 0.7 | 1.1 | 17.3 |
| steady | publish | 32103 | 0.6 | 1.8 | 27.0 | 87.9 |
| steady | token | 8018 | 110.1 | 140.3 | 280.6 | 746.6 |
| steady | txn | 15841 | 2.2 | 4.0 | 6.6 | 25.4 |
| steady | typing | 1140381 | 0.8 | 2.3 | 30.8 | 150.7 |
| run | member | 527933 | 3.3 | 7.8 | 54.0 | 624.3 |
| run | message | 527936 | 3.1 | 7.4 | 51.5 | 624.3 |
| run | message_commit_to_client | 527936 | 2.8 | 6.8 | 50.9 | 623.9 |
| run | message_write_to_commit | 527936 | 0.3 | 0.7 | 1.1 | 17.3 |
| run | open | 12000 | 45.8 | 89.6 | 1036.3 | 1853.3 |
| run | publish | 35398 | 0.6 | 1.9 | 34.6 | 212.2 |
| run | token | 8825 | 110.1 | 144.4 | 305.2 | 1906.5 |
| run | txn | 19023 | 2.2 | 3.9 | 6.3 | 25.4 |
| run | typing | 1216890 | 0.8 | 2.4 | 41.2 | 299.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 527913 | 527936 | 100.00 |
| member | 527913 | 527933 | 100.00 |
| typing | 1216860 | 1216890 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2293 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| publish_failed:canceled | 1 |
| sub_ok | 96000 |
| token_ok | 8825 |
| warning:replica_identity_insufficient | 12000 |
