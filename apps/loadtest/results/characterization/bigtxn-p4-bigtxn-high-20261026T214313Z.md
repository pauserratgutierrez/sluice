# Sluice load test — bigtxn — p4-bigtxn-high

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T21:23:38Z, finished 2026-10-26T21:43:13Z, seed 12112974758391924444
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | full | 30s | 19m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 56 (202) | 89 | 0.69 | 412.9 | 258.4 | 0.02 / 0.03 | 51 | 41 / 461 | 1.63 | 0.00 | 0.34 | 0 | 37.5 (354) | 0.20 |
| steady | 1145 | 128 (474) | 381 | 11.90 | 81.0 | 27.3 | 0.01 / 0.12 | 180 | 170 / 461 | 1.63 | 0.00 | 0.10 | 0 | 14.3 (404) | 0.33 |
| run | 1175 | 127 (474) | 373 | 12.59 | 84.7 | 28.7 | 0.01 / 0.12 | 180 | 170 / 461 | 1.63 | 0.00 | 0.34 | 0 | 14.9 (404) | 0.33 |

Arrival: 30.0 s to open every stream (complete: true); 1.73 ms of Sluice CPU per open; 65.9 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 406 | 4.1 | 9.8 | 9.9 | 59.2 |
| arrival | message | 406 | 4.1 | 9.8 | 9.9 | 59.2 |
| arrival | message_commit_to_client | 406 | 3.7 | 9.2 | 9.3 | 58.7 |
| arrival | message_write_to_commit | 406 | 0.4 | 0.7 | 0.8 | 0.8 |
| arrival | open | 400 | 28.3 | 38.7 | 42.2 | 44.2 |
| arrival | publish | 40 | 0.7 | 1.2 | 1.3 | 1.3 |
| arrival | token | 3 | 24.2 | 24.2 | 24.2 | 28.5 |
| arrival | txn | 30 | 3.2 | 8.5 | 8.5 | 58.3 |
| arrival | typing | 859 | 0.8 | 1.2 | 1.3 | 1.3 |
| steady | member | 30400 | 4.5 | 7.7 | 14.8 | 90.1 |
| steady | message | 30400 | 4.5 | 7.7 | 14.8 | 90.1 |
| steady | message_commit_to_client | 30400 | 4.1 | 7.3 | 14.3 | 89.4 |
| steady | message_write_to_commit | 30400 | 0.4 | 0.6 | 0.8 | 1.7 |
| steady | notification | 3525 | 72.2 | 407.6 | 407.6 | 408.6 |
| steady | publish | 2850 | 0.8 | 1.5 | 1.8 | 6.3 |
| steady | token | 520 | 29.3 | 39.2 | 41.2 | 43.4 |
| steady | txn | 1140 | 3.3 | 5.5 | 12.7 | 55.7 |
| steady | typing | 82650 | 0.9 | 1.4 | 1.9 | 8.0 |
| run | member | 30806 | 4.5 | 7.7 | 14.8 | 90.1 |
| run | message | 30806 | 4.4 | 7.7 | 14.8 | 90.1 |
| run | message_commit_to_client | 30806 | 4.1 | 7.3 | 14.3 | 89.4 |
| run | message_write_to_commit | 30806 | 0.4 | 0.6 | 0.8 | 1.7 |
| run | notification | 3525 | 72.2 | 407.6 | 407.6 | 408.6 |
| run | open | 400 | 28.3 | 38.7 | 42.2 | 44.2 |
| run | publish | 2890 | 0.8 | 1.4 | 1.8 | 6.3 |
| run | token | 523 | 29.3 | 39.2 | 41.2 | 43.4 |
| run | txn | 1170 | 3.3 | 5.6 | 13.2 | 58.3 |
| run | typing | 83509 | 0.9 | 1.4 | 1.9 | 8.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 30806 | 30806 | 100.00 |
| member | 30806 | 30806 | 100.00 |
| typing | 83509 | 83509 | 100.00 |
| notification | 0 | 3525 | 0.00 |

## Big transactions

| # | Rows | Streamed | Delivered | Txn ms | Sent ms | Dispatched ms | Last delivery ms | Backlog max MiB | Sluice CPU s | Others' message p95 before → during |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1500 | 375 | 375/375 | 8 | 22 | 22 | 14 | 0.4 | 0.29 | 8 → 7 |
| 2 | 1500 | 375 | 375/375 | 8 | 22 | 22 | 9 | 0.4 | 0.24 | 11 → 6 |
| 3 | 1500 | 375 | 375/375 | 7 | 22 | 22 | 19 | 0.5 | 0.25 | 12 → 8 |
| 4 | 10000 | 400 | 400/400 | 47 | 47 | 69 | 39 | 2.8 | 0.43 | 7 → 10 |
| 5 | 10000 | 400 | 400/400 | 41 | 54 | 54 | 34 | 3.2 | 0.32 | 8 → 6 |
| 6 | 10000 | 400 | 400/400 | 35 | 64 | 64 | 21 | 2.7 | 0.33 | 11 → 6 |
| 7 | 100000 | 400 | 400/400 | 377 | 414 | 460 | 31 | 21.1 | 0.77 | 14 → 10 |
| 8 | 100000 | 400 | 400/400 | 375 | 471 | 491 | 20 | 3.1 | 0.74 | 8 → 11 |
| 9 | 100000 | 400 | 400/400 | 368 | 484 | 505 | 18 | 2.5 | 0.77 | 6 → 7 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 9 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.notifications | 146 | — | — | 60.2 |
| public.room_members | 76 | 60.0 | 60.0 | 60.2 |
| public.room_messages | 1 | 60.0 | 60.0 | 60.2 |

## Indexes and memory, start and end of the steady phase

| | 30 s | 1175 s |
| --- | ---: | ---: |
| streams | 400 | 400 |
| holds.constants | 400 | 400 |
| holds.counted | 415 | 415 |
| holds.routed | 2800 | 2800 |
| holds.routed_cap | 3200 | 3272 |
| holds.unindexed | 0 | 0 |
| holds.watches | 2800 | 2800 |
| subscriptions.constants | 815 | 815 |
| subscriptions.routed | 2800 | 2800 |
| subscriptions.routed_cap | 4112 | 4112 |
| subscriptions.subscriptions | 2800 | 2800 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 16.0 | 66.6 |
| memory floor MiB | 23.8 | 77.2 |

Live heap after each collection (MiB): 30 s 16.0, 55 s 19.2, 85 s 20.3, 110 s 20.2, 125 s 20.3, 150 s 20.5, 180 s 20.4, 205 s 20.4, 240 s 20.4, 265 s 20.6, 300 s 20.6, 330 s 20.6, 365 s 21.7, 390 s 20.9, 415 s 21.4, 440 s 21.1, 465 s 21.1, 485 s 24.7, 510 s 24.9, 540 s 24.6, 575 s 24.7, 605 s 29.9, 620 s 29.6, 650 s 27.1, 685 s 27.2, 715 s 27.3, 725 s 32.0, 760 s 26.8, 790 s 26.7, 825 s 26.9, 845 s 70.0, 910 s 66.8, 965 s 73.5, 1015 s 66.6, 1085 s 75.4, 1115 s 66.6.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 523 |
| warning:replica_identity_insufficient | 400 |
