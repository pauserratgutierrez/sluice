# Sluice load test — reconnect — p4-fix-refill-r2

- Target: `sluice:p4-fix`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T22:12:13Z, finished 2026-10-26T22:25:18Z, seed 13109711888863154276
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 914 (1454) | 1160 | 36.75 | 133.9 | 105.5 | 0.12 / 0.26 | 450 | 440 / 461 | 0.13 | 3.42 | 3.51 | 0 | 170.5 (1305) | 0.32 |
| steady | 485 | 1066 (8051) | 1148 | 80.14 | 155.0 | 143.9 | 0.17 / 1.01 | 450 | 440 / 461 | 3.40 | 1.35 | 2.92 | 0 | 178.6 (1896) | 0.95 |
| run | 785 | 1008 (8051) | 1153 | 116.89 | 147.7 | 129.2 | 0.15 / 1.01 | 450 | 440 / 461 | 3.40 | 3.42 | 3.51 | 0 | 175.5 (1896) | 0.95 |

Arrival: 300.1 s to open every stream (complete: false); 6.51 ms of Sluice CPU per open; 86.8 KiB of live heap per stream after arrival.

Opens refused for capacity: 178296 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 76830 | 3.6 | 6.0 | 11.2 | 60.4 |
| arrival | message | 76830 | 3.6 | 6.0 | 11.2 | 60.4 |
| arrival | message_commit_to_client | 76830 | 3.2 | 5.5 | 10.6 | 60.1 |
| arrival | message_write_to_commit | 76830 | 0.3 | 0.6 | 0.9 | 2.4 |
| arrival | open | 4550 | 46.3 | 57.6 | 62.2 | 86.1 |
| arrival | publish | 4978 | 0.7 | 1.3 | 1.9 | 34.6 |
| arrival | token | 1310 | 45.8 | 56.1 | 59.6 | 102.8 |
| arrival | txn | 3961 | 2.4 | 3.8 | 5.2 | 59.0 |
| arrival | typing | 120733 | 0.8 | 1.5 | 2.3 | 35.9 |
| steady | member | 141228 | 3.7 | 6.4 | 3719.2 | 11031.5 |
| steady | message | 141242 | 3.7 | 6.4 | 3719.2 | 11031.5 |
| steady | message_commit_to_client | 141242 | 3.3 | 6.0 | 3719.2 | 11031.2 |
| steady | message_write_to_commit | 141242 | 0.3 | 0.5 | 0.8 | 2.3 |
| steady | open | 4564 | 7831.6 | 11993.1 | 12779.5 | 13572.9 |
| steady | publish | 8148 | 0.7 | 1.4 | 30.6 | 825.2 |
| steady | replayed | 14440 | 15663.1 | 31850.5 | 35389.4 | 37786.3 |
| steady | token | 2326 | 46.3 | 57.6 | 1122.3 | 9154.9 |
| steady | txn | 6336 | 2.3 | 3.6 | 4.6 | 37.5 |
| steady | typing | 220111 | 0.8 | 1.6 | 47.4 | 10584.4 |
| run | member | 218058 | 3.7 | 6.2 | 1695.7 | 11031.5 |
| run | message | 218072 | 3.7 | 6.2 | 1695.7 | 11031.5 |
| run | message_commit_to_client | 218072 | 3.3 | 5.7 | 1695.7 | 11031.2 |
| run | message_write_to_commit | 218072 | 0.3 | 0.6 | 0.8 | 2.4 |
| run | open | 9114 | 81.4 | 11468.8 | 12517.4 | 13572.9 |
| run | publish | 13126 | 0.7 | 1.3 | 9.0 | 825.2 |
| run | replayed | 14440 | 15663.1 | 31850.5 | 35389.4 | 37786.3 |
| run | token | 3636 | 45.8 | 57.1 | 88.6 | 9154.9 |
| run | txn | 10297 | 2.3 | 3.7 | 4.9 | 59.0 |
| run | typing | 340844 | 0.8 | 1.6 | 11.5 | 10584.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 217658 | 225292 | 103.51 |
| member | 217658 | 225278 | 103.50 |
| typing | 339760 | 340844 | 100.32 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4557 / 3473 | 17.70 / 22.02 / 23.11 / 23.21 | 17.58 / 22.99 | 69029 | 1896 | 0 | 3473 of 3473 | 0 | 41.13 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3473 of 3473 | 100.0 | 502.2 / 557.9 | 0.0 / 0.0 |
| 1 | public.room_members | 3473 of 3473 | 100.0 | 15.8 / 34.9 | 0.0 / 0.0 |
| 1 | public.room_messages | 3473 of 3473 | 100.0 | 15.8 / 34.9 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1142 | — | — | 60.0 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4550 | 4557 |
| holds.constants | 4550 | 4557 |
| holds.counted | 4748 | 4755 |
| holds.routed | 31850 | 31899 |
| holds.routed_cap | 36400 | 36456 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31850 | 31899 |
| subscriptions.constants | 9298 | 9312 |
| subscriptions.routed | 31850 | 31899 |
| subscriptions.routed_cap | 47731 | 47763 |
| subscriptions.subscriptions | 31850 | 31899 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 202.6 | 205.9 |
| memory floor MiB | 239.1 | 242.7 |

Live heap after each collection (MiB): 300 s 202.6, 305 s 196.4, 325 s 193.6, 350 s 202.1, 365 s 196.0, 385 s 194.3, 410 s 202.0, 425 s 195.2, 445 s 193.8, 465 s 201.2, 485 s 193.6, 505 s 193.1, 525 s 200.8, 540 s 195.3, 550 s 166.9, 555 s 207.3, 560 s 221.3, 565 s 219.1, 570 s 198.4, 585 s 204.0, 595 s 197.6, 610 s 199.3, 625 s 205.1, 635 s 203.8, 655 s 211.4, 670 s 199.1, 685 s 195.7, 705 s 201.8, 720 s 196.3, 740 s 204.3, 750 s 196.8, 775 s 205.9.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3473 |
| open_failed:connection_refused | 18906 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 178296 |
| resumed_true:public.notifications | 3473 |
| resumed_true:public.room_members | 17365 |
| resumed_true:public.room_messages | 3473 |
| stream_end:unexpected_eof | 4557 |
| sub_ok | 72912 |
| token_ok | 3636 |
| warning:replica_identity_insufficient | 9114 |
