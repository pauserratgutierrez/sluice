# Sluice load test — reconnect — p4-fix-refill-r1

- Target: `sluice:p4-fix`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T21:58:39Z, finished 2026-10-26T22:11:45Z, seed 10008547259370488948
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 952 (1634) | 1160 | 39.82 | 139.3 | 114.4 | 0.13 / 0.27 | 449 | 439 / 461 | 1.84 | 3.43 | 3.50 | 0 | 169.6 (1311) | 0.37 |
| steady | 485 | 1116 (7286) | 1148 | 83.85 | 155.0 | 150.6 | 0.17 / 1.01 | 455 | 445 / 461 | 32.09 | 1.60 | 3.28 | 0 | 178.5 (1755) | 1.01 |
| run | 785 | 1053 (7286) | 1153 | 123.67 | 149.6 | 136.7 | 0.16 / 1.01 | 455 | 445 / 461 | 32.09 | 3.43 | 3.50 | 0 | 175.1 (1755) | 1.01 |

Arrival: 300.1 s to open every stream (complete: false); 7.10 ms of Sluice CPU per open; 80.7 KiB of live heap per stream after arrival.

Opens refused for capacity: 180160 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 76655 | 3.7 | 6.0 | 9.5 | 60.1 |
| arrival | message | 76655 | 3.7 | 6.0 | 9.5 | 60.1 |
| arrival | message_commit_to_client | 76655 | 3.2 | 5.3 | 8.9 | 59.7 |
| arrival | message_write_to_commit | 76655 | 0.4 | 0.6 | 1.0 | 2.6 |
| arrival | open | 4547 | 46.8 | 59.1 | 81.4 | 153.6 |
| arrival | publish | 5436 | 0.7 | 1.3 | 2.0 | 49.8 |
| arrival | token | 1271 | 45.8 | 56.1 | 60.2 | 105.0 |
| arrival | txn | 3961 | 2.4 | 3.8 | 5.0 | 39.0 |
| arrival | typing | 132476 | 0.8 | 1.5 | 2.4 | 60.7 |
| steady | member | 141423 | 3.8 | 6.6 | 5275.6 | 13376.7 |
| steady | message | 141424 | 3.7 | 6.6 | 5275.6 | 13270.0 |
| steady | message_commit_to_client | 141424 | 3.4 | 6.0 | 5275.6 | 13269.6 |
| steady | message_write_to_commit | 141424 | 0.3 | 0.6 | 0.8 | 1.5 |
| steady | open | 4547 | 6586.4 | 10420.2 | 11075.6 | 11679.2 |
| steady | publish | 9424 | 0.7 | 1.4 | 55.6 | 778.8 |
| steady | replayed | 13974 | 15007.7 | 32374.8 | 35389.4 | 38293.1 |
| steady | token | 2380 | 45.8 | 57.6 | 1105.9 | 8668.4 |
| steady | txn | 6336 | 2.4 | 3.6 | 4.6 | 10.0 |
| steady | typing | 244238 | 0.8 | 1.7 | 75.3 | 10597.6 |
| run | member | 218078 | 3.7 | 6.3 | 2211.8 | 13376.7 |
| run | message | 218079 | 3.7 | 6.3 | 2211.8 | 13270.0 |
| run | message_commit_to_client | 218079 | 3.3 | 5.8 | 2211.8 | 13269.6 |
| run | message_write_to_commit | 218079 | 0.3 | 0.6 | 0.9 | 2.6 |
| run | open | 9094 | 115.2 | 9764.9 | 10813.4 | 11679.2 |
| run | publish | 14860 | 0.7 | 1.4 | 34.0 | 778.8 |
| run | replayed | 13974 | 15007.7 | 32374.8 | 35389.4 | 38293.1 |
| run | token | 3651 | 45.8 | 57.1 | 162.8 | 8668.4 |
| run | txn | 10297 | 2.4 | 3.7 | 4.8 | 39.0 |
| run | typing | 376714 | 0.8 | 1.6 | 40.7 | 10597.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 217583 | 225066 | 103.44 |
| member | 217583 | 225065 | 103.44 |
| typing | 375402 | 376714 | 100.35 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4547 / 3486 | 17.49 / 22.50 / 23.40 / 23.50 | 17.36 / 23.28 | 69954 | 1755 | 7 | 3486 of 3486 | 0 | 42.23 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3486 of 3486 | 100.0 | 504.3 / 557.2 | 0.0 / 0.0 |
| 1 | public.room_members | 3486 of 3486 | 100.0 | 16.1 / 35.9 | 0.0 / 0.0 |
| 1 | public.room_messages | 3486 of 3486 | 100.0 | 16.1 / 35.9 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1141 | — | — | 60.1 |
| public.room_messages | 13 | — | — | 60.1 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4547 | 4547 |
| holds.constants | 4547 | 4547 |
| holds.counted | 4745 | 4745 |
| holds.routed | 31829 | 31829 |
| holds.routed_cap | 36376 | 36376 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31829 | 31829 |
| subscriptions.constants | 9292 | 9292 |
| subscriptions.routed | 31829 | 31829 |
| subscriptions.routed_cap | 47704 | 47720 |
| subscriptions.subscriptions | 31829 | 31829 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.9 | 198.2 |
| memory floor MiB | 230.0 | 234.4 |

Live heap after each collection (MiB): 300 s 193.9, 315 s 200.9, 330 s 194.3, 355 s 209.2, 370 s 197.6, 385 s 193.7, 405 s 201.9, 420 s 194.0, 445 s 203.1, 460 s 197.6, 480 s 194.8, 500 s 202.9, 515 s 196.7, 535 s 193.9, 550 s 127.8, 555 s 207.9, 560 s 215.8, 565 s 196.0, 585 s 201.4, 600 s 198.2, 620 s 205.9, 635 s 202.4, 655 s 216.5, 665 s 198.9, 685 s 221.7, 690 s 196.3, 710 s 204.8, 720 s 196.2, 735 s 202.0, 745 s 195.1, 765 s 204.3, 780 s 198.2.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3486 |
| open_failed:connection_refused | 18837 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 180160 |
| resumed_true:public.notifications | 3486 |
| resumed_true:public.room_members | 17430 |
| resumed_true:public.room_messages | 3486 |
| stream_end:unexpected_eof | 4547 |
| sub_error:issuer_unavailable | 7 |
| sub_ok | 72752 |
| token_failed:unexpected_eof | 1 |
| token_ok | 3650 |
| warning:replica_identity_insufficient | 9094 |
