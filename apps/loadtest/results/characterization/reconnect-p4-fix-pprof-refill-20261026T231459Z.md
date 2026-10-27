# Sluice load test — reconnect — p4-fix-pprof-refill

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T23:01:54Z, finished 2026-10-26T23:14:59Z, seed 6428811274824331271
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 890 (1503) | 1162 | 33.56 | 125.6 | 96.3 | 0.11 / 0.22 | 450 | 439 / 461 | 0.24 | 1.94 | 3.52 | 0 | 170.9 (1340) | 0.28 |
| steady | 485 | 1118 (5403) | 1148 | 74.07 | 136.6 | 133.0 | 0.15 / 0.96 | 460 | 449 / 461 | 1.22 | 0.59 | 2.80 | 0 | 178.5 (1827) | 0.88 |
| run | 785 | 1031 (5403) | 1153 | 107.63 | 133.0 | 118.9 | 0.14 / 0.96 | 460 | 449 / 461 | 1.22 | 1.94 | 3.52 | 0 | 175.6 (1827) | 0.88 |

Arrival: 300.1 s to open every stream (complete: false); 6.01 ms of Sluice CPU per open; 80.3 KiB of live heap per stream after arrival.

Opens refused for capacity: 180538 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 76895 | 3.6 | 6.1 | 10.8 | 50.6 |
| arrival | message | 76895 | 3.6 | 6.1 | 10.8 | 50.5 |
| arrival | message_commit_to_client | 76895 | 3.2 | 5.6 | 10.4 | 50.2 |
| arrival | message_write_to_commit | 76895 | 0.3 | 0.5 | 0.8 | 2.1 |
| arrival | open | 4543 | 45.8 | 57.6 | 72.2 | 141.5 |
| arrival | publish | 4603 | 0.7 | 1.4 | 2.3 | 60.3 |
| arrival | token | 1368 | 45.3 | 56.1 | 60.2 | 110.2 |
| arrival | txn | 3960 | 2.3 | 3.8 | 5.5 | 22.0 |
| arrival | typing | 113382 | 0.8 | 1.6 | 2.9 | 60.9 |
| steady | member | 141777 | 3.6 | 6.8 | 1630.2 | 6963.8 |
| steady | message | 141780 | 3.6 | 6.8 | 1630.2 | 6951.9 |
| steady | message_commit_to_client | 141780 | 3.3 | 6.3 | 1630.2 | 6951.6 |
| steady | message_write_to_commit | 141780 | 0.3 | 0.5 | 0.6 | 4.9 |
| steady | open | 4575 | 3588.1 | 8355.8 | 8847.4 | 9594.1 |
| steady | publish | 9279 | 0.7 | 1.4 | 58.1 | 560.6 |
| steady | replayed | 13532 | 14352.4 | 31064.1 | 34865.2 | 37867.0 |
| steady | token | 2302 | 45.8 | 58.1 | 2441.2 | 6258.5 |
| steady | txn | 6337 | 2.2 | 3.6 | 5.2 | 39.1 |
| steady | typing | 245099 | 0.8 | 1.7 | 78.3 | 7632.1 |
| run | member | 218672 | 3.6 | 6.4 | 757.8 | 6963.8 |
| run | message | 218675 | 3.6 | 6.4 | 757.8 | 6951.9 |
| run | message_commit_to_client | 218675 | 3.3 | 5.9 | 757.8 | 6951.6 |
| run | message_write_to_commit | 218675 | 0.3 | 0.5 | 0.7 | 4.9 |
| run | open | 9118 | 89.6 | 7569.4 | 8716.3 | 9594.1 |
| run | publish | 13882 | 0.7 | 1.4 | 31.6 | 560.6 |
| run | replayed | 13532 | 14352.4 | 31064.1 | 34865.2 | 37867.0 |
| run | token | 3670 | 45.3 | 57.1 | 1105.9 | 6258.5 |
| run | txn | 10297 | 2.3 | 3.7 | 5.3 | 39.1 |
| run | typing | 358481 | 0.8 | 1.7 | 30.1 | 7632.1 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 218524 | 225441 | 103.17 |
| member | 218524 | 225438 | 103.16 |
| typing | 357541 | 358481 | 100.26 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4559 / 3530 | 13.50 / 22.01 / 22.91 / 23.04 | 13.36 / 22.82 | 70087 | 1827 | 0 | 3530 of 3530 | 0 | 37.87 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3530 of 3530 | 100.0 | 506.2 / 557.4 | 0.0 / 0.0 |
| 1 | public.room_members | 3530 of 3530 | 100.0 | 16.8 / 35.1 | 0.0 / 0.0 |
| 1 | public.room_messages | 3530 of 3530 | 100.0 | 16.8 / 35.1 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1142 | — | — | 60.0 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4543 | 4559 |
| holds.constants | 4543 | 4559 |
| holds.counted | 4741 | 4757 |
| holds.routed | 31801 | 31913 |
| holds.routed_cap | 36344 | 36472 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31801 | 31913 |
| subscriptions.constants | 9284 | 9316 |
| subscriptions.routed | 31801 | 31913 |
| subscriptions.routed_cap | 47668 | 47812 |
| subscriptions.subscriptions | 31801 | 31913 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.2 | 196.6 |
| memory floor MiB | 229.1 | 233.2 |

Live heap after each collection (MiB): 300 s 193.2, 315 s 201.6, 330 s 194.3, 355 s 193.4, 375 s 200.0, 390 s 195.4, 415 s 193.5, 435 s 201.2, 450 s 194.5, 475 s 209.4, 485 s 193.8, 500 s 202.4, 510 s 195.0, 525 s 199.8, 535 s 195.1, 550 s 156.0, 555 s 166.5, 560 s 214.4, 565 s 196.1, 580 s 201.3, 595 s 221.9, 600 s 197.5, 620 s 206.2, 630 s 198.1, 645 s 207.2, 660 s 197.6, 675 s 205.0, 690 s 197.5, 715 s 205.6, 725 s 198.3, 745 s 212.5, 750 s 196.5, 765 s 203.6, 775 s 196.6.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3530 |
| open_failed:connection_refused | 18844 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 180538 |
| resumed_true:public.notifications | 3530 |
| resumed_true:public.room_members | 17650 |
| resumed_true:public.room_messages | 3530 |
| stream_end:unexpected_eof | 4559 |
| sub_ok | 72944 |
| token_ok | 3670 |
| warning:replica_identity_insufficient | 9118 |
