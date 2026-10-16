# Sluice load test — reconnect — refill-batch-newadm-r2

- Target: `sluice:refill-batch-newadm`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T16:14:33Z, finished 2026-10-16T16:27:38Z, seed 14819414674164801328
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1269 (2079) | 1160 | 36.81 | 96.7 | 105.8 | 0.12 / 0.24 | 450 | 438 / 461 | 0.36 | 3.43 | 3.50 | 0 | 209.0 (1351) | 0.10 |
| steady | 485 | 1578 (2467) | 1148 | 62.72 | 81.9 | 112.6 | 0.13 / 0.87 | 452 | 440 / 461 | 52.83 | 0.01 | 2.53 | 0 | 225.2 (2176) | 0.97 |
| run | 785 | 1460 (2467) | 1152 | 99.53 | 86.8 | 110.0 | 0.13 / 0.87 | 452 | 440 / 461 | 52.83 | 3.43 | 3.50 | 0 | 219.0 (2176) | 0.97 |

Arrival: 300.0 s to open every stream (complete: false); 6.15 ms of Sluice CPU per open; 68.5 KiB of live heap per stream after arrival.

Opens refused for capacity: 30989 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 93134 | 3.7 | 7.5 | 13.1 | 79.5 |
| arrival | message | 93134 | 3.7 | 7.5 | 13.1 | 79.5 |
| arrival | message_commit_to_client | 93134 | 3.4 | 7.0 | 12.9 | 79.0 |
| arrival | message_write_to_commit | 93134 | 0.3 | 0.6 | 0.8 | 2.4 |
| arrival | open | 5779 | 46.8 | 58.1 | 63.2 | 119.4 |
| arrival | publish | 5986 | 0.9 | 2.1 | 3.8 | 72.2 |
| arrival | token | 1546 | 46.3 | 57.1 | 64.8 | 121.4 |
| arrival | txn | 3960 | 2.1 | 3.9 | 7.2 | 22.7 |
| arrival | typing | 194376 | 1.0 | 2.7 | 4.0 | 72.3 |
| steady | member | 179804 | 3.9 | 8.3 | 22.4 | 604.8 |
| steady | message | 179804 | 3.9 | 8.3 | 22.4 | 604.8 |
| steady | message_commit_to_client | 179804 | 3.5 | 7.8 | 22.1 | 604.5 |
| steady | message_write_to_commit | 179804 | 0.3 | 0.5 | 0.7 | 2.5 |
| steady | open | 5702 | 177.2 | 1843.2 | 1892.4 | 1939.2 |
| steady | publish | 11744 | 0.9 | 2.3 | 4.6 | 78.4 |
| steady | replayed | 9429 | 12386.3 | 25296.9 | 31064.1 | 42045.5 |
| steady | token | 3014 | 45.8 | 57.6 | 83.5 | 733.0 |
| steady | txn | 6337 | 2.1 | 4.2 | 8.3 | 50.9 |
| steady | typing | 396427 | 1.1 | 2.8 | 4.9 | 83.1 |
| run | member | 272938 | 3.8 | 8.0 | 18.6 | 604.8 |
| run | message | 272938 | 3.8 | 8.0 | 18.6 | 604.8 |
| run | message_commit_to_client | 272938 | 3.5 | 7.5 | 18.3 | 604.5 |
| run | message_write_to_commit | 272938 | 0.3 | 0.5 | 0.7 | 2.5 |
| run | open | 11481 | 50.9 | 1777.7 | 1876.0 | 1939.2 |
| run | publish | 17730 | 0.9 | 2.2 | 4.3 | 78.4 |
| run | replayed | 9429 | 12386.3 | 25296.9 | 31064.1 | 42045.5 |
| run | token | 4561 | 46.3 | 57.6 | 77.3 | 733.0 |
| run | txn | 10297 | 2.1 | 4.0 | 7.9 | 50.9 |
| run | typing | 590803 | 1.1 | 2.8 | 4.5 | 83.1 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 272946 | 282209 | 103.39 |
| member | 272946 | 273096 | 100.05 |
| typing | 590786 | 590803 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5779 / 5491 | 9.11 / 16.35 / 21.80 / 33.54 | 8.96 / 21.66 | 38430 | 2176 | 0 | 0 of 5491 | 0 | 28.90 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5491 of 5491 | 100.0 | 491.5 / 560.9 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5491 | 0.0 | 0.0 / 0.0 | 17.0 / 5.1 |
| 1 | public.room_messages | 5491 of 5491 | 100.0 | 17.0 / 42.0 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1146 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5779 | 5702 |
| holds.constants | 5779 | 5702 |
| holds.counted | 5977 | 5900 |
| holds.routed | 40453 | 39914 |
| holds.routed_cap | 46232 | 45616 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40453 | 39914 |
| subscriptions.constants | 11756 | 11602 |
| subscriptions.routed | 40453 | 39914 |
| subscriptions.routed_cap | 59235 | 58542 |
| subscriptions.subscriptions | 40453 | 39914 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.1 | 191.3 |
| memory floor MiB | 234.7 | 232.9 |

Live heap after each collection (MiB): 300 s 193.1, 305 s 190.5, 325 s 191.2, 330 s 189.5, 350 s 190.5, 360 s 193.3, 375 s 192.0, 385 s 192.6, 400 s 190.8, 415 s 206.4, 420 s 192.6, 430 s 190.0, 440 s 189.8, 445 s 191.7, 450 s 189.5, 465 s 190.4, 480 s 193.4, 495 s 190.3, 505 s 191.8, 520 s 189.4, 535 s 189.8, 545 s 189.8, 550 s 117.4, 555 s 154.9, 560 s 167.5, 565 s 185.0, 570 s 190.8, 575 s 188.6, 585 s 190.4, 595 s 195.0, 610 s 189.0, 620 s 188.5, 630 s 191.5, 640 s 190.6, 655 s 210.8, 660 s 191.4, 670 s 189.6, 680 s 190.2, 685 s 192.3, 690 s 189.5, 705 s 189.9, 715 s 192.3, 725 s 189.6, 740 s 189.5, 750 s 191.4, 760 s 189.9, 775 s 202.7, 780 s 191.3.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5491 |
| open_failed:connection_refused | 22216 |
| probe_failed | 52 |
| publish_failed:unexpected_eof | 1 |
| refused_capacity:http_503:too_many_streams | 30989 |
| resumed_false:public.room_members | 27455 |
| resumed_true:public.notifications | 5491 |
| resumed_true:public.room_messages | 5491 |
| stream_end:unexpected_eof | 5779 |
| sub_ok | 91848 |
| token_ok | 4560 |
| warning:replica_identity_insufficient | 11481 |
