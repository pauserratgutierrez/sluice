# Sluice load test — reconnect — refill-allowance-prof

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T14:36:11Z, finished 2026-10-16T14:49:16Z, seed 2484259783698499800
- **Verdict: FAIL: message p95 6390 ms > 500ms; member p95 6259 ms > 500ms; Go memory peaked at 518 MiB, GOMEMLIMIT is 461 MiB**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1271 (2087) | 1167 | 49.54 | 129.9 | 141.4 | 0.17 / 0.40 | 452 | 440 / 461 | 1.19 | 3.48 | 3.55 | 0 | 353.0 (1653) | 0.16 |
| steady | 485 | 1288 (6626) | 1148 | 146.67 | 234.7 | 263.4 | 0.30 / 4.21 | 484 | 518 / 461 | 54.44 | 8.49 | 9.53 | 0 | 317.1 (5603) | 1.02 |
| run | 785 | 1282 (6626) | 1155 | 196.21 | 195.0 | 216.3 | 0.25 / 4.21 | 484 | 518 / 461 | 54.44 | 8.49 | 9.53 | 0 | 330.9 (5603) | 1.02 |

Arrival: 300.1 s to open every stream (complete: false); 8.29 ms of Sluice CPU per open; 70.3 KiB of live heap per stream after arrival.

Opens refused for capacity: 70783 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 93069 | 3.8 | 8.0 | 17.5 | 173.2 |
| arrival | message | 93070 | 3.8 | 7.9 | 17.5 | 173.2 |
| arrival | message_commit_to_client | 93070 | 3.5 | 7.3 | 17.3 | 171.2 |
| arrival | message_write_to_commit | 93070 | 0.3 | 0.6 | 1.3 | 3.7 |
| arrival | open | 5772 | 46.8 | 58.1 | 87.6 | 253.5 |
| arrival | publish | 6185 | 0.8 | 2.0 | 5.6 | 63.0 |
| arrival | token | 1494 | 110.1 | 132.1 | 175.1 | 237.5 |
| arrival | txn | 3960 | 2.2 | 3.8 | 6.8 | 40.0 |
| arrival | typing | 195328 | 1.0 | 2.6 | 6.6 | 64.0 |
| steady | member | 148685 | 3.8 | 6258.7 | 39583.7 | 79301.2 |
| steady | message | 148799 | 3.8 | 6389.8 | 39583.7 | 81808.7 |
| steady | message_commit_to_client | 148799 | 3.4 | 6389.8 | 39583.7 | 81808.3 |
| steady | message_write_to_commit | 148799 | 0.3 | 0.5 | 0.7 | 3.2 |
| steady | open | 5437 | 3751.9 | 85458.9 | 85458.9 | 85765.8 |
| steady | publish | 9889 | 0.8 | 41.2 | 2089.0 | 21110.1 |
| steady | replayed | 17426 | 35913.7 | 96993.3 | 106430.5 | 109294.9 |
| steady | token | 2588 | 112.1 | 16056.3 | 30001.2 | 30001.2 |
| steady | txn | 6336 | 2.1 | 3.5 | 6.1 | 15.7 |
| steady | typing | 309917 | 1.0 | 3.0 | 100.9 | 21134.6 |
| run | member | 241754 | 3.8 | 25.5 | 35389.4 | 79301.2 |
| run | message | 241869 | 3.8 | 27.0 | 35389.4 | 81808.7 |
| run | message_commit_to_client | 241869 | 3.4 | 26.8 | 35389.4 | 81808.3 |
| run | message_write_to_commit | 241869 | 0.3 | 0.6 | 0.9 | 3.7 |
| run | open | 11209 | 54.0 | 83361.8 | 85458.9 | 85765.8 |
| run | publish | 16074 | 0.8 | 3.7 | 1302.5 | 21110.1 |
| run | replayed | 17426 | 35913.7 | 96993.3 | 106430.5 | 109294.9 |
| run | token | 4082 | 111.1 | 272.4 | 30001.2 | 30001.2 |
| run | txn | 10296 | 2.1 | 3.7 | 6.6 | 40.0 |
| run | typing | 505245 | 1.0 | 2.8 | 55.6 | 21134.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 243275 | 253499 | 104.20 |
| member | 243275 | 247550 | 101.76 |
| typing | 505492 | 505245 | 99.95 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5772 / 4581 | 85.29 / 99.79 / 122.64 / 179.24 | 85.17 / 122.48 | 68464 | 5603 | 21950 | 0 of 4581 | 70 | 105.53 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3394 of 3394 | 100.0 | 507.3 / 717.3 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 3941 | 0.0 | 0.0 / 0.0 | 17.7 / 5.3 |
| 1 | public.room_messages | 2646 of 3418 | 77.4 | 15.1 / 32.6 | 110.3 / 96.5 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1147 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5772 | 5437 |
| holds.constants | 5772 | 5437 |
| holds.counted | 5970 | 5635 |
| holds.routed | 40404 | 37432 |
| holds.routed_cap | 46176 | 43486 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40404 | 37432 |
| subscriptions.constants | 11742 | 10974 |
| subscriptions.routed | 40404 | 37433 |
| subscriptions.routed_cap | 59172 | 54868 |
| subscriptions.subscriptions | 40404 | 37433 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 197.3 | 184.4 |
| memory floor MiB | 239.0 | 226.8 |

Live heap after each collection (MiB): 300 s 197.3, 305 s 191.1, 315 s 188.9, 325 s 194.1, 340 s 190.1, 350 s 190.8, 360 s 192.5, 370 s 190.2, 385 s 210.1, 390 s 192.2, 395 s 189.1, 405 s 191.4, 415 s 191.5, 430 s 189.4, 445 s 190.5, 450 s 189.3, 465 s 189.4, 475 s 195.1, 485 s 189.1, 500 s 189.7, 510 s 191.2, 520 s 189.7, 535 s 191.5, 540 s 189.3, 550 s 124.4, 590 s 210.0, 605 s 227.7, 607 s 236.0, 611 s 227.3, 615 s 216.5, 620 s 245.2, 625 s 225.2, 630 s 222.8, 635 s 196.2, 640 s 165.5, 645 s 187.2, 650 s 176.1, 655 s 200.4, 660 s 185.1, 665 s 183.4, 675 s 187.0, 680 s 186.4, 685 s 187.4, 695 s 186.4, 705 s 186.2, 715 s 192.5, 720 s 185.4, 735 s 186.5, 745 s 186.8, 750 s 186.2, 760 s 175.1, 770 s 178.3, 775 s 187.2, 785 s 184.4.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 2646 |
| msg_gap_after_resume | 70 |
| msg_gap_unresumed | 1121 |
| open_failed:connection_refused | 22174 |
| probe_failed | 52 |
| publish_failed:connection_refused | 1 |
| publish_failed:http_403:channel_not_subscribed | 19 |
| publish_failed:unexpected_eof | 1 |
| refused_capacity:http_503:too_many_streams | 70783 |
| resubscribe | 24503 |
| resubscribe_failed | 1225 |
| resumed_false:public.room_members | 18748 |
| resumed_false:public.room_messages | 1425 |
| resumed_true:public.notifications | 4047 |
| resumed_true:public.room_messages | 2646 |
| stream_end:unexpected_eof | 5772 |
| sub_error:hold_check_unavailable | 66 |
| sub_error:issuer_unavailable | 547 |
| sub_error:shape_not_authorized | 21 |
| sub_forgotten | 635 |
| sub_ok | 89655 |
| sub_refused:channel_hook_unavailable | 1278 |
| sub_refused:duplicate_sub | 610 |
| sub_refused:issuer_unavailable | 21403 |
| sub_refused:shape_not_authorized | 4 |
| token_failed:timeout | 96 |
| token_failed:unexpected_eof | 3 |
| token_ok | 3983 |
| token_revoked_subscriptions | 13 |
| warning:replica_identity_insufficient | 11227 |
