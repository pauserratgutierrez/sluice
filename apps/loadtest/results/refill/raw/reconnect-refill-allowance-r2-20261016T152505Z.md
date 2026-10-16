# Sluice load test — reconnect — refill-allowance-r2

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T15:11:59Z, finished 2026-10-16T15:25:05Z, seed 6193697428182643453
- **Verdict: FAIL: message p95 8454 ms > 500ms; member p95 8454 ms > 500ms; Go memory peaked at 539 MiB, GOMEMLIMIT is 461 MiB**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1268 (2212) | 1160 | 68.25 | 179.4 | 196.0 | 0.23 / 0.46 | 450 | 438 / 461 | 4.64 | 3.44 | 3.55 | 0 | 350.6 (1698) | 0.27 |
| steady | 485 | 1374 (3550) | 1148 | 167.20 | 250.8 | 300.3 | 0.34 / 6.38 | 495 | 539 / 461 | 50.20 | 9.56 | 10.31 | 0 | 335.9 (4546) | 1.07 |
| run | 785 | 1334 (3550) | 1153 | 235.45 | 224.9 | 260.2 | 0.30 / 6.38 | 495 | 539 / 461 | 50.20 | 9.56 | 10.31 | 0 | 341.5 (4546) | 1.07 |

Arrival: 300.0 s to open every stream (complete: false); 11.39 ms of Sluice CPU per open; 66.4 KiB of live heap per stream after arrival.

Opens refused for capacity: 48629 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92213 | 4.4 | 12.7 | 61.2 | 356.6 |
| arrival | message | 92213 | 4.4 | 12.5 | 61.2 | 356.5 |
| arrival | message_commit_to_client | 92213 | 3.9 | 11.6 | 60.2 | 352.1 |
| arrival | message_write_to_commit | 92213 | 0.4 | 1.0 | 2.0 | 8.9 |
| arrival | open | 5721 | 46.3 | 57.6 | 74.2 | 125.2 |
| arrival | publish | 6279 | 1.0 | 3.1 | 25.2 | 68.3 |
| arrival | token | 1502 | 114.2 | 140.3 | 248.8 | 471.2 |
| arrival | txn | 3960 | 2.3 | 4.1 | 7.3 | 43.7 |
| arrival | typing | 196091 | 1.3 | 5.1 | 33.0 | 79.5 |
| steady | member | 156392 | 3.9 | 8454.1 | 41156.6 | 85539.3 |
| steady | message | 156246 | 3.9 | 8454.1 | 41156.6 | 89045.9 |
| steady | message_commit_to_client | 156246 | 3.6 | 8454.1 | 41156.6 | 89045.5 |
| steady | message_write_to_commit | 156246 | 0.3 | 0.5 | 0.7 | 4.2 |
| steady | open | 5782 | 13959.2 | 91750.4 | 92799.0 | 92997.3 |
| steady | publish | 9893 | 0.9 | 25.7 | 1728.5 | 11195.7 |
| steady | replayed | 24429 | 38535.2 | 99090.4 | 111673.3 | 119269.7 |
| steady | token | 2666 | 113.2 | 27656.2 | 30001.2 | 30001.2 |
| steady | txn | 6336 | 2.1 | 3.4 | 5.3 | 25.5 |
| steady | typing | 329486 | 1.1 | 4.3 | 70.1 | 15007.3 |
| run | member | 248605 | 4.1 | 78.3 | 35913.7 | 85539.3 |
| run | message | 248459 | 4.1 | 77.3 | 35913.7 | 89045.9 |
| run | message_commit_to_client | 248459 | 3.7 | 77.3 | 35913.7 | 89045.5 |
| run | message_write_to_commit | 248459 | 0.4 | 0.7 | 1.2 | 8.9 |
| run | open | 11503 | 55.0 | 86507.5 | 92799.0 | 92997.3 |
| run | publish | 16172 | 0.9 | 6.5 | 733.2 | 11195.7 |
| run | replayed | 24429 | 38535.2 | 99090.4 | 111673.3 | 119269.7 |
| run | token | 4168 | 113.2 | 5734.4 | 30001.2 | 30001.2 |
| run | txn | 10296 | 2.2 | 3.7 | 6.4 | 43.7 |
| run | typing | 525577 | 1.1 | 4.8 | 55.0 | 15007.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 249236 | 263875 | 105.87 |
| member | 249236 | 257618 | 103.36 |
| typing | 525808 | 525577 | 99.96 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5761 / 5513 | 92.23 / 107.44 / 110.27 / 110.75 | 92.16 / 110.09 | 52409 | 4546 | 23499 | 0 of 5513 | 59 | 117.88 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 4216 of 4216 | 100.0 | 511.4 / 649.6 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5153 | 0.0 | 0.0 / 0.0 | 19.2 / 5.3 |
| 1 | public.room_messages | 3067 of 4216 | 72.7 | 16.0 / 41.9 | 114.6 / 103.2 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1140 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5721 | 5742 |
| holds.constants | 5721 | 5742 |
| holds.counted | 5919 | 5940 |
| holds.routed | 40047 | 39849 |
| holds.routed_cap | 45768 | 45932 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40047 | 39849 |
| subscriptions.constants | 11640 | 11650 |
| subscriptions.routed | 40047 | 39851 |
| subscriptions.routed_cap | 58713 | 58188 |
| subscriptions.subscriptions | 40047 | 39851 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.4 | 203.7 |
| memory floor MiB | 233.6 | 248.9 |

Live heap after each collection (MiB): 300 s 192.4, 310 s 190.7, 320 s 189.0, 325 s 197.6, 330 s 192.8, 340 s 190.1, 355 s 207.6, 360 s 191.0, 370 s 189.1, 375 s 188.8, 385 s 211.8, 390 s 190.1, 400 s 193.1, 405 s 190.0, 415 s 207.7, 420 s 191.7, 430 s 190.6, 435 s 189.0, 445 s 210.6, 450 s 191.7, 460 s 190.0, 465 s 188.2, 475 s 210.7, 480 s 192.0, 490 s 191.8, 495 s 189.9, 505 s 210.8, 510 s 193.2, 520 s 192.9, 525 s 190.4, 535 s 209.4, 540 s 193.3, 550 s 187.1, 555 s 206.4, 560 s 216.3, 567 s 220.6, 571 s 219.4, 612 s 269.8, 616 s 263.9, 620 s 251.6, 625 s 265.6, 630 s 263.6, 635 s 226.0, 640 s 226.0, 645 s 221.1, 650 s 219.5, 655 s 221.0, 660 s 216.3, 665 s 217.2, 670 s 217.4, 675 s 217.1, 680 s 218.7, 685 s 219.7, 690 s 217.8, 695 s 218.1, 700 s 218.4, 705 s 216.3, 710 s 216.3, 715 s 220.3, 720 s 216.2, 725 s 216.9, 730 s 216.2, 735 s 215.9, 740 s 217.3, 745 s 218.2, 750 s 209.8, 755 s 211.5, 760 s 212.1, 770 s 209.8, 775 s 201.8, 780 s 203.7.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3067 |
| msg_gap_after_resume | 59 |
| msg_gap_unresumed | 1282 |
| open_failed:connection_refused | 22334 |
| probe_failed | 52 |
| publish_failed:http_403:channel_not_subscribed | 19 |
| publish_failed:unexpected_eof | 2 |
| refused_capacity:http_503:too_many_streams | 48629 |
| resubscribe | 26173 |
| resubscribe_failed | 1054 |
| resumed_false:public.room_members | 20858 |
| resumed_false:public.room_messages | 1149 |
| resumed_true:public.notifications | 4216 |
| resumed_true:public.room_messages | 3067 |
| stream_end:unexpected_eof | 5761 |
| sub_error:hold_check_unavailable | 62 |
| sub_error:issuer_unavailable | 642 |
| sub_error:shape_not_authorized | 27 |
| sub_forgotten | 349 |
| sub_ok | 92388 |
| sub_refused:channel_hook_unavailable | 1575 |
| sub_refused:duplicate_sub | 320 |
| sub_refused:issuer_unavailable | 22857 |
| sub_refused:shape_not_authorized | 3 |
| token_failed:timeout | 126 |
| token_ok | 4042 |
| token_revoked_subscriptions | 46 |
| warning:replica_identity_insufficient | 11566 |
