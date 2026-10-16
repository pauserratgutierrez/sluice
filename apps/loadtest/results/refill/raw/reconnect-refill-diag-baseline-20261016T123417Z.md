# Sluice load test — reconnect — refill-diag-baseline

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T12:19:59Z, finished 2026-10-16T12:34:17Z, seed 2592679815100534097
- **Verdict: FAIL: typing p95 2736 ms > 500ms; Go memory peaked at 700 MiB, GOMEMLIMIT is 461 MiB; backlog grew from 0.0 to 11.2 MiB over the steady phase**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1248 (2279) | 1160 | 65.27 | 174.2 | 187.5 | 0.22 / 0.44 | 449 | 437 / 461 | 1.69 | 0.00 | 3.47 | 0 | 354.1 (1679) | 0.27 |
| steady | 558 | 790 (2920) | 573 | 360.46 | 818.7 | 1128.7 | 0.65 / 27.38 | 494 | 700 / 461 | 54.63 | 47.55 | 47.55 | 0 | 210.8 (4257) | 1.70 |
| run | 858 | 950 (2920) | 778 | 425.73 | 522.4 | 637.9 | 0.50 / 27.38 | 494 | 700 / 461 | 54.63 | 47.55 | 47.55 | 0 | 260.9 (4257) | 1.70 |

Arrival: 300.1 s to open every stream (complete: false); 11.02 ms of Sluice CPU per open; 66.8 KiB of live heap per stream after arrival.

Opens refused for capacity: 56690 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92636 | 3.9 | 9.5 | 61.7 | 316.6 |
| arrival | message | 92636 | 3.9 | 9.5 | 60.7 | 316.6 |
| arrival | message_commit_to_client | 92636 | 3.5 | 8.8 | 60.2 | 314.8 |
| arrival | message_write_to_commit | 92636 | 0.4 | 0.8 | 1.3 | 7.7 |
| arrival | open | 5771 | 47.9 | 59.6 | 85.5 | 229.6 |
| arrival | publish | 6113 | 0.9 | 1.8 | 18.0 | 69.0 |
| arrival | token | 1563 | 111.1 | 134.1 | 226.3 | 403.3 |
| arrival | txn | 3961 | 2.2 | 4.6 | 25.5 | 314.7 |
| arrival | typing | 189327 | 1.0 | 2.5 | 23.2 | 79.9 |
| steady | member | 96357 | 5.0 | 41.7 | 128450.6 | 281856.9 |
| steady | message | 96366 | 5.0 | 41.7 | 125304.8 | 280956.3 |
| steady | message_commit_to_client | 96366 | 4.5 | 40.7 | 125304.8 | 280956.0 |
| steady | message_write_to_commit | 96366 | 0.4 | 0.8 | 1.2 | 32.1 |
| steady | open | 3591 | 5013.5 | 115867.6 | 123207.7 | 302039.5 |
| steady | publish | 7960 | 1.2 | 3850.2 | 18743.3 | 30001.0 |
| steady | replayed | 4738 | 13959.2 | 125304.8 | 278921.2 | 324958.9 |
| steady | token | 2571 | 130.6 | 30015.5 | 30015.5 | 30017.3 |
| steady | txn | 7242 | 2.4 | 4.0 | 6.1 | 51.3 |
| steady | typing | 242836 | 1.6 | 2736.1 | 15663.1 | 31166.8 |
| run | member | 188993 | 4.4 | 14.5 | 45350.9 | 281856.9 |
| run | message | 189002 | 4.4 | 14.5 | 44826.6 | 280956.3 |
| run | message_commit_to_client | 189002 | 4.0 | 13.8 | 44826.6 | 280956.0 |
| run | message_write_to_commit | 189002 | 0.4 | 0.8 | 1.3 | 32.1 |
| run | open | 9362 | 54.0 | 23199.7 | 116916.2 | 302039.5 |
| run | publish | 14073 | 1.0 | 2146.3 | 13697.0 | 30001.0 |
| run | replayed | 4738 | 13959.2 | 125304.8 | 278921.2 | 324958.9 |
| run | token | 4180 | 119.3 | 30015.5 | 30015.5 | 30017.3 |
| run | txn | 11203 | 2.4 | 4.1 | 12.6 | 314.7 |
| run | typing | 432163 | 1.3 | 962.6 | 8454.1 | 31166.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 252479 | 193432 | 76.61 |
| member | 252479 | 189301 | 74.98 |
| typing | 433277 | 432163 | 99.74 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5771 / 3405 | 13.59 / 39.02 / 132.34 / 137.62 | 13.38 / 132.12 | 48518 | 4257 | 27142 | 0 of 3405 | 0 | 11.83 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 2381 of 2381 | 100.0 | 490.7 / 552.2 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 2548 | 0.0 | 0.0 / 0.0 | 15.4 / 5.4 |
| 1 | public.room_messages | 2384 of 2384 | 100.0 | 15.0 / 31.7 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1140 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 850 s |
| --- | ---: | ---: |
| streams | 5771 | 3884 |
| holds.constants | 5771 | 3841 |
| holds.counted | 5969 | 4039 |
| holds.routed | 40397 | 21212 |
| holds.routed_cap | 46168 | 28968 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40397 | 21212 |
| subscriptions.constants | 11740 | 6815 |
| subscriptions.routed | 40397 | 20079 |
| subscriptions.routed_cap | 59163 | 30341 |
| subscriptions.subscriptions | 40397 | 20079 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 194.4 | 294.0 |
| memory floor MiB | 235.7 | 395.6 |

Live heap after each collection (MiB): 300 s 194.4, 310 s 191.5, 315 s 190.7, 325 s 210.2, 330 s 195.1, 340 s 193.1, 345 s 189.3, 355 s 212.1, 360 s 195.4, 370 s 193.1, 375 s 192.3, 385 s 207.8, 390 s 193.4, 400 s 195.9, 410 s 191.0, 415 s 201.2, 420 s 193.5, 430 s 193.0, 440 s 190.3, 445 s 201.2, 450 s 193.9, 460 s 192.0, 470 s 190.6, 475 s 196.0, 485 s 190.9, 495 s 190.8, 505 s 193.6, 515 s 191.0, 530 s 191.4, 540 s 199.2, 550 s 109.8, 555 s 106.1, 828 s 350.0, 841 s 303.5, 846 s 306.1, 850 s 294.0.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 2399 |
| msg_gap_unresumed | 179 |
| open_failed:connection_refused | 22324 |
| pprof_failed:goroutine | 1 |
| probe_failed | 47 |
| publish_failed:canceled | 8 |
| publish_failed:http_403:channel_not_subscribed | 51 |
| publish_failed:timeout | 8 |
| refused_capacity:http_503:too_many_streams | 56690 |
| resubscribe | 80592 |
| resubscribe_failed | 21080 |
| resumed_false:public.room_members | 12102 |
| resumed_true:public.notifications | 2387 |
| resumed_true:public.room_messages | 2399 |
| stream_end:unexpected_eof | 5775 |
| sub_error:hold_check_unavailable | 124 |
| sub_error:issuer_unavailable | 888 |
| sub_forgotten | 3723 |
| sub_ok | 67449 |
| sub_refused:channel_hook_unavailable | 3026 |
| sub_refused:duplicate_sub | 3723 |
| sub_refused:issuer_unavailable | 56672 |
| token_failed:timeout | 999 |
| token_failed:unexpected_eof | 1 |
| token_ok | 3134 |
| token_revoked_subscriptions | 37 |
| warning:replica_identity_insufficient | 8284 |
