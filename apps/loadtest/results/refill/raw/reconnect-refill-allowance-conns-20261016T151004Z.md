# Sluice load test — reconnect — refill-allowance-conns

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T14:52:50Z, finished 2026-10-16T15:10:04Z, seed 3066924447684567113
- **Verdict: FAIL: message p95 103285 ms > 500ms; member p95 102236 ms > 500ms; Go memory peaked at 898 MiB, GOMEMLIMIT is 461 MiB; backlog grew from 0.0 to 12.7 MiB over the steady phase**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1289 (2366) | 1160 | 51.09 | 132.1 | 146.8 | 0.17 / 0.33 | 452 | 441 / 461 | 0.20 | 3.42 | 3.50 | 0 | 355.1 (1731) | 0.15 |
| steady | 722 | 589 (2369) | 515 | 250.05 | 588.0 | 672.3 | 0.35 / 11.28 | 494 | 898 / 461 | 41.98 | 26.47 | 56.50 | 0 | 134.4 (3249) | 15.42 |
| run | 1022 | 794 (2369) | 704 | 301.14 | 370.8 | 418.2 | 0.29 / 11.28 | 494 | 898 / 461 | 41.98 | 26.47 | 56.50 | 0 | 199.2 (3249) | 15.42 |

Arrival: 300.0 s to open every stream (complete: false); 8.55 ms of Sluice CPU per open; 67.0 KiB of live heap per stream after arrival.

Opens refused for capacity: 22461 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92674 | 3.8 | 7.1 | 27.3 | 215.3 |
| arrival | message | 92674 | 3.8 | 7.1 | 27.3 | 212.9 |
| arrival | message_commit_to_client | 92674 | 3.5 | 6.6 | 27.0 | 212.5 |
| arrival | message_write_to_commit | 92674 | 0.3 | 0.6 | 0.9 | 3.8 |
| arrival | open | 5763 | 45.8 | 56.6 | 61.7 | 355.5 |
| arrival | publish | 6359 | 0.9 | 1.8 | 4.0 | 153.0 |
| arrival | token | 1592 | 111.1 | 134.1 | 166.9 | 416.3 |
| arrival | txn | 3960 | 2.2 | 3.5 | 5.9 | 17.4 |
| arrival | typing | 201503 | 1.0 | 2.2 | 4.7 | 155.4 |
| steady | member | 100291 | 4.0 | 102236.2 | 295698.4 | 377753.2 |
| steady | message | 100403 | 4.0 | 103284.7 | 316670.0 | 377751.3 |
| steady | message_commit_to_client | 100403 | 3.6 | 102236.2 | 316670.0 | 377750.8 |
| steady | message_write_to_commit | 100403 | 0.3 | 0.7 | 61.2 | 2584.9 |
| steady | open | 5219 | 7307.3 | 46923.8 | 135266.3 | 136879.0 |
| steady | publish | 7109 | 0.9 | 30015.5 | 31326.2 | 37692.3 |
| steady | replayed | 4754 | 104333.3 | 371195.9 | 375390.2 | 377493.8 |
| steady | token | 4041 | 21626.9 | 42729.5 | 48496.6 | 60252.9 |
| steady | txn | 8556 | 4.7 | 13303.8 | 18743.3 | 26479.7 |
| steady | typing | 219972 | 1.1 | 3.0 | 89653.2 | 189400.0 |
| run | member | 193284 | 3.9 | 25.0 | 283115.5 | 387337.4 |
| run | message | 193396 | 3.9 | 26.2 | 283115.5 | 387337.4 |
| run | message_commit_to_client | 193396 | 3.5 | 26.0 | 283115.5 | 387255.2 |
| run | message_write_to_commit | 193396 | 0.3 | 0.6 | 1.2 | 2584.9 |
| run | open | 10982 | 57.1 | 21102.6 | 129499.1 | 136879.0 |
| run | publish | 13468 | 0.9 | 6979.6 | 30539.8 | 37692.3 |
| run | replayed | 4870 | 104333.3 | 375390.2 | 383778.8 | 387135.1 |
| run | token | 5646 | 127.5 | 40108.0 | 46923.8 | 60252.9 |
| run | txn | 12516 | 2.5 | 12124.2 | 17956.9 | 26479.7 |
| run | typing | 421713 | 1.0 | 2.5 | 7045.1 | 189400.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 318203 | 196637 | 61.80 |
| member | 318203 | 194159 | 61.02 |
| typing | 420037 | 421475 | 100.34 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5770 / 5007 | 16.68 / 33.01 / 144.93 / 144.94 | 16.52 / 144.74 | 33467 | 3249 | 77799 | 0 of 5007 | 1 | 214.23 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 1441 of 1441 | 100.0 | 488.1 / 549.2 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 1675 | 0.0 | 0.0 / 0.0 | 13.7 / 5.3 |
| 1 | public.room_messages | 1440 of 1440 | 100.0 | 13.6 / 23.9 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 3 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1151 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 718 s |
| --- | ---: | ---: |
| streams | 5763 | 5212 |
| holds.constants | 5763 | 2114 |
| holds.counted | 5961 | 2312 |
| holds.routed | 40341 | 9543 |
| holds.routed_cap | 46104 | 10948 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40341 | 9543 |
| subscriptions.constants | 11724 | 3654 |
| subscriptions.routed | 40341 | 9543 |
| subscriptions.routed_cap | 59091 | 13239 |
| subscriptions.subscriptions | 40341 | 9543 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.0 | 371.1 |
| memory floor MiB | 233.2 | 516.0 |

Live heap after each collection (MiB): 300 s 192.0, 310 s 190.1, 325 s 191.1, 330 s 190.0, 345 s 191.7, 355 s 194.0, 370 s 188.6, 385 s 190.4, 390 s 189.7, 405 s 189.5, 415 s 194.6, 425 s 191.0, 435 s 190.5, 445 s 193.7, 455 s 189.6, 470 s 193.2, 475 s 192.6, 490 s 191.5, 500 s 190.5, 510 s 193.7, 520 s 190.3, 535 s 190.0, 540 s 192.3, 550 s 127.3, 662 s 322.4, 666 s 318.5, 670 s 322.7, 675 s 351.9, 680 s 338.2, 685 s 340.7, 690 s 340.4, 695 s 329.6, 701 s 329.5, 706 s 345.8, 718 s 371.1.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 1440 |
| msg_gap_after_resume | 1 |
| msg_gap_unresumed | 86 |
| open_failed:connection_refused | 22245 |
| open_failed:connection_reset | 2189 |
| open_failed:no_local_port | 5 |
| open_failed:timeout | 1270 |
| probe_failed | 37 |
| publish_failed:canceled | 66 |
| publish_failed:connection_reset | 18 |
| publish_failed:http_403:channel_not_subscribed | 122 |
| publish_failed:timeout | 482 |
| publish_failed:unexpected_eof | 1 |
| refused_capacity:http_503:too_many_streams | 22461 |
| resubscribe | 409371 |
| resubscribe_failed | 331610 |
| resumed_false:public.room_members | 6400 |
| resumed_true:public.notifications | 1441 |
| resumed_true:public.room_messages | 1440 |
| stream_end:unexpected_eof | 5770 |
| sub_error:hold_check_unavailable | 7 |
| sub_error:issuer_unavailable | 1067 |
| sub_error:shape_not_authorized | 2 |
| sub_forgotten | 2 |
| sub_ok | 59531 |
| sub_refused:channel_hook_unavailable | 7117 |
| sub_refused:issuer_unavailable | 77174 |
| token_failed:connection_reset | 59 |
| token_failed:no_local_port | 2 |
| token_failed:timeout | 2182 |
| token_ok | 3390 |
| token_revoked_subscriptions | 76 |
| warning:replica_identity_insufficient | 7340 |
