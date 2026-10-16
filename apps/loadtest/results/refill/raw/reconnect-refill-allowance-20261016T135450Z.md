# Sluice load test — reconnect — refill-allowance

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T13:41:45Z, finished 2026-10-16T13:54:50Z, seed 8441881227141852621
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1288 (2529) | 1165 | 43.76 | 113.2 | 125.1 | 0.15 / 0.28 | 454 | 442 / 461 | 0.29 | 3.43 | 3.51 | 0 | 356.1 (1703) | 0.12 |
| steady | 485 | 1580 (3552) | 1148 | 97.67 | 127.5 | 175.4 | 0.20 / 1.07 | 463 | 453 / 461 | 55.03 | 0.50 | 2.57 | 0 | 334.0 (4320) | 1.05 |
| run | 785 | 1468 (3552) | 1155 | 141.43 | 122.7 | 156.0 | 0.18 / 1.07 | 463 | 453 / 461 | 55.03 | 3.43 | 3.51 | 0 | 342.4 (4320) | 1.05 |

Arrival: 300.1 s to open every stream (complete: false); 7.31 ms of Sluice CPU per open; 67.9 KiB of live heap per stream after arrival.

Opens refused for capacity: 33873 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 93523 | 3.6 | 7.2 | 13.2 | 78.3 |
| arrival | message | 93523 | 3.6 | 7.2 | 13.2 | 78.3 |
| arrival | message_commit_to_client | 93523 | 3.3 | 6.8 | 12.7 | 77.8 |
| arrival | message_write_to_commit | 93523 | 0.3 | 0.5 | 0.8 | 2.1 |
| arrival | open | 5767 | 45.8 | 56.1 | 60.2 | 105.1 |
| arrival | publish | 6222 | 0.8 | 1.8 | 6.4 | 68.1 |
| arrival | token | 1605 | 109.1 | 132.1 | 162.8 | 265.6 |
| arrival | txn | 3960 | 2.0 | 3.5 | 6.0 | 20.8 |
| arrival | typing | 199490 | 0.9 | 2.4 | 6.1 | 69.8 |
| steady | member | 176492 | 3.8 | 14.1 | 362.5 | 12839.8 |
| steady | message | 176535 | 3.8 | 14.1 | 362.5 | 13709.5 |
| steady | message_commit_to_client | 176535 | 3.5 | 13.9 | 362.5 | 13709.2 |
| steady | message_write_to_commit | 176535 | 0.3 | 0.5 | 0.6 | 3.1 |
| steady | open | 5783 | 5472.3 | 17694.7 | 17956.9 | 18228.7 |
| steady | publish | 11716 | 0.9 | 3.2 | 179.2 | 4083.9 |
| steady | replayed | 13769 | 17170.4 | 43778.0 | 47972.4 | 53244.2 |
| steady | token | 2925 | 110.1 | 164.9 | 11993.1 | 20691.3 |
| steady | txn | 6336 | 2.0 | 3.6 | 6.1 | 26.0 |
| steady | typing | 399483 | 1.0 | 3.2 | 78.3 | 4083.6 |
| run | member | 270015 | 3.7 | 9.0 | 195.6 | 12839.8 |
| run | message | 270058 | 3.7 | 9.0 | 209.9 | 13709.5 |
| run | message_commit_to_client | 270058 | 3.4 | 8.6 | 209.9 | 13709.2 |
| run | message_write_to_commit | 270058 | 0.3 | 0.5 | 0.7 | 3.1 |
| run | open | 11550 | 56.6 | 16580.6 | 17956.9 | 18228.7 |
| run | publish | 17938 | 0.9 | 2.7 | 70.1 | 4083.9 |
| run | replayed | 13769 | 17170.4 | 43778.0 | 47972.4 | 53244.2 |
| run | token | 4530 | 110.1 | 140.3 | 8224.8 | 20691.3 |
| run | txn | 10296 | 2.0 | 3.5 | 6.1 | 26.0 |
| run | typing | 598973 | 1.0 | 3.0 | 54.5 | 4083.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 270059 | 282783 | 104.71 |
| member | 270059 | 271059 | 100.37 |
| typing | 599001 | 598973 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5783 / 5561 | 16.25 / 36.24 / 39.03 / 39.66 | 16.07 / 38.93 | 41490 | 4320 | 2496 | 0 of 5561 | 0 | 57.10 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5183 of 5183 | 100.0 | 497.8 / 578.3 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5561 | 0.0 | 0.0 / 0.0 | 19.3 / 5.5 |
| 1 | public.room_messages | 5188 of 5188 | 100.0 | 19.5 / 53.2 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1146 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5767 | 5767 |
| holds.constants | 5767 | 5767 |
| holds.counted | 5965 | 5965 |
| holds.routed | 40369 | 40369 |
| holds.routed_cap | 46136 | 46136 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40369 | 40369 |
| subscriptions.constants | 11732 | 11732 |
| subscriptions.routed | 40369 | 40369 |
| subscriptions.routed_cap | 59127 | 59127 |
| subscriptions.subscriptions | 40369 | 40369 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 191.6 | 202.3 |
| memory floor MiB | 233.1 | 245.5 |

Live heap after each collection (MiB): 300 s 191.6, 310 s 190.0, 325 s 199.8, 330 s 191.7, 335 s 188.6, 345 s 190.1, 350 s 192.5, 355 s 192.2, 365 s 191.5, 380 s 191.5, 390 s 192.4, 400 s 189.2, 415 s 190.2, 420 s 190.4, 435 s 190.3, 445 s 194.6, 455 s 190.6, 470 s 190.3, 480 s 195.0, 490 s 190.2, 505 s 193.5, 510 s 196.7, 520 s 192.4, 530 s 189.0, 535 s 192.3, 550 s 177.6, 555 s 198.1, 560 s 199.7, 565 s 210.3, 570 s 215.7, 575 s 225.9, 580 s 218.7, 585 s 215.4, 590 s 215.4, 595 s 218.0, 600 s 217.2, 605 s 216.3, 610 s 217.2, 615 s 215.6, 625 s 219.1, 630 s 218.5, 635 s 217.1, 640 s 216.9, 645 s 216.9, 650 s 215.8, 655 s 219.7, 660 s 216.4, 665 s 217.6, 670 s 215.8, 675 s 215.9, 680 s 215.3, 685 s 218.2, 690 s 213.4, 695 s 201.8, 700 s 200.9, 705 s 198.0, 715 s 197.4, 720 s 201.3, 725 s 198.6, 730 s 198.4, 735 s 203.4, 740 s 201.7, 745 s 200.0, 750 s 199.5, 755 s 196.5, 765 s 197.3, 770 s 197.3, 775 s 204.0, 780 s 202.3.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5188 |
| msg_gap_unresumed | 373 |
| open_failed:connection_refused | 22351 |
| probe_failed | 52 |
| publish_failed:connection_refused | 1 |
| refused_capacity:http_503:too_many_streams | 33873 |
| resubscribe | 2572 |
| resumed_false:public.room_members | 26222 |
| resumed_true:public.notifications | 5183 |
| resumed_true:public.room_messages | 5188 |
| stream_end:unexpected_eof | 5783 |
| sub_error:issuer_unavailable | 3 |
| sub_ok | 92403 |
| sub_refused:channel_hook_unavailable | 76 |
| sub_refused:issuer_unavailable | 2493 |
| token_failed:unexpected_eof | 1 |
| token_ok | 4529 |
| token_revoked_subscriptions | 3 |
| warning:replica_identity_insufficient | 11550 |
