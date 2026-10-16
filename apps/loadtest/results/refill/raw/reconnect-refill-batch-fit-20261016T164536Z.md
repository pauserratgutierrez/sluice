# Sluice load test — reconnect — refill-batch-fit

- Target: `sluice:refill-batch-fit`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T16:32:30Z, finished 2026-10-16T16:45:36Z, seed 15403064273799724486
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1288 (2191) | 1170 | 37.27 | 96.4 | 106.2 | 0.12 / 0.25 | 450 | 439 / 461 | 1.64 | 3.43 | 3.51 | 0 | 209.3 (1349) | 0.13 |
| steady | 485 | 1630 (2463) | 1148 | 67.10 | 84.9 | 120.5 | 0.14 / 0.86 | 454 | 442 / 461 | 50.54 | 0.00 | 2.02 | 0 | 226.5 (1725) | 1.11 |
| run | 785 | 1499 (2463) | 1156 | 104.37 | 88.7 | 115.0 | 0.13 / 0.86 | 454 | 442 / 461 | 50.54 | 3.43 | 3.51 | 0 | 220.0 (1725) | 1.11 |

Arrival: 300.1 s to open every stream (complete: false); 6.22 ms of Sluice CPU per open; 64.5 KiB of live heap per stream after arrival.

Opens refused for capacity: 27923 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 93171 | 3.8 | 7.5 | 18.0 | 109.3 |
| arrival | message | 93171 | 3.8 | 7.5 | 18.0 | 109.3 |
| arrival | message_commit_to_client | 93171 | 3.4 | 7.1 | 17.8 | 108.9 |
| arrival | message_write_to_commit | 93171 | 0.3 | 0.6 | 0.9 | 3.0 |
| arrival | open | 5773 | 46.8 | 57.6 | 62.2 | 98.0 |
| arrival | publish | 6283 | 0.9 | 1.8 | 3.5 | 63.3 |
| arrival | token | 1606 | 46.3 | 57.1 | 61.7 | 120.8 |
| arrival | txn | 3960 | 2.2 | 3.9 | 6.7 | 20.0 |
| arrival | typing | 200255 | 1.0 | 2.3 | 6.8 | 72.3 |
| steady | member | 180832 | 3.9 | 8.3 | 28.0 | 796.0 |
| steady | message | 180832 | 3.9 | 8.3 | 28.0 | 796.0 |
| steady | message_commit_to_client | 180832 | 3.5 | 7.7 | 27.5 | 795.6 |
| steady | message_write_to_commit | 180832 | 0.3 | 0.5 | 0.8 | 14.1 |
| steady | open | 5773 | 741.4 | 1712.1 | 1761.3 | 1817.7 |
| steady | publish | 12222 | 0.9 | 2.0 | 13.4 | 65.9 |
| steady | replayed | 9495 | 12255.2 | 23986.2 | 26345.5 | 28942.6 |
| steady | token | 2993 | 46.8 | 58.1 | 100.9 | 1259.6 |
| steady | txn | 6336 | 2.1 | 4.0 | 7.4 | 20.2 |
| steady | typing | 419386 | 1.1 | 2.7 | 14.9 | 80.1 |
| run | member | 274003 | 3.9 | 7.9 | 23.9 | 796.0 |
| run | message | 274003 | 3.9 | 7.9 | 23.9 | 796.0 |
| run | message_commit_to_client | 274003 | 3.5 | 7.5 | 23.7 | 795.6 |
| run | message_write_to_commit | 274003 | 0.3 | 0.5 | 0.8 | 14.1 |
| run | open | 11546 | 54.5 | 1630.2 | 1744.9 | 1817.7 |
| run | publish | 18505 | 0.9 | 2.0 | 8.0 | 65.9 |
| run | replayed | 9495 | 12255.2 | 23986.2 | 26345.5 | 28942.6 |
| run | token | 4599 | 46.3 | 58.1 | 88.6 | 1259.6 |
| run | txn | 10296 | 2.1 | 3.9 | 7.1 | 20.2 |
| run | typing | 619641 | 1.0 | 2.5 | 12.0 | 80.1 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 273996 | 283312 | 103.40 |
| member | 273996 | 274189 | 100.07 |
| typing | 619629 | 619641 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5773 / 5551 | 9.43 / 12.68 / 14.00 / 14.29 | 9.31 / 13.86 | 35591 | 1725 | 0 | 0 of 5551 | 0 | 29.04 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5551 of 5551 | 100.0 | 491.7 / 553.8 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5551 | 0.0 | 0.0 / 0.0 | 16.8 / 5.5 |
| 1 | public.room_messages | 5551 of 5551 | 100.0 | 16.8 / 28.9 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1147 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5773 | 5773 |
| holds.constants | 5773 | 5773 |
| holds.counted | 5971 | 5971 |
| holds.routed | 40411 | 40411 |
| holds.routed_cap | 46184 | 46184 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40411 | 40411 |
| subscriptions.constants | 11744 | 11744 |
| subscriptions.routed | 40411 | 40411 |
| subscriptions.routed_cap | 59181 | 59181 |
| subscriptions.subscriptions | 40411 | 40411 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 189.1 | 195.4 |
| memory floor MiB | 230.3 | 237.2 |

Live heap after each collection (MiB): 300 s 189.1, 315 s 189.6, 325 s 194.1, 335 s 189.9, 345 s 191.1, 360 s 194.1, 370 s 191.3, 385 s 198.3, 395 s 192.0, 410 s 192.7, 420 s 193.6, 430 s 190.7, 445 s 200.9, 450 s 192.9, 460 s 190.2, 470 s 190.7, 475 s 192.8, 485 s 189.5, 500 s 191.5, 510 s 191.9, 525 s 190.2, 535 s 192.4, 550 s 148.9, 555 s 187.7, 560 s 191.5, 565 s 193.2, 575 s 191.7, 580 s 190.2, 595 s 191.9, 600 s 191.6, 615 s 190.4, 625 s 195.3, 635 s 192.3, 650 s 193.6, 655 s 193.4, 670 s 191.2, 680 s 191.4, 690 s 195.1, 700 s 190.8, 715 s 201.8, 720 s 193.6, 725 s 192.1, 735 s 192.8, 745 s 191.5, 760 s 192.2, 770 s 191.6, 780 s 195.4.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5551 |
| open_failed:connection_refused | 22369 |
| probe_failed | 52 |
| publish_failed:connection_refused | 1 |
| publish_failed:unexpected_eof | 1 |
| refused_capacity:http_503:too_many_streams | 27923 |
| resumed_false:public.room_members | 27755 |
| resumed_true:public.notifications | 5551 |
| resumed_true:public.room_messages | 5551 |
| stream_end:unexpected_eof | 5773 |
| sub_ok | 92368 |
| token_ok | 4599 |
| warning:replica_identity_insufficient | 11546 |
