# Sluice load test — reconnect — refill-batch-oldadm

- Target: `sluice:refill-batch-oldadm`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T15:46:27Z, finished 2026-10-16T15:59:32Z, seed 4926888568196519720
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1270 (2190) | 1160 | 35.56 | 93.3 | 102.2 | 0.12 / 0.21 | 448 | 436 / 461 | 3.92 | 0.00 | 3.50 | 0 | 208.7 (1328) | 0.11 |
| steady | 485 | 1587 (2463) | 1148 | 67.69 | 87.9 | 121.6 | 0.14 / 0.87 | 453 | 441 / 461 | 3.92 | 0.00 | 2.38 | 0 | 223.9 (1871) | 0.98 |
| run | 785 | 1466 (2463) | 1152 | 103.25 | 89.7 | 114.1 | 0.13 / 0.87 | 453 | 441 / 461 | 3.92 | 0.00 | 3.50 | 0 | 218.1 (1871) | 0.98 |

Arrival: 300.1 s to open every stream (complete: false); 5.94 ms of Sluice CPU per open; 64.7 KiB of live heap per stream after arrival.

Opens refused for capacity: 33729 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 93273 | 3.8 | 7.7 | 13.6 | 46.9 |
| arrival | message | 93273 | 3.8 | 7.6 | 13.6 | 46.9 |
| arrival | message_commit_to_client | 93273 | 3.4 | 7.2 | 13.1 | 46.5 |
| arrival | message_write_to_commit | 93273 | 0.3 | 0.5 | 0.9 | 8.2 |
| arrival | open | 5775 | 46.8 | 57.6 | 62.2 | 113.3 |
| arrival | publish | 6262 | 0.9 | 2.0 | 3.6 | 62.0 |
| arrival | token | 1512 | 46.3 | 57.1 | 66.0 | 133.9 |
| arrival | txn | 3961 | 2.2 | 4.0 | 7.3 | 19.7 |
| arrival | typing | 194482 | 1.0 | 2.4 | 3.9 | 62.9 |
| steady | member | 179141 | 3.9 | 7.6 | 17.5 | 320.2 |
| steady | message | 179141 | 3.9 | 7.6 | 17.5 | 320.2 |
| steady | message_commit_to_client | 179141 | 3.5 | 7.2 | 17.3 | 319.6 |
| steady | message_write_to_commit | 179141 | 0.3 | 0.5 | 0.7 | 2.7 |
| steady | open | 5699 | 242.7 | 1695.7 | 1761.3 | 1803.3 |
| steady | publish | 12022 | 0.9 | 2.0 | 6.4 | 68.8 |
| steady | replayed | 9657 | 13041.7 | 34865.2 | 46399.5 | 85593.3 |
| steady | token | 3078 | 46.8 | 58.6 | 97.8 | 581.8 |
| steady | txn | 6336 | 2.1 | 3.6 | 6.3 | 19.9 |
| steady | typing | 401944 | 1.0 | 2.5 | 6.8 | 70.7 |
| run | member | 272414 | 3.8 | 7.6 | 15.8 | 320.2 |
| run | message | 272414 | 3.8 | 7.6 | 15.8 | 320.2 |
| run | message_commit_to_client | 272414 | 3.5 | 7.2 | 15.4 | 319.6 |
| run | message_write_to_commit | 272414 | 0.3 | 0.5 | 0.8 | 8.2 |
| run | open | 11474 | 51.5 | 1630.2 | 1744.9 | 1803.3 |
| run | publish | 18284 | 0.9 | 2.0 | 4.9 | 68.8 |
| run | replayed | 9657 | 13041.7 | 34865.2 | 46399.5 | 85593.3 |
| run | token | 4590 | 46.3 | 58.1 | 89.6 | 581.8 |
| run | txn | 10297 | 2.1 | 3.8 | 6.8 | 19.9 |
| run | typing | 596426 | 1.0 | 2.5 | 5.8 | 70.7 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 272415 | 281973 | 103.51 |
| member | 272415 | 272512 | 100.04 |
| typing | 596425 | 596426 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5775 / 5489 | 9.23 / 27.39 / 42.86 / 104.59 | 9.07 / 42.66 | 40518 | 1871 | 0 | 0 of 5489 | 0 | 31.33 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5489 of 5489 | 100.0 | 493.6 / 642.8 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5489 | 0.0 | 0.0 / 0.0 | 16.8 / 5.1 |
| 1 | public.room_messages | 5461 of 5489 | 99.5 | 16.7 / 85.5 | 108.5 / 86.1 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1150 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5775 | 5699 |
| holds.constants | 5775 | 5699 |
| holds.counted | 5973 | 5897 |
| holds.routed | 40425 | 39893 |
| holds.routed_cap | 46200 | 45592 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40425 | 39893 |
| subscriptions.constants | 11748 | 11596 |
| subscriptions.routed | 40425 | 39893 |
| subscriptions.routed_cap | 59199 | 58515 |
| subscriptions.subscriptions | 40425 | 39893 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.1 | 190.9 |
| memory floor MiB | 233.3 | 232.2 |

Live heap after each collection (MiB): 300 s 192.1, 315 s 192.2, 330 s 194.9, 340 s 191.6, 355 s 196.4, 365 s 190.0, 380 s 189.0, 390 s 191.8, 405 s 189.4, 415 s 193.2, 425 s 189.3, 445 s 189.3, 450 s 189.3, 465 s 190.0, 480 s 192.5, 495 s 190.4, 505 s 195.8, 515 s 190.0, 530 s 189.2, 540 s 192.8, 550 s 113.1, 555 s 181.6, 565 s 169.2, 570 s 168.5, 575 s 184.7, 585 s 191.2, 595 s 206.5, 600 s 191.6, 610 s 188.2, 620 s 188.9, 630 s 191.4, 635 s 188.2, 650 s 189.8, 660 s 190.5, 675 s 189.7, 685 s 193.0, 695 s 190.1, 710 s 190.4, 720 s 193.6, 730 s 189.3, 745 s 188.9, 750 s 191.9, 765 s 189.6, 775 s 197.6, 780 s 190.9.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5461 |
| open_failed:connection_refused | 22261 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 33729 |
| resumed_false:public.room_members | 27445 |
| resumed_false:public.room_messages | 28 |
| resumed_true:public.notifications | 5489 |
| resumed_true:public.room_messages | 5461 |
| stream_end:unexpected_eof | 5775 |
| sub_ok | 91792 |
| token_ok | 4590 |
| warning:replica_identity_insufficient | 11474 |
