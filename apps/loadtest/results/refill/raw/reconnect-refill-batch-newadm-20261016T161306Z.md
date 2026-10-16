# Sluice load test — reconnect — refill-batch-newadm

- Target: `sluice:refill-batch-newadm`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T16:00:00Z, finished 2026-10-16T16:13:06Z, seed 16528354336769905296
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1294 (2364) | 1160 | 38.76 | 99.8 | 111.3 | 0.13 / 0.26 | 450 | 439 / 461 | 0.24 | 0.00 | 3.52 | 0 | 208.8 (1333) | 0.14 |
| steady | 485 | 1636 (2466) | 1148 | 65.45 | 82.5 | 117.6 | 0.13 / 0.85 | 452 | 441 / 461 | 1.86 | 0.00 | 1.84 | 0 | 226.3 (1617) | 1.05 |
| run | 785 | 1506 (2466) | 1153 | 104.21 | 88.2 | 115.2 | 0.13 / 0.85 | 452 | 441 / 461 | 1.86 | 0.00 | 3.52 | 0 | 219.6 (1617) | 1.05 |

Arrival: 300.1 s to open every stream (complete: false); 6.47 ms of Sluice CPU per open; 64.8 KiB of live heap per stream after arrival.

Opens refused for capacity: 30201 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92821 | 3.7 | 7.8 | 20.4 | 188.6 |
| arrival | message | 92821 | 3.7 | 7.8 | 20.4 | 178.4 |
| arrival | message_commit_to_client | 92821 | 3.3 | 7.3 | 19.8 | 178.0 |
| arrival | message_write_to_commit | 92821 | 0.3 | 0.6 | 0.8 | 2.1 |
| arrival | open | 5757 | 46.8 | 58.1 | 67.1 | 324.9 |
| arrival | publish | 6447 | 0.8 | 1.9 | 9.7 | 57.8 |
| arrival | token | 1591 | 46.3 | 57.6 | 66.0 | 276.0 |
| arrival | txn | 3962 | 2.1 | 3.5 | 6.3 | 19.3 |
| arrival | typing | 202760 | 1.0 | 2.4 | 8.8 | 57.8 |
| steady | member | 180397 | 3.8 | 8.2 | 25.2 | 740.4 |
| steady | message | 180398 | 3.8 | 8.2 | 25.2 | 994.5 |
| steady | message_commit_to_client | 180398 | 3.4 | 7.7 | 24.7 | 994.3 |
| steady | message_write_to_commit | 180398 | 0.3 | 0.5 | 0.7 | 7.9 |
| steady | open | 5761 | 370.7 | 1515.5 | 1548.3 | 1605.7 |
| steady | publish | 12346 | 0.9 | 2.0 | 9.2 | 67.1 |
| steady | replayed | 9410 | 11862.0 | 23986.2 | 26345.5 | 28781.6 |
| steady | token | 3053 | 46.8 | 58.1 | 97.8 | 1032.7 |
| steady | txn | 6336 | 2.1 | 3.7 | 6.5 | 74.7 |
| steady | typing | 423432 | 1.0 | 2.6 | 11.3 | 116.8 |
| run | member | 273218 | 3.8 | 8.0 | 21.9 | 740.4 |
| run | message | 273219 | 3.8 | 8.0 | 21.9 | 994.5 |
| run | message_commit_to_client | 273219 | 3.4 | 7.6 | 21.6 | 994.3 |
| run | message_write_to_commit | 273219 | 0.3 | 0.5 | 0.8 | 7.9 |
| run | open | 11518 | 53.5 | 1466.4 | 1531.9 | 1605.7 |
| run | publish | 18793 | 0.9 | 2.0 | 9.5 | 67.1 |
| run | replayed | 9410 | 11862.0 | 23986.2 | 26345.5 | 28781.6 |
| run | token | 4644 | 46.3 | 58.1 | 89.6 | 1032.7 |
| run | txn | 10298 | 2.1 | 3.6 | 6.4 | 74.7 |
| run | typing | 626192 | 1.0 | 2.6 | 10.4 | 116.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 273210 | 282488 | 103.40 |
| member | 273210 | 273359 | 100.05 |
| typing | 626191 | 626192 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5761 / 5526 | 9.26 / 12.80 / 14.17 / 14.44 | 9.12 / 14.05 | 36167 | 1617 | 0 | 0 of 5526 | 0 | 29.91 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5526 of 5526 | 100.0 | 492.6 / 553.9 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5526 | 0.0 | 0.0 / 0.0 | 16.9 / 5.1 |
| 1 | public.room_messages | 5526 of 5526 | 100.0 | 16.9 / 28.7 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1145 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5757 | 5757 |
| holds.constants | 5757 | 5757 |
| holds.counted | 5955 | 5955 |
| holds.routed | 40299 | 40299 |
| holds.routed_cap | 46056 | 46056 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40299 | 40299 |
| subscriptions.constants | 11712 | 11712 |
| subscriptions.routed | 40299 | 40299 |
| subscriptions.routed_cap | 59037 | 59037 |
| subscriptions.subscriptions | 40299 | 40299 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 190.6 | 193.1 |
| memory floor MiB | 231.7 | 235.4 |

Live heap after each collection (MiB): 300 s 190.6, 305 s 189.8, 325 s 191.2, 335 s 189.4, 350 s 190.3, 360 s 191.6, 375 s 191.3, 390 s 191.0, 400 s 190.0, 415 s 192.1, 430 s 189.6, 445 s 191.0, 455 s 190.0, 470 s 191.7, 480 s 191.6, 495 s 188.9, 505 s 191.0, 520 s 189.5, 535 s 191.7, 550 s 119.8, 555 s 184.0, 560 s 190.9, 565 s 193.3, 575 s 192.9, 585 s 189.7, 595 s 204.9, 600 s 192.1, 610 s 191.8, 620 s 191.2, 625 s 192.3, 630 s 190.1, 645 s 191.3, 655 s 192.6, 670 s 192.8, 680 s 193.1, 690 s 194.0, 700 s 191.0, 715 s 192.5, 720 s 190.4, 735 s 191.0, 745 s 198.5, 750 s 190.2, 765 s 190.6, 775 s 193.1.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5526 |
| open_failed:connection_refused | 22185 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 30201 |
| resumed_false:public.room_members | 27630 |
| resumed_true:public.notifications | 5526 |
| resumed_true:public.room_messages | 5526 |
| stream_end:unexpected_eof | 5761 |
| sub_ok | 92144 |
| token_failed:unexpected_eof | 1 |
| token_ok | 4643 |
| warning:replica_identity_insufficient | 11518 |
