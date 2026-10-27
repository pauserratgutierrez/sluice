# Sluice load test — reconnect — p4-refill

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T18:35:36Z, finished 2026-10-26T18:48:42Z, seed 16830075803042573206
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1285 (2334) | 1169 | 34.19 | 88.7 | 97.5 | 0.11 / 0.20 | 448 | 438 / 461 | 4.46 | 2.82 | 3.51 | 0 | 208.8 (1344) | 0.11 |
| steady | 485 | 1602 (2443) | 1148 | 70.46 | 90.7 | 126.5 | 0.15 / 0.79 | 452 | 443 / 461 | 4.46 | 0.00 | 2.54 | 0 | 226.6 (2229) | 1.31 |
| run | 785 | 1481 (2443) | 1156 | 104.65 | 90.0 | 115.3 | 0.13 / 0.79 | 452 | 443 / 461 | 4.46 | 2.82 | 3.51 | 0 | 219.8 (2229) | 1.31 |

Arrival: 300.0 s to open every stream (complete: false); 5.70 ms of Sluice CPU per open; 64.4 KiB of live heap per stream after arrival.

Opens refused for capacity: 28145 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92793 | 4.0 | 7.7 | 11.8 | 71.0 |
| arrival | message | 92793 | 4.0 | 7.7 | 11.8 | 71.0 |
| arrival | message_commit_to_client | 92793 | 3.6 | 7.1 | 11.3 | 70.7 |
| arrival | message_write_to_commit | 92793 | 0.3 | 0.6 | 1.0 | 2.4 |
| arrival | open | 5768 | 46.8 | 57.6 | 63.2 | 107.7 |
| arrival | publish | 6345 | 0.9 | 2.0 | 4.0 | 55.2 |
| arrival | token | 1521 | 45.8 | 57.1 | 61.2 | 141.6 |
| arrival | txn | 3960 | 2.5 | 4.6 | 7.5 | 69.2 |
| arrival | typing | 199891 | 1.0 | 2.6 | 4.7 | 56.3 |
| steady | member | 180538 | 4.2 | 8.0 | 18.0 | 120.2 |
| steady | message | 180538 | 4.2 | 8.0 | 18.0 | 120.1 |
| steady | message_commit_to_client | 180538 | 3.8 | 7.3 | 17.3 | 119.8 |
| steady | message_write_to_commit | 180538 | 0.4 | 0.7 | 1.1 | 18.6 |
| steady | open | 5768 | 954.4 | 1876.0 | 1990.7 | 2039.0 |
| steady | publish | 12019 | 1.0 | 2.0 | 7.0 | 74.9 |
| steady | replayed | 9446 | 12255.2 | 24510.5 | 26869.8 | 28664.9 |
| steady | token | 3061 | 46.3 | 57.6 | 92.7 | 1562.4 |
| steady | txn | 6337 | 2.6 | 4.5 | 6.2 | 22.6 |
| steady | typing | 406526 | 1.1 | 2.5 | 6.1 | 125.6 |
| run | member | 273331 | 4.1 | 7.8 | 14.7 | 120.2 |
| run | message | 273331 | 4.1 | 7.8 | 14.7 | 120.1 |
| run | message_commit_to_client | 273331 | 3.7 | 7.3 | 13.9 | 119.8 |
| run | message_write_to_commit | 273331 | 0.4 | 0.7 | 1.1 | 18.6 |
| run | open | 11536 | 55.0 | 1810.4 | 1990.7 | 2039.0 |
| run | publish | 18364 | 0.9 | 2.0 | 5.2 | 74.9 |
| run | replayed | 9446 | 12255.2 | 24510.5 | 26869.8 | 28664.9 |
| run | token | 4582 | 46.3 | 57.6 | 80.4 | 1562.4 |
| run | txn | 10297 | 2.5 | 4.5 | 6.9 | 69.2 |
| run | typing | 606417 | 1.1 | 2.5 | 5.2 | 125.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 273302 | 282777 | 103.47 |
| member | 273302 | 273331 | 100.01 |
| typing | 606415 | 606417 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5768 / 5541 | 9.56 / 12.63 / 13.96 / 14.25 | 9.37 / 13.78 | 35499 | 2229 | 0 | 0 of 5541 | 0 | 33.38 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5541 of 5541 | 100.0 | 491.1 / 552.5 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5541 | 0.0 | 0.0 / 0.0 | 17.4 / 5.3 |
| 1 | public.room_messages | 5541 of 5541 | 100.0 | 17.4 / 28.6 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1161 | — | — | 3.5 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5768 | 5768 |
| holds.constants | 5768 | 5768 |
| holds.counted | 5966 | 5966 |
| holds.routed | 40376 | 40376 |
| holds.routed_cap | 46144 | 46144 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40376 | 40376 |
| subscriptions.constants | 11734 | 11734 |
| subscriptions.routed | 40376 | 40376 |
| subscriptions.routed_cap | 59136 | 59136 |
| subscriptions.subscriptions | 40376 | 40376 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.5 | 193.4 |
| memory floor MiB | 233.9 | 235.0 |

Live heap after each collection (MiB): 300 s 192.5, 325 s 190.1, 335 s 190.6, 355 s 192.0, 380 s 191.7, 390 s 188.7, 415 s 189.4, 425 s 189.9, 445 s 191.9, 465 s 189.3, 480 s 193.5, 500 s 190.1, 515 s 190.3, 535 s 200.4, 540 s 192.4, 550 s 141.0, 555 s 191.3, 565 s 192.5, 575 s 190.6, 590 s 191.6, 600 s 194.3, 615 s 190.9, 630 s 197.2, 650 s 191.1, 660 s 195.3, 675 s 191.4, 690 s 193.7, 705 s 190.6, 720 s 193.0, 735 s 190.7, 750 s 193.4, 770 s 192.4, 780 s 193.4.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 5541 |
| open_failed:connection_refused | 22273 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 28145 |
| resumed_false:public.room_members | 27705 |
| resumed_true:public.notifications | 5541 |
| resumed_true:public.room_messages | 5541 |
| stream_end:unexpected_eof | 5768 |
| sub_ok | 92288 |
| token_ok | 4582 |
| warning:replica_identity_insufficient | 11536 |
