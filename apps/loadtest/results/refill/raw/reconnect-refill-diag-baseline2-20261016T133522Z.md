# Sluice load test — reconnect — refill-diag-baseline2

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-16T13:22:17Z, finished 2026-10-16T13:35:22Z, seed 8866704284831870598
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1250 (2305) | 1174 | 43.14 | 115.0 | 122.5 | 0.14 / 0.29 | 454 | 443 / 461 | 0.23 | 3.43 | 3.51 | 0 | 353.3 (1745) | 0.12 |
| steady | 485 | 1484 (3085) | 1148 | 84.41 | 117.2 | 151.6 | 0.17 / 1.07 | 453 | 441 / 461 | 55.15 | 0.10 | 2.72 | 0 | 325.1 (4497) | 1.00 |
| run | 785 | 1395 (3085) | 1158 | 127.55 | 116.5 | 140.3 | 0.16 / 1.07 | 454 | 443 / 461 | 55.15 | 3.43 | 3.51 | 0 | 335.9 (4497) | 1.00 |

Arrival: 300.0 s to open every stream (complete: false); 7.21 ms of Sluice CPU per open; 67.6 KiB of live heap per stream after arrival.

Opens refused for capacity: 48964 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 92878 | 3.7 | 7.5 | 20.1 | 60.5 |
| arrival | message | 92878 | 3.7 | 7.5 | 20.1 | 60.4 |
| arrival | message_commit_to_client | 92878 | 3.3 | 7.1 | 19.3 | 60.0 |
| arrival | message_write_to_commit | 92878 | 0.3 | 0.5 | 0.8 | 3.5 |
| arrival | open | 5749 | 45.8 | 56.1 | 61.7 | 121.9 |
| arrival | publish | 6184 | 0.8 | 1.8 | 3.4 | 68.2 |
| arrival | token | 1532 | 110.1 | 134.1 | 162.8 | 251.6 |
| arrival | txn | 3961 | 2.1 | 3.9 | 7.3 | 24.2 |
| arrival | typing | 189343 | 1.0 | 2.3 | 4.0 | 68.8 |
| steady | member | 172755 | 3.8 | 8.8 | 99.8 | 2102.3 |
| steady | message | 172751 | 3.8 | 8.8 | 94.7 | 3298.9 |
| steady | message_commit_to_client | 172751 | 3.5 | 8.4 | 94.7 | 3298.6 |
| steady | message_write_to_commit | 172751 | 0.3 | 0.5 | 0.7 | 2.4 |
| steady | open | 5572 | 2703.4 | 6324.2 | 6979.6 | 7171.5 |
| steady | publish | 11363 | 0.9 | 2.3 | 214.0 | 1900.8 |
| steady | replayed | 11579 | 16580.6 | 43778.0 | 65273.9 | 84281.1 |
| steady | token | 2961 | 111.1 | 142.3 | 3031.0 | 7207.0 |
| steady | txn | 6337 | 2.1 | 3.8 | 6.9 | 22.8 |
| steady | typing | 362893 | 1.0 | 2.7 | 52.0 | 2778.8 |
| run | member | 265633 | 3.7 | 8.3 | 55.6 | 2102.3 |
| run | message | 265629 | 3.7 | 8.3 | 55.6 | 3298.9 |
| run | message_commit_to_client | 265629 | 3.4 | 7.8 | 55.0 | 3298.6 |
| run | message_write_to_commit | 265629 | 0.3 | 0.5 | 0.7 | 3.5 |
| run | open | 11321 | 54.0 | 5537.8 | 6979.6 | 7171.5 |
| run | publish | 17547 | 0.8 | 2.1 | 46.8 | 1900.8 |
| run | replayed | 11579 | 16580.6 | 43778.0 | 65273.9 | 84281.1 |
| run | token | 4493 | 110.1 | 136.2 | 1859.6 | 7207.0 |
| run | txn | 10298 | 2.1 | 3.8 | 7.2 | 24.2 |
| run | typing | 552236 | 1.0 | 2.5 | 38.1 | 2778.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 265649 | 276909 | 104.24 |
| member | 265649 | 265932 | 100.11 |
| typing | 552223 | 552236 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5758 / 5335 | 16.81 / 39.89 / 105.03 / 165.56 | 16.68 / 104.87 | 52502 | 4497 | 938 | 0 of 5335 | 0 | 44.07 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 5181 of 5181 | 100.0 | 501.2 / 700.9 | 0.0 / 0.0 |
| 1 | public.room_members | 0 of 5335 | 0.0 | 0.0 / 0.0 | 22.5 / 5.3 |
| 1 | public.room_messages | 4957 of 5183 | 95.6 | 21.6 / 84.2 | 76.7 / 69.4 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1147 | — | — | 3.6 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5749 | 5563 |
| holds.constants | 5749 | 5563 |
| holds.counted | 5947 | 5761 |
| holds.routed | 40243 | 38941 |
| holds.routed_cap | 45992 | 44504 |
| holds.unindexed | 0 | 0 |
| holds.watches | 40243 | 38941 |
| subscriptions.constants | 11696 | 11324 |
| subscriptions.routed | 40243 | 38941 |
| subscriptions.routed_cap | 58965 | 57291 |
| subscriptions.subscriptions | 40243 | 38941 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.0 | 192.6 |
| memory floor MiB | 234.3 | 234.4 |

Live heap after each collection (MiB): 300 s 193.0, 310 s 188.9, 325 s 206.0, 330 s 190.9, 335 s 190.2, 345 s 189.1, 350 s 188.8, 355 s 191.9, 365 s 190.2, 380 s 189.7, 390 s 193.3, 400 s 190.1, 415 s 190.4, 420 s 189.1, 435 s 189.6, 445 s 200.7, 450 s 192.1, 455 s 189.8, 465 s 189.6, 470 s 188.6, 475 s 191.3, 485 s 189.3, 500 s 189.0, 510 s 193.2, 520 s 189.2, 535 s 189.4, 540 s 189.2, 550 s 66.3, 555 s 166.2, 560 s 200.2, 565 s 193.4, 570 s 187.0, 575 s 172.6, 585 s 189.7, 595 s 178.7, 600 s 179.7, 610 s 185.2, 620 s 187.7, 625 s 189.2, 640 s 187.5, 650 s 188.2, 660 s 192.0, 670 s 189.1, 685 s 205.8, 690 s 191.9, 700 s 188.1, 705 s 186.6, 715 s 210.5, 720 s 191.6, 725 s 189.8, 735 s 191.8, 740 s 189.6, 745 s 192.4, 755 s 189.9, 770 s 190.5, 780 s 192.6.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 4957 |
| msg_gap_unresumed | 152 |
| open_failed:connection_refused | 22222 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 48964 |
| resubscribe | 929 |
| resumed_false:public.room_members | 26118 |
| resumed_false:public.room_messages | 226 |
| resumed_true:public.notifications | 5181 |
| resumed_true:public.room_messages | 4957 |
| stream_end:unexpected_eof | 5758 |
| sub_error:issuer_unavailable | 10 |
| sub_ok | 90569 |
| sub_refused:issuer_unavailable | 928 |
| token_failed:unexpected_eof | 4 |
| token_ok | 4489 |
| token_revoked_subscriptions | 1 |
| warning:replica_identity_insufficient | 11321 |
