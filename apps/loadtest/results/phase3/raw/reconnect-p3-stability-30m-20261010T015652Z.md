# Sluice load test — reconnect — p3-stability-30m

- Target: `sluice:p3-final`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-10T01:26:16Z, finished 2026-10-10T01:56:52Z, seed 1638866937914087289
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 225 (589) | 343 | 4.58 | 676.4 | 444.2 | 0.15 / 0.32 | 135 | 126 / 461 | 0.20 | 0.00 | 1.09 | 0 | 471.9 (1199) | 0.08 |
| steady | 1805 | 500 (2772) | 336 | 140.37 | 155.5 | 231.4 | 0.08 / 0.96 | 232 | 222 / 461 | 54.77 | 0.00 | 0.89 | 0 | 159.8 (2381) | 0.33 |
| run | 1835 | 496 (2772) | 336 | 144.95 | 159.4 | 235.0 | 0.08 / 0.96 | 232 | 222 / 461 | 54.77 | 0.00 | 1.09 | 0 | 164.9 (2381) | 0.33 |

Arrival: 30.1 s to open every stream (complete: true); 3.05 ms of Sluice CPU per open; 66.6 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1483 | 3.9 | 5.6 | 13.1 | 17.1 |
| arrival | message | 1483 | 3.8 | 5.5 | 13.1 | 17.1 |
| arrival | message_commit_to_client | 1483 | 3.3 | 4.8 | 12.2 | 15.7 |
| arrival | message_write_to_commit | 1483 | 0.4 | 1.0 | 1.1 | 1.4 |
| arrival | open | 1500 | 45.8 | 57.1 | 70.1 | 118.4 |
| arrival | publish | 187 | 0.7 | 1.2 | 1.7 | 4.6 |
| arrival | token | 173 | 108.0 | 125.4 | 130.6 | 135.2 |
| arrival | txn | 111 | 2.4 | 3.9 | 4.8 | 6.5 |
| arrival | typing | 3805 | 0.8 | 1.3 | 1.6 | 4.6 |
| steady | member | 178730 | 4.3 | 11.2 | 44.3 | 499.1 |
| steady | message | 178730 | 4.3 | 11.2 | 43.8 | 499.1 |
| steady | message_commit_to_client | 178730 | 4.0 | 10.8 | 43.3 | 498.8 |
| steady | message_write_to_commit | 178730 | 0.3 | 0.5 | 0.7 | 6.9 |
| steady | open | 2400 | 1630.2 | 6586.4 | 6651.9 | 6710.0 |
| steady | publish | 17872 | 1.1 | 2.3 | 21.4 | 101.9 |
| steady | replayed | 4913 | 11206.7 | 25821.2 | 27131.9 | 29505.0 |
| steady | token | 22366 | 109.1 | 130.6 | 142.3 | 6205.6 |
| steady | txn | 6600 | 2.3 | 4.4 | 13.8 | 35.2 |
| steady | typing | 540432 | 1.3 | 2.4 | 22.4 | 102.2 |
| run | member | 180213 | 4.3 | 11.2 | 43.8 | 499.1 |
| run | message | 180213 | 4.3 | 11.2 | 43.8 | 499.1 |
| run | message_commit_to_client | 180213 | 4.0 | 10.8 | 43.3 | 498.8 |
| run | message_write_to_commit | 180213 | 0.3 | 0.5 | 0.7 | 6.9 |
| run | open | 3900 | 268.3 | 6455.3 | 6651.9 | 6710.0 |
| run | publish | 18059 | 1.1 | 2.3 | 20.9 | 101.9 |
| run | replayed | 4913 | 11206.7 | 25821.2 | 27131.9 | 29505.0 |
| run | token | 22539 | 109.1 | 130.6 | 142.3 | 6205.6 |
| run | txn | 6711 | 2.3 | 4.4 | 13.8 | 35.2 |
| run | typing | 544237 | 1.2 | 2.4 | 21.4 | 102.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 180212 | 183952 | 102.08 |
| member | 180212 | 181387 | 100.65 |
| typing | 544232 | 544237 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 1500 / 1500 | 12.51 / 12.82 / 14.32 / 14.70 | 12.49 / 14.30 | 7151 | 2381 | 0 | 26 of 1500 | 0 | 8.15 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 1500 of 1500 | 100.0 | 923.6 / 943.1 | 0.0 / 0.0 |
| 1 | public.room_members | 209 of 1500 | 13.9 | 7.7 / 11.2 | 17.7 / 8.9 |
| 1 | public.room_messages | 1500 of 1500 | 100.0 | 16.6 / 29.5 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 10.0 | 10.0 | 12.3 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 30 s | 1835 s |
| --- | ---: | ---: |
| streams | 1500 | 1500 |
| holds.constants | 1500 | 1500 |
| holds.counted | 1555 | 1555 |
| holds.routed | 10500 | 10500 |
| holds.routed_cap | 12000 | 12056 |
| holds.unindexed | 0 | 0 |
| holds.watches | 10500 | 10500 |
| subscriptions.constants | 3055 | 3055 |
| subscriptions.routed | 10500 | 10500 |
| subscriptions.routed_cap | 15420 | 15420 |
| subscriptions.subscriptions | 10500 | 10500 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 52.3 | 55.7 |
| memory floor MiB | 111.5 | 135.4 |

Live heap after each collection (MiB): 30 s 52.3, 45 s 53.2, 55 s 52.5, 65 s 74.9, 70 s 52.3, 80 s 52.6, 85 s 52.8, 95 s 52.6, 110 s 53.0, 120 s 53.2, 125 s 63.7, 135 s 52.5, 145 s 54.2, 155 s 52.9, 165 s 53.9, 175 s 57.0, 185 s 73.3, 190 s 52.8, 200 s 53.4, 205 s 53.1, 220 s 53.4, 230 s 53.7, 240 s 52.5, 245 s 73.3, 255 s 53.0, 265 s 54.1, 275 s 53.6, 280 s 52.5, 295 s 53.4, 305 s 76.5, 315 s 53.1, 325 s 53.2, 335 s 53.3, 345 s 53.5, 355 s 53.8, 365 s 52.8, 375 s 53.5, 385 s 54.7, 395 s 53.1, 405 s 53.1, 415 s 53.7, 425 s 67.5, 440 s 54.2, 445 s 53.5, 455 s 52.8, 470 s 54.5, 475 s 53.6, 485 s 54.9, 495 s 53.6, 505 s 56.9, 515 s 53.8, 525 s 52.9, 535 s 54.7, 545 s 78.6, 555 s 53.4, 565 s 57.4, 575 s 53.8, 585 s 53.4, 595 s 53.4, 605 s 57.1, 615 s 53.0, 625 s 53.6, 630 s 52.7, 640 s 52.8, 655 s 53.3, 665 s 72.8, 680 s 53.0, 685 s 53.5, 695 s 52.9, 710 s 53.8, 715 s 53.6, 725 s 74.6, 730 s 52.8, 740 s 52.9, 750 s 53.0, 755 s 52.8, 770 s 54.1, 780 s 53.9, 785 s 75.0, 800 s 53.8, 805 s 53.6, 815 s 54.1, 830 s 54.0, 835 s 53.5, 845 s 76.3, 850 s 52.9, 860 s 52.8, 865 s 53.0, 875 s 53.2, 885 s 53.0, 895 s 53.4, 905 s 70.3, 920 s 53.8, 925 s 54.1, 940 s 50.2, 945 s 67.6, 950 s 54.1, 955 s 54.2, 965 s 64.5, 970 s 53.4, 980 s 53.6, 985 s 53.4, 995 s 54.0, 1005 s 53.6, 1015 s 54.1, 1025 s 52.9, 1040 s 54.2, 1045 s 54.0, 1055 s 54.1, 1070 s 54.4, 1080 s 54.1, 1085 s 79.4, 1090 s 53.2, 1105 s 54.2, 1110 s 53.1, 1120 s 53.6, 1130 s 53.4, 1140 s 54.1, 1145 s 57.1, 1155 s 54.3, 1165 s 54.8, 1175 s 53.2, 1185 s 54.3, 1195 s 54.0, 1205 s 53.2, 1220 s 54.1, 1225 s 54.7, 1235 s 54.7, 1245 s 53.9, 1255 s 54.6, 1265 s 54.2, 1280 s 54.3, 1285 s 54.2, 1295 s 54.5, 1310 s 53.5, 1320 s 54.8, 1325 s 79.3, 1330 s 53.6, 1340 s 53.5, 1345 s 53.6, 1355 s 53.7, 1365 s 54.5, 1380 s 54.7, 1385 s 78.4, 1395 s 53.5, 1405 s 54.6, 1415 s 54.8, 1425 s 54.6, 1435 s 54.1, 1445 s 71.5, 1450 s 53.6, 1460 s 54.0, 1470 s 54.8, 1480 s 54.3, 1490 s 54.8, 1500 s 54.7, 1505 s 74.3, 1510 s 53.5, 1525 s 53.7, 1530 s 53.5, 1540 s 53.8, 1555 s 53.6, 1565 s 71.0, 1580 s 54.4, 1585 s 54.2, 1595 s 54.8, 1605 s 54.7, 1615 s 56.4, 1625 s 53.7, 1635 s 53.9, 1645 s 54.6, 1655 s 54.6, 1665 s 54.7, 1680 s 54.5, 1685 s 54.4, 1695 s 54.3, 1705 s 58.7, 1715 s 57.6, 1725 s 53.5, 1735 s 54.4, 1745 s 53.3, 1755 s 54.4, 1765 s 55.6, 1775 s 54.7, 1785 s 53.7, 1795 s 55.1, 1805 s 55.3, 1815 s 53.9, 1825 s 55.5, 1835 s 55.7.

## Counters

| Counter | Value |
| --- | ---: |
| churned | 900 |
| msg_duplicate | 2469 |
| open_failed:connection_refused | 5651 |
| resumed_false:public.room_members | 8116 |
| resumed_true:public.notifications | 2400 |
| resumed_true:public.room_members | 3884 |
| resumed_true:public.room_messages | 2400 |
| stream_end:unexpected_eof | 1500 |
| sub_ok | 31200 |
| token_ok | 22539 |
| warning:replica_identity_insufficient | 3900 |
