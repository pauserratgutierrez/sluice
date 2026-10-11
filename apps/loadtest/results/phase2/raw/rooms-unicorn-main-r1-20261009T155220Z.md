# Sluice load test — rooms — unicorn-main-r1

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T14:50:14Z, finished 2026-10-09T15:52:20Z, seed 6198414982210806657
- **Verdict: FAIL: message p95 1166017 ms > 500ms; member p95 1166017 ms > 500ms; backlog peaked at 435.3 MiB > 64 MiB; backlog grew from 26.2 to 363.2 MiB over the steady phase; message delivered 65.33% < 99%; member delivered 65.33% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 1h0m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 121 | 1390 (3846) | 2259 | 84.39 | 501.7 | 308.8 | 0.70 / 1.00 | 1411 | 1401 / 3686 | 33.19 | 6.85 | 6.92 | 0 | 999.1 (2092) | 0.24 |
| steady | 3605 | 2934 (5115) | 1503 | 3566.81 | 337.2 | 658.4 | 0.99 / 1.02 | 2728 | 2717 / 3686 | 51.60 | 435.35 | 436.41 | 0 | 504.6 (1398) | 0.21 |
| run | 3726 | 2884 (5115) | 1527 | 3651.20 | 339.7 | 641.6 | 0.98 / 1.02 | 2728 | 2717 / 3686 | 51.60 | 435.35 | 436.41 | 0 | 520.7 (2092) | 0.24 |

Arrival: 120.9 s to open every stream (complete: true); 7.03 ms of Sluice CPU per open; 113.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 45435 | 234.5 | 3850.2 | 4816.9 | 5525.3 |
| arrival | message | 45436 | 203.8 | 3817.5 | 4816.9 | 5525.3 |
| arrival | open | 12000 | 56.6 | 618.5 | 864.3 | 1111.0 |
| arrival | publish | 3291 | 1.1 | 57.6 | 74.2 | 182.5 |
| arrival | token | 832 | 205.8 | 1028.1 | 1286.1 | 1433.1 |
| arrival | txn | 3195 | 2.1 | 4.6 | 9.4 | 39.0 |
| arrival | typing | 77246 | 5.6 | 71.2 | 92.7 | 183.8 |
| steady | member | 1868431 | 413138.9 | 1166016.5 | 1249902.6 | 1275048.0 |
| steady | message | 1868442 | 413138.9 | 1166016.5 | 1249902.6 | 1275023.4 |
| steady | publish | 192606 | 5.3 | 66.0 | 83.5 | 266.9 |
| steady | token | 48062 | 403.5 | 1056.8 | 1269.8 | 1838.4 |
| steady | txn | 95042 | 2.1 | 5.5 | 13.0 | 68.4 |
| steady | typing | 6841962 | 10.2 | 81.4 | 134.1 | 382.3 |
| run | member | 1913866 | 400556.0 | 1166016.5 | 1249902.6 | 1275048.0 |
| run | message | 1913878 | 400556.0 | 1166016.5 | 1249902.6 | 1275023.4 |
| run | open | 12000 | 56.6 | 618.5 | 864.3 | 1111.0 |
| run | publish | 195897 | 5.2 | 66.0 | 82.4 | 266.9 |
| run | token | 48895 | 399.4 | 1044.5 | 1269.8 | 1838.4 |
| run | txn | 98237 | 2.1 | 5.4 | 12.9 | 68.4 |
| run | typing | 6919208 | 10.2 | 80.4 | 134.1 | 382.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 2929410 | 1913878 | 65.33 |
| member | 2929410 | 1913866 | 65.33 |
| typing | 6919177 | 6919208 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 30 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1443 | 2.0 | 1.0 | 2.8 |
| public.room_messages | 17 | 60.0 | 45.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| publish_failed:canceled | 1 |
| sub_ok | 96000 |
| token_ok | 48894 |
| warning:replica_identity_insufficient | 12000 |
