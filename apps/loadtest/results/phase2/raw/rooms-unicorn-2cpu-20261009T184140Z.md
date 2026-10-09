# Sluice load test — rooms — unicorn-2cpu

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 2, memory 4g, cpuset 2-5
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T18:09:35Z, finished 2026-10-09T18:41:40Z, seed 16233159630546814620
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 30m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1476 (4072) | 2334 | 99.48 | 561.4 | 354.9 | 0.83 / 1.62 | 1565 | 1555 / 3686 | 0.27 | 6.82 | 6.90 | 0 | 1007.3 (2133) | 0.42 |
| steady | 1805 | 3491 (4730) | 2314 | 2334.28 | 370.5 | 559.0 | 1.29 / 1.70 | 2197 | 2187 / 3686 | 1.86 | 0.23 | 5.36 | 0 | 508.2 (1406) | 0.28 |
| run | 1925 | 3365 (4730) | 2315 | 2433.76 | 375.7 | 546.1 | 1.26 / 1.70 | 2197 | 2187 / 3686 | 1.86 | 6.82 | 6.90 | 0 | 539.4 (2133) | 0.42 |

Arrival: 120.1 s to open every stream (complete: true); 8.29 ms of Sluice CPU per open; 89.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48089 | 13.2 | 292.9 | 448.5 | 645.2 |
| arrival | message | 48101 | 4.8 | 252.9 | 407.6 | 627.5 |
| arrival | message_commit_to_client | 48101 | 4.2 | 252.9 | 407.6 | 626.3 |
| arrival | message_write_to_commit | 48101 | 0.4 | 0.8 | 1.4 | 2.4 |
| arrival | open | 12000 | 48.4 | 134.1 | 284.7 | 684.5 |
| arrival | publish | 3360 | 0.5 | 9.7 | 19.3 | 63.3 |
| arrival | token | 776 | 117.2 | 297.0 | 489.5 | 698.6 |
| arrival | txn | 3173 | 2.1 | 3.5 | 5.1 | 52.8 |
| arrival | typing | 81011 | 0.8 | 15.6 | 32.4 | 150.4 |
| steady | member | 1440157 | 74.2 | 460.8 | 700.4 | 1230.7 |
| steady | message | 1440140 | 44.3 | 436.2 | 675.8 | 1184.9 |
| steady | message_commit_to_client | 1440140 | 44.3 | 436.2 | 675.8 | 1183.9 |
| steady | message_write_to_commit | 1440140 | 0.3 | 0.5 | 0.8 | 8.5 |
| steady | publish | 96300 | 0.6 | 13.5 | 21.4 | 86.4 |
| steady | token | 24063 | 126.5 | 354.3 | 436.2 | 815.4 |
| steady | txn | 47520 | 2.1 | 3.6 | 5.9 | 109.4 |
| steady | typing | 3420900 | 1.1 | 21.9 | 39.7 | 119.4 |
| run | member | 1488246 | 71.2 | 460.8 | 692.2 | 1230.7 |
| run | message | 1488241 | 41.2 | 432.1 | 675.8 | 1184.9 |
| run | message_commit_to_client | 1488241 | 40.7 | 432.1 | 667.6 | 1183.9 |
| run | message_write_to_commit | 1488241 | 0.3 | 0.5 | 0.8 | 8.5 |
| run | open | 12000 | 48.4 | 134.1 | 284.7 | 684.5 |
| run | publish | 99660 | 0.6 | 13.4 | 21.4 | 86.4 |
| run | token | 24839 | 125.4 | 350.2 | 436.2 | 815.4 |
| run | txn | 50693 | 2.1 | 3.6 | 5.9 | 109.4 |
| run | typing | 3501911 | 1.1 | 21.9 | 39.7 | 150.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 1488196 | 1488241 | 100.00 |
| member | 1488196 | 1488246 | 100.00 |
| typing | 3501909 | 3501911 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2293 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 24839 |
| warning:replica_identity_insufficient | 12000 |
