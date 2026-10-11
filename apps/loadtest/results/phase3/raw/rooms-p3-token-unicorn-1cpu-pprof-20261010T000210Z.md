# Sluice load test — rooms — p3-token-unicorn-1cpu-pprof

- Target: `sluice:0.8.1-pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T23:50:04Z, finished 2026-10-10T00:02:10Z, seed 11949193243570367627
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1437 (3734) | 2371 | 57.94 | 335.5 | 203.3 | 0.48 / 0.75 | 1191 | 1178 / 3686 | 0.16 | 6.88 | 6.96 | 0 | 1374.6 (2496) | 0.32 |
| steady | 605 | 3472 (4205) | 2301 | 289.33 | 137.8 | 207.9 | 0.48 / 0.77 | 1577 | 1563 / 3686 | 0.37 | 0.02 | 5.48 | 0 | 1204.6 (2604) | 0.24 |
| run | 725 | 3134 (4205) | 2312 | 347.27 | 152.8 | 207.1 | 0.48 / 0.77 | 1577 | 1563 / 3686 | 0.37 | 6.88 | 6.96 | 0 | 1232.8 (2604) | 0.32 |

Arrival: 120.2 s to open every stream (complete: true); 4.83 ms of Sluice CPU per open; 90.3 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48067 | 3.4 | 42.2 | 232.4 | 1399.6 |
| arrival | message | 48071 | 3.0 | 41.2 | 214.0 | 1645.0 |
| arrival | message_commit_to_client | 48071 | 2.6 | 40.7 | 212.0 | 1644.7 |
| arrival | message_write_to_commit | 48071 | 0.4 | 0.7 | 1.2 | 3.6 |
| arrival | open | 12000 | 47.4 | 158.7 | 1028.1 | 2135.9 |
| arrival | publish | 3451 | 0.5 | 23.2 | 63.7 | 180.8 |
| arrival | token | 6020 | 115.2 | 473.1 | 1892.4 | 2265.0 |
| arrival | txn | 3172 | 2.1 | 3.6 | 5.3 | 44.6 |
| arrival | typing | 76565 | 0.7 | 36.6 | 116.2 | 263.3 |
| steady | member | 480000 | 3.6 | 28.8 | 70.1 | 253.8 |
| steady | message | 480000 | 3.0 | 27.5 | 67.1 | 250.4 |
| steady | message_commit_to_client | 480000 | 2.7 | 27.0 | 66.0 | 250.1 |
| steady | message_write_to_commit | 480000 | 0.3 | 0.5 | 0.8 | 8.6 |
| steady | publish | 32101 | 0.5 | 13.1 | 49.4 | 96.2 |
| steady | token | 60427 | 111.1 | 366.6 | 741.4 | 1740.6 |
| steady | txn | 15840 | 2.0 | 3.7 | 6.2 | 38.8 |
| steady | typing | 1140327 | 0.7 | 16.8 | 55.6 | 178.8 |
| run | member | 528067 | 3.6 | 29.6 | 72.2 | 1399.6 |
| run | message | 528071 | 3.0 | 28.3 | 69.1 | 1645.0 |
| run | message_commit_to_client | 528071 | 2.7 | 27.8 | 68.1 | 1644.7 |
| run | message_write_to_commit | 528071 | 0.3 | 0.5 | 0.8 | 8.6 |
| run | open | 12000 | 47.4 | 158.7 | 1028.1 | 2135.9 |
| run | publish | 35552 | 0.5 | 13.8 | 50.4 | 180.8 |
| run | token | 66447 | 112.1 | 374.8 | 757.8 | 2265.0 |
| run | txn | 19012 | 2.0 | 3.6 | 6.2 | 44.6 |
| run | typing | 1216892 | 0.7 | 18.3 | 57.6 | 263.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528050 | 528071 | 100.00 |
| member | 528050 | 528067 | 100.00 |
| typing | 1216891 | 1216892 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2293 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 66447 |
| warning:replica_identity_insufficient | 12000 |
