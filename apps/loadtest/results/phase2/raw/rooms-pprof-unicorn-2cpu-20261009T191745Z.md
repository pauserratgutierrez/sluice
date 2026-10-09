# Sluice load test — rooms — pprof-unicorn-2cpu

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 2, memory 4g, cpuset 2-5
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T19:05:39Z, finished 2026-10-09T19:17:45Z, seed 15460259053428650076
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1424 (3795) | 2349 | 91.80 | 536.4 | 325.2 | 0.76 / 1.58 | 1608 | 1595 / 3686 | 0.46 | 6.83 | 6.92 | 0 | 1038.8 (2146) | 0.25 |
| steady | 605 | 3472 (4363) | 2302 | 676.54 | 322.0 | 485.8 | 1.12 / 1.47 | 1819 | 1806 / 3686 | 0.46 | 0.19 | 5.50 | 0 | 511.0 (1404) | 0.18 |
| run | 725 | 3133 (4363) | 2309 | 768.34 | 338.2 | 458.7 | 1.06 / 1.58 | 1819 | 1806 / 3686 | 0.46 | 6.83 | 6.92 | 0 | 598.5 (2146) | 0.25 |

Arrival: 120.2 s to open every stream (complete: true); 7.65 ms of Sluice CPU per open; 93.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48081 | 12.2 | 216.1 | 399.4 | 529.1 |
| arrival | message | 48075 | 4.1 | 199.7 | 370.7 | 516.3 |
| arrival | message_commit_to_client | 48075 | 3.5 | 197.6 | 370.7 | 515.9 |
| arrival | message_write_to_commit | 48075 | 0.3 | 0.6 | 1.2 | 7.1 |
| arrival | open | 12000 | 47.9 | 109.1 | 195.6 | 356.8 |
| arrival | publish | 3215 | 0.4 | 5.7 | 15.8 | 34.8 |
| arrival | token | 809 | 115.2 | 246.8 | 350.2 | 438.9 |
| arrival | txn | 3174 | 2.0 | 3.4 | 5.2 | 19.1 |
| arrival | typing | 74998 | 0.7 | 10.7 | 25.5 | 53.3 |
| steady | member | 480254 | 29.8 | 284.7 | 423.9 | 670.4 |
| steady | message | 480252 | 16.5 | 259.1 | 399.4 | 654.8 |
| steady | message_commit_to_client | 480252 | 16.1 | 259.1 | 399.4 | 654.4 |
| steady | message_write_to_commit | 480252 | 0.3 | 0.5 | 0.7 | 10.9 |
| steady | publish | 32102 | 0.5 | 10.3 | 19.1 | 51.2 |
| steady | token | 7996 | 121.3 | 292.9 | 374.8 | 536.1 |
| steady | txn | 15840 | 2.1 | 3.9 | 7.5 | 26.6 |
| steady | typing | 1140411 | 0.9 | 15.4 | 29.6 | 76.9 |
| run | member | 528335 | 27.5 | 280.6 | 423.9 | 670.4 |
| run | message | 528327 | 15.3 | 255.0 | 395.3 | 654.8 |
| run | message_commit_to_client | 528327 | 14.9 | 255.0 | 395.3 | 654.4 |
| run | message_write_to_commit | 528327 | 0.3 | 0.5 | 0.8 | 10.9 |
| run | open | 12000 | 47.9 | 109.1 | 195.6 | 356.8 |
| run | publish | 35317 | 0.5 | 10.0 | 19.1 | 51.2 |
| run | token | 8805 | 120.3 | 288.8 | 374.8 | 536.1 |
| run | txn | 19014 | 2.1 | 3.8 | 7.1 | 26.6 |
| run | typing | 1215409 | 0.9 | 15.2 | 29.1 | 76.9 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528295 | 528327 | 100.01 |
| member | 528295 | 528335 | 100.01 |
| typing | 1215407 | 1215409 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2307 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 8805 |
| warning:replica_identity_insufficient | 12000 |
