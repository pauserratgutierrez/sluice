# Sluice load test — rooms — unicorn-2cpu

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 2, memory 4g, cpuset 2-5
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T17:25:49Z, finished 2026-10-09T17:57:55Z, seed 5013215559064622296
- **Verdict: FAIL: message p95 16450 ms > 500ms; member p95 16581 ms > 500ms**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 30m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1437 (4450) | 2320 | 94.49 | 547.3 | 338.8 | 0.79 / 1.71 | 1543 | 1532 / 3686 | 0.71 | 6.86 | 6.93 | 0 | 1014.4 (2143) | 0.25 |
| steady | 1805 | 3476 (5070) | 2292 | 2448.18 | 390.1 | 591.6 | 1.36 / 2.10 | 2253 | 2243 / 3686 | 0.71 | 7.04 | 8.64 | 0 | 506.4 (1395) | 0.26 |
| run | 1925 | 3349 (5070) | 2294 | 2542.67 | 394.4 | 575.7 | 1.32 / 2.10 | 2253 | 2243 / 3686 | 0.71 | 7.04 | 8.64 | 0 | 538.1 (2143) | 0.26 |

Arrival: 120.1 s to open every stream (complete: true); 7.87 ms of Sluice CPU per open; 93.5 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48047 | 13.5 | 946.2 | 1171.5 | 1282.8 |
| arrival | message | 48113 | 5.0 | 913.4 | 1155.1 | 1263.4 |
| arrival | open | 12000 | 47.9 | 111.1 | 242.7 | 439.4 |
| arrival | publish | 3413 | 0.4 | 6.3 | 17.3 | 49.8 |
| arrival | token | 789 | 114.2 | 236.5 | 432.1 | 492.6 |
| arrival | txn | 3169 | 2.2 | 3.5 | 5.5 | 60.0 |
| arrival | typing | 76491 | 0.7 | 12.0 | 27.3 | 56.2 |
| steady | member | 1427088 | 493.6 | 16580.6 | 20054.0 | 21774.6 |
| steady | message | 1427046 | 473.1 | 16449.5 | 20054.0 | 21738.6 |
| steady | publish | 96301 | 0.6 | 15.4 | 24.4 | 89.4 |
| steady | token | 24072 | 128.5 | 391.2 | 493.6 | 860.5 |
| steady | txn | 47520 | 2.1 | 3.7 | 6.2 | 137.5 |
| steady | typing | 3420984 | 1.2 | 25.5 | 45.8 | 146.0 |
| run | member | 1475171 | 456.7 | 16318.5 | 20054.0 | 21774.6 |
| run | message | 1475176 | 436.2 | 16318.5 | 20054.0 | 21738.6 |
| run | open | 12000 | 47.9 | 111.1 | 242.7 | 439.4 |
| run | publish | 99714 | 0.6 | 15.3 | 24.4 | 89.4 |
| run | token | 24861 | 128.5 | 391.2 | 493.6 | 860.5 |
| run | txn | 50689 | 2.1 | 3.7 | 6.1 | 137.5 |
| run | typing | 3497475 | 1.2 | 25.5 | 45.3 | 146.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 1488349 | 1475159 | 99.11 |
| member | 1488349 | 1475135 | 99.11 |
| typing | 3497472 | 3497475 | 100.00 |

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
| token_ok | 24861 |
| warning:replica_identity_insufficient | 12000 |
