# Sluice load test — rooms — veryhigh-main-r3

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T16:54:37Z, finished 2026-10-09T17:25:12Z, seed 8436570177525028021
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 189 (580) | 335 | 6.39 | 1120.9 | 632.0 | 0.21 / 0.33 | 183 | 173 / 461 | 0.25 | 0.00 | 1.09 | 0 | 438.5 (1446) | 0.09 |
| steady | 1805 | 501 (863) | 336 | 273.47 | 302.3 | 450.8 | 0.15 / 0.31 | 269 | 259 / 461 | 15.52 | 0.02 | 0.75 | 0 | 63.4 (1235) | 0.05 |
| run | 1835 | 496 (863) | 336 | 279.86 | 307.4 | 453.7 | 0.15 / 0.33 | 269 | 259 / 461 | 15.52 | 0.02 | 1.09 | 0 | 69.6 (1446) | 0.09 |

Arrival: 30.1 s to open every stream (complete: true); 4.26 ms of Sluice CPU per open; 77.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1444 | 5.5 | 39.2 | 280.6 | 285.8 |
| arrival | message | 1444 | 3.4 | 16.3 | 280.6 | 280.6 |
| arrival | open | 1500 | 45.8 | 58.1 | 156.7 | 311.8 |
| arrival | publish | 133 | 0.6 | 1.8 | 33.0 | 49.8 |
| arrival | token | 25 | 112.1 | 130.6 | 146.4 | 351.6 |
| arrival | txn | 110 | 2.6 | 4.1 | 4.5 | 7.8 |
| arrival | typing | 2813 | 0.7 | 5.3 | 38.7 | 51.5 |
| steady | member | 180025 | 10.8 | 95.7 | 150.5 | 491.9 |
| steady | message | 180025 | 4.1 | 59.1 | 119.3 | 486.1 |
| steady | publish | 18002 | 0.9 | 1.9 | 8.4 | 90.3 |
| steady | token | 3009 | 112.1 | 140.3 | 185.3 | 380.4 |
| steady | txn | 6601 | 2.5 | 4.7 | 13.0 | 152.2 |
| steady | typing | 544548 | 1.0 | 2.0 | 9.5 | 96.4 |
| run | member | 181469 | 10.8 | 95.7 | 150.5 | 491.9 |
| run | message | 181469 | 4.1 | 58.6 | 119.3 | 486.1 |
| run | open | 1500 | 45.8 | 58.1 | 156.7 | 311.8 |
| run | publish | 18135 | 0.9 | 1.9 | 8.5 | 90.3 |
| run | token | 3034 | 112.1 | 140.3 | 189.4 | 380.4 |
| run | txn | 6711 | 2.5 | 4.6 | 12.7 | 152.2 |
| run | typing | 547361 | 1.0 | 2.0 | 9.8 | 96.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 181466 | 181469 | 100.00 |
| member | 181466 | 181469 | 100.00 |
| typing | 547361 | 547361 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 10.0 | 10.0 | 12.3 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 3034 |
| warning:replica_identity_insufficient | 1500 |
