# Sluice load test — rooms — veryhigh-main-r1

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T13:17:27Z, finished 2026-10-09T13:48:02Z, seed 3509283772501673992
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 205 (525) | 336 | 4.75 | 770.0 | 469.8 | 0.16 / 0.21 | 170 | 160 / 461 | 0.55 | 0.00 | 1.11 | 0 | 441.5 (1016) | 0.06 |
| steady | 1805 | 501 (763) | 336 | 241.65 | 267.2 | 398.4 | 0.13 / 0.22 | 258 | 248 / 461 | 7.17 | 0.03 | 0.78 | 0 | 63.4 (961) | 0.05 |
| run | 1835 | 496 (763) | 336 | 246.40 | 270.6 | 399.5 | 0.13 / 0.22 | 258 | 248 / 461 | 7.17 | 0.03 | 1.11 | 0 | 69.6 (1016) | 0.06 |

Arrival: 30.1 s to open every stream (complete: true); 3.17 ms of Sluice CPU per open; 89.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1487 | 5.0 | 18.0 | 29.8 | 44.9 |
| arrival | message | 1487 | 3.4 | 11.5 | 27.8 | 27.9 |
| arrival | open | 1500 | 44.8 | 56.1 | 62.7 | 90.8 |
| arrival | publish | 168 | 0.5 | 1.0 | 1.3 | 7.5 |
| arrival | token | 21 | 111.1 | 129.5 | 129.5 | 142.0 |
| arrival | txn | 110 | 2.4 | 3.4 | 4.4 | 5.9 |
| arrival | typing | 3195 | 0.6 | 1.1 | 1.7 | 7.4 |
| steady | member | 180000 | 10.4 | 70.1 | 118.3 | 210.6 |
| steady | message | 180000 | 4.6 | 17.8 | 56.1 | 210.2 |
| steady | publish | 18000 | 0.9 | 1.8 | 21.4 | 71.5 |
| steady | token | 3011 | 111.1 | 138.2 | 183.3 | 356.7 |
| steady | txn | 6600 | 2.5 | 5.0 | 14.3 | 55.4 |
| steady | typing | 544500 | 0.9 | 1.8 | 21.4 | 113.8 |
| run | member | 181487 | 10.4 | 69.1 | 118.3 | 210.6 |
| run | message | 181487 | 4.6 | 17.8 | 56.1 | 210.2 |
| run | open | 1500 | 44.8 | 56.1 | 62.7 | 90.8 |
| run | publish | 18168 | 0.9 | 1.8 | 21.1 | 71.5 |
| run | token | 3032 | 111.1 | 138.2 | 183.3 | 356.7 |
| run | txn | 6710 | 2.5 | 4.9 | 14.3 | 55.4 |
| run | typing | 547695 | 0.9 | 1.7 | 21.4 | 113.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 181486 | 181487 | 100.00 |
| member | 181486 | 181487 | 100.00 |
| typing | 547695 | 547695 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 327 | 10.0 | 10.0 | 12.5 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 3032 |
| warning:replica_identity_insufficient | 1500 |
