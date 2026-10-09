# Sluice load test — rooms — realistic-main-r1

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T15:52:40Z, finished 2026-10-09T16:23:14Z, seed 10043040563688855979
- **Verdict: FAIL: message delivered 76.41% < 99%; member delivered 71.86% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 5 × 80 / 20 / 0.4 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 100 streams, 27 changes/s, 32 events/s (7 messages, 7 member updates, 19 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 29 | 30 (144) | 51617 | 7.56 | 8760.1 | 5.0 | 0.26 / 0.99 | 38 | 28 / 461 | 20.97 | 222.68 | 434.63 | 0 | 30.7 (130) | 0.01 |
| steady | 1805 | 29 (90) | 27 | 12.40 | 240.9 | 255.1 | 0.01 / 0.01 | 43 | 34 / 461 | 5.80 | 0.00 | 0.12 | 0 | 4.2 (109) | 0.01 |
| run | 1834 | 29 (144) | 844 | 19.96 | 381.3 | 12.9 | 0.01 / 0.99 | 43 | 34 / 461 | 20.97 | 222.68 | 434.63 | 0 | 4.6 (130) | 0.01 |

Arrival: 29.0 s to open every stream (complete: true); 75.60 ms of Sluice CPU per open; 90.5 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 71 | 3.9 | 4.9 | 6.7 | 6.7 |
| arrival | message | 623 | 183500.8 | 455082.0 | 517996.5 | 568632.8 |
| arrival | open | 100 | 47.4 | 71.2 | 73.2 | 85.1 |
| arrival | publish | 10 | 1.0 | 1.2 | 1.2 | 1.3 |
| arrival | token | 2 | 14.3 | 14.3 | 14.3 | 147.0 |
| arrival | txn | 10 | 2.5 | 3.4 | 3.4 | 3.8 |
| arrival | typing | 169 | 1.0 | 1.7 | 4.0 | 4.1 |
| steady | member | 8640 | 8.3 | 15.2 | 23.4 | 37.3 |
| steady | message | 8640 | 6.8 | 14.4 | 22.9 | 34.2 |
| steady | publish | 1800 | 0.9 | 1.2 | 2.4 | 9.1 |
| steady | token | 200 | 102.9 | 128.5 | 136.2 | 143.5 |
| steady | txn | 600 | 2.9 | 5.6 | 17.5 | 32.4 |
| steady | typing | 34200 | 0.9 | 1.1 | 2.8 | 10.4 |
| run | member | 8711 | 8.2 | 15.2 | 23.4 | 37.3 |
| run | message | 9263 | 7.7 | 86507.5 | 362807.3 | 568632.8 |
| run | open | 100 | 47.4 | 71.2 | 73.2 | 85.1 |
| run | publish | 1810 | 0.9 | 1.2 | 2.4 | 9.1 |
| run | token | 202 | 102.9 | 128.5 | 136.2 | 147.0 |
| run | txn | 610 | 2.9 | 5.5 | 17.0 | 32.4 |
| run | typing | 34369 | 0.9 | 1.1 | 2.8 | 10.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 12123 | 9263 | 76.41 |
| member | 12123 | 8711 | 71.86 |
| typing | 34369 | 34369 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 27 | 60.0 | 60.0 | 60.0 |
| public.room_messages | 0 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_error:shape_not_authorized | 196 |
| sub_forgotten | 196 |
| sub_ok | 800 |
| token_ok | 202 |
| warning:replica_identity_insufficient | 100 |
