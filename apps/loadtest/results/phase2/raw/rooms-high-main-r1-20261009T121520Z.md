# Sluice load test — rooms — high-main-r1

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T11:44:45Z, finished 2026-10-09T12:15:20Z, seed 341077870392061484
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 60 (190) | 89 | 1.70 | 934.1 | 636.7 | 0.06 / 0.12 | 57 | 48 / 461 | 2.33 | 0.00 | 0.36 | 0 | 114.8 (430) | 0.03 |
| steady | 1805 | 125 (273) | 89 | 56.84 | 250.9 | 354.8 | 0.03 / 0.06 | 90 | 80 / 461 | 7.57 | 0.00 | 0.21 | 0 | 17.1 (408) | 0.01 |
| run | 1835 | 124 (273) | 89 | 58.54 | 256.4 | 359.4 | 0.03 / 0.12 | 90 | 80 / 461 | 7.57 | 0.00 | 0.36 | 0 | 18.7 (430) | 0.03 |

Arrival: 30.1 s to open every stream (complete: true); 4.25 ms of Sluice CPU per open; 83.3 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 412 | 5.3 | 27.8 | 32.6 | 34.1 |
| arrival | message | 412 | 3.4 | 14.4 | 14.5 | 14.5 |
| arrival | open | 400 | 45.8 | 57.6 | 62.2 | 76.3 |
| arrival | publish | 58 | 0.7 | 1.2 | 1.7 | 3.6 |
| arrival | token | 3 | 114.2 | 114.2 | 114.2 | 116.0 |
| arrival | txn | 30 | 2.8 | 5.8 | 6.0 | 6.1 |
| arrival | typing | 996 | 0.7 | 1.5 | 1.6 | 3.6 |
| steady | member | 48000 | 10.3 | 56.1 | 64.3 | 81.8 |
| steady | message | 48000 | 5.2 | 15.9 | 20.6 | 37.6 |
| steady | publish | 4500 | 1.0 | 1.6 | 1.8 | 12.4 |
| steady | token | 802 | 110.1 | 130.6 | 138.2 | 146.9 |
| steady | txn | 1800 | 2.7 | 4.4 | 13.8 | 36.5 |
| steady | typing | 130500 | 1.0 | 1.6 | 1.7 | 12.5 |
| run | member | 48412 | 10.3 | 56.1 | 64.3 | 81.8 |
| run | message | 48412 | 5.2 | 15.8 | 20.6 | 37.6 |
| run | open | 400 | 45.8 | 57.6 | 62.2 | 76.3 |
| run | publish | 4558 | 1.0 | 1.6 | 1.8 | 12.4 |
| run | token | 805 | 110.1 | 130.6 | 138.2 | 146.9 |
| run | txn | 1830 | 2.7 | 4.4 | 13.4 | 36.5 |
| run | typing | 131496 | 1.0 | 1.6 | 1.7 | 12.5 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48412 | 48412 | 100.00 |
| member | 48412 | 48412 | 100.00 |
| typing | 131496 | 131496 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 64 | 45.0 | 45.0 | 60.0 |
| public.room_messages | 1 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 805 |
| warning:replica_identity_insufficient | 400 |
