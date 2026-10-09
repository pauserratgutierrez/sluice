# Sluice load test — rooms — high-main-r3

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T12:46:32Z, finished 2026-10-09T13:17:07Z, seed 15103343846928122108
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 45 (133) | 89 | 1.29 | 942.3 | 483.1 | 0.04 / 0.07 | 57 | 48 / 461 | 3.07 | 0.00 | 0.37 | 0 | 115.7 (431) | 0.02 |
| steady | 1805 | 125 (247) | 89 | 57.57 | 254.2 | 359.4 | 0.03 / 0.09 | 89 | 79 / 461 | 6.78 | 0.00 | 0.30 | 0 | 17.1 (408) | 0.01 |
| run | 1835 | 124 (247) | 89 | 58.86 | 258.3 | 361.4 | 0.03 / 0.09 | 89 | 79 / 461 | 6.78 | 0.00 | 0.37 | 0 | 18.7 (431) | 0.02 |

Arrival: 30.1 s to open every stream (complete: true); 3.23 ms of Sluice CPU per open; 79.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 362 | 6.4 | 24.7 | 29.1 | 30.3 |
| arrival | message | 362 | 4.6 | 11.5 | 15.0 | 15.2 |
| arrival | open | 400 | 44.8 | 56.1 | 59.6 | 78.3 |
| arrival | publish | 27 | 0.8 | 1.6 | 3.5 | 3.6 |
| arrival | token | 5 | 111.1 | 112.1 | 112.1 | 116.1 |
| arrival | txn | 30 | 2.7 | 5.0 | 6.4 | 14.3 |
| arrival | typing | 645 | 0.8 | 1.6 | 3.8 | 4.0 |
| steady | member | 48000 | 9.9 | 47.4 | 59.6 | 174.3 |
| steady | message | 48000 | 5.5 | 15.9 | 20.6 | 125.1 |
| steady | publish | 4500 | 1.0 | 1.6 | 1.9 | 11.0 |
| steady | token | 802 | 111.1 | 130.6 | 140.3 | 146.8 |
| steady | txn | 1800 | 2.7 | 7.5 | 15.4 | 20.2 |
| steady | typing | 130500 | 1.0 | 1.5 | 2.1 | 11.7 |
| run | member | 48362 | 9.9 | 47.4 | 59.6 | 174.3 |
| run | message | 48362 | 5.5 | 15.9 | 20.6 | 125.1 |
| run | open | 400 | 44.8 | 56.1 | 59.6 | 78.3 |
| run | publish | 4527 | 1.0 | 1.6 | 1.9 | 11.0 |
| run | token | 807 | 111.1 | 130.6 | 140.3 | 146.8 |
| run | txn | 1830 | 2.7 | 7.4 | 15.3 | 20.2 |
| run | typing | 131145 | 1.0 | 1.5 | 2.1 | 11.7 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48362 | 48362 | 100.00 |
| member | 48362 | 48362 | 100.00 |
| typing | 131145 | 131145 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 32 | 45.0 | 45.0 | 60.0 |
| public.room_messages | 0 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 807 |
| warning:replica_identity_insufficient | 400 |
