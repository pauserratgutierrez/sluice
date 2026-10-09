# Sluice load test — rooms — pprof-high

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T17:58:24Z, finished 2026-10-09T18:09:00Z, seed 10444604198032703181
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 10m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 55 (183) | 89 | 1.73 | 1053.0 | 647.9 | 0.06 / 0.12 | 60 | 49 / 461 | 0.17 | 0.00 | 0.38 | 0 | 118.6 (434) | 0.06 |
| steady | 605 | 125 (345) | 88 | 19.88 | 263.3 | 372.3 | 0.03 / 0.06 | 82 | 70 / 461 | 8.76 | 0.00 | 0.26 | 0 | 16.8 (408) | 0.01 |
| run | 635 | 121 (345) | 88 | 21.61 | 280.1 | 385.4 | 0.03 / 0.12 | 82 | 70 / 461 | 8.76 | 0.00 | 0.38 | 0 | 21.7 (434) | 0.06 |

Arrival: 30.1 s to open every stream (complete: true); 4.33 ms of Sluice CPU per open; 78.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 363 | 5.3 | 24.7 | 29.3 | 30.4 |
| arrival | message | 363 | 3.6 | 8.5 | 9.3 | 9.7 |
| arrival | message_commit_to_client | 363 | 3.0 | 8.1 | 8.9 | 9.3 |
| arrival | message_write_to_commit | 363 | 0.5 | 0.7 | 1.0 | 1.0 |
| arrival | open | 400 | 45.3 | 55.0 | 60.2 | 78.1 |
| arrival | publish | 45 | 0.8 | 1.1 | 1.3 | 1.3 |
| arrival | token | 7 | 99.8 | 126.5 | 126.5 | 129.1 |
| arrival | txn | 30 | 2.7 | 3.7 | 3.9 | 4.4 |
| arrival | typing | 917 | 0.8 | 1.3 | 1.3 | 1.4 |
| steady | member | 16000 | 9.3 | 44.8 | 52.5 | 64.4 |
| steady | message | 16000 | 4.1 | 14.7 | 18.3 | 24.1 |
| steady | message_commit_to_client | 16000 | 3.6 | 14.3 | 17.8 | 23.7 |
| steady | message_write_to_commit | 16000 | 0.4 | 0.7 | 1.0 | 3.1 |
| steady | publish | 1500 | 1.0 | 1.6 | 2.1 | 5.9 |
| steady | token | 257 | 111.1 | 134.1 | 144.4 | 150.3 |
| steady | txn | 600 | 2.6 | 4.5 | 13.2 | 19.3 |
| steady | typing | 43500 | 1.0 | 1.6 | 2.1 | 13.4 |
| run | member | 16363 | 9.2 | 44.8 | 52.5 | 64.4 |
| run | message | 16363 | 4.1 | 14.7 | 18.3 | 24.1 |
| run | message_commit_to_client | 16363 | 3.6 | 14.3 | 17.8 | 23.7 |
| run | message_write_to_commit | 16363 | 0.4 | 0.7 | 1.0 | 3.1 |
| run | open | 400 | 45.3 | 55.0 | 60.2 | 78.1 |
| run | publish | 1545 | 1.0 | 1.6 | 2.0 | 5.9 |
| run | token | 264 | 111.1 | 132.1 | 144.4 | 150.3 |
| run | txn | 630 | 2.6 | 4.4 | 11.3 | 19.3 |
| run | typing | 44417 | 1.0 | 1.6 | 2.1 | 13.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 16363 | 16363 | 100.00 |
| member | 16363 | 16363 | 100.00 |
| typing | 44417 | 44417 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 85 | 45.0 | 30.0 | 48.2 |
| public.room_messages | 1 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 264 |
| warning:replica_identity_insufficient | 400 |
