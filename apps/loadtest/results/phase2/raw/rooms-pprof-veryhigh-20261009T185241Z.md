# Sluice load test — rooms — pprof-veryhigh

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T18:42:06Z, finished 2026-10-09T18:52:41Z, seed 8069989864456191359
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 187 (541) | 336 | 5.34 | 947.8 | 528.2 | 0.18 / 0.48 | 178 | 167 / 461 | 0.26 | 0.01 | 1.12 | 0 | 434.6 (1442) | 0.21 |
| steady | 605 | 498 (789) | 334 | 84.13 | 279.0 | 416.1 | 0.14 / 0.22 | 225 | 213 / 461 | 2.74 | 0.03 | 0.79 | 0 | 63.1 (1274) | 0.03 |
| run | 635 | 484 (789) | 334 | 89.47 | 291.3 | 421.4 | 0.14 / 0.48 | 225 | 213 / 461 | 2.74 | 0.03 | 1.12 | 0 | 80.7 (1442) | 0.21 |

Arrival: 30.1 s to open every stream (complete: true); 3.56 ms of Sluice CPU per open; 70.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1466 | 4.8 | 23.7 | 47.4 | 56.2 |
| arrival | message | 1466 | 3.1 | 9.2 | 14.1 | 16.6 |
| arrival | message_commit_to_client | 1466 | 2.5 | 8.5 | 13.4 | 15.9 |
| arrival | message_write_to_commit | 1466 | 0.4 | 1.0 | 1.4 | 1.4 |
| arrival | open | 1500 | 45.3 | 56.1 | 61.7 | 100.1 |
| arrival | publish | 149 | 0.5 | 1.0 | 1.6 | 5.5 |
| arrival | token | 25 | 106.0 | 132.1 | 134.1 | 139.2 |
| arrival | txn | 110 | 2.3 | 3.7 | 4.0 | 5.2 |
| arrival | typing | 2654 | 0.6 | 1.5 | 4.0 | 7.4 |
| steady | member | 60000 | 9.0 | 71.2 | 128.5 | 168.9 |
| steady | message | 60000 | 3.6 | 14.8 | 31.1 | 79.2 |
| steady | message_commit_to_client | 60000 | 3.2 | 14.3 | 30.8 | 78.4 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.7 | 1.0 | 6.8 |
| steady | publish | 6001 | 0.9 | 1.9 | 22.9 | 55.8 |
| steady | token | 1011 | 113.2 | 136.2 | 158.7 | 205.0 |
| steady | txn | 2200 | 2.5 | 4.2 | 11.6 | 21.2 |
| steady | typing | 181549 | 1.0 | 2.0 | 22.9 | 64.6 |
| run | member | 61466 | 8.9 | 71.2 | 128.5 | 168.9 |
| run | message | 61466 | 3.6 | 14.7 | 31.1 | 79.2 |
| run | message_commit_to_client | 61466 | 3.2 | 14.3 | 30.6 | 78.4 |
| run | message_write_to_commit | 61466 | 0.4 | 0.7 | 1.0 | 6.8 |
| run | open | 1500 | 45.3 | 56.1 | 61.7 | 100.1 |
| run | publish | 6150 | 0.9 | 1.9 | 22.1 | 55.8 |
| run | token | 1036 | 113.2 | 136.2 | 158.7 | 205.0 |
| run | txn | 2310 | 2.5 | 4.2 | 11.5 | 21.2 |
| run | typing | 184203 | 1.0 | 2.0 | 22.7 | 64.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61464 | 61466 | 100.00 |
| member | 61464 | 61466 | 100.00 |
| typing | 184203 | 184203 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 10.0 | 10.0 | 12.3 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1036 |
| warning:replica_identity_insufficient | 1500 |
