# Sluice load test — rooms — v082-token-unicorn-1cpu

- Target: `sluice:0.8.2-rc1`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-10T00:10:55Z, finished 2026-10-10T00:23:01Z, seed 6673920600833106704
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1485 (3920) | 2350 | 51.93 | 291.1 | 184.0 | 0.43 / 0.77 | 1353 | 1343 / 3686 | 0.43 | 6.83 | 6.92 | 0 | 1365.3 (2473) | 0.24 |
| steady | 605 | 3471 (4405) | 2300 | 265.22 | 126.3 | 190.6 | 0.44 / 0.72 | 1490 | 1478 / 3686 | 0.43 | 0.16 | 5.17 | 0 | 1204.9 (2495) | 0.27 |
| run | 725 | 3142 (4405) | 2309 | 317.15 | 139.2 | 189.4 | 0.44 / 0.77 | 1490 | 1478 / 3686 | 0.43 | 6.83 | 6.92 | 0 | 1231.5 (2495) | 0.27 |

Arrival: 120.1 s to open every stream (complete: true); 4.33 ms of Sluice CPU per open; 100.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 47746 | 3.2 | 41.7 | 100.9 | 818.3 |
| arrival | message | 47749 | 2.8 | 37.6 | 99.8 | 818.3 |
| arrival | message_commit_to_client | 47749 | 2.4 | 37.1 | 99.8 | 818.0 |
| arrival | message_write_to_commit | 47749 | 0.3 | 0.6 | 1.1 | 2.8 |
| arrival | open | 12000 | 46.8 | 173.1 | 823.3 | 1309.9 |
| arrival | publish | 3552 | 0.4 | 16.8 | 57.1 | 201.7 |
| arrival | token | 6006 | 113.2 | 514.0 | 1318.9 | 1729.5 |
| arrival | txn | 3170 | 2.0 | 3.6 | 5.2 | 19.3 |
| arrival | typing | 82920 | 0.6 | 30.6 | 76.3 | 219.9 |
| steady | member | 480000 | 3.4 | 22.1 | 72.2 | 452.3 |
| steady | message | 480000 | 2.9 | 21.4 | 70.1 | 437.0 |
| steady | message_commit_to_client | 480000 | 2.6 | 21.1 | 69.1 | 434.5 |
| steady | message_write_to_commit | 480000 | 0.3 | 0.5 | 0.7 | 10.7 |
| steady | publish | 32102 | 0.4 | 5.2 | 46.3 | 207.6 |
| steady | token | 60447 | 110.1 | 288.8 | 481.3 | 1785.0 |
| steady | txn | 15840 | 2.0 | 3.6 | 5.8 | 69.5 |
| steady | typing | 1140354 | 0.7 | 8.2 | 50.9 | 341.6 |
| run | member | 527746 | 3.3 | 23.2 | 75.3 | 818.3 |
| run | message | 527749 | 2.9 | 22.1 | 71.2 | 818.3 |
| run | message_commit_to_client | 527749 | 2.5 | 21.6 | 71.2 | 818.0 |
| run | message_write_to_commit | 527749 | 0.3 | 0.5 | 0.8 | 10.7 |
| run | open | 12000 | 46.8 | 173.1 | 823.3 | 1309.9 |
| run | publish | 35654 | 0.4 | 6.0 | 47.9 | 207.6 |
| run | token | 66453 | 111.1 | 297.0 | 634.9 | 1785.0 |
| run | txn | 19010 | 2.0 | 3.6 | 5.7 | 69.5 |
| run | typing | 1223274 | 0.7 | 10.0 | 53.0 | 341.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 527716 | 527749 | 100.01 |
| member | 527716 | 527746 | 100.01 |
| typing | 1223274 | 1223274 | 100.00 |

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
| token_ok | 66453 |
| warning:replica_identity_insufficient | 12000 |
