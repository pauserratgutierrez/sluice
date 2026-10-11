# Sluice load test — bigtxn — bigtxn-high

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T19:50:56Z, finished 2026-10-09T20:10:31Z, seed 6915675955830264104
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 14 × 80 / 25 / 0.3, 1 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 19m0s |

Expected with every stream open: 400 streams, 89 changes/s, 126 events/s (27 messages, 27 member updates, 72 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 61 (192) | 89 | 1.42 | 781.1 | 531.8 | 0.05 / 0.07 | 61 | 51 / 461 | 8.24 | 0.00 | 0.35 | 0 | 116.1 (442) | 0.02 |
| steady | 1145 | 128 (472) | 381 | 36.31 | 247.0 | 83.3 | 0.03 / 0.23 | 89 | 78 / 461 | 8.24 | 0.00 | 10.71 | 0 | 17.0 (400) | 0.38 |
| run | 1175 | 127 (472) | 373 | 37.73 | 253.6 | 86.0 | 0.03 / 0.23 | 89 | 78 / 461 | 8.24 | 0.00 | 10.71 | 0 | 19.5 (442) | 0.38 |

Arrival: 30.0 s to open every stream (complete: true); 3.55 ms of Sluice CPU per open; 84.5 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 410 | 6.2 | 29.8 | 33.0 | 34.9 |
| arrival | message | 410 | 3.9 | 13.4 | 16.1 | 16.1 |
| arrival | message_commit_to_client | 410 | 3.2 | 12.7 | 15.7 | 15.7 |
| arrival | message_write_to_commit | 410 | 0.4 | 0.9 | 1.0 | 1.4 |
| arrival | open | 400 | 45.3 | 55.6 | 60.2 | 65.0 |
| arrival | publish | 58 | 0.7 | 1.2 | 1.4 | 2.0 |
| arrival | token | 7 | 106.0 | 115.2 | 115.2 | 119.1 |
| arrival | txn | 30 | 2.6 | 3.9 | 4.3 | 15.3 |
| arrival | typing | 998 | 0.8 | 1.3 | 2.0 | 2.2 |
| steady | member | 30400 | 10.9 | 53.0 | 63.7 | 79.2 |
| steady | message | 30400 | 8.2 | 15.9 | 20.6 | 24.8 |
| steady | message_commit_to_client | 30400 | 7.8 | 15.4 | 20.1 | 24.5 |
| steady | message_write_to_commit | 30400 | 0.4 | 0.6 | 0.7 | 1.7 |
| steady | notification | 3525 | 48.4 | 460.8 | 464.9 | 465.3 |
| steady | publish | 2850 | 1.0 | 1.7 | 5.7 | 42.8 |
| steady | token | 503 | 110.1 | 130.6 | 140.3 | 150.7 |
| steady | txn | 1140 | 2.8 | 6.0 | 14.9 | 23.6 |
| steady | typing | 82650 | 1.0 | 1.7 | 5.9 | 42.9 |
| run | member | 30810 | 10.8 | 52.5 | 63.7 | 79.2 |
| run | message | 30810 | 8.1 | 15.9 | 20.6 | 24.8 |
| run | message_commit_to_client | 30810 | 7.7 | 15.4 | 20.1 | 24.5 |
| run | message_write_to_commit | 30810 | 0.4 | 0.6 | 0.8 | 1.7 |
| run | notification | 3525 | 48.4 | 460.8 | 464.9 | 465.3 |
| run | open | 400 | 45.3 | 55.6 | 60.2 | 65.0 |
| run | publish | 2908 | 1.0 | 1.7 | 5.5 | 42.8 |
| run | token | 510 | 110.1 | 130.6 | 140.3 | 150.7 |
| run | txn | 1170 | 2.8 | 5.9 | 15.3 | 23.6 |
| run | typing | 83648 | 1.0 | 1.7 | 5.9 | 42.9 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 30810 | 30810 | 100.00 |
| member | 30810 | 30810 | 100.00 |
| typing | 83648 | 83648 | 100.00 |
| notification | 0 | 3525 | 0.00 |

## Big transactions

| # | Rows | Streamed | Delivered | Txn ms | Sent ms | Dispatched ms | Last delivery ms | Backlog max MiB | Sluice CPU s | Others' message p95 before → during |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 1 | 1500 | 375 | 375/375 | 7 | 22 | 22 | 14 | 0.4 | 0.84 | 17 → 14 |
| 2 | 1500 | 375 | 375/375 | 8 | 22 | 22 | 9 | 0.4 | 0.91 | 20 → 16 |
| 3 | 1500 | 375 | 375/375 | 8 | 23 | 23 | 8 | 0.5 | 0.89 | 16 → 17 |
| 4 | 10000 | 400 | 400/400 | 35 | 49 | 49 | 16 | 2.8 | 0.95 | 15 → 14 |
| 5 | 10000 | 400 | 400/400 | 33 | 65 | 65 | 17 | 3.2 | 0.95 | 16 → 14 |
| 6 | 10000 | 400 | 400/400 | 32 | 33 | 54 | 33 | 2.7 | 0.99 | 15 → 17 |
| 7 | 100000 | 400 | 400/400 | 341 | 396 | 494 | 94 | 25.7 | 1.42 | 15 → 14 |
| 8 | 100000 | 400 | 400/400 | 365 | 428 | 494 | 100 | 22.0 | 1.38 | 19 → 16 |
| 9 | 100000 | 400 | 400/400 | 349 | 381 | 469 | 36 | 8.1 | 1.40 | 16 → 15 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 9 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.notifications | 146 | — | — | 28.0 |
| public.room_members | 76 | 45.0 | 45.0 | 54.2 |
| public.room_messages | 1 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 3200 |
| token_ok | 510 |
| warning:replica_identity_insufficient | 400 |
