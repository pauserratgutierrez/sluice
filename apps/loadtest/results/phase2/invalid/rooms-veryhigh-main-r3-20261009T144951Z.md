# Sluice load test — rooms — veryhigh-main-r3

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T14:19:15Z, finished 2026-10-09T14:49:51Z, seed 8961294982523740790
- **Verdict: FAIL: Sluice restarted 1 times (OOM kill?)**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 196 (513) | 336 | 4.87 | 826.4 | 481.7 | 0.16 / 0.82 | 162 | 153 / 461 | 0.30 | 0.00 | 1.12 | 0 | 436.4 (1457) | 0.08 |
| steady | 1805 | 501 (714) | 400 | 285.68 | 315.8 | 395.2 | 0.16 / 8.61 | 259 | 248 / 461 | 6.98 | 0.09 | 0.78 | 0 | 63.4 (1254) | 0.06 |
| run | 1835 | 496 (714) | 399 | 290.55 | 319.1 | 396.4 | 0.16 / 8.61 | 259 | 248 / 461 | 6.98 | 0.09 | 1.12 | 0 | 69.5 (1457) | 0.08 |

Arrival: 30.1 s to open every stream (complete: true); 3.25 ms of Sluice CPU per open; 71.1 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1477 | 4.7 | 13.5 | 20.1 | 21.8 |
| arrival | message | 1477 | 3.0 | 10.2 | 12.1 | 19.0 |
| arrival | open | 1500 | 45.3 | 56.1 | 59.6 | 75.7 |
| arrival | publish | 139 | 0.6 | 1.1 | 1.7 | 7.8 |
| arrival | token | 21 | 107.0 | 126.5 | 126.5 | 128.1 |
| arrival | txn | 110 | 2.4 | 3.7 | 4.4 | 8.0 |
| arrival | typing | 2939 | 0.6 | 1.2 | 6.6 | 8.1 |
| steady | member | 180000 | 10.8 | 60.7 | 91.6 | 175.1 |
| steady | message | 180000 | 4.4 | 16.1 | 25.7 | 106.7 |
| steady | publish | 18001 | 0.9 | 1.8 | 5.9 | 63.7 |
| steady | token | 3007 | 112.1 | 136.2 | 156.7 | 220.9 |
| steady | txn | 6600 | 2.6 | 5.6 | 14.4 | 58.2 |
| steady | typing | 544524 | 0.9 | 1.7 | 6.8 | 63.7 |
| run | member | 181477 | 10.8 | 60.7 | 91.6 | 175.1 |
| run | message | 181477 | 4.4 | 15.9 | 25.7 | 106.7 |
| run | open | 1500 | 45.3 | 56.1 | 59.6 | 75.7 |
| run | publish | 18140 | 0.9 | 1.8 | 5.9 | 63.7 |
| run | token | 3028 | 112.1 | 136.2 | 156.7 | 220.9 |
| run | txn | 6710 | 2.6 | 5.5 | 14.4 | 58.2 |
| run | typing | 547463 | 0.9 | 1.7 | 6.8 | 63.7 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 181474 | 181477 | 100.00 |
| member | 181474 | 181477 | 100.00 |
| typing | 547463 | 547463 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 15 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 331 | 10.0 | 10.0 | 12.4 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 3028 |
| warning:replica_identity_insufficient | 1500 |
