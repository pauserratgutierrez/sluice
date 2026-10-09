# Sluice load test — rooms — veryhigh-main-r2

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T13:48:20Z, finished 2026-10-09T14:18:56Z, seed 1191973501438976442
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | full | 30s | 30m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 202 (538) | 336 | 4.75 | 781.5 | 469.8 | 0.16 / 0.21 | 174 | 164 / 461 | 0.28 | 0.00 | 1.10 | 0 | 434.8 (1226) | 0.08 |
| steady | 1805 | 501 (837) | 336 | 243.82 | 269.6 | 401.9 | 0.14 / 0.25 | 253 | 243 / 461 | 6.84 | 0.03 | 0.91 | 0 | 63.4 (1171) | 0.04 |
| run | 1835 | 496 (837) | 336 | 248.57 | 273.0 | 403.1 | 0.14 / 0.25 | 253 | 243 / 461 | 6.84 | 0.03 | 1.10 | 0 | 69.5 (1226) | 0.08 |

Arrival: 30.1 s to open every stream (complete: true); 3.17 ms of Sluice CPU per open; 80.3 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1518 | 5.0 | 26.5 | 56.1 | 58.6 |
| arrival | message | 1518 | 3.3 | 12.7 | 56.1 | 56.3 |
| arrival | open | 1500 | 45.3 | 56.6 | 63.7 | 97.7 |
| arrival | publish | 144 | 0.5 | 1.1 | 2.4 | 13.6 |
| arrival | token | 27 | 109.1 | 132.1 | 134.1 | 139.6 |
| arrival | txn | 110 | 2.4 | 5.2 | 6.2 | 13.2 |
| arrival | typing | 3042 | 0.6 | 1.3 | 5.0 | 13.6 |
| steady | member | 180000 | 11.1 | 72.2 | 123.4 | 239.2 |
| steady | message | 180000 | 4.9 | 23.4 | 71.2 | 177.8 |
| steady | publish | 18001 | 0.9 | 1.8 | 10.8 | 62.3 |
| steady | token | 3007 | 112.1 | 138.2 | 175.1 | 290.9 |
| steady | txn | 6600 | 2.5 | 6.0 | 14.4 | 39.5 |
| steady | typing | 544524 | 0.9 | 1.8 | 10.6 | 76.3 |
| run | member | 181518 | 10.9 | 72.2 | 123.4 | 239.2 |
| run | message | 181518 | 4.8 | 23.4 | 71.2 | 177.8 |
| run | open | 1500 | 45.3 | 56.6 | 63.7 | 97.7 |
| run | publish | 18145 | 0.9 | 1.8 | 10.6 | 62.3 |
| run | token | 3035 | 111.1 | 138.2 | 175.1 | 290.9 |
| run | txn | 6710 | 2.5 | 5.9 | 14.3 | 39.5 |
| run | typing | 547566 | 0.9 | 1.8 | 10.6 | 76.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 181516 | 181518 | 100.00 |
| member | 181516 | 181518 | 100.00 |
| typing | 547566 | 547566 | 100.00 |

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
