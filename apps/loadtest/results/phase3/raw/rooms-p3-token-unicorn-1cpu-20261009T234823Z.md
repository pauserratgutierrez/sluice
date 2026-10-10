# Sluice load test — rooms — p3-token-unicorn-1cpu

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.1`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T23:36:17Z, finished 2026-10-09T23:48:23Z, seed 6656806617301509181
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1452 (4034) | 2375 | 65.01 | 372.8 | 227.9 | 0.54 / 0.86 | 1198 | 1188 / 3686 | 0.40 | 6.87 | 6.92 | 0 | 1361.7 (2484) | 0.35 |
| steady | 605 | 3472 (4432) | 2301 | 277.84 | 132.3 | 199.6 | 0.46 / 0.73 | 1568 | 1557 / 3686 | 0.44 | 0.06 | 5.64 | 0 | 1205.7 (2847) | 0.21 |
| run | 725 | 3137 (4432) | 2313 | 342.85 | 150.7 | 204.4 | 0.47 / 0.86 | 1568 | 1557 / 3686 | 0.44 | 6.87 | 6.92 | 0 | 1231.5 (2847) | 0.35 |

Arrival: 120.1 s to open every stream (complete: true); 5.42 ms of Sluice CPU per open; 89.6 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48208 | 3.7 | 69.1 | 197.6 | 1235.7 |
| arrival | message | 48221 | 3.2 | 68.1 | 195.6 | 1661.5 |
| arrival | message_commit_to_client | 48221 | 2.8 | 68.1 | 195.6 | 1661.2 |
| arrival | message_write_to_commit | 48221 | 0.4 | 0.8 | 1.4 | 5.9 |
| arrival | open | 12000 | 47.9 | 222.2 | 1400.8 | 1926.5 |
| arrival | publish | 3353 | 0.5 | 41.7 | 85.5 | 295.2 |
| arrival | token | 5993 | 117.2 | 978.9 | 1941.5 | 2171.2 |
| arrival | txn | 3171 | 2.2 | 3.8 | 5.4 | 43.0 |
| arrival | typing | 77937 | 0.8 | 58.6 | 162.8 | 400.6 |
| steady | member | 480028 | 3.5 | 32.1 | 74.2 | 578.7 |
| steady | message | 480028 | 2.9 | 31.1 | 72.2 | 570.8 |
| steady | message_commit_to_client | 480028 | 2.6 | 30.8 | 71.2 | 570.6 |
| steady | message_write_to_commit | 480028 | 0.3 | 0.5 | 0.7 | 3.6 |
| steady | publish | 32100 | 0.5 | 17.3 | 58.1 | 140.1 |
| steady | token | 60409 | 112.1 | 528.4 | 1384.4 | 2092.9 |
| steady | txn | 15841 | 2.0 | 3.4 | 5.6 | 46.8 |
| steady | typing | 1140321 | 0.7 | 25.0 | 64.3 | 230.3 |
| run | member | 528236 | 3.5 | 36.6 | 89.6 | 1235.7 |
| run | message | 528249 | 3.0 | 35.1 | 80.4 | 1661.5 |
| run | message_commit_to_client | 528249 | 2.6 | 35.1 | 80.4 | 1661.2 |
| run | message_write_to_commit | 528249 | 0.3 | 0.5 | 0.8 | 5.9 |
| run | open | 12000 | 47.9 | 222.2 | 1400.8 | 1926.5 |
| run | publish | 35453 | 0.5 | 21.1 | 60.2 | 295.2 |
| run | token | 66402 | 112.1 | 569.3 | 1482.8 | 2171.2 |
| run | txn | 19012 | 2.0 | 3.5 | 5.6 | 46.8 |
| run | typing | 1218258 | 0.7 | 29.3 | 68.1 | 400.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528216 | 528249 | 100.01 |
| member | 528216 | 528236 | 100.00 |
| typing | 1218254 | 1218258 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2286 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 66402 |
| warning:replica_identity_insufficient | 12000 |
