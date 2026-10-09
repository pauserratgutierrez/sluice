# Sluice load test — rooms — veryhigh-pk

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T19:18:10Z, finished 2026-10-09T19:28:45Z, seed 12240171683573734665
- **Verdict: FAIL: message delivered 2.99% < 99%; member delivered 0.54% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every 2s | pk | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 140 (410) | 336 | 3.85 | 913.0 | 380.8 | 0.13 / 0.16 | 124 | 114 / 461 | 0.18 | 0.00 | 1.10 | 0 | 429.2 (1436) | 0.07 |
| steady | 605 | 301 (407) | 334 | 14.34 | 78.7 | 70.9 | 0.02 / 0.06 | 139 | 129 / 461 | 6.94 | 0.00 | 0.76 | 0 | 51.3 (1240) | 0.04 |
| run | 635 | 294 (410) | 334 | 18.19 | 97.5 | 85.7 | 0.03 / 0.16 | 139 | 129 / 461 | 6.94 | 0.00 | 1.10 | 0 | 69.2 (1436) | 0.07 |

Arrival: 30.1 s to open every stream (complete: true); 2.57 ms of Sluice CPU per open; 63.1 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 196 | 4.4 | 10.4 | 13.2 | 20.2 |
| arrival | message | 1206 | 4.0 | 9.0 | 11.7 | 20.2 |
| arrival | message_commit_to_client | 1206 | 3.4 | 8.0 | 10.8 | 19.9 |
| arrival | message_write_to_commit | 1206 | 0.4 | 1.1 | 1.4 | 1.6 |
| arrival | open | 1500 | 44.8 | 55.0 | 59.6 | 80.1 |
| arrival | publish | 153 | 0.5 | 0.9 | 1.1 | 1.4 |
| arrival | token | 19 | 31.1 | 116.2 | 116.2 | 118.4 |
| arrival | txn | 110 | 2.4 | 4.0 | 5.8 | 13.8 |
| arrival | typing | 2815 | 0.6 | 1.0 | 1.1 | 1.4 |
| steady | member | 139 | 5.2 | 14.0 | 17.0 | 21.6 |
| steady | message | 632 | 5.7 | 17.3 | 21.6 | 24.2 |
| steady | message_commit_to_client | 632 | 5.2 | 17.0 | 20.6 | 23.8 |
| steady | message_write_to_commit | 632 | 0.4 | 1.0 | 1.0 | 1.1 |
| steady | publish | 6000 | 0.9 | 1.6 | 1.9 | 10.9 |
| steady | token | 1007 | 14.1 | 21.1 | 32.6 | 121.8 |
| steady | txn | 2200 | 2.6 | 5.2 | 15.4 | 58.1 |
| steady | typing | 181500 | 1.0 | 1.6 | 1.8 | 11.0 |
| run | member | 335 | 4.6 | 10.6 | 17.0 | 21.6 |
| run | message | 1838 | 4.5 | 11.8 | 20.1 | 24.2 |
| run | message_commit_to_client | 1838 | 4.0 | 11.5 | 19.8 | 23.8 |
| run | message_write_to_commit | 1838 | 0.4 | 1.0 | 1.4 | 1.6 |
| run | open | 1500 | 44.8 | 55.0 | 59.6 | 80.1 |
| run | publish | 6153 | 0.9 | 1.6 | 1.8 | 10.9 |
| run | token | 1026 | 14.1 | 21.4 | 41.2 | 121.8 |
| run | txn | 2310 | 2.6 | 5.2 | 15.2 | 58.1 |
| run | typing | 184315 | 1.0 | 1.6 | 1.8 | 11.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61534 | 1838 | 2.99 |
| member | 61534 | 335 | 0.54 |
| typing | 184315 | 184315 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 334 | — | — | 12.3 |
| public.room_messages | 4 | — | — | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_error:shape_not_authorized | 1144570 |
| sub_forgotten | 10500 |
| sub_ok | 12000 |
| token_ok | 1026 |
| warning:replica_identity_insufficient | 1500 |
