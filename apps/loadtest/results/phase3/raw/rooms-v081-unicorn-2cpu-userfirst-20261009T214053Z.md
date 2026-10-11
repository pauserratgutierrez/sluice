# Sluice load test — rooms — v081-unicorn-2cpu-userfirst

- Target: `sluice:0.8.1-rc`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 2, memory 4g, cpuset 2-5
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T21:28:47Z, finished 2026-10-09T21:40:53Z, seed 14682711089055224506
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1471 (4054) | 2341 | 41.80 | 236.6 | 148.7 | 0.35 / 0.63 | 1131 | 1123 / 3686 | 0.20 | 6.84 | 6.91 | 0 | 1045.4 (2200) | 0.19 |
| steady | 605 | 3472 (4105) | 2301 | 147.54 | 70.2 | 106.0 | 0.24 / 0.60 | 1305 | 1295 / 3686 | 0.35 | 0.00 | 5.16 | 0 | 509.3 (1415) | 0.14 |
| run | 725 | 3140 (4105) | 2307 | 189.34 | 83.2 | 113.2 | 0.26 / 0.63 | 1305 | 1295 / 3686 | 0.35 | 6.84 | 6.91 | 0 | 598.1 (2200) | 0.19 |

Arrival: 120.1 s to open every stream (complete: true); 3.48 ms of Sluice CPU per open; 81.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48197 | 3.1 | 5.7 | 10.4 | 31.8 |
| arrival | message | 48197 | 2.7 | 5.0 | 9.9 | 31.8 |
| arrival | message_commit_to_client | 48197 | 2.4 | 4.4 | 9.4 | 31.2 |
| arrival | message_write_to_commit | 48197 | 0.3 | 0.6 | 1.0 | 2.0 |
| arrival | open | 12000 | 45.3 | 56.1 | 64.3 | 168.0 |
| arrival | publish | 3259 | 0.4 | 0.8 | 1.7 | 23.4 |
| arrival | token | 811 | 109.1 | 130.6 | 140.3 | 212.7 |
| arrival | txn | 3171 | 2.0 | 3.4 | 5.1 | 13.1 |
| arrival | typing | 80263 | 0.6 | 1.2 | 3.9 | 60.6 |
| steady | member | 480000 | 3.3 | 6.7 | 10.9 | 150.8 |
| steady | message | 480000 | 3.1 | 6.4 | 10.6 | 98.8 |
| steady | message_commit_to_client | 480000 | 2.8 | 6.0 | 10.2 | 97.6 |
| steady | message_write_to_commit | 480000 | 0.3 | 0.5 | 0.8 | 7.4 |
| steady | publish | 32101 | 0.6 | 1.5 | 2.5 | 39.7 |
| steady | token | 7983 | 109.1 | 130.6 | 140.3 | 415.6 |
| steady | txn | 15840 | 2.0 | 4.2 | 7.3 | 41.8 |
| steady | typing | 1140327 | 0.7 | 1.8 | 2.7 | 77.8 |
| run | member | 528197 | 3.3 | 6.6 | 10.9 | 150.8 |
| run | message | 528197 | 3.0 | 6.4 | 10.6 | 98.8 |
| run | message_commit_to_client | 528197 | 2.7 | 5.9 | 10.2 | 97.6 |
| run | message_write_to_commit | 528197 | 0.3 | 0.5 | 0.9 | 7.4 |
| run | open | 12000 | 45.3 | 56.1 | 64.3 | 168.0 |
| run | publish | 35360 | 0.6 | 1.5 | 2.5 | 39.7 |
| run | token | 8794 | 109.1 | 130.6 | 140.3 | 415.6 |
| run | txn | 19011 | 2.0 | 4.0 | 6.8 | 41.8 |
| run | typing | 1220590 | 0.7 | 1.7 | 2.7 | 77.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528183 | 528197 | 100.00 |
| member | 528183 | 528197 | 100.00 |
| typing | 1220590 | 1220590 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2293 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_error:issuer_unavailable | 7 |
| sub_ok | 96000 |
| token_ok | 8794 |
| warning:replica_identity_insufficient | 12000 |
