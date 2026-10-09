# Sluice load test — rooms — v081f-unicorn-1cpu-full

- Target: `sluice:0.8.1-rc2`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T22:27:41Z, finished 2026-10-09T22:39:47Z, seed 8120496520044538402
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1437 (4290) | 2327 | 42.75 | 247.6 | 152.9 | 0.36 / 0.60 | 1142 | 1133 / 3686 | 0.36 | 1.40 | 6.93 | 0 | 1034.1 (2200) | 0.22 |
| steady | 605 | 3472 (4625) | 2301 | 148.48 | 70.7 | 106.7 | 0.25 / 0.44 | 1302 | 1291 / 3686 | 0.45 | 0.17 | 5.42 | 0 | 510.7 (1419) | 0.16 |
| run | 725 | 3135 (4625) | 2305 | 191.23 | 84.1 | 114.4 | 0.26 / 0.60 | 1302 | 1291 / 3686 | 0.45 | 1.40 | 6.93 | 0 | 597.4 (2200) | 0.22 |

Arrival: 120.1 s to open every stream (complete: true); 3.56 ms of Sluice CPU per open; 82.3 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 47945 | 3.2 | 9.2 | 59.1 | 163.5 |
| arrival | message | 47946 | 2.8 | 7.8 | 55.6 | 163.4 |
| arrival | message_commit_to_client | 47946 | 2.5 | 7.3 | 55.0 | 163.0 |
| arrival | message_write_to_commit | 47946 | 0.3 | 0.6 | 1.2 | 7.9 |
| arrival | open | 12000 | 45.8 | 69.1 | 160.8 | 264.1 |
| arrival | publish | 3250 | 0.4 | 2.4 | 38.1 | 73.6 |
| arrival | token | 757 | 111.1 | 209.9 | 333.8 | 404.9 |
| arrival | txn | 3171 | 2.1 | 3.7 | 5.3 | 11.3 |
| arrival | typing | 76757 | 0.6 | 7.0 | 48.4 | 78.8 |
| steady | member | 480000 | 3.5 | 10.9 | 56.6 | 537.0 |
| steady | message | 480000 | 3.3 | 10.3 | 54.0 | 531.2 |
| steady | message_commit_to_client | 480000 | 3.0 | 9.8 | 54.0 | 531.0 |
| steady | message_write_to_commit | 480000 | 0.3 | 0.5 | 0.8 | 18.6 |
| steady | publish | 32102 | 0.6 | 1.9 | 31.4 | 109.5 |
| steady | token | 8027 | 109.1 | 140.3 | 280.6 | 862.0 |
| steady | txn | 15840 | 2.1 | 4.6 | 8.5 | 36.3 |
| steady | typing | 1140411 | 0.7 | 2.2 | 34.0 | 189.2 |
| run | member | 527945 | 3.5 | 10.8 | 57.1 | 537.0 |
| run | message | 527946 | 3.2 | 10.2 | 54.0 | 531.2 |
| run | message_commit_to_client | 527946 | 2.9 | 9.7 | 54.0 | 531.0 |
| run | message_write_to_commit | 527946 | 0.3 | 0.5 | 1.0 | 18.6 |
| run | open | 12000 | 45.8 | 69.1 | 160.8 | 264.1 |
| run | publish | 35352 | 0.5 | 1.9 | 31.9 | 109.5 |
| run | token | 8784 | 109.1 | 144.4 | 288.8 | 862.0 |
| run | txn | 19011 | 2.1 | 4.5 | 8.0 | 36.3 |
| run | typing | 1217168 | 0.7 | 2.2 | 35.1 | 189.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 527934 | 527946 | 100.00 |
| member | 527934 | 527945 | 100.00 |
| typing | 1217168 | 1217168 | 100.00 |

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
| token_ok | 8784 |
| warning:replica_identity_insufficient | 12000 |
