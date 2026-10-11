# Sluice load test — rooms — p3i7-unicorn-1cpu-5m

- Target: `sluice:p3-i7`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-10T01:15:39Z, finished 2026-10-10T01:22:45Z, seed 13213827299022769012
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 5m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1446 (3793) | 2362 | 40.93 | 235.6 | 144.3 | 0.34 / 0.56 | 901 | 893 / 3686 | 4.47 | 6.83 | 6.90 | 0 | 1049.9 (2105) | 0.21 |
| steady | 305 | 3443 (4041) | 2282 | 71.50 | 68.1 | 102.7 | 0.23 / 0.40 | 1036 | 1027 / 3686 | 4.47 | 0.02 | 5.39 | 0 | 514.0 (1402) | 0.12 |
| run | 425 | 2879 (4041) | 2305 | 112.43 | 91.9 | 114.8 | 0.26 / 0.56 | 1036 | 1027 / 3686 | 4.47 | 6.83 | 6.90 | 0 | 665.4 (2105) | 0.21 |

Arrival: 120.1 s to open every stream (complete: true); 3.41 ms of Sluice CPU per open; 65.9 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48161 | 3.4 | 7.9 | 54.5 | 109.7 |
| arrival | message | 48162 | 3.3 | 7.8 | 54.5 | 108.8 |
| arrival | message_commit_to_client | 48162 | 3.0 | 7.2 | 54.5 | 108.4 |
| arrival | message_write_to_commit | 48162 | 0.3 | 0.7 | 1.2 | 8.9 |
| arrival | open | 12000 | 45.8 | 60.2 | 148.5 | 266.7 |
| arrival | publish | 3298 | 0.4 | 1.3 | 32.1 | 69.3 |
| arrival | token | 812 | 109.1 | 152.6 | 276.5 | 412.2 |
| arrival | txn | 3171 | 2.1 | 3.4 | 4.7 | 23.1 |
| arrival | typing | 77401 | 0.6 | 2.0 | 41.2 | 69.9 |
| steady | member | 240028 | 3.6 | 9.3 | 52.0 | 284.2 |
| steady | message | 240028 | 3.6 | 9.3 | 52.0 | 284.1 |
| steady | message_commit_to_client | 240028 | 3.3 | 8.5 | 52.0 | 283.7 |
| steady | message_write_to_commit | 240028 | 0.3 | 0.5 | 0.9 | 6.1 |
| steady | publish | 16051 | 0.6 | 2.0 | 33.0 | 74.8 |
| steady | token | 4082 | 109.1 | 142.3 | 288.8 | 489.3 |
| steady | txn | 7921 | 2.1 | 4.0 | 7.2 | 34.3 |
| steady | typing | 570177 | 0.7 | 2.3 | 35.1 | 76.5 |
| run | member | 288189 | 3.6 | 9.0 | 52.5 | 284.2 |
| run | message | 288190 | 3.6 | 9.0 | 52.5 | 284.1 |
| run | message_commit_to_client | 288190 | 3.3 | 8.4 | 52.0 | 283.7 |
| run | message_write_to_commit | 288190 | 0.3 | 0.6 | 1.0 | 8.9 |
| run | open | 12000 | 45.8 | 60.2 | 148.5 | 266.7 |
| run | publish | 19349 | 0.5 | 1.9 | 33.0 | 74.8 |
| run | token | 4894 | 109.1 | 144.4 | 288.8 | 489.3 |
| run | txn | 11092 | 2.1 | 3.8 | 6.5 | 34.3 |
| run | typing | 647578 | 0.7 | 2.3 | 35.6 | 76.5 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 288172 | 288190 | 100.01 |
| member | 288172 | 288189 | 100.01 |
| typing | 647575 | 647578 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 2 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2300 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 4894 |
| warning:replica_identity_insufficient | 12000 |
