# Sluice load test — rooms — p4-veryhigh-pk

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T19:31:17Z, finished 2026-10-26T19:41:52Z, seed 9509748762179632727
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | pk | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 207 (551) | 338 | 2.83 | 453.2 | 277.7 | 0.09 / 0.12 | 131 | 121 / 461 | 1.33 | 0.00 | 1.09 | 0 | 142.5 (844) | 0.07 |
| steady | 605 | 498 (840) | 334 | 15.80 | 52.4 | 78.1 | 0.03 / 0.07 | 172 | 162 / 461 | 1.33 | 0.00 | 0.38 | 0 | 53.1 (802) | 0.02 |
| run | 635 | 485 (840) | 335 | 18.63 | 60.5 | 87.7 | 0.03 / 0.12 | 172 | 162 / 461 | 1.33 | 0.00 | 1.09 | 0 | 57.3 (844) | 0.07 |

Arrival: 30.1 s to open every stream (complete: true); 1.89 ms of Sluice CPU per open; 72.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1499 | 3.9 | 5.1 | 6.0 | 7.7 |
| arrival | message | 1499 | 3.9 | 5.1 | 6.0 | 7.7 |
| arrival | message_commit_to_client | 1499 | 3.4 | 4.4 | 5.2 | 6.5 |
| arrival | message_write_to_commit | 1499 | 0.5 | 1.0 | 1.5 | 1.8 |
| arrival | open | 1500 | 28.5 | 39.2 | 43.8 | 64.9 |
| arrival | publish | 157 | 0.5 | 1.1 | 1.4 | 4.0 |
| arrival | token | 35 | 27.8 | 36.1 | 36.1 | 38.7 |
| arrival | txn | 111 | 3.1 | 4.4 | 5.2 | 5.9 |
| arrival | typing | 3247 | 0.6 | 1.1 | 1.3 | 4.0 |
| steady | member | 60025 | 4.2 | 7.5 | 14.1 | 33.7 |
| steady | message | 60025 | 4.2 | 7.5 | 14.1 | 33.7 |
| steady | message_commit_to_client | 60025 | 3.9 | 7.0 | 13.8 | 33.2 |
| steady | message_write_to_commit | 60025 | 0.3 | 0.6 | 0.9 | 1.9 |
| steady | publish | 6002 | 0.9 | 1.5 | 1.9 | 16.5 |
| steady | token | 1020 | 28.3 | 38.7 | 41.2 | 45.0 |
| steady | txn | 2201 | 3.2 | 5.0 | 12.4 | 32.1 |
| steady | typing | 181548 | 0.9 | 1.5 | 1.9 | 16.6 |
| run | member | 61524 | 4.2 | 7.4 | 14.1 | 33.7 |
| run | message | 61524 | 4.2 | 7.4 | 14.1 | 33.7 |
| run | message_commit_to_client | 61524 | 3.8 | 6.9 | 13.8 | 33.2 |
| run | message_write_to_commit | 61524 | 0.3 | 0.6 | 0.9 | 1.9 |
| run | open | 1500 | 28.5 | 39.2 | 43.8 | 64.9 |
| run | publish | 6159 | 0.9 | 1.5 | 1.9 | 16.5 |
| run | token | 1055 | 28.3 | 38.1 | 41.2 | 45.0 |
| run | txn | 2312 | 3.2 | 5.0 | 12.0 | 32.1 |
| run | typing | 184795 | 0.9 | 1.5 | 1.9 | 16.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61524 | 61524 | 100.00 |
| member | 61524 | 61524 | 100.00 |
| typing | 184795 | 184795 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 60.0 | 60.0 | 60.1 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.1 |

## Indexes and memory, start and end of the steady phase

| | 30 s | 635 s |
| --- | ---: | ---: |
| streams | 1500 | 1500 |
| holds.constants | 1500 | 1500 |
| holds.counted | 1555 | 1555 |
| holds.routed | 10500 | 10500 |
| holds.routed_cap | 12000 | 12040 |
| holds.unindexed | 0 | 0 |
| holds.watches | 10500 | 10500 |
| subscriptions.constants | 3055 | 3055 |
| subscriptions.routed | 10500 | 10500 |
| subscriptions.routed_cap | 15420 | 15420 |
| subscriptions.subscriptions | 10500 | 10500 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 48.3 | 63.4 |
| memory floor MiB | 62.3 | 79.0 |

Live heap after each collection (MiB): 30 s 48.3, 35 s 54.8, 60 s 60.0, 85 s 66.6, 115 s 62.8, 150 s 63.0, 180 s 62.8, 210 s 62.5, 245 s 62.6, 280 s 63.0, 315 s 63.3, 350 s 64.1, 385 s 66.0, 420 s 62.7, 450 s 62.7, 485 s 62.3, 515 s 62.8, 550 s 62.8, 585 s 63.2, 620 s 63.4.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1055 |
| warning:replica_identity_insufficient | 1500 |
