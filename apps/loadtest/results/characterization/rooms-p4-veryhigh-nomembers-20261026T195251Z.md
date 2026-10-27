# Sluice load test — rooms — p4-veryhigh-nomembers

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T19:42:16Z, finished 2026-10-26T19:52:51Z, seed 11547146680728489466
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | off | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 4 changes/s, 402 events/s (100 messages, 0 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 154 (475) | 4 | 2.11 | 454.2 | 19181.8 | 0.07 / 0.09 | 116 | 106 / 461 | 0.60 | 0.00 | 1.10 | 0 | 139.7 (1214) | 0.06 |
| steady | 605 | 399 (515) | 4 | 13.33 | 55.2 | 6056.3 | 0.02 / 0.06 | 151 | 141 / 461 | 0.60 | 0.00 | 0.02 | 0 | 53.0 (1237) | 0.02 |
| run | 635 | 388 (515) | 4 | 15.44 | 62.7 | 6681.1 | 0.02 / 0.09 | 151 | 141 / 461 | 0.60 | 0.00 | 1.10 | 0 | 57.1 (1237) | 0.06 |

Arrival: 30.1 s to open every stream (complete: true); 1.41 ms of Sluice CPU per open; 63.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | message | 1560 | 3.0 | 4.1 | 8.6 | 8.8 |
| arrival | message_commit_to_client | 1560 | 2.8 | 3.9 | 8.5 | 8.6 |
| arrival | message_write_to_commit | 1560 | 0.2 | 0.3 | 0.5 | 1.0 |
| arrival | open | 1500 | 28.5 | 38.7 | 42.8 | 63.7 |
| arrival | publish | 156 | 0.5 | 1.0 | 1.4 | 2.3 |
| arrival | token | 22 | 28.3 | 33.0 | 33.0 | 33.9 |
| arrival | txn | 110 | 2.5 | 3.6 | 5.0 | 6.7 |
| arrival | typing | 3086 | 0.6 | 1.1 | 2.2 | 2.9 |
| steady | message | 60025 | 3.5 | 6.4 | 14.1 | 29.4 |
| steady | message_commit_to_client | 60025 | 3.3 | 6.2 | 13.9 | 29.3 |
| steady | message_write_to_commit | 60025 | 0.2 | 0.3 | 0.5 | 2.1 |
| steady | publish | 6000 | 0.8 | 1.5 | 1.9 | 6.4 |
| steady | token | 987 | 28.0 | 39.2 | 41.2 | 47.1 |
| steady | txn | 2201 | 2.8 | 5.6 | 13.5 | 28.4 |
| steady | typing | 181500 | 0.9 | 1.5 | 1.9 | 14.4 |
| run | message | 61585 | 3.5 | 6.4 | 14.1 | 29.4 |
| run | message_commit_to_client | 61585 | 3.2 | 6.2 | 13.9 | 29.3 |
| run | message_write_to_commit | 61585 | 0.2 | 0.3 | 0.5 | 2.1 |
| run | open | 1500 | 28.5 | 38.7 | 42.8 | 63.7 |
| run | publish | 6156 | 0.8 | 1.5 | 1.9 | 6.4 |
| run | token | 1009 | 28.0 | 39.2 | 41.2 | 47.1 |
| run | txn | 2311 | 2.8 | 5.4 | 12.7 | 28.4 |
| run | typing | 184586 | 0.9 | 1.5 | 1.9 | 14.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61585 | 61585 | 100.00 |
| typing | 184585 | 184586 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 0 | — | — | 60.1 |
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
| live heap MiB | 45.0 | 56.2 |
| memory floor MiB | 58.6 | 71.8 |

Live heap after each collection (MiB): 30 s 45.0, 55 s 55.5, 90 s 52.7, 145 s 55.9, 185 s 52.5, 235 s 57.1, 280 s 52.6, 325 s 54.7, 365 s 52.5, 415 s 55.1, 460 s 52.4, 505 s 53.1, 545 s 52.3, 595 s 56.2.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1009 |
| warning:replica_identity_insufficient | 1500 |
