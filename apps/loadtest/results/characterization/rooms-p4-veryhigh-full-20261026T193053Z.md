# Sluice load test — rooms — p4-veryhigh-full

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T19:20:17Z, finished 2026-10-26T19:30:53Z, seed 1027702810862134915
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | full | 30s | 10m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 230 (595) | 335 | 2.41 | 347.0 | 238.4 | 0.08 / 0.15 | 133 | 123 / 461 | 0.15 | 0.00 | 1.09 | 0 | 139.7 (1047) | 0.09 |
| steady | 605 | 498 (713) | 334 | 17.39 | 57.7 | 86.0 | 0.03 / 0.07 | 185 | 174 / 461 | 0.15 | 0.00 | 0.42 | 0 | 53.1 (1213) | 0.05 |
| run | 635 | 486 (713) | 334 | 19.80 | 64.2 | 93.3 | 0.03 / 0.15 | 185 | 174 / 461 | 0.15 | 0.00 | 1.09 | 0 | 57.2 (1213) | 0.09 |

Arrival: 30.1 s to open every stream (complete: true); 1.61 ms of Sluice CPU per open; 64.6 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1570 | 4.3 | 7.5 | 35.1 | 36.2 |
| arrival | message | 1570 | 4.2 | 7.5 | 35.1 | 36.1 |
| arrival | message_commit_to_client | 1570 | 3.8 | 6.5 | 32.6 | 33.6 |
| arrival | message_write_to_commit | 1570 | 0.4 | 1.0 | 2.6 | 2.6 |
| arrival | open | 1500 | 28.3 | 38.1 | 41.2 | 58.4 |
| arrival | publish | 188 | 0.6 | 1.1 | 2.5 | 6.4 |
| arrival | token | 33 | 27.5 | 35.6 | 37.1 | 40.3 |
| arrival | txn | 110 | 3.2 | 5.2 | 6.1 | 27.0 |
| arrival | typing | 3806 | 0.7 | 2.1 | 3.2 | 6.9 |
| steady | member | 60000 | 4.6 | 7.7 | 13.2 | 24.4 |
| steady | message | 60000 | 4.5 | 7.7 | 13.2 | 24.4 |
| steady | message_commit_to_client | 60000 | 4.1 | 7.1 | 12.9 | 24.1 |
| steady | message_write_to_commit | 60000 | 0.4 | 0.7 | 1.0 | 2.2 |
| steady | publish | 6000 | 0.8 | 1.5 | 1.8 | 5.1 |
| steady | token | 1035 | 28.3 | 38.7 | 41.7 | 46.0 |
| steady | txn | 2200 | 3.2 | 5.5 | 11.2 | 22.8 |
| steady | typing | 181500 | 0.9 | 1.5 | 1.8 | 5.1 |
| run | member | 61570 | 4.5 | 7.7 | 13.8 | 36.2 |
| run | message | 61570 | 4.5 | 7.6 | 13.8 | 36.1 |
| run | message_commit_to_client | 61570 | 4.1 | 7.1 | 13.2 | 33.6 |
| run | message_write_to_commit | 61570 | 0.4 | 0.7 | 1.0 | 2.6 |
| run | open | 1500 | 28.3 | 38.1 | 41.2 | 58.4 |
| run | publish | 6188 | 0.8 | 1.5 | 1.8 | 6.4 |
| run | token | 1068 | 28.3 | 38.7 | 41.7 | 46.0 |
| run | txn | 2310 | 3.2 | 5.5 | 10.8 | 27.0 |
| run | typing | 185306 | 0.9 | 1.5 | 1.9 | 6.9 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 61569 | 61570 | 100.00 |
| member | 61569 | 61570 | 100.00 |
| typing | 185306 | 185306 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 60.0 | 60.0 | 60.4 |
| public.room_messages | 4 | 60.0 | 60.0 | 60.4 |

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
| live heap MiB | 53.4 | 66.5 |
| memory floor MiB | 67.4 | 82.5 |

Live heap after each collection (MiB): 30 s 53.4, 55 s 61.9, 85 s 66.0, 110 s 68.6, 145 s 69.5, 175 s 73.5, 205 s 66.4, 235 s 73.9, 265 s 66.8, 295 s 72.5, 325 s 66.3, 355 s 72.2, 385 s 66.3, 415 s 71.8, 445 s 66.3, 475 s 71.8, 505 s 66.6, 535 s 66.8, 565 s 66.4, 600 s 66.6, 630 s 66.5.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 12000 |
| token_ok | 1068 |
| warning:replica_identity_insufficient | 1500 |
