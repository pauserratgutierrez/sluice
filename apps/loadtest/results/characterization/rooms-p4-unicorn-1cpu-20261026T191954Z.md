# Sluice load test — rooms — p4-unicorn-1cpu

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T19:07:48Z, finished 2026-10-26T19:19:54Z, seed 990708592275904316
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1482 (4324) | 2324 | 25.59 | 143.7 | 91.6 | 0.21 / 1.14 | 1096 | 1086 / 3686 | 18.38 | 6.84 | 6.92 | 0 | 411.8 (1461) | 0.18 |
| steady | 605 | 3471 (4479) | 2300 | 112.46 | 53.5 | 80.8 | 0.19 / 0.38 | 1247 | 1237 / 3686 | 18.38 | 0.02 | 5.44 | 0 | 429.8 (1272) | 0.14 |
| run | 725 | 3142 (4479) | 2304 | 138.05 | 60.6 | 82.6 | 0.19 / 1.14 | 1247 | 1237 / 3686 | 18.38 | 6.84 | 6.92 | 0 | 426.8 (1461) | 0.18 |

Arrival: 120.1 s to open every stream (complete: true); 2.13 ms of Sluice CPU per open; 87.1 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48841 | 3.2 | 5.9 | 29.3 | 68.2 |
| arrival | message | 48841 | 3.1 | 5.9 | 29.3 | 68.2 |
| arrival | message_commit_to_client | 48841 | 2.8 | 5.4 | 29.1 | 67.8 |
| arrival | message_write_to_commit | 48841 | 0.3 | 0.7 | 1.4 | 7.4 |
| arrival | open | 12000 | 28.0 | 39.2 | 58.1 | 156.1 |
| arrival | publish | 3415 | 0.4 | 1.0 | 11.2 | 60.3 |
| arrival | token | 846 | 27.5 | 38.7 | 64.3 | 101.5 |
| arrival | txn | 3169 | 2.1 | 3.7 | 5.0 | 13.6 |
| arrival | typing | 80097 | 0.6 | 1.4 | 20.6 | 67.8 |
| steady | member | 480056 | 3.4 | 6.9 | 31.6 | 240.6 |
| steady | message | 480056 | 3.3 | 6.9 | 31.4 | 240.2 |
| steady | message_commit_to_client | 480056 | 3.1 | 6.5 | 31.1 | 239.5 |
| steady | message_write_to_commit | 480056 | 0.3 | 0.5 | 0.8 | 18.5 |
| steady | publish | 32104 | 0.6 | 1.4 | 7.9 | 73.9 |
| steady | token | 8067 | 27.8 | 39.2 | 72.2 | 246.0 |
| steady | txn | 15841 | 2.1 | 4.1 | 7.3 | 33.5 |
| steady | typing | 1140465 | 0.7 | 1.8 | 10.7 | 87.2 |
| run | member | 528897 | 3.3 | 6.9 | 31.1 | 240.6 |
| run | message | 528897 | 3.3 | 6.9 | 30.8 | 240.2 |
| run | message_commit_to_client | 528897 | 3.0 | 6.4 | 30.6 | 239.5 |
| run | message_write_to_commit | 528897 | 0.3 | 0.5 | 0.8 | 18.5 |
| run | open | 12000 | 28.0 | 39.2 | 58.1 | 156.1 |
| run | publish | 35519 | 0.5 | 1.4 | 8.3 | 73.9 |
| run | token | 8915 | 27.8 | 39.2 | 71.2 | 246.0 |
| run | txn | 19010 | 2.1 | 4.0 | 6.8 | 33.5 |
| run | typing | 1220562 | 0.7 | 1.8 | 12.5 | 87.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528895 | 528897 | 100.00 |
| member | 528895 | 528897 | 100.00 |
| typing | 1220560 | 1220562 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2318 | 60.0 | 60.0 | 60.0 |
| public.room_messages | 27 | 60.0 | 60.0 | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 121 s | 725 s |
| --- | ---: | ---: |
| streams | 12000 | 12000 |
| holds.constants | 12000 | 12000 |
| holds.counted | 12396 | 12396 |
| holds.routed | 84000 | 84000 |
| holds.routed_cap | 96000 | 96040 |
| holds.unindexed | 0 | 0 |
| holds.watches | 84000 | 84000 |
| subscriptions.constants | 24396 | 24396 |
| subscriptions.routed | 84000 | 84000 |
| subscriptions.routed_cap | 122448 | 122448 |
| subscriptions.subscriptions | 84000 | 84000 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 443.6 | 485.9 |
| memory floor MiB | 523.6 | 573.5 |

Live heap after each collection (MiB): 121 s 443.6, 125 s 486.6, 155 s 484.5, 185 s 485.3, 215 s 488.3, 245 s 490.7, 280 s 487.0, 310 s 489.1, 345 s 488.3, 375 s 494.0, 410 s 493.8, 445 s 500.4, 480 s 521.8, 510 s 477.5, 540 s 519.4, 570 s 479.8, 600 s 479.9, 630 s 480.2, 660 s 481.6, 690 s 486.1, 720 s 485.9.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 8913 |
| warning:replica_identity_insufficient | 12000 |
