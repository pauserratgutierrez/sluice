# Sluice load test — rooms — p4-pprof-unicorn-1cpu

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T18:23:03Z, finished 2026-10-26T18:35:09Z, seed 13997630390156473163
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1488 (3878) | 2349 | 27.38 | 153.1 | 97.0 | 0.23 / 0.44 | 893 | 883 / 3686 | 0.21 | 6.85 | 6.89 | 0 | 410.1 (1479) | 0.20 |
| steady | 605 | 3472 (4670) | 2300 | 116.86 | 55.6 | 84.0 | 0.19 / 0.37 | 1043 | 1032 / 3686 | 0.21 | 0.17 | 3.38 | 0 | 429.9 (1270) | 0.13 |
| run | 725 | 3143 (4670) | 2309 | 144.24 | 63.3 | 86.2 | 0.20 / 0.44 | 1043 | 1032 / 3686 | 0.21 | 6.85 | 6.89 | 0 | 426.6 (1479) | 0.20 |

Arrival: 120.1 s to open every stream (complete: true); 2.28 ms of Sluice CPU per open; 71.5 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48388 | 3.1 | 6.2 | 26.8 | 65.1 |
| arrival | message | 48388 | 3.1 | 6.1 | 26.5 | 64.5 |
| arrival | message_commit_to_client | 48388 | 2.8 | 5.6 | 26.0 | 63.9 |
| arrival | message_write_to_commit | 48388 | 0.3 | 0.6 | 1.1 | 7.9 |
| arrival | open | 12000 | 28.3 | 39.2 | 60.7 | 159.7 |
| arrival | publish | 3517 | 0.5 | 0.9 | 19.1 | 60.1 |
| arrival | token | 829 | 27.8 | 38.7 | 45.8 | 82.1 |
| arrival | txn | 3172 | 2.1 | 3.8 | 5.5 | 34.3 |
| arrival | typing | 82015 | 0.6 | 1.3 | 28.0 | 61.5 |
| steady | member | 480000 | 3.4 | 8.8 | 40.2 | 522.0 |
| steady | message | 480000 | 3.4 | 8.8 | 40.2 | 522.0 |
| steady | message_commit_to_client | 480000 | 3.1 | 8.3 | 39.7 | 521.8 |
| steady | message_write_to_commit | 480000 | 0.3 | 0.5 | 0.8 | 7.5 |
| steady | publish | 32101 | 0.6 | 1.7 | 19.3 | 155.9 |
| steady | token | 8078 | 27.8 | 39.2 | 78.3 | 605.8 |
| steady | txn | 15840 | 2.1 | 4.6 | 15.0 | 42.0 |
| steady | typing | 1140327 | 0.7 | 2.0 | 23.4 | 174.9 |
| run | member | 528388 | 3.4 | 8.5 | 39.2 | 522.0 |
| run | message | 528388 | 3.4 | 8.5 | 39.2 | 522.0 |
| run | message_commit_to_client | 528388 | 3.1 | 8.0 | 38.7 | 521.8 |
| run | message_write_to_commit | 528388 | 0.3 | 0.5 | 0.9 | 7.9 |
| run | open | 12000 | 28.3 | 39.2 | 60.7 | 159.7 |
| run | publish | 35618 | 0.5 | 1.6 | 19.3 | 155.9 |
| run | token | 8907 | 27.8 | 39.2 | 75.3 | 605.8 |
| run | txn | 19012 | 2.1 | 4.5 | 14.0 | 42.0 |
| run | typing | 1222342 | 0.7 | 2.0 | 24.2 | 174.9 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528387 | 528388 | 100.00 |
| member | 528387 | 528388 | 100.00 |
| typing | 1222342 | 1222342 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2294 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

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
| live heap MiB | 358.5 | 397.3 |
| memory floor MiB | 434.0 | 479.9 |

Live heap after each collection (MiB): 121 s 358.5, 125 s 399.5, 150 s 400.1, 180 s 399.2, 210 s 416.8, 230 s 393.7, 250 s 395.0, 270 s 397.5, 295 s 393.8, 315 s 393.9, 335 s 395.9, 365 s 400.7, 390 s 400.2, 420 s 400.4, 445 s 409.5, 465 s 393.7, 485 s 398.4, 510 s 399.5, 530 s 391.5, 550 s 392.9, 575 s 396.0, 600 s 398.0, 630 s 398.6, 660 s 412.7, 680 s 389.2, 695 s 401.0, 720 s 397.3.

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 8907 |
| warning:replica_identity_insufficient | 12000 |
