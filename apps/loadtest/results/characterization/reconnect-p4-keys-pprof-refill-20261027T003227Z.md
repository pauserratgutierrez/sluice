# Sluice load test — reconnect — p4-keys-pprof-refill

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-27T00:19:21Z, finished 2026-10-27T00:32:27Z, seed 11631835733893313733
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 959 (1515) | 1160 | 46.55 | 161.7 | 133.7 | 0.16 / 0.39 | 449 | 437 / 461 | 0.22 | 3.43 | 3.52 | 0 | 166.8 (1297) | 0.47 |
| steady | 485 | 1092 (4232) | 1148 | 80.79 | 152.5 | 145.1 | 0.17 / 0.63 | 454 | 443 / 461 | 5.54 | 0.01 | 2.36 | 0 | 173.9 (1739) | 1.00 |
| run | 785 | 1041 (4232) | 1152 | 127.34 | 155.8 | 140.7 | 0.16 / 0.63 | 454 | 443 / 461 | 5.54 | 3.43 | 3.52 | 0 | 171.2 (1739) | 1.00 |

Arrival: 300.0 s to open every stream (complete: false); 8.35 ms of Sluice CPU per open; 78.4 KiB of live heap per stream after arrival.

Opens refused for capacity: 197004 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 75051 | 3.9 | 6.7 | 10.2 | 66.1 |
| arrival | message | 75051 | 3.9 | 6.7 | 10.2 | 66.1 |
| arrival | message_commit_to_client | 75051 | 3.5 | 6.0 | 9.4 | 65.7 |
| arrival | message_write_to_commit | 75051 | 0.4 | 0.8 | 1.2 | 6.5 |
| arrival | open | 4431 | 46.8 | 58.1 | 72.2 | 113.0 |
| arrival | publish | 5355 | 0.8 | 1.5 | 2.5 | 65.5 |
| arrival | token | 1291 | 46.3 | 56.6 | 61.2 | 101.8 |
| arrival | txn | 3961 | 2.6 | 4.0 | 5.4 | 15.8 |
| arrival | typing | 137689 | 0.9 | 1.9 | 3.5 | 66.9 |
| steady | member | 137810 | 3.8 | 6.7 | 22.4 | 688.4 |
| steady | message | 137810 | 3.8 | 6.7 | 22.1 | 662.1 |
| steady | message_commit_to_client | 137810 | 3.5 | 6.1 | 21.9 | 661.7 |
| steady | message_write_to_commit | 137810 | 0.3 | 0.6 | 0.9 | 4.0 |
| steady | open | 4431 | 905.2 | 2277.4 | 2637.8 | 2801.6 |
| steady | publish | 9356 | 0.7 | 1.4 | 3.6 | 139.1 |
| steady | replayed | 12482 | 14221.3 | 29229.1 | 31850.5 | 35221.4 |
| steady | token | 2310 | 46.3 | 56.6 | 62.7 | 1684.8 |
| steady | txn | 6336 | 2.5 | 3.8 | 4.8 | 11.3 |
| steady | typing | 241579 | 0.9 | 1.7 | 3.7 | 1705.0 |
| run | member | 212861 | 3.8 | 6.7 | 17.0 | 688.4 |
| run | message | 212861 | 3.8 | 6.7 | 16.8 | 662.1 |
| run | message_commit_to_client | 212861 | 3.5 | 6.0 | 16.5 | 661.7 |
| run | message_write_to_commit | 212861 | 0.3 | 0.7 | 1.0 | 6.5 |
| run | open | 8862 | 57.1 | 2072.6 | 2539.5 | 2801.6 |
| run | publish | 14711 | 0.7 | 1.5 | 3.0 | 139.1 |
| run | replayed | 12482 | 14221.3 | 29229.1 | 31850.5 | 35221.4 |
| run | token | 3601 | 46.3 | 56.6 | 62.2 | 1684.8 |
| run | txn | 10297 | 2.5 | 3.9 | 5.0 | 15.8 |
| run | typing | 379268 | 0.9 | 1.8 | 3.6 | 1705.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 212842 | 219102 | 102.94 |
| member | 212842 | 219102 | 102.94 |
| typing | 379121 | 379268 | 100.04 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4431 / 3288 | 14.87 / 17.50 / 20.39 / 20.52 | 14.73 / 20.30 | 75632 | 1739 | 0 | 3288 of 3288 | 0 | 36.31 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3288 of 3288 | 100.0 | 507.9 / 559.6 | 0.0 / 0.0 |
| 1 | public.room_members | 3288 of 3288 | 100.0 | 19.6 / 35.2 | 0.0 / 0.0 |
| 1 | public.room_messages | 3288 of 3288 | 100.0 | 19.6 / 35.2 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1141 | — | — | 60.0 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4431 | 4431 |
| holds.constants | 4431 | 4431 |
| holds.counted | 4629 | 4629 |
| holds.routed | 31017 | 31017 |
| holds.routed_cap | 35448 | 35448 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31017 | 31017 |
| subscriptions.constants | 9060 | 9060 |
| subscriptions.routed | 31017 | 31017 |
| subscriptions.routed_cap | 46437 | 46485 |
| subscriptions.subscriptions | 31017 | 31017 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.6 | 196.0 |
| memory floor MiB | 228.5 | 231.1 |

Live heap after each collection (MiB): 300 s 193.6, 320 s 201.5, 330 s 197.5, 355 s 201.0, 370 s 198.6, 390 s 195.6, 415 s 202.0, 430 s 198.1, 450 s 194.8, 470 s 201.7, 490 s 197.2, 505 s 194.6, 530 s 201.7, 550 s 134.3, 555 s 177.8, 560 s 195.5, 565 s 196.7, 580 s 202.5, 595 s 207.5, 605 s 197.1, 625 s 210.8, 640 s 200.0, 655 s 197.2, 670 s 201.3, 685 s 197.8, 705 s 203.6, 720 s 198.3, 740 s 202.9, 755 s 196.8, 775 s 210.7, 785 s 196.0.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3288 |
| open_failed:connection_refused | 18450 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 197004 |
| resumed_true:public.notifications | 3288 |
| resumed_true:public.room_members | 16440 |
| resumed_true:public.room_messages | 3288 |
| stream_end:unexpected_eof | 4431 |
| sub_ok | 70896 |
| token_ok | 3601 |
| warning:replica_identity_insufficient | 8862 |
