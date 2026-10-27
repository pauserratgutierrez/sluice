# Sluice load test — reconnect — p4-fix-refill-age30

- Target: `sluice:p4-fix`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T22:48:21Z, finished 2026-10-26T23:01:26Z, seed 14529866412653188542
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1075 (1725) | 1160 | 32.22 | 99.9 | 92.5 | 0.11 / 0.21 | 449 | 438 / 461 | 0.29 | 3.41 | 3.49 | 0 | 182.6 (1340) | 0.22 |
| steady | 485 | 1309 (8110) | 1148 | 71.29 | 112.3 | 128.0 | 0.15 / 1.01 | 452 | 441 / 461 | 49.32 | 1.09 | 2.85 | 0 | 194.7 (1809) | 0.90 |
| run | 785 | 1220 (8110) | 1153 | 103.51 | 108.1 | 114.4 | 0.13 / 1.01 | 452 | 441 / 461 | 49.32 | 3.41 | 3.49 | 0 | 190.1 (1809) | 0.90 |

Arrival: 300.0 s to open every stream (complete: false); 5.55 ms of Sluice CPU per open; 70.4 KiB of live heap per stream after arrival.

Opens refused for capacity: 129939 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 82286 | 3.6 | 6.1 | 11.1 | 54.2 |
| arrival | message | 82286 | 3.6 | 6.1 | 11.1 | 54.2 |
| arrival | message_commit_to_client | 82286 | 3.3 | 5.7 | 10.3 | 53.8 |
| arrival | message_write_to_commit | 82286 | 0.3 | 0.5 | 0.8 | 7.0 |
| arrival | open | 4949 | 45.8 | 57.1 | 61.2 | 91.9 |
| arrival | publish | 5555 | 0.7 | 1.5 | 2.3 | 46.9 |
| arrival | token | 1347 | 45.8 | 56.1 | 59.6 | 82.2 |
| arrival | txn | 3960 | 2.3 | 3.8 | 5.8 | 36.1 |
| arrival | typing | 158002 | 0.8 | 1.7 | 2.6 | 46.9 |
| steady | member | 154018 | 3.7 | 10.0 | 2375.7 | 10658.0 |
| steady | message | 154019 | 3.7 | 10.0 | 2375.7 | 10626.3 |
| steady | message_commit_to_client | 154019 | 3.4 | 9.4 | 2375.7 | 10625.9 |
| steady | message_write_to_commit | 154019 | 0.3 | 0.5 | 0.6 | 3.2 |
| steady | open | 4985 | 5144.6 | 10813.4 | 11337.7 | 12900.5 |
| steady | publish | 10589 | 0.7 | 1.7 | 41.7 | 682.3 |
| steady | replayed | 16524 | 15007.7 | 32112.6 | 34865.2 | 37495.8 |
| steady | token | 2565 | 45.8 | 58.1 | 954.4 | 7800.4 |
| steady | txn | 6336 | 2.3 | 4.0 | 14.4 | 52.6 |
| steady | typing | 310285 | 0.9 | 1.9 | 70.1 | 10558.1 |
| run | member | 236304 | 3.7 | 7.3 | 872.4 | 10658.0 |
| run | message | 236305 | 3.7 | 7.3 | 831.5 | 10626.3 |
| run | message_commit_to_client | 236305 | 3.3 | 6.9 | 831.5 | 10625.9 |
| run | message_write_to_commit | 236305 | 0.3 | 0.5 | 0.7 | 7.0 |
| run | open | 9934 | 84.5 | 10420.2 | 11075.6 | 12900.5 |
| run | publish | 16144 | 0.7 | 1.6 | 20.1 | 682.3 |
| run | replayed | 16524 | 15007.7 | 32112.6 | 34865.2 | 37495.8 |
| run | token | 3912 | 45.8 | 57.6 | 464.9 | 7800.4 |
| run | txn | 10296 | 2.3 | 3.9 | 11.8 | 52.6 |
| run | typing | 468287 | 0.9 | 1.8 | 19.8 | 10558.1 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 236049 | 244567 | 103.61 |
| member | 236049 | 244566 | 103.61 |
| typing | 466599 | 468287 | 100.36 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4967 / 4127 | 18.75 / 21.25 / 23.02 / 23.12 | 18.61 / 22.86 | 58610 | 1809 | 0 | 4127 of 4127 | 0 | 37.05 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 4127 of 4127 | 100.0 | 500.6 / 559.8 | 0.0 / 0.0 |
| 1 | public.room_members | 4127 of 4127 | 100.0 | 16.2 / 36.3 | 0.0 / 0.0 |
| 1 | public.room_messages | 4127 of 4127 | 100.0 | 16.2 / 36.3 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=30s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1138 | — | — | 30.2 |
| public.room_messages | 13 | — | — | 30.2 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4949 | 4967 |
| holds.constants | 4949 | 4967 |
| holds.counted | 5147 | 5165 |
| holds.routed | 34643 | 34769 |
| holds.routed_cap | 39592 | 39736 |
| holds.unindexed | 0 | 0 |
| holds.watches | 34643 | 34769 |
| subscriptions.constants | 10096 | 10132 |
| subscriptions.routed | 34643 | 34769 |
| subscriptions.routed_cap | 51765 | 51753 |
| subscriptions.subscriptions | 34643 | 34769 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.9 | 196.2 |
| memory floor MiB | 230.3 | 234.2 |

Live heap after each collection (MiB): 300 s 192.9, 325 s 216.6, 335 s 194.1, 355 s 219.7, 360 s 192.1, 380 s 201.9, 390 s 193.2, 410 s 201.3, 420 s 193.5, 445 s 203.2, 460 s 195.1, 480 s 193.6, 500 s 200.8, 515 s 192.7, 535 s 191.3, 550 s 110.7, 555 s 219.5, 560 s 212.7, 565 s 214.4, 575 s 196.2, 595 s 216.5, 600 s 199.9, 620 s 208.1, 625 s 195.1, 640 s 200.2, 655 s 213.4, 670 s 199.0, 685 s 195.3, 700 s 200.1, 715 s 195.9, 735 s 201.9, 750 s 196.9, 770 s 204.7, 785 s 196.2.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 4122 |
| open_failed:connection_refused | 19974 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 129939 |
| resumed_true:public.notifications | 4127 |
| resumed_true:public.room_members | 20635 |
| resumed_true:public.room_messages | 4127 |
| stream_end:unexpected_eof | 4967 |
| sub_ok | 79472 |
| token_ok | 3912 |
| warning:replica_identity_insufficient | 9934 |
