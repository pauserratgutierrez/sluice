# Sluice load test — reconnect — p4-keys-refill-age30

- Target: `sluice:p4-keys`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-27T00:05:48Z, finished 2026-10-27T00:18:53Z, seed 10636374541193198123
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1049 (1529) | 1160 | 33.84 | 107.5 | 97.2 | 0.11 / 0.21 | 451 | 441 / 461 | 0.25 | 0.03 | 3.51 | 0 | 180.6 (1418) | 0.25 |
| steady | 485 | 1287 (2725) | 1148 | 69.57 | 111.4 | 125.0 | 0.14 / 0.91 | 449 | 439 / 461 | 50.62 | 0.03 | 2.74 | 0 | 192.3 (2137) | 1.02 |
| run | 785 | 1196 (2725) | 1152 | 103.41 | 110.1 | 114.3 | 0.13 / 0.91 | 451 | 441 / 461 | 50.62 | 0.03 | 3.51 | 0 | 187.8 (2137) | 1.02 |

Arrival: 300.1 s to open every stream (complete: false); 5.81 ms of Sluice CPU per open; 73.5 KiB of live heap per stream after arrival.

Opens refused for capacity: 137830 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 81361 | 3.6 | 6.1 | 13.5 | 62.7 |
| arrival | message | 81361 | 3.6 | 6.1 | 13.5 | 62.7 |
| arrival | message_commit_to_client | 81361 | 3.3 | 5.6 | 12.6 | 62.2 |
| arrival | message_write_to_commit | 81361 | 0.3 | 0.5 | 0.9 | 13.2 |
| arrival | open | 4868 | 45.8 | 57.1 | 61.7 | 92.7 |
| arrival | publish | 5311 | 0.7 | 1.4 | 2.4 | 17.2 |
| arrival | token | 1355 | 45.3 | 56.1 | 61.2 | 187.9 |
| arrival | txn | 3961 | 2.3 | 4.0 | 6.5 | 20.0 |
| arrival | typing | 151975 | 0.8 | 1.7 | 2.8 | 62.3 |
| steady | member | 153429 | 3.8 | 6.7 | 37.6 | 264.8 |
| steady | message | 153431 | 3.8 | 6.7 | 37.1 | 339.6 |
| steady | message_commit_to_client | 153431 | 3.4 | 6.2 | 36.6 | 339.3 |
| steady | message_write_to_commit | 153431 | 0.3 | 0.5 | 0.8 | 4.6 |
| steady | open | 4924 | 741.4 | 1826.8 | 2146.3 | 2366.3 |
| steady | publish | 10524 | 0.7 | 1.5 | 7.9 | 173.2 |
| steady | replayed | 12888 | 12255.2 | 24248.3 | 26607.6 | 27980.2 |
| steady | token | 2593 | 46.3 | 56.6 | 102.9 | 726.3 |
| steady | txn | 6336 | 2.3 | 3.7 | 5.3 | 16.3 |
| steady | typing | 304495 | 0.9 | 1.8 | 4.8 | 730.7 |
| run | member | 234790 | 3.7 | 6.4 | 23.4 | 264.8 |
| run | message | 234792 | 3.7 | 6.4 | 22.9 | 339.6 |
| run | message_commit_to_client | 234792 | 3.4 | 6.0 | 22.4 | 339.3 |
| run | message_write_to_commit | 234792 | 0.3 | 0.5 | 0.8 | 13.2 |
| run | open | 9792 | 80.4 | 1548.3 | 2023.4 | 2366.3 |
| run | publish | 15835 | 0.7 | 1.5 | 3.4 | 173.2 |
| run | replayed | 12888 | 12255.2 | 24248.3 | 26607.6 | 27980.2 |
| run | token | 3948 | 45.8 | 56.6 | 87.6 | 726.3 |
| run | txn | 10297 | 2.3 | 3.8 | 5.9 | 20.0 |
| run | typing | 456470 | 0.9 | 1.8 | 3.4 | 730.7 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 234791 | 241236 | 102.74 |
| member | 234791 | 241234 | 102.74 |
| typing | 456327 | 456470 | 100.03 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4896 / 3969 | 9.45 / 12.72 / 13.22 / 13.24 | 9.36 / 13.12 | 59799 | 2137 | 0 | 3969 of 3969 | 0 | 32.22 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3969 of 3969 | 100.0 | 500.0 / 552.1 | 0.0 / 0.0 |
| 1 | public.room_members | 3969 of 3969 | 100.0 | 16.1 / 27.4 | 0.0 / 0.0 |
| 1 | public.room_messages | 3969 of 3969 | 100.0 | 16.1 / 27.4 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=30s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1150 | — | — | 30.0 |
| public.room_messages | 13 | — | — | 30.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4868 | 4896 |
| holds.constants | 4868 | 4896 |
| holds.counted | 5066 | 5094 |
| holds.routed | 34076 | 34272 |
| holds.routed_cap | 38944 | 39168 |
| holds.unindexed | 0 | 0 |
| holds.watches | 34076 | 34272 |
| subscriptions.constants | 9934 | 9990 |
| subscriptions.routed | 34076 | 34272 |
| subscriptions.routed_cap | 51036 | 51209 |
| subscriptions.subscriptions | 34076 | 34272 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 194.4 | 212.5 |
| memory floor MiB | 231.3 | 250.7 |

Live heap after each collection (MiB): 300 s 194.4, 315 s 201.3, 330 s 194.0, 355 s 219.0, 360 s 193.6, 380 s 201.1, 385 s 192.9, 400 s 199.6, 415 s 192.2, 435 s 200.4, 450 s 194.5, 475 s 212.6, 485 s 196.0, 505 s 211.2, 520 s 198.0, 535 s 196.5, 550 s 135.7, 555 s 209.1, 565 s 208.3, 570 s 197.8, 585 s 202.2, 595 s 197.1, 610 s 200.9, 625 s 197.4, 645 s 204.5, 660 s 199.2, 680 s 206.0, 690 s 197.2, 710 s 206.9, 720 s 198.3, 745 s 207.4, 755 s 198.2, 775 s 212.5.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3969 |
| open_failed:connection_refused | 19854 |
| open_failed:connection_reset | 1 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 137830 |
| resumed_true:public.notifications | 3969 |
| resumed_true:public.room_members | 19845 |
| resumed_true:public.room_messages | 3969 |
| stream_end:unexpected_eof | 4896 |
| sub_ok | 78336 |
| token_failed:connection_refused | 1 |
| token_ok | 3947 |
| warning:replica_identity_insufficient | 9792 |
