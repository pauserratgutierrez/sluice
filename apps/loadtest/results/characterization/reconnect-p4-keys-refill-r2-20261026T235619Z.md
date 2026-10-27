# Sluice load test — reconnect — p4-keys-refill-r2

- Target: `sluice:p4-keys`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T23:43:13Z, finished 2026-10-26T23:56:19Z, seed 4796993004991075211
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 936 (1436) | 1164 | 33.16 | 118.0 | 95.0 | 0.11 / 0.21 | 449 | 439 / 461 | 4.52 | 0.43 | 3.53 | 0 | 166.4 (1359) | 0.26 |
| steady | 485 | 1149 (3437) | 1148 | 65.49 | 117.6 | 117.6 | 0.14 / 0.93 | 450 | 440 / 461 | 4.52 | 0.02 | 2.22 | 0 | 174.7 (1785) | 0.87 |
| run | 785 | 1067 (3437) | 1154 | 98.65 | 117.7 | 108.9 | 0.13 / 0.93 | 450 | 440 / 461 | 4.52 | 0.43 | 3.53 | 0 | 171.5 (1785) | 0.87 |

Arrival: 300.1 s to open every stream (complete: false); 6.00 ms of Sluice CPU per open; 87.3 KiB of live heap per stream after arrival.

Opens refused for capacity: 194562 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 75228 | 3.7 | 5.8 | 9.3 | 48.3 |
| arrival | message | 75228 | 3.6 | 5.8 | 9.3 | 48.3 |
| arrival | message_commit_to_client | 75228 | 3.3 | 5.3 | 8.8 | 48.0 |
| arrival | message_write_to_commit | 75228 | 0.3 | 0.5 | 0.7 | 6.1 |
| arrival | open | 4419 | 46.3 | 57.1 | 61.2 | 103.6 |
| arrival | publish | 4782 | 0.7 | 1.4 | 2.1 | 39.9 |
| arrival | token | 1262 | 45.3 | 56.1 | 58.6 | 84.9 |
| arrival | txn | 3960 | 2.4 | 3.8 | 5.6 | 18.3 |
| arrival | typing | 130565 | 0.8 | 1.5 | 2.3 | 40.4 |
| steady | member | 138770 | 3.6 | 6.7 | 120.3 | 437.8 |
| steady | message | 138771 | 3.6 | 6.6 | 119.3 | 437.8 |
| steady | message_commit_to_client | 138771 | 3.3 | 6.2 | 119.3 | 437.5 |
| steady | message_write_to_commit | 138771 | 0.3 | 0.5 | 0.6 | 6.5 |
| steady | open | 4477 | 806.9 | 1810.4 | 1925.1 | 2141.6 |
| steady | publish | 9582 | 0.7 | 1.3 | 5.2 | 79.9 |
| steady | replayed | 11888 | 13303.8 | 26607.6 | 28704.8 | 30532.7 |
| steady | token | 2343 | 45.8 | 57.1 | 81.4 | 1547.6 |
| steady | txn | 6336 | 2.3 | 3.8 | 106.0 | 432.6 |
| steady | typing | 267628 | 0.8 | 1.6 | 4.3 | 1048.6 |
| run | member | 213998 | 3.6 | 6.2 | 80.4 | 437.8 |
| run | message | 213999 | 3.6 | 6.2 | 71.2 | 437.8 |
| run | message_commit_to_client | 213999 | 3.3 | 5.8 | 71.2 | 437.5 |
| run | message_write_to_commit | 213999 | 0.3 | 0.5 | 0.7 | 6.5 |
| run | open | 8896 | 57.1 | 1663.0 | 1892.4 | 2141.6 |
| run | publish | 14364 | 0.7 | 1.4 | 2.7 | 79.9 |
| run | replayed | 11888 | 13303.8 | 26607.6 | 28704.8 | 30532.7 |
| run | token | 3605 | 45.8 | 56.6 | 63.2 | 1547.6 |
| run | txn | 10296 | 2.3 | 3.8 | 33.5 | 432.6 |
| run | typing | 398193 | 0.8 | 1.6 | 2.9 | 1048.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 213997 | 219943 | 102.78 |
| member | 213997 | 219942 | 102.78 |
| typing | 398100 | 398193 | 100.02 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4448 / 3368 | 12.44 / 15.06 / 15.65 / 15.75 | 12.29 / 15.54 | 73937 | 1785 | 0 | 3368 of 3368 | 0 | 29.05 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3368 of 3368 | 100.0 | 506.3 / 555.2 | 0.0 / 0.0 |
| 1 | public.room_members | 3368 of 3368 | 100.0 | 18.1 / 30.5 | 0.0 / 0.0 |
| 1 | public.room_messages | 3368 of 3368 | 100.0 | 18.1 / 30.5 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1149 | — | — | 60.1 |
| public.room_messages | 13 | — | — | 60.1 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4419 | 4448 |
| holds.constants | 4419 | 4448 |
| holds.counted | 4617 | 4646 |
| holds.routed | 30933 | 31136 |
| holds.routed_cap | 35352 | 35584 |
| holds.unindexed | 0 | 0 |
| holds.watches | 30933 | 31136 |
| subscriptions.constants | 9036 | 9094 |
| subscriptions.routed | 30933 | 31136 |
| subscriptions.routed_cap | 46441 | 46844 |
| subscriptions.subscriptions | 30933 | 31136 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 201.1 | 196.8 |
| memory floor MiB | 236.6 | 232.7 |

Live heap after each collection (MiB): 300 s 201.1, 310 s 197.4, 325 s 193.7, 350 s 201.0, 365 s 197.0, 385 s 194.6, 410 s 201.5, 425 s 197.9, 445 s 193.9, 470 s 201.6, 485 s 197.0, 505 s 195.3, 530 s 202.2, 550 s 133.5, 555 s 209.0, 560 s 206.9, 565 s 195.4, 575 s 199.2, 590 s 203.8, 600 s 196.8, 620 s 203.4, 635 s 199.8, 655 s 196.5, 675 s 201.8, 690 s 197.3, 710 s 204.1, 720 s 197.8, 745 s 203.9, 760 s 199.5, 775 s 196.8.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3368 |
| open_failed:connection_refused | 18572 |
| open_failed:connection_reset | 1 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 194562 |
| resumed_true:public.notifications | 3368 |
| resumed_true:public.room_members | 16840 |
| resumed_true:public.room_messages | 3368 |
| stream_end:unexpected_eof | 4448 |
| sub_ok | 71168 |
| token_failed:connection_refused | 1 |
| token_failed:unexpected_eof | 1 |
| token_ok | 3603 |
| warning:replica_identity_insufficient | 8896 |
