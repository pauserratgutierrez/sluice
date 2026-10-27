# Sluice load test — reconnect — p4-refill-r2

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T20:06:49Z, finished 2026-10-26T20:19:54Z, seed 3302196755296678423
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 949 (1505) | 1163 | 31.92 | 112.2 | 91.4 | 0.11 / 0.22 | 450 | 440 / 461 | 0.43 | 3.43 | 3.51 | 0 | 171.0 (1331) | 0.23 |
| steady | 485 | 1156 (19766) | 1148 | 92.17 | 164.5 | 165.6 | 0.19 / 1.24 | 451 | 440 / 461 | 1.55 | 5.68 | 6.90 | 0 | 178.8 (1730) | 0.81 |
| run | 785 | 1076 (19766) | 1154 | 124.09 | 146.8 | 137.0 | 0.16 / 1.24 | 451 | 440 / 461 | 1.55 | 5.68 | 6.90 | 0 | 175.9 (1730) | 0.81 |

Arrival: 300.0 s to open every stream (complete: false); 5.67 ms of Sluice CPU per open; 77.0 KiB of live heap per stream after arrival.

Opens refused for capacity: 180607 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 76718 | 3.6 | 6.2 | 10.3 | 66.4 |
| arrival | message | 76718 | 3.6 | 6.2 | 10.3 | 66.4 |
| arrival | message_commit_to_client | 76718 | 3.3 | 5.7 | 9.8 | 66.1 |
| arrival | message_write_to_commit | 76718 | 0.3 | 0.5 | 0.7 | 6.0 |
| arrival | open | 4569 | 45.8 | 57.6 | 64.3 | 90.4 |
| arrival | publish | 5432 | 0.7 | 1.3 | 2.0 | 45.6 |
| arrival | token | 1298 | 45.3 | 56.6 | 59.6 | 83.3 |
| arrival | txn | 3960 | 2.3 | 4.0 | 6.4 | 19.4 |
| arrival | typing | 131161 | 0.7 | 1.5 | 2.2 | 46.6 |
| steady | member | 138745 | 3.7 | 25.2 | 21626.9 | 30158.2 |
| steady | message | 138742 | 3.7 | 25.2 | 21626.9 | 30052.9 |
| steady | message_commit_to_client | 138742 | 3.4 | 24.4 | 21626.9 | 30052.6 |
| steady | message_write_to_commit | 138742 | 0.3 | 0.5 | 0.6 | 4.4 |
| steady | open | 4573 | 12124.2 | 28180.5 | 29229.1 | 30345.7 |
| steady | publish | 9849 | 0.7 | 3.1 | 610.3 | 1701.6 |
| steady | replayed | 20140 | 25034.8 | 52166.7 | 56885.2 | 60738.1 |
| steady | token | 2331 | 45.8 | 60.7 | 10682.4 | 14940.5 |
| steady | txn | 6336 | 2.3 | 3.9 | 6.0 | 15.1 |
| steady | typing | 262837 | 0.8 | 3.0 | 2375.7 | 27716.3 |
| run | member | 215463 | 3.6 | 8.0 | 17170.4 | 30158.2 |
| run | message | 215460 | 3.6 | 8.0 | 17170.4 | 30052.9 |
| run | message_commit_to_client | 215460 | 3.3 | 7.5 | 17170.4 | 30052.6 |
| run | message_write_to_commit | 215460 | 0.3 | 0.5 | 0.7 | 6.0 |
| run | open | 9142 | 79.4 | 26345.5 | 28966.9 | 30345.7 |
| run | publish | 15281 | 0.7 | 1.6 | 448.5 | 1701.6 |
| run | replayed | 20140 | 25034.8 | 52166.7 | 56885.2 | 60738.1 |
| run | token | 3629 | 45.8 | 58.1 | 8355.8 | 14940.5 |
| run | txn | 10296 | 2.3 | 4.0 | 6.2 | 19.4 |
| run | typing | 393998 | 0.8 | 1.8 | 1204.2 | 27716.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 214746 | 225530 | 105.02 |
| member | 214746 | 225533 | 105.02 |
| typing | 390747 | 393998 | 100.83 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4571 / 3516 | 27.14 / 43.90 / 45.64 / 45.91 | 27.03 / 45.54 | 72454 | 1730 | 7 | 3510 of 3516 | 0 | 58.14 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3516 of 3516 | 100.0 | 508.2 / 576.0 | 0.0 / 0.0 |
| 1 | public.room_members | 3516 of 3516 | 100.0 | 21.2 / 52.1 | 0.0 / 0.0 |
| 1 | public.room_messages | 3516 of 3516 | 100.0 | 21.2 / 52.1 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1141 | — | — | 60.4 |
| public.room_messages | 13 | — | — | 60.4 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4569 | 4571 |
| holds.constants | 4569 | 4571 |
| holds.counted | 4767 | 4769 |
| holds.routed | 31983 | 31997 |
| holds.routed_cap | 36552 | 36568 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31983 | 31997 |
| subscriptions.constants | 9336 | 9340 |
| subscriptions.routed | 31983 | 31997 |
| subscriptions.routed_cap | 47823 | 47951 |
| subscriptions.subscriptions | 31983 | 31997 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 194.5 | 207.5 |
| memory floor MiB | 230.5 | 244.9 |

Live heap after each collection (MiB): 300 s 194.5, 325 s 204.6, 340 s 197.9, 355 s 194.5, 380 s 203.3, 395 s 196.2, 415 s 193.3, 435 s 200.9, 455 s 195.1, 475 s 208.9, 490 s 199.0, 510 s 195.5, 535 s 203.7, 550 s 136.9, 555 s 133.0, 560 s 224.8, 570 s 222.0, 585 s 222.5, 590 s 198.0, 600 s 197.4, 620 s 206.6, 630 s 201.8, 650 s 206.5, 660 s 197.5, 685 s 207.0, 700 s 200.2, 715 s 197.8, 735 s 204.3, 750 s 196.4, 775 s 207.5.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3516 |
| open_failed:connection_refused | 18890 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 180607 |
| resubscribe | 16 |
| resumed_true:public.notifications | 3516 |
| resumed_true:public.room_members | 17580 |
| resumed_true:public.room_messages | 3516 |
| stream_end:unexpected_eof | 4571 |
| sub_error:issuer_unavailable | 7 |
| sub_ok | 73143 |
| sub_refused:channel_hook_unavailable | 9 |
| token_ok | 3629 |
| token_revoked_subscriptions | 7 |
| warning:replica_identity_insufficient | 9143 |
