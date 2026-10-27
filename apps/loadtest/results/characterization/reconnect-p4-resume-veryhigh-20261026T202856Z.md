# Sluice load test — reconnect — p4-resume-veryhigh

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T20:20:21Z, finished 2026-10-26T20:28:56Z, seed 14353287946411641521
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | full | 30s | 8m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 207 (559) | 336 | 2.02 | 324.8 | 199.8 | 0.07 / 0.09 | 135 | 125 / 461 | 0.72 | 0.00 | 1.10 | 0 | 138.5 (1143) | 0.05 |
| steady | 485 | 498 (1134) | 334 | 15.49 | 64.2 | 95.8 | 0.03 / 0.51 | 195 | 184 / 461 | 1.42 | 0.00 | 0.80 | 0 | 59.0 (1091) | 0.30 |
| run | 515 | 481 (1134) | 334 | 17.51 | 70.7 | 101.9 | 0.03 / 0.51 | 195 | 184 / 461 | 1.42 | 0.00 | 1.10 | 0 | 63.6 (1143) | 0.30 |

Arrival: 30.1 s to open every stream (complete: true); 1.35 ms of Sluice CPU per open; 64.5 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1457 | 4.1 | 9.4 | 13.5 | 13.6 |
| arrival | message | 1457 | 4.1 | 9.4 | 13.5 | 13.6 |
| arrival | message_commit_to_client | 1457 | 3.7 | 9.2 | 13.1 | 13.2 |
| arrival | message_write_to_commit | 1457 | 0.4 | 0.9 | 1.1 | 1.6 |
| arrival | open | 1500 | 29.1 | 39.7 | 45.8 | 59.4 |
| arrival | publish | 143 | 0.8 | 1.4 | 1.8 | 2.2 |
| arrival | token | 27 | 28.5 | 36.1 | 39.7 | 40.6 |
| arrival | txn | 110 | 3.0 | 5.7 | 7.6 | 12.6 |
| arrival | typing | 3306 | 0.9 | 1.7 | 2.1 | 2.3 |
| steady | member | 47126 | 4.8 | 9.3 | 16.8 | 28.2 |
| steady | message | 47126 | 4.8 | 9.3 | 16.8 | 28.2 |
| steady | message_commit_to_client | 47126 | 4.4 | 8.8 | 16.3 | 27.8 |
| steady | message_write_to_commit | 47126 | 0.3 | 0.5 | 0.7 | 2.1 |
| steady | open | 1500 | 30.1 | 42.2 | 55.0 | 70.0 |
| steady | publish | 4724 | 1.2 | 2.1 | 2.5 | 14.7 |
| steady | replayed | 4748 | 11862.0 | 23986.2 | 26869.8 | 29049.5 |
| steady | token | 778 | 27.8 | 38.1 | 40.7 | 43.3 |
| steady | txn | 1760 | 3.2 | 5.6 | 13.9 | 21.9 |
| steady | typing | 142416 | 1.3 | 2.3 | 2.6 | 14.8 |
| run | member | 48583 | 4.7 | 9.3 | 16.5 | 28.2 |
| run | message | 48583 | 4.7 | 9.3 | 16.5 | 28.2 |
| run | message_commit_to_client | 48583 | 4.4 | 8.9 | 16.3 | 27.8 |
| run | message_write_to_commit | 48583 | 0.3 | 0.5 | 0.8 | 2.1 |
| run | open | 3000 | 29.6 | 41.2 | 50.4 | 70.0 |
| run | publish | 4867 | 1.2 | 2.1 | 2.5 | 14.7 |
| run | replayed | 4748 | 11862.0 | 23986.2 | 26869.8 | 29049.5 |
| run | token | 805 | 27.8 | 38.1 | 40.7 | 43.3 |
| run | txn | 1870 | 3.2 | 5.6 | 13.4 | 21.9 |
| run | typing | 145722 | 1.3 | 2.3 | 2.6 | 14.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48583 | 50957 | 104.89 |
| member | 48583 | 50957 | 104.89 |
| typing | 145722 | 145722 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 1500 / 1500 | 7.22 / 12.12 / 14.14 / 14.98 | 7.20 / 14.11 | 7102 | 830 | 0 | 1500 of 1500 | 0 | 4.00 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 1500 of 1500 | 100.0 | 263.5 / 283.6 | 0.0 / 0.0 |
| 1 | public.room_members | 1500 of 1500 | 100.0 | 16.4 / 29.0 | 0.0 / 0.0 |
| 1 | public.room_messages | 1500 of 1500 | 100.0 | 16.4 / 29.0 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 329 | 60.0 | 15.0 | 60.0 |
| public.room_messages | 4 | 60.0 | 15.0 | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 30 s | 515 s |
| --- | ---: | ---: |
| streams | 1500 | 1500 |
| holds.constants | 1500 | 1500 |
| holds.counted | 1555 | 1555 |
| holds.routed | 10500 | 10500 |
| holds.routed_cap | 12000 | 12016 |
| holds.unindexed | 0 | 0 |
| holds.watches | 10500 | 10500 |
| subscriptions.constants | 3055 | 3055 |
| subscriptions.routed | 10500 | 10500 |
| subscriptions.routed_cap | 15420 | 15420 |
| subscriptions.subscriptions | 10500 | 10500 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 55.1 | 72.5 |
| memory floor MiB | 69.5 | 88.7 |

Live heap after each collection (MiB): 30 s 55.1, 55 s 62.7, 85 s 73.8, 115 s 66.4, 145 s 72.3, 175 s 66.4, 205 s 73.6, 235 s 66.4, 265 s 72.4, 280 s 45.4, 285 s 67.5, 315 s 69.3, 350 s 69.7, 380 s 70.4, 415 s 74.5, 445 s 67.4, 475 s 73.2, 505 s 72.5.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 1500 |
| open_failed:connection_refused | 5602 |
| probe_failed | 5 |
| resumed_true:public.notifications | 1500 |
| resumed_true:public.room_members | 7500 |
| resumed_true:public.room_messages | 1500 |
| stream_end:unexpected_eof | 1500 |
| sub_ok | 24000 |
| token_ok | 805 |
| warning:replica_identity_insufficient | 3000 |
