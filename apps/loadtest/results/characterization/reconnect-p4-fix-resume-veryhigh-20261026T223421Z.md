# Sluice load test — reconnect — p4-fix-resume-veryhigh

- Target: `sluice:p4-fix`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T22:25:46Z, finished 2026-10-26T22:34:21Z, seed 11721183886355131788
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | full | 30s | 8m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 186 (601) | 336 | 2.72 | 484.3 | 269.0 | 0.09 / 0.13 | 129 | 119 / 461 | 0.20 | 0.00 | 1.10 | 0 | 138.7 (964) | 0.08 |
| steady | 485 | 498 (1123) | 334 | 17.00 | 70.4 | 105.1 | 0.04 / 0.38 | 182 | 172 / 461 | 1.50 | 0.00 | 0.92 | 0 | 59.1 (935) | 0.37 |
| run | 515 | 479 (1123) | 334 | 19.72 | 79.9 | 114.7 | 0.04 / 0.38 | 182 | 172 / 461 | 1.50 | 0.00 | 1.10 | 0 | 63.7 (964) | 0.37 |

Arrival: 30.1 s to open every stream (complete: true); 1.81 ms of Sluice CPU per open; 71.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1360 | 4.5 | 7.5 | 14.5 | 15.2 |
| arrival | message | 1360 | 4.5 | 7.5 | 14.5 | 15.2 |
| arrival | message_commit_to_client | 1360 | 4.0 | 6.9 | 13.9 | 14.7 |
| arrival | message_write_to_commit | 1360 | 0.5 | 1.1 | 1.3 | 1.3 |
| arrival | open | 1500 | 29.1 | 40.7 | 51.5 | 74.6 |
| arrival | publish | 138 | 0.9 | 1.3 | 1.4 | 2.2 |
| arrival | token | 31 | 27.5 | 34.6 | 37.6 | 40.3 |
| arrival | txn | 110 | 3.2 | 5.6 | 9.9 | 15.8 |
| arrival | typing | 2896 | 0.9 | 1.4 | 1.7 | 2.3 |
| steady | member | 46986 | 4.6 | 7.4 | 12.5 | 26.8 |
| steady | message | 46986 | 4.6 | 7.4 | 12.5 | 26.8 |
| steady | message_commit_to_client | 46986 | 4.2 | 6.8 | 11.8 | 26.4 |
| steady | message_write_to_commit | 46986 | 0.4 | 0.7 | 1.0 | 7.1 |
| steady | open | 1500 | 29.3 | 39.7 | 42.2 | 45.4 |
| steady | publish | 4722 | 1.1 | 1.9 | 2.5 | 7.9 |
| steady | replayed | 5028 | 12124.2 | 24772.6 | 27131.9 | 29267.7 |
| steady | token | 807 | 28.0 | 38.7 | 41.2 | 53.8 |
| steady | txn | 1760 | 3.1 | 5.0 | 8.4 | 25.7 |
| steady | typing | 142336 | 1.2 | 2.1 | 2.7 | 10.8 |
| run | member | 48346 | 4.6 | 7.4 | 12.5 | 26.8 |
| run | message | 48346 | 4.6 | 7.4 | 12.5 | 26.8 |
| run | message_commit_to_client | 48346 | 4.2 | 6.8 | 12.0 | 26.4 |
| run | message_write_to_commit | 48346 | 0.4 | 0.7 | 1.0 | 7.1 |
| run | open | 3000 | 29.1 | 40.2 | 43.8 | 74.6 |
| run | publish | 4860 | 1.1 | 1.9 | 2.5 | 7.9 |
| run | replayed | 5028 | 12124.2 | 24772.6 | 27131.9 | 29267.7 |
| run | token | 838 | 28.0 | 38.7 | 41.2 | 53.8 |
| run | txn | 1870 | 3.1 | 5.0 | 8.5 | 25.7 |
| run | typing | 145232 | 1.2 | 2.1 | 2.7 | 10.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48346 | 50860 | 105.20 |
| member | 48346 | 50860 | 105.20 |
| typing | 145231 | 145232 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 1500 / 1500 | 7.36 / 12.44 / 14.32 / 14.98 | 7.31 / 14.30 | 7190 | 720 | 0 | 1500 of 1500 | 0 | 3.16 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 1500 of 1500 | 100.0 | 263.6 / 283.4 | 0.0 / 0.0 |
| 1 | public.room_members | 1500 of 1500 | 100.0 | 17.9 / 29.2 | 0.0 / 0.0 |
| 1 | public.room_messages | 1500 of 1500 | 100.0 | 17.9 / 29.2 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 60.0 | 2.0 | 60.1 |
| public.room_messages | 4 | 60.0 | 2.0 | 60.1 |

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
| live heap MiB | 46.6 | 67.7 |
| memory floor MiB | 60.0 | 83.9 |

Live heap after each collection (MiB): 30 s 46.6, 35 s 56.8, 55 s 62.6, 85 s 66.6, 115 s 66.5, 145 s 66.6, 180 s 66.4, 210 s 66.3, 245 s 66.9, 275 s 39.4, 280 s 47.5, 285 s 60.0, 295 s 66.3, 325 s 67.3, 355 s 67.4, 385 s 67.4, 415 s 67.4, 450 s 67.2, 485 s 67.7.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 1500 |
| open_failed:connection_refused | 5690 |
| probe_failed | 10 |
| resumed_true:public.notifications | 1500 |
| resumed_true:public.room_members | 7500 |
| resumed_true:public.room_messages | 1500 |
| stream_end:unexpected_eof | 1500 |
| sub_ok | 24000 |
| token_ok | 838 |
| warning:replica_identity_insufficient | 3000 |
