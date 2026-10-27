# Sluice load test — reconnect — p4-keys-resume-veryhigh

- Target: `sluice:p4-keys`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 0 per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T23:56:46Z, finished 2026-10-27T00:05:21Z, seed 10040127280168479660
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 50 × 80 / 25 / 0.3, 5 × 200 / 50 / 1 | 7 | 15s | every member | every 2s | full | 30s | 8m0s |

Expected with every stream open: 1500 streams, 337 changes/s, 502 events/s (100 messages, 100 member updates, 302 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 30 | 193 (580) | 336 | 2.06 | 354.7 | 203.8 | 0.07 / 0.10 | 129 | 119 / 461 | 0.89 | 0.00 | 1.10 | 0 | 136.7 (1102) | 0.06 |
| steady | 485 | 498 (1098) | 334 | 14.27 | 59.1 | 88.2 | 0.03 / 0.25 | 183 | 173 / 461 | 0.89 | 0.00 | 0.94 | 0 | 59.0 (1237) | 0.34 |
| run | 515 | 480 (1098) | 334 | 16.33 | 66.0 | 95.0 | 0.03 / 0.25 | 183 | 173 / 461 | 0.89 | 0.00 | 1.10 | 0 | 63.6 (1237) | 0.34 |

Arrival: 30.1 s to open every stream (complete: true); 1.37 ms of Sluice CPU per open; 71.4 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1487 | 4.2 | 6.1 | 23.4 | 23.4 |
| arrival | message | 1487 | 4.2 | 6.1 | 23.4 | 23.4 |
| arrival | message_commit_to_client | 1487 | 3.9 | 5.2 | 22.9 | 23.0 |
| arrival | message_write_to_commit | 1487 | 0.4 | 1.2 | 1.5 | 1.9 |
| arrival | open | 1500 | 28.8 | 39.7 | 42.8 | 66.4 |
| arrival | publish | 155 | 0.8 | 1.6 | 2.4 | 3.0 |
| arrival | token | 24 | 28.5 | 36.6 | 38.7 | 41.8 |
| arrival | txn | 110 | 3.1 | 4.6 | 5.7 | 15.5 |
| arrival | typing | 2833 | 0.9 | 1.8 | 2.6 | 3.0 |
| steady | member | 47232 | 4.8 | 9.9 | 17.8 | 26.1 |
| steady | message | 47232 | 4.7 | 9.9 | 17.8 | 26.1 |
| steady | message_commit_to_client | 47232 | 4.4 | 9.2 | 17.5 | 25.8 |
| steady | message_write_to_commit | 47232 | 0.4 | 0.6 | 0.9 | 7.9 |
| steady | open | 1500 | 28.5 | 38.7 | 40.7 | 45.3 |
| steady | publish | 4723 | 1.2 | 2.1 | 2.8 | 15.8 |
| steady | replayed | 4586 | 11337.7 | 22675.5 | 25296.9 | 28071.2 |
| steady | token | 793 | 28.0 | 39.2 | 41.7 | 54.8 |
| steady | txn | 1761 | 3.2 | 6.2 | 13.8 | 24.7 |
| steady | typing | 142389 | 1.3 | 2.3 | 3.0 | 16.2 |
| run | member | 48719 | 4.7 | 9.8 | 17.8 | 26.1 |
| run | message | 48719 | 4.7 | 9.8 | 17.8 | 26.1 |
| run | message_commit_to_client | 48719 | 4.3 | 9.2 | 17.5 | 25.8 |
| run | message_write_to_commit | 48719 | 0.4 | 0.6 | 1.1 | 7.9 |
| run | open | 3000 | 28.8 | 39.2 | 41.7 | 66.4 |
| run | publish | 4878 | 1.2 | 2.1 | 2.8 | 15.8 |
| run | replayed | 4586 | 11337.7 | 22675.5 | 25296.9 | 28071.2 |
| run | token | 817 | 28.0 | 39.2 | 41.7 | 54.8 |
| run | txn | 1871 | 3.2 | 5.9 | 13.8 | 24.7 |
| run | typing | 145222 | 1.3 | 2.3 | 3.0 | 16.2 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 48719 | 51012 | 104.71 |
| member | 48719 | 51012 | 104.71 |
| typing | 145222 | 145222 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 1500 / 1500 | 7.21 / 12.29 / 14.34 / 14.99 | 7.17 / 14.31 | 7183 | 785 | 0 | 1500 of 1500 | 0 | 2.07 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 1500 of 1500 | 100.0 | 263.1 / 283.3 | 0.0 / 0.0 |
| 1 | public.room_members | 1500 of 1500 | 100.0 | 15.4 / 28.0 | 0.0 / 0.0 |
| 1 | public.room_messages | 1500 of 1500 | 100.0 | 15.4 / 28.0 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 333 | 60.0 | 2.0 | 60.0 |
| public.room_messages | 4 | 60.0 | 2.0 | 60.0 |

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
| live heap MiB | 46.3 | 65.9 |
| memory floor MiB | 60.0 | 82.1 |

Live heap after each collection (MiB): 30 s 46.3, 35 s 58.1, 60 s 63.7, 90 s 64.0, 120 s 64.2, 150 s 64.0, 180 s 64.0, 210 s 64.1, 240 s 64.2, 270 s 64.3, 280 s 49.2, 285 s 61.6, 300 s 64.8, 330 s 65.1, 365 s 65.1, 395 s 65.2, 425 s 65.4, 460 s 65.8, 490 s 65.9.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 1500 |
| open_failed:connection_refused | 5683 |
| probe_failed | 10 |
| resumed_true:public.notifications | 1500 |
| resumed_true:public.room_members | 7500 |
| resumed_true:public.room_messages | 1500 |
| stream_end:unexpected_eof | 1500 |
| sub_ok | 24000 |
| token_ok | 817 |
| warning:replica_identity_insufficient | 3000 |
