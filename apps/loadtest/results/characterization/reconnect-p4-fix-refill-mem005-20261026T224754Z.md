# Sluice load test — reconnect — p4-fix-refill-mem005

- Target: `sluice:p4-fix`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T22:34:48Z, finished 2026-10-26T22:47:54Z, seed 8208469711143991585
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 1138 (1711) | 1163 | 38.77 | 113.6 | 111.1 | 0.13 / 0.29 | 450 | 440 / 461 | 0.51 | 3.37 | 3.49 | 0 | 187.8 (1333) | 0.24 |
| steady | 485 | 1370 (11074) | 1148 | 72.91 | 109.7 | 131.0 | 0.15 / 1.01 | 457 | 446 / 461 | 2.73 | 1.71 | 3.35 | 0 | 199.1 (1656) | 1.11 |
| run | 785 | 1281 (11074) | 1154 | 111.68 | 111.0 | 123.3 | 0.14 / 1.01 | 457 | 446 / 461 | 2.73 | 3.37 | 3.49 | 0 | 194.8 (1656) | 1.11 |

Arrival: 300.1 s to open every stream (complete: false); 6.60 ms of Sluice CPU per open; 77.9 KiB of live heap per stream after arrival.

Opens refused for capacity: 112944 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 84197 | 3.8 | 6.4 | 10.6 | 75.0 |
| arrival | message | 84197 | 3.8 | 6.4 | 10.6 | 75.0 |
| arrival | message_commit_to_client | 84197 | 3.4 | 5.9 | 9.9 | 74.6 |
| arrival | message_write_to_commit | 84197 | 0.4 | 0.7 | 1.0 | 5.9 |
| arrival | open | 5077 | 46.8 | 57.6 | 62.2 | 114.7 |
| arrival | publish | 6254 | 0.8 | 1.4 | 2.3 | 70.5 |
| arrival | token | 1452 | 46.3 | 57.1 | 61.2 | 190.3 |
| arrival | txn | 3960 | 2.4 | 3.9 | 5.3 | 12.4 |
| arrival | typing | 172965 | 0.9 | 1.7 | 2.6 | 76.1 |
| steady | member | 157191 | 3.8 | 7.1 | 3457.0 | 13503.8 |
| steady | message | 157197 | 3.8 | 7.1 | 3522.6 | 13450.4 |
| steady | message_commit_to_client | 157197 | 3.4 | 6.6 | 3522.6 | 13450.2 |
| steady | message_write_to_commit | 157197 | 0.3 | 0.5 | 0.6 | 1.9 |
| steady | open | 5091 | 8454.1 | 11862.0 | 12255.2 | 12769.9 |
| steady | publish | 11095 | 0.7 | 1.6 | 32.4 | 719.8 |
| steady | replayed | 18562 | 16187.4 | 33161.2 | 36438.0 | 38667.1 |
| steady | token | 2555 | 45.8 | 57.6 | 1646.6 | 10297.7 |
| steady | txn | 6336 | 2.3 | 3.8 | 5.6 | 50.7 |
| steady | typing | 331680 | 0.9 | 1.9 | 46.8 | 11702.4 |
| run | member | 241388 | 3.8 | 6.8 | 978.9 | 13503.8 |
| run | message | 241394 | 3.8 | 6.8 | 987.1 | 13450.4 |
| run | message_commit_to_client | 241394 | 3.4 | 6.3 | 987.1 | 13450.2 |
| run | message_write_to_commit | 241394 | 0.3 | 0.5 | 0.8 | 5.9 |
| run | open | 10168 | 103.9 | 11468.8 | 12124.2 | 12769.9 |
| run | publish | 17349 | 0.7 | 1.5 | 12.2 | 719.8 |
| run | replayed | 18562 | 16187.4 | 33161.2 | 36438.0 | 38667.1 |
| run | token | 4007 | 45.8 | 57.1 | 288.8 | 10297.7 |
| run | txn | 10296 | 2.4 | 3.8 | 5.5 | 50.7 |
| run | typing | 504645 | 0.9 | 1.8 | 18.8 | 11702.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 240984 | 250675 | 104.02 |
| member | 240984 | 250669 | 104.02 |
| typing | 503164 | 504645 | 100.29 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 5084 / 4291 | 17.72 / 22.84 / 23.90 / 23.93 | 17.57 / 23.71 | 54896 | 1656 | 0 | 4291 of 4291 | 0 | 37.95 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 4291 of 4291 | 100.0 | 499.7 / 557.6 | 0.0 / 0.0 |
| 1 | public.room_members | 4291 of 4291 | 100.0 | 16.6 / 33.2 | 0.0 / 0.0 |
| 1 | public.room_messages | 4291 of 4291 | 100.0 | 16.6 / 33.2 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.05, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1146 | — | — | 43.4 |
| public.room_messages | 13 | — | — | 43.4 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 5077 | 5084 |
| holds.constants | 5077 | 5084 |
| holds.counted | 5275 | 5282 |
| holds.routed | 35539 | 35588 |
| holds.routed_cap | 40616 | 40672 |
| holds.unindexed | 0 | 0 |
| holds.watches | 35539 | 35588 |
| subscriptions.constants | 10352 | 10366 |
| subscriptions.routed | 35539 | 35588 |
| subscriptions.routed_cap | 52917 | 52980 |
| subscriptions.subscriptions | 35539 | 35588 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 192.3 | 197.5 |
| memory floor MiB | 231.1 | 236.4 |

Live heap after each collection (MiB): 300 s 192.3, 305 s 192.1, 325 s 196.8, 345 s 192.3, 360 s 191.5, 385 s 200.0, 400 s 192.4, 415 s 194.2, 435 s 192.3, 450 s 193.4, 475 s 192.3, 490 s 191.6, 510 s 195.5, 530 s 193.0, 550 s 128.9, 555 s 207.8, 560 s 211.6, 565 s 203.1, 575 s 195.3, 595 s 195.4, 605 s 195.9, 620 s 195.2, 630 s 196.5, 655 s 195.7, 670 s 195.1, 685 s 197.2, 705 s 195.8, 720 s 196.0, 745 s 195.3, 755 s 195.4, 775 s 197.5.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 4291 |
| open_failed:connection_refused | 20346 |
| open_failed:connection_reset | 1 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 112944 |
| resumed_true:public.notifications | 4291 |
| resumed_true:public.room_members | 21455 |
| resumed_true:public.room_messages | 4291 |
| stream_end:unexpected_eof | 5084 |
| sub_ok | 81344 |
| token_ok | 4007 |
| token_revoked_subscriptions | 14 |
| warning:replica_identity_insufficient | 10168 |
