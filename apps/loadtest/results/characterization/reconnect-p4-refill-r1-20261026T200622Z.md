# Sluice load test — reconnect — p4-refill-r1

- Target: `sluice:p4-main`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T19:53:17Z, finished 2026-10-26T20:06:22Z, seed 2231328081288643961
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 999 (1623) | 1160 | 32.14 | 107.2 | 92.3 | 0.11 / 0.22 | 449 | 439 / 461 | 0.12 | 0.00 | 3.50 | 0 | 169.5 (1358) | 0.24 |
| steady | 485 | 1182 (16674) | 1148 | 80.63 | 140.6 | 144.8 | 0.17 / 1.15 | 451 | 441 / 461 | 1.14 | 3.91 | 7.04 | 0 | 180.1 (2060) | 0.84 |
| run | 785 | 1112 (16674) | 1153 | 112.77 | 129.1 | 124.6 | 0.14 / 1.15 | 451 | 441 / 461 | 1.14 | 3.91 | 7.04 | 0 | 176.1 (2060) | 0.84 |

Arrival: 300.1 s to open every stream (complete: false); 5.69 ms of Sluice CPU per open; 79.5 KiB of live heap per stream after arrival.

Opens refused for capacity: 176736 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 76816 | 3.5 | 6.2 | 9.5 | 21.8 |
| arrival | message | 76816 | 3.5 | 6.2 | 9.5 | 21.8 |
| arrival | message_commit_to_client | 76816 | 3.2 | 5.8 | 8.9 | 21.5 |
| arrival | message_write_to_commit | 76816 | 0.3 | 0.5 | 0.7 | 8.3 |
| arrival | open | 4562 | 45.8 | 57.1 | 63.2 | 91.9 |
| arrival | publish | 5620 | 0.7 | 1.3 | 2.1 | 29.7 |
| arrival | token | 1208 | 45.8 | 56.6 | 59.1 | 85.7 |
| arrival | txn | 3962 | 2.3 | 4.1 | 6.2 | 18.9 |
| arrival | typing | 146173 | 0.8 | 1.6 | 2.4 | 31.1 |
| steady | member | 141512 | 3.6 | 8.8 | 13697.0 | 25309.2 |
| steady | message | 141515 | 3.6 | 8.8 | 13697.0 | 25245.0 |
| steady | message_commit_to_client | 141515 | 3.3 | 8.3 | 13697.0 | 25244.6 |
| steady | message_write_to_commit | 141515 | 0.3 | 0.4 | 0.6 | 3.3 |
| steady | open | 4638 | 11862.0 | 17694.7 | 19267.6 | 20481.6 |
| steady | publish | 9762 | 0.7 | 1.5 | 169.0 | 1248.9 |
| steady | replayed | 17110 | 21364.7 | 39059.5 | 43778.0 | 47634.2 |
| steady | token | 2418 | 45.3 | 58.6 | 7176.2 | 16519.3 |
| steady | txn | 6336 | 2.2 | 3.8 | 5.4 | 17.6 |
| steady | typing | 273361 | 0.8 | 1.8 | 456.7 | 18099.0 |
| run | member | 218328 | 3.6 | 7.2 | 9109.5 | 25309.2 |
| run | message | 218331 | 3.6 | 7.2 | 9109.5 | 25245.0 |
| run | message_commit_to_client | 218331 | 3.3 | 6.8 | 9109.5 | 25244.6 |
| run | message_write_to_commit | 218331 | 0.3 | 0.5 | 0.6 | 8.3 |
| run | open | 9200 | 78.3 | 16711.7 | 18743.3 | 20481.6 |
| run | publish | 15382 | 0.7 | 1.4 | 79.4 | 1248.9 |
| run | replayed | 17110 | 21364.7 | 39059.5 | 43778.0 | 47634.2 |
| run | token | 3626 | 45.8 | 57.6 | 4816.9 | 16519.3 |
| run | txn | 10298 | 2.3 | 3.9 | 5.8 | 18.9 |
| run | typing | 419534 | 0.8 | 1.7 | 146.4 | 18099.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 217279 | 226886 | 104.42 |
| member | 217279 | 226883 | 104.42 |
| typing | 417414 | 419534 | 100.51 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4600 / 3553 | 22.74 / 30.93 / 32.64 / 32.80 | 22.64 / 32.55 | 69074 | 2060 | 0 | 3553 of 3553 | 0 | 45.98 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3553 of 3553 | 100.0 | 503.6 / 561.2 | 0.0 / 0.0 |
| 1 | public.room_members | 3553 of 3553 | 100.0 | 16.7 / 38.3 | 0.0 / 0.0 |
| 1 | public.room_messages | 3553 of 3553 | 100.0 | 16.7 / 38.3 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1153 | — | — | 60.0 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4562 | 4600 |
| holds.constants | 4562 | 4600 |
| holds.counted | 4760 | 4798 |
| holds.routed | 31934 | 32200 |
| holds.routed_cap | 36496 | 36800 |
| holds.unindexed | 0 | 0 |
| holds.watches | 31934 | 32200 |
| subscriptions.constants | 9322 | 9398 |
| subscriptions.routed | 31934 | 32200 |
| subscriptions.routed_cap | 47918 | 48339 |
| subscriptions.subscriptions | 31934 | 32200 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 193.8 | 207.4 |
| memory floor MiB | 229.8 | 244.8 |

Live heap after each collection (MiB): 300 s 193.8, 315 s 201.8, 330 s 195.1, 355 s 193.6, 375 s 201.9, 390 s 194.4, 415 s 210.6, 425 s 196.8, 445 s 192.9, 460 s 200.3, 475 s 195.2, 495 s 202.4, 510 s 196.0, 535 s 209.8, 545 s 196.4, 550 s 153.9, 555 s 192.7, 560 s 207.6, 565 s 218.1, 570 s 215.7, 575 s 201.2, 580 s 198.6, 595 s 204.1, 600 s 198.1, 615 s 206.0, 625 s 204.5, 645 s 207.1, 660 s 198.2, 685 s 207.6, 695 s 200.5, 715 s 197.3, 735 s 204.4, 750 s 198.9, 770 s 207.4.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3553 |
| open_failed:connection_refused | 18875 |
| probe_failed | 52 |
| refused_capacity:http_503:too_many_streams | 176736 |
| resumed_true:public.notifications | 3553 |
| resumed_true:public.room_members | 17765 |
| resumed_true:public.room_messages | 3553 |
| stream_end:unexpected_eof | 4600 |
| sub_ok | 73600 |
| token_ok | 3626 |
| warning:replica_identity_insufficient | 9200 |
