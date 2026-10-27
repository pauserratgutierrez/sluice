# Sluice load test — reconnect — p4-keys-refill-r1

- Target: `sluice:p4-keys`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms per call and 2ms per shape, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-26T23:29:41Z, finished 2026-10-26T23:42:46Z, seed 776265885036037802
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Member updates | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | --- | ---: | ---: |
| 190 × 80 / 28 / 0.24, 8 × 250 / 85 / 1 | 7 | 15s | every member | every 2s | full | 2m0s | 8m0s |

Expected with every stream open: 6000 streams, 1160 changes/s, 1744 events/s (400 messages, 400 member updates, 944 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 300 | 939 (1671) | 1160 | 33.68 | 119.5 | 96.8 | 0.11 / 0.21 | 449 | 440 / 461 | 0.48 | 0.00 | 3.52 | 0 | 166.0 (1349) | 0.26 |
| steady | 485 | 1142 (3024) | 1148 | 65.06 | 117.5 | 116.9 | 0.13 / 0.69 | 452 | 442 / 461 | 1.38 | 0.00 | 1.92 | 0 | 175.1 (2323) | 0.91 |
| run | 785 | 1064 (3024) | 1152 | 98.74 | 118.2 | 109.1 | 0.13 / 0.69 | 452 | 442 / 461 | 1.38 | 0.00 | 3.52 | 0 | 171.6 (2323) | 0.91 |

Arrival: 300.0 s to open every stream (complete: false); 6.05 ms of Sluice CPU per open; 79.6 KiB of live heap per stream after arrival.

Opens refused for capacity: 194286 (`503 too_many_streams` with `Retry-After`: load shed as designed, not counted as failed opens).

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 75039 | 3.6 | 5.7 | 9.7 | 25.2 |
| arrival | message | 75039 | 3.6 | 5.7 | 9.7 | 25.2 |
| arrival | message_commit_to_client | 75039 | 3.3 | 5.3 | 9.2 | 24.7 |
| arrival | message_write_to_commit | 75039 | 0.3 | 0.5 | 0.8 | 1.6 |
| arrival | open | 4406 | 45.8 | 57.1 | 61.2 | 91.4 |
| arrival | publish | 5275 | 0.7 | 1.2 | 2.0 | 39.2 |
| arrival | token | 1222 | 45.3 | 56.6 | 59.6 | 70.1 |
| arrival | txn | 3960 | 2.4 | 3.8 | 5.5 | 17.1 |
| arrival | typing | 131740 | 0.8 | 1.5 | 2.4 | 40.2 |
| steady | member | 139605 | 3.7 | 6.0 | 16.8 | 185.8 |
| steady | message | 139604 | 3.6 | 6.0 | 16.2 | 169.0 |
| steady | message_commit_to_client | 139604 | 3.3 | 5.6 | 15.7 | 168.7 |
| steady | message_write_to_commit | 139604 | 0.3 | 0.4 | 0.6 | 1.8 |
| steady | open | 4538 | 268.3 | 1400.8 | 1482.8 | 1626.5 |
| steady | publish | 9781 | 0.7 | 1.3 | 2.6 | 75.3 |
| steady | replayed | 10824 | 11993.1 | 23724.0 | 26869.8 | 33263.9 |
| steady | token | 2351 | 45.8 | 56.6 | 61.2 | 1125.8 |
| steady | txn | 6336 | 2.3 | 3.6 | 4.8 | 17.3 |
| steady | typing | 263764 | 0.8 | 1.5 | 2.7 | 368.3 |
| run | member | 214644 | 3.6 | 5.9 | 12.7 | 185.8 |
| run | message | 214643 | 3.6 | 5.9 | 12.7 | 169.0 |
| run | message_commit_to_client | 214643 | 3.3 | 5.5 | 12.4 | 168.7 |
| run | message_write_to_commit | 214643 | 0.3 | 0.5 | 0.6 | 1.8 |
| run | open | 8944 | 54.0 | 1351.7 | 1466.4 | 1626.5 |
| run | publish | 15056 | 0.7 | 1.2 | 2.3 | 75.3 |
| run | replayed | 10824 | 11993.1 | 23724.0 | 26869.8 | 33263.9 |
| run | token | 3573 | 45.8 | 56.6 | 60.7 | 1125.8 |
| run | txn | 10296 | 2.4 | 3.7 | 5.2 | 17.3 |
| run | typing | 395504 | 0.8 | 1.5 | 2.6 | 368.3 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 214645 | 220055 | 102.52 |
| member | 214645 | 220056 | 102.52 |
| typing | 395485 | 395504 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 4472 / 3375 | 9.71 / 12.61 / 18.41 / 18.57 | 9.57 / 18.27 | 71892 | 2323 | 0 | 3375 of 3375 | 0 | 28.47 |

Resume per table (position age = seconds since the position a stream sent was current):

| # | Table | Resumed | % | Age p50 / max resumed s | Age p50 / min not resumed s |
| ---: | --- | ---: | ---: | --- | --- |
| 1 | public.notifications | 3375 of 3375 | 100.0 | 505.3 / 557.3 | 0.0 / 0.0 |
| 1 | public.room_members | 3375 of 3375 | 100.0 | 16.0 / 33.2 | 0.0 / 0.0 |
| 1 | public.room_messages | 3375 of 3375 | 100.0 | 16.0 / 33.2 | 0.0 / 0.0 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 4 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and as Sluice reports it covers every table (`sluice_resume_buffer_seconds`; `SLUICE_RING_MEMORY`=0.1, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Reported s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1150 | — | — | 60.0 |
| public.room_messages | 13 | — | — | 60.0 |

## Indexes and memory, start and end of the steady phase

| | 300 s | 785 s |
| --- | ---: | ---: |
| streams | 4406 | 4472 |
| holds.constants | 4406 | 4472 |
| holds.counted | 4604 | 4670 |
| holds.routed | 30842 | 31304 |
| holds.routed_cap | 35248 | 35776 |
| holds.unindexed | 0 | 0 |
| holds.watches | 30842 | 31304 |
| subscriptions.constants | 9010 | 9142 |
| subscriptions.routed | 30842 | 31304 |
| subscriptions.routed_cap | 46371 | 46791 |
| subscriptions.subscriptions | 30842 | 31304 |
| subscriptions.unindexed | 0 | 0 |
| live heap MiB | 195.1 | 197.2 |
| memory floor MiB | 230.5 | 233.4 |

Live heap after each collection (MiB): 300 s 195.1, 320 s 200.5, 335 s 197.4, 355 s 194.0, 380 s 201.0, 395 s 195.3, 415 s 192.8, 435 s 202.0, 450 s 194.9, 475 s 206.2, 485 s 196.1, 500 s 201.2, 510 s 193.9, 520 s 200.1, 535 s 193.0, 550 s 116.3, 555 s 215.1, 560 s 200.7, 570 s 197.5, 580 s 202.0, 595 s 213.1, 605 s 198.5, 620 s 203.8, 630 s 198.0, 645 s 202.7, 655 s 196.0, 675 s 203.5, 690 s 199.0, 710 s 204.3, 720 s 198.6, 745 s 204.5, 755 s 199.5, 775 s 197.2.

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 3363 |
| open_failed:connection_refused | 18589 |
| probe_failed | 52 |
| publish_failed:unexpected_eof | 1 |
| refused_capacity:http_503:too_many_streams | 194286 |
| resumed_true:public.notifications | 3375 |
| resumed_true:public.room_members | 16875 |
| resumed_true:public.room_messages | 3375 |
| stream_end:unexpected_eof | 4472 |
| sub_ok | 71552 |
| token_ok | 3573 |
| warning:replica_identity_insufficient | 8944 |
