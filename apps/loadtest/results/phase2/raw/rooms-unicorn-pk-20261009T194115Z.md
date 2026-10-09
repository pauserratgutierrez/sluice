# Sluice load test — rooms — unicorn-pk

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T19:29:09Z, finished 2026-10-09T19:41:15Z, seed 5136142406933279495
- **Verdict: FAIL: 4 streams closed with stream_lagging; message delivered 2.78% < 99%; member delivered 0.53% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | pk | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 816 (2288) | 2333 | 46.42 | 473.7 | 165.6 | 0.39 / 0.63 | 769 | 759 / 3686 | 0.56 | 6.87 | 6.95 | 4 | 1001.0 (2085) | 0.35 |
| steady | 605 | 1888 (2358) | 2301 | 103.32 | 90.5 | 74.2 | 0.17 / 0.50 | 887 | 878 / 3686 | 3.52 | 0.01 | 4.78 | 0 | 417.7 (1252) | 0.33 |
| run | 725 | 1710 (2358) | 2306 | 149.74 | 120.7 | 89.5 | 0.21 / 0.63 | 887 | 878 / 3686 | 3.52 | 6.87 | 6.95 | 4 | 514.4 (2085) | 0.35 |

Arrival: 120.1 s to open every stream (complete: true); 3.87 ms of Sluice CPU per open; 56.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 2373 | 3.6 | 21.6 | 89.6 | 212.0 |
| arrival | message | 13206 | 2.6 | 10.2 | 66.0 | 204.1 |
| arrival | message_commit_to_client | 13206 | 2.3 | 9.7 | 66.0 | 203.8 |
| arrival | message_write_to_commit | 13206 | 0.3 | 0.8 | 1.6 | 3.5 |
| arrival | open | 12004 | 45.8 | 84.5 | 177.2 | 393.5 |
| arrival | publish | 3449 | 0.4 | 7.7 | 48.4 | 95.8 |
| arrival | replayed | 1 | 86.5 | 86.5 | 86.5 | 86.8 |
| arrival | token | 792 | 17.8 | 113.2 | 128.5 | 332.9 |
| arrival | txn | 3170 | 2.1 | 3.4 | 5.3 | 11.5 |
| arrival | typing | 82414 | 0.6 | 14.8 | 56.6 | 96.0 |
| steady | member | 404 | 4.9 | 60.2 | 96.8 | 178.8 |
| steady | message | 1473 | 3.1 | 44.3 | 94.7 | 188.3 |
| steady | message_commit_to_client | 1473 | 2.6 | 44.3 | 93.7 | 187.7 |
| steady | message_write_to_commit | 1473 | 0.4 | 0.7 | 1.7 | 2.1 |
| steady | publish | 32101 | 0.7 | 2.1 | 20.4 | 102.8 |
| steady | token | 8073 | 14.0 | 21.1 | 50.4 | 197.3 |
| steady | txn | 15840 | 2.1 | 4.0 | 8.0 | 81.7 |
| steady | typing | 1140327 | 0.8 | 2.3 | 21.9 | 165.6 |
| run | member | 2777 | 3.7 | 29.6 | 96.8 | 212.0 |
| run | message | 14679 | 2.6 | 15.6 | 70.1 | 204.1 |
| run | message_commit_to_client | 14679 | 2.3 | 15.2 | 70.1 | 203.8 |
| run | message_write_to_commit | 14679 | 0.3 | 0.8 | 1.6 | 3.5 |
| run | open | 12004 | 45.8 | 84.5 | 177.2 | 393.5 |
| run | publish | 35550 | 0.7 | 2.1 | 25.5 | 102.8 |
| run | replayed | 1 | 86.5 | 86.5 | 86.5 | 86.8 |
| run | token | 8865 | 14.3 | 24.2 | 100.9 | 332.9 |
| run | txn | 19010 | 2.1 | 3.9 | 7.5 | 81.7 |
| run | typing | 1222741 | 0.8 | 2.3 | 26.8 | 165.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528547 | 14679 | 2.78 |
| member | 528547 | 2778 | 0.53 |
| typing | 1222739 | 1222741 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2297 | — | — | 1.8 |
| public.room_messages | 26 | — | — | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| resumed_false:public.room_members | 5 |
| resumed_true:public.room_members | 3 |
| stream_end:eof | 4 |
| stream_error:stream_lagging | 4 |
| sub_error:shape_not_authorized | 10850783 |
| sub_forgotten | 84000 |
| sub_ok | 96012 |
| token_ok | 8865 |
| token_revoked_subscriptions | 7 |
| warning:replica_identity_insufficient | 12000 |
