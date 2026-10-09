# Sluice load test — rooms — pprof-unicorn-1cpu

- Target: `sluice:pprof`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-09T18:53:08Z, finished 2026-10-09T19:05:14Z, seed 13236251132190196069
- **Verdict: FAIL: message p95 124256 ms > 500ms; member p95 124256 ms > 500ms; backlog grew from 8.1 to 40.0 MiB over the steady phase; message delivered 80.94% < 99%; member delivered 80.94% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1294 (3699) | 2229 | 86.92 | 558.5 | 324.3 | 0.72 / 1.02 | 1410 | 1398 / 3686 | 0.29 | 6.86 | 6.95 | 0 | 994.2 (1979) | 0.23 |
| steady | 605 | 3157 (4849) | 1844 | 598.22 | 313.2 | 536.2 | 0.99 / 1.02 | 1811 | 1799 / 3686 | 34.60 | 44.51 | 45.80 | 0 | 507.2 (1161) | 0.21 |
| run | 725 | 2848 (4849) | 1908 | 685.14 | 331.7 | 495.2 | 0.94 / 1.02 | 1811 | 1799 / 3686 | 34.60 | 44.51 | 45.80 | 0 | 587.9 (1979) | 0.23 |

Arrival: 120.2 s to open every stream (complete: true); 7.24 ms of Sluice CPU per open; 87.1 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 42807 | 378.9 | 6193.2 | 7307.3 | 7893.2 |
| arrival | message | 42812 | 354.3 | 6193.2 | 7241.7 | 7893.2 |
| arrival | message_commit_to_client | 42812 | 350.2 | 6193.2 | 7241.7 | 7892.9 |
| arrival | message_write_to_commit | 42812 | 0.4 | 0.8 | 1.4 | 3.0 |
| arrival | open | 12000 | 63.2 | 790.5 | 1138.7 | 1803.1 |
| arrival | publish | 2811 | 2.1 | 63.7 | 79.4 | 114.3 |
| arrival | token | 809 | 248.8 | 1171.5 | 1499.1 | 1886.4 |
| arrival | txn | 3173 | 2.2 | 3.7 | 6.1 | 39.7 |
| arrival | typing | 69653 | 6.0 | 75.3 | 95.7 | 178.1 |
| steady | member | 384785 | 62652.4 | 124256.3 | 129499.1 | 132069.7 |
| steady | message | 384776 | 62652.4 | 124256.3 | 129499.1 | 131456.5 |
| steady | message_commit_to_client | 384776 | 62652.4 | 124256.3 | 129499.1 | 131456.0 |
| steady | message_write_to_commit | 384776 | 0.3 | 0.6 | 1.1 | 13.2 |
| steady | publish | 32107 | 5.1 | 68.1 | 82.4 | 251.7 |
| steady | token | 8127 | 358.4 | 962.6 | 1155.1 | 1767.2 |
| steady | txn | 15841 | 2.1 | 4.8 | 11.1 | 29.4 |
| steady | typing | 1140603 | 8.3 | 82.4 | 120.3 | 267.5 |
| run | member | 427592 | 57409.5 | 123207.7 | 129499.1 | 132069.7 |
| run | message | 427588 | 57409.5 | 123207.7 | 129499.1 | 131456.5 |
| run | message_commit_to_client | 427588 | 57409.5 | 123207.7 | 129499.1 | 131456.0 |
| run | message_write_to_commit | 427588 | 0.3 | 0.6 | 1.1 | 13.2 |
| run | open | 12000 | 63.2 | 790.5 | 1138.7 | 1803.1 |
| run | publish | 34918 | 4.8 | 68.1 | 82.4 | 251.7 |
| run | token | 8936 | 342.0 | 978.9 | 1204.2 | 1886.4 |
| run | txn | 19014 | 2.1 | 4.6 | 10.6 | 39.7 |
| run | typing | 1210256 | 8.1 | 82.4 | 116.2 | 267.5 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 528280 | 427588 | 80.94 |
| member | 528280 | 427592 | 80.94 |
| typing | 1210222 | 1210256 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 5 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 1779 | 1.0 | 1.0 | 2.3 |
| public.room_messages | 20 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| publish_failed:canceled | 1 |
| sub_ok | 96000 |
| token_ok | 8936 |
| warning:replica_identity_insufficient | 12000 |
