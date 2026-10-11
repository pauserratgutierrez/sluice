# Sluice load test — rooms — p3i6-unicorn-1cpu-5m

- Target: `sluice:p3-i6`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds user-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-10T01:08:06Z, finished 2026-10-10T01:15:12Z, seed 11101439143332514536
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 5m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1455 (3994) | 2335 | 55.98 | 319.7 | 199.2 | 0.47 / 0.85 | 1316 | 1308 / 3686 | 0.32 | 6.85 | 6.94 | 0 | 1030.0 (2197) | 0.37 |
| steady | 305 | 3443 (4296) | 2281 | 83.75 | 79.7 | 120.3 | 0.27 / 0.52 | 1461 | 1450 / 3686 | 0.34 | 0.00 | 5.17 | 0 | 518.3 (1387) | 0.22 |
| run | 425 | 2881 (4296) | 2297 | 139.73 | 114.0 | 143.0 | 0.33 / 0.85 | 1461 | 1450 / 3686 | 0.34 | 6.85 | 6.94 | 0 | 663.1 (2197) | 0.37 |

Arrival: 120.3 s to open every stream (complete: true); 4.67 ms of Sluice CPU per open; 94.1 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 48298 | 3.8 | 138.2 | 700.4 | 1472.3 |
| arrival | message | 48309 | 3.3 | 132.1 | 692.2 | 2070.7 |
| arrival | message_commit_to_client | 48309 | 2.9 | 132.1 | 692.2 | 2069.9 |
| arrival | message_write_to_commit | 48309 | 0.4 | 0.9 | 1.6 | 6.8 |
| arrival | open | 12000 | 47.4 | 183.3 | 1892.4 | 2639.9 |
| arrival | publish | 3179 | 0.6 | 59.6 | 305.2 | 980.1 |
| arrival | token | 788 | 112.1 | 391.2 | 2146.3 | 2677.0 |
| arrival | txn | 3175 | 2.3 | 3.9 | 6.2 | 19.1 |
| arrival | typing | 78495 | 0.9 | 127.5 | 403.5 | 1137.0 |
| steady | member | 240000 | 3.5 | 8.4 | 53.0 | 333.6 |
| steady | message | 240000 | 3.2 | 8.0 | 51.5 | 328.4 |
| steady | message_commit_to_client | 240000 | 2.9 | 7.5 | 50.9 | 327.8 |
| steady | message_write_to_commit | 240000 | 0.3 | 0.6 | 0.9 | 2.9 |
| steady | publish | 16052 | 0.6 | 2.0 | 40.7 | 81.6 |
| steady | token | 4032 | 110.1 | 197.6 | 354.3 | 2203.6 |
| steady | txn | 7920 | 2.1 | 3.8 | 6.1 | 20.3 |
| steady | typing | 570204 | 0.8 | 2.6 | 43.8 | 88.2 |
| run | member | 288298 | 3.6 | 11.7 | 154.6 | 1472.3 |
| run | message | 288309 | 3.2 | 10.6 | 140.3 | 2070.7 |
| run | message_commit_to_client | 288309 | 2.9 | 9.8 | 138.2 | 2069.9 |
| run | message_write_to_commit | 288309 | 0.3 | 0.7 | 1.1 | 6.8 |
| run | open | 12000 | 47.4 | 183.3 | 1892.4 | 2639.9 |
| run | publish | 19231 | 0.6 | 3.0 | 60.2 | 980.1 |
| run | token | 4821 | 110.1 | 224.3 | 569.3 | 2677.0 |
| run | txn | 11095 | 2.2 | 3.9 | 6.2 | 20.3 |
| run | typing | 648699 | 0.8 | 3.6 | 68.1 | 1137.0 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 288266 | 288309 | 100.01 |
| member | 288266 | 288298 | 100.01 |
| typing | 648697 | 648699 | 100.00 |

## Resume buffer

How far back a stream could resume each table: probed (median and minimum over 2 rounds, ages [500ms 1s 2s 3s 5s 7.5s 10s 15s 20s 30s 45s 1m0s 1m30s]), and derived from the table's change rate (`SLUICE_RING_EVENTS`=4096, `SLUICE_RING_MAX_AGE`=1m0s).

| Table | Changes/s | Probed median s | Probed min s | Derived s |
| --- | ---: | ---: | ---: | ---: |
| public.room_members | 2292 | 1.0 | 1.0 | 1.8 |
| public.room_messages | 26 | 60.0 | 60.0 | 60.0 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 96000 |
| token_ok | 4820 |
| warning:replica_identity_insufficient | 12000 |
