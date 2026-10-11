# Sluice load test — reconnect — smoke-restart

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- Started 2026-10-09T10:35:12Z, finished 2026-10-09T10:36:18Z, seed 6879423273518527425
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 10 × 40 / 20 / 1 | 7 | 5s | every 2s | full | 0s | 1m0s |

Expected with every stream open: 200 streams, 82 changes/s, 175 events/s (40 messages, 40 member updates, 95 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 1 | 2 (2) | 89 | 0.50 | 250000.0 | 0.54 / 0.50 | 65 | 56 / 461 | 0.47 | 0.00 | 0.28 | 0 | 405.1 (374) | 0.09 |
| steady | 65 | 157 (288) | 86 | 3.94 | 385.4 | 0.06 / 0.41 | 69 | 60 / 461 | 19.27 | 0.00 | 0.28 | 0 | 29.0 (649) | 0.05 |
| run | 66 | 155 (288) | 86 | 4.44 | 434.2 | 0.07 / 0.50 | 69 | 60 / 461 | 19.27 | 0.00 | 0.28 | 0 | 34.3 (649) | 0.09 |

Arrival: 0.9 s to open every stream (complete: true); 2.50 ms of Sluice CPU per open; 180.7 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 1 | 178.3 | 178.3 | 178.3 | 178.3 |
| arrival | message | 1 | 177.2 | 177.2 | 177.2 | 178.2 |
| arrival | open | 200 | 766.0 | 806.9 | 814.6 | 814.6 |
| arrival | txn | 2 | 2.3 | 2.3 | 2.3 | 2.7 |
| steady | member | 2152 | 5.7 | 9.8 | 13.0 | 13.7 |
| steady | message | 2152 | 4.3 | 6.6 | 8.5 | 11.6 |
| steady | open | 200 | 53.5 | 99.8 | 120.3 | 1072.0 |
| steady | publish | 269 | 1.1 | 2.2 | 2.9 | 3.3 |
| steady | replayed | 896 | 5603.3 | 11599.9 | 13697.0 | 14231.5 |
| steady | token | 12 | 113.2 | 125.4 | 125.4 | 127.7 |
| steady | txn | 120 | 3.3 | 5.2 | 5.8 | 7.0 |
| steady | typing | 5024 | 1.1 | 2.2 | 2.9 | 3.6 |
| run | member | 2153 | 5.7 | 9.8 | 13.0 | 178.3 |
| run | message | 2153 | 4.3 | 6.6 | 8.5 | 178.2 |
| run | open | 400 | 708.6 | 798.7 | 815.1 | 1072.0 |
| run | publish | 269 | 1.1 | 2.2 | 2.9 | 3.3 |
| run | replayed | 896 | 5603.3 | 11599.9 | 13697.0 | 14231.5 |
| run | token | 12 | 113.2 | 125.4 | 125.4 | 127.7 |
| run | txn | 122 | 3.2 | 5.2 | 5.8 | 7.0 |
| run | typing | 5024 | 1.1 | 2.2 | 2.9 | 3.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 2152 | 2601 | 120.86 |
| member | 2152 | 2601 | 120.86 |
| typing | 5024 | 5024 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | restart | 200 / 200 | 5.24 / 9.18 / 9.97 / 10.09 | 5.20 / 9.93 | 291 | 649 | 0 | 0 of 200 | 0 | 2.61 |

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 200 |
| open_failed:connection_reset | 91 |
| resumed_false:public.notifications | 200 |
| resumed_true:public.room_members | 1000 |
| resumed_true:public.room_messages | 200 |
| stream_end:eof | 200 |
| stream_error:server_shutdown | 200 |
| sub_ok | 3200 |
| token_ok | 12 |
| warning:replica_identity_insufficient | 400 |
