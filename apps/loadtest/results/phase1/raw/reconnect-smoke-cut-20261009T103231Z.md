# Sluice load test — reconnect — smoke-cut

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- Started 2026-10-09T10:31:05Z, finished 2026-10-09T10:32:31Z, seed 15846181537823440689
- **Verdict: FAIL: message p95 3817 ms > 500ms; member p95 3817 ms > 500ms**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 10 × 40 / 20 / 1 | 7 | 5s | every 2s | full | 0s | 1m20s |

Expected with every stream open: 200 streams, 82 changes/s, 175 events/s (40 messages, 40 member updates, 95 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 1 | 43 (0) | 50 | 0.48 | 13714.3 | 0.58 / 0.56 | 65 | 56 / 461 | 3.27 | 0.00 | 0.18 | 0 | 1506.3 (1239) | 0.09 |
| steady | 85 | 172 (480) | 77 | 4.60 | 315.0 | 0.05 / 0.39 | 67 | 57 / 461 | 52.47 | 0.00 | 0.18 | 0 | 45.9 (1018) | 0.07 |
| run | 86 | 171 (480) | 77 | 5.08 | 347.0 | 0.06 / 0.56 | 67 | 57 / 461 | 52.47 | 0.00 | 0.18 | 0 | 59.9 (1239) | 0.09 |

Arrival: 0.8 s to open every stream (complete: true); 2.40 ms of Sluice CPU per open; 227.3 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | open | 200 | 651.3 | 692.2 | 700.4 | 703.2 |
| arrival | publish | 2 | 0.6 | 0.6 | 0.6 | 1.8 |
| arrival | txn | 1 | 4.6 | 4.6 | 4.6 | 4.7 |
| arrival | typing | 35 | 0.7 | 1.9 | 1.9 | 1.9 |
| steady | member | 3605 | 6.3 | 3817.5 | 5144.6 | 5864.9 |
| steady | message | 3601 | 4.6 | 3817.5 | 5210.1 | 5864.9 |
| steady | open | 400 | 518.1 | 675.8 | 692.2 | 752.4 |
| steady | publish | 390 | 1.2 | 2.3 | 2.9 | 10.8 |
| steady | token | 15 | 115.2 | 136.2 | 136.2 | 267.5 |
| steady | txn | 160 | 2.9 | 5.9 | 7.8 | 9.4 |
| steady | typing | 7398 | 1.2 | 2.3 | 2.9 | 10.9 |
| run | member | 3605 | 6.3 | 3817.5 | 5144.6 | 5864.9 |
| run | message | 3601 | 4.6 | 3817.5 | 5210.1 | 5864.9 |
| run | open | 600 | 593.9 | 684.0 | 700.4 | 752.4 |
| run | publish | 392 | 1.2 | 2.3 | 2.9 | 10.8 |
| run | token | 15 | 115.2 | 136.2 | 136.2 | 267.5 |
| run | txn | 161 | 2.9 | 5.9 | 7.8 | 9.4 |
| run | typing | 7433 | 1.2 | 2.3 | 2.9 | 10.9 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 3152 | 3601 | 114.24 |
| member | 3152 | 3605 | 114.37 |
| typing | 7433 | 7433 | 100.00 |

## Reconnect waves

| # | Mode | Down / back | p50 / p90 / p99 / all back s | Downtime p50 / p99 s | Opens | Issuer peak/s | issuer_unavailable | Fully resumed | Gaps after resume | Sluice CPU s |
| ---: | --- | --- | --- | --- | ---: | ---: | ---: | --- | ---: | ---: |
| 1 | cut | 200 / 200 | 0.84 / 1.12 / 1.13 / 1.13 | 0.83 / 1.12 | 200 | 1018 | 0 | 200 of 200 | 0 | 1.26 |
| 2 | cut | 200 / 200 | 0.75 / 1.04 / 1.06 / 1.06 | 0.75 / 1.05 | 200 | 965 | 0 | 200 of 200 | 0 | 1.19 |

## Counters

| Counter | Value |
| --- | ---: |
| msg_duplicate | 401 |
| resumed_true:public.notifications | 400 |
| resumed_true:public.room_members | 2000 |
| resumed_true:public.room_messages | 400 |
| stream_end:unexpected_eof | 400 |
| sub_ok | 4800 |
| token_ok | 15 |
| warning:replica_identity_insufficient | 600 |
