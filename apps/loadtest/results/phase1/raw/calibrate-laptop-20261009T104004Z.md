# Sluice load test — calibrate — laptop

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- Started 2026-10-09T10:36:45Z, finished 2026-10-09T10:40:04Z, seed 1
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 40 × 100 / 25 / 2 | 7 | 5s | every 2s | full | 0s | 3m10s |

Expected with every stream open: 1000 streams, 808 changes/s, 1360 events/s (200 messages, 200 member updates, 960 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 3 | 0 (0) | 0 | 2.44 | 0.0 | 0.71 / 0.70 | 155 | 145 / 461 | 32.94 | 0.00 | 0.90 | 0 | 1570.6 (2834) | 0.18 |
| steady | 195 | 1325 (1534) | 787 | 48.56 | 187.9 | 0.25 / 0.30 | 176 | 166 / 461 | 32.94 | 0.00 | 1.79 | 0 | 32.9 (764) | 0.56 |
| run | 199 | 1302 (1534) | 774 | 51.00 | 197.3 | 0.26 / 0.70 | 176 | 166 / 461 | 32.94 | 0.00 | 1.79 | 0 | 59.5 (2834) | 0.56 |

Arrival: 3.4 s to open every stream (complete: true); 2.44 ms of Sluice CPU per open; 116.2 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | open | 1000 | 823.3 | 995.3 | 1028.1 | 1077.2 |
| steady | member | 38025 | 7.8 | 27.5 | 41.7 | 106.9 |
| steady | message | 38025 | 3.7 | 20.9 | 37.6 | 70.1 |
| steady | publish | 7602 | 0.8 | 1.7 | 6.9 | 41.6 |
| steady | token | 53 | 111.1 | 136.2 | 140.3 | 170.4 |
| steady | txn | 1521 | 2.6 | 4.6 | 5.9 | 59.8 |
| steady | typing | 182448 | 0.8 | 1.8 | 7.6 | 42.8 |
| run | member | 38025 | 7.8 | 27.5 | 41.7 | 106.9 |
| run | message | 38025 | 3.7 | 20.9 | 37.6 | 70.1 |
| run | open | 1000 | 823.3 | 995.3 | 1028.1 | 1077.2 |
| run | publish | 7602 | 0.8 | 1.7 | 6.9 | 41.6 |
| run | token | 53 | 111.1 | 136.2 | 140.3 | 170.4 |
| run | txn | 1521 | 2.6 | 4.6 | 5.9 | 59.8 |
| run | typing | 182448 | 0.8 | 1.8 | 7.6 | 42.8 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 38025 | 38025 | 100.00 |
| member | 38025 | 38025 | 100.00 |
| typing | 182448 | 182448 | 100.00 |

## Calibration

**183.6 ms of Sluice CPU per 1000 events** (median of 3 windows), 2.44 ms per stream open.

| Window | Events | Events/s | CPU s | ms / 1k events |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 81600 | 1360 | 14.98 | 183.6 |
| 2 | 81600 | 1360 | 14.95 | 183.2 |
| 3 | 81650 | 1360 | 16.03 | 196.3 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 8000 |
| token_ok | 53 |
| warning:replica_identity_insufficient | 1000 |
