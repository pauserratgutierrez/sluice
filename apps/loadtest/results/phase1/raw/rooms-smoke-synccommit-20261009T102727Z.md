# Sluice load test — rooms — smoke-synccommit

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.0`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 461 MiB, cpus 1, memory 512m, cpuset 2,3
- Started 2026-10-09T10:26:42Z, finished 2026-10-09T10:27:27Z, seed 10822411600095498516
- **Verdict: PASS**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 4 × 20 / 5 / 1 | 7 | 5s | every 2s | full | 0s | 40s |

Expected with every stream open: 20 streams, 17 changes/s, 16 events/s (4 messages, 4 member updates, 8 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 0 | 36 (0) | 0 | 0.06 | 7500.0 | 0.27 / 0.22 | 24 | 15 / 461 | 27.38 | 0.00 | 0.12 | 0 | 725.1 (160) | 0.07 |
| steady | 45 | 14 (42) | 15 | 0.21 | 328.1 | 0.00 / 0.01 | 29 | 20 / 461 | 27.38 | 0.00 | 0.23 | 0 | 4.4 (160) | 0.03 |
| run | 45 | 14 (42) | 15 | 0.27 | 416.7 | 0.01 / 0.22 | 29 | 20 / 461 | 27.38 | 0.00 | 0.23 | 0 | 4.3 (160) | 0.07 |

Arrival: 0.2 s to open every stream (complete: true); 3.00 ms of Sluice CPU per open; 274.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | open | 20 | 100.9 | 112.1 | 112.1 | 118.5 |
| arrival | publish | 2 | 0.5 | 0.5 | 0.5 | 1.6 |
| arrival | typing | 8 | 0.6 | 1.6 | 1.6 | 1.6 |
| steady | member | 160 | 3.4 | 10.2 | 14.4 | 14.5 |
| steady | message | 160 | 3.2 | 10.2 | 14.4 | 14.4 |
| steady | publish | 80 | 0.9 | 1.8 | 2.0 | 3.4 |
| steady | token | 2 | 91.6 | 91.6 | 91.6 | 114.3 |
| steady | txn | 32 | 2.7 | 4.3 | 4.8 | 5.0 |
| steady | typing | 320 | 0.9 | 1.8 | 2.2 | 3.4 |
| run | member | 160 | 3.4 | 10.2 | 14.4 | 14.5 |
| run | message | 160 | 3.2 | 10.2 | 14.4 | 14.4 |
| run | open | 20 | 100.9 | 112.1 | 112.1 | 118.5 |
| run | publish | 82 | 0.9 | 1.7 | 2.0 | 3.4 |
| run | token | 2 | 91.6 | 91.6 | 91.6 | 114.3 |
| run | txn | 32 | 2.7 | 4.3 | 4.8 | 5.0 |
| run | typing | 328 | 0.9 | 1.8 | 2.2 | 3.4 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 160 | 160 | 100.00 |
| member | 160 | 160 | 100.00 |
| typing | 328 | 328 | 100.00 |

## Counters

| Counter | Value |
| --- | ---: |
| sub_ok | 160 |
| token_ok | 2 |
| warning:replica_identity_insufficient | 20 |
