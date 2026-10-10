# Sluice load test — rooms — p3-v081-unicorn-1cpu-roomfirst

- Target: `ghcr.io/pauserratgutierrez/sluice:0.8.1`
- Host: 12th Gen Intel(R) Core(TM) i7-12700H, kernel 6.6.87.2-microsoft-standard-WSL2; runner sees 6 CPUs
- Sluice: GOMAXPROCS 2, GOMEMLIMIT 3686 MiB, cpus 1, memory 4g, cpuset 2,3
- PostgreSQL: PostgreSQL 18.6 (Debian 18.6-1.pgdg13+2), synchronous_commit on
- Issuer: latency 5ms-20ms, 32 calls at once, hook ttl 0, membership holds room-first
- Host power plan: Alto rendimiento 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
- Started 2026-10-10T00:25:00Z, finished 2026-10-10T00:28:15Z, seed 14847304515327594037
- **Verdict: FAIL: message p95 41681 ms > 500ms; member p95 41681 ms > 500ms; message delivered 70.92% < 99%; member delivered 70.91% < 99%**

## Load

| Rooms (count × members / streams / typing) | Shapes | Message every | Typing | Replica identity | Arrival | Duration |
| --- | ---: | ---: | --- | --- | ---: | ---: |
| 380 × 80 / 28 / 0.24, 16 × 250 / 85 / 1 | 7 | 15s | every 2s | full | 2m0s | 10m0s |

Expected with every stream open: 12000 streams, 2320 changes/s, 3500 events/s (800 messages, 800 member updates, 1900 typing).

## Phases

| Phase | s | Events/s (peak) | Changes/s | Sluice CPU s | ms CPU / 1k events | ms CPU / 1k changes | Cores avg / peak | RSS peak MiB | Go mem / limit MiB | GC pause max ms | Backlog max MiB | Slot lag max MiB | Lagging | Issuer calls/s (peak) | Runner cores peak |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| arrival | 120 | 1198 (3114) | 2001 | 92.89 | 645.6 | 386.5 | 0.77 / 1.02 | 1392 | 1381 / 3686 | 33.31 | 6.86 | 7.63 | 0 | 982.0 (2058) | 0.33 |
| steady | 74 | 2999 (4148) | 1503 | 68.97 | 309.9 | 618.5 | 0.93 / 1.01 | 1557 | 1545 / 3686 | 33.31 | 13.62 | 15.10 | 0 | 502.5 (1129) | 0.21 |
| run | 194 | 1886 (4148) | 1811 | 161.86 | 441.7 | 460.0 | 0.83 / 1.02 | 1557 | 1545 / 3686 | 33.31 | 13.62 | 15.10 | 0 | 798.9 (2058) | 0.33 |

Arrival: 120.1 s to open every stream (complete: true); 7.74 ms of Sluice CPU per open; 89.0 KiB of live heap per stream after arrival.

## Latency (ms)

| Phase | Kind | Count | p50 | p95 | p99 | max |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| arrival | member | 35209 | 358.4 | 16580.6 | 19267.6 | 19471.7 |
| arrival | message | 35217 | 337.9 | 16449.5 | 19267.6 | 19432.7 |
| arrival | message_commit_to_client | 35217 | 337.9 | 16449.5 | 19267.6 | 19432.2 |
| arrival | message_write_to_commit | 35217 | 0.5 | 1.1 | 1.6 | 4.9 |
| arrival | open | 12000 | 96.8 | 1302.5 | 1990.7 | 2661.4 |
| arrival | publish | 3110 | 5.3 | 69.1 | 94.7 | 291.5 |
| arrival | token | 756 | 333.8 | 1777.7 | 2408.4 | 2709.7 |
| arrival | txn | 3170 | 2.3 | 4.1 | 6.4 | 17.8 |
| arrival | typing | 73426 | 12.5 | 92.7 | 169.0 | 293.6 |
| steady | member | 40819 | 31064.1 | 41680.9 | 41680.9 | 42350.5 |
| steady | message | 40829 | 31064.1 | 41680.9 | 41680.9 | 42342.5 |
| steady | message_commit_to_client | 40829 | 31064.1 | 41680.9 | 41680.9 | 42342.3 |
| steady | message_write_to_commit | 40829 | 0.4 | 0.8 | 1.4 | 4.1 |
| steady | publish | 3970 | 7.5 | 68.1 | 89.6 | 185.7 |
| steady | token | 961 | 423.9 | 1187.8 | 1531.9 | 1750.7 |
| steady | txn | 1963 | 2.2 | 4.3 | 8.4 | 21.2 |
| steady | typing | 140944 | 12.4 | 87.6 | 156.7 | 258.9 |
| run | member | 76028 | 20840.4 | 40632.3 | 41680.9 | 42350.5 |
| run | message | 76046 | 20578.3 | 40632.3 | 41680.9 | 42342.5 |
| run | message_commit_to_client | 76046 | 20578.3 | 40632.3 | 41680.9 | 42342.3 |
| run | message_write_to_commit | 76046 | 0.4 | 1.0 | 1.6 | 4.9 |
| run | open | 12000 | 96.8 | 1302.5 | 1990.7 | 2661.4 |
| run | publish | 7080 | 6.5 | 68.1 | 92.7 | 291.5 |
| run | token | 1717 | 366.6 | 1499.1 | 2179.1 | 2709.7 |
| run | txn | 5133 | 2.3 | 4.1 | 7.1 | 21.2 |
| run | typing | 214370 | 12.5 | 88.6 | 160.8 | 293.6 |

## Delivery (whole run)

| Kind | Expected | Received | % |
| --- | ---: | ---: | ---: |
| message | 107223 | 76046 | 70.92 |
| member | 107223 | 76028 | 70.91 |
| typing | 214354 | 214370 | 100.01 |

## Counters

| Counter | Value |
| --- | ---: |
| publish_failed:canceled | 2 |
| sub_ok | 96000 |
| token_failed:canceled | 8 |
| token_ok | 1709 |
| warning:replica_identity_insufficient | 12000 |
