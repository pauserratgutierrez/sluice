# Refill after a reconnect of every stream (2026-10-16)

Goal: after a cut of every stream at the admission threshold, every stream that fit comes back, quickly, without a fixed cap and without a kill. 0.9.x reached 96% after 4 minutes and advised capping `SLUICE_MAX_STREAMS` at about 85% of what admission let in.

Outcome: with every shape of a request in one issuer call and the reconnect headroom, the node was full again within 10 s of the cut (`refill-batch-fit`), with no `issuer_unavailable`, Go memory at its usual peak, and message p99 28 ms through the refill.

Setup: the phase 3 storm (`../../plans/phase3-storm.plan`) on 1 CPU and 512 MB, issuer mode, the rooms profile offering 6,000 streams with 7 shapes and a hook channel each, a proxy cut 4 minutes into the steady phase (5 s down). Plan: `../../plans/refill.plan`; reports in `raw/`. Every run used a host with nothing else running, except where noted.

## Results

Streams in Sluice after the cut, as a share of those open before it; then the clients that had a stream before the cut and got it back (the rest of the node's streams went to clients that had been waiting since the arrival phase).

| Run | Change | 10 s | 30 s | 60 s | End | Back, p50 / p99 / last s | `issuer_unavailable` | Wave CPU s | Message p99 / max ms | Go memory peak MiB (limit 461) | Verdict |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: | ---: | --- | ---: | --- |
| `refill-diag-baseline2` | 0.10.0 as released to main | 48% | 84% | 94% | 96.6% | 92.7%, 16.8 / 105 / 166 | 938 | 44.1 | 95 / 3,299 | 441 | pass |
| `refill-allowance` | reconnect headroom | 62% | 94% | 99.7% | 99.7% | 96.2%, 16.3 / 39 / 40 | 2,496 | 57.1 | 363 / 13,710 | 453 | pass |
| `refill-allowance-r2` | the same, again | 46% | 79% | — | 99.7% | 95.7%, 92 / 110 / 111 | 23,499 | 117.9 | 41,157 / 89,046 | 539 | fail |
| `refill-allowance-conns` | headroom + 64 connections per host to the issuer | — | — | — | 90.3% | 86.8%, 17 / 145 / 145 | 77,799 | 214.2 | 316,670 / 377,751 | 898 | fail |
| `refill-batch-oldadm` | one issuer call per request, 0.9 admission | 58% | 96% | 98.0% | 98.7% | 95.0%, 9.2 / 43 / 105 | 0 | 31.3 | 17.5 / 320 | 441 | pass |
| `refill-batch-newadm` | one call per request + headroom | 59% | 99.9% | 99.9% | 99.9% | 95.9%, 9.3 / 14.2 / 14.4 | 0 | 29.9 | 25 / 995 | 441 | pass |
| `refill-batch-newadm-r2` | the same, again | 58% | 98.7% | 98.7% | 98.7% | 95.0%, 9.1 / 21.8 / 33.5 | 0 | 28.9 | 22 / 605 | 440 | pass |
| `refill-batch-fit` | the same, the count that fit taken at admission | 100% | 100% | 100% | 100% | 96.2%, 9.4 / 14.0 / 14.3 | 0 | 29.0 | 28 / 796 | 442 | pass |

"10 s" is the sample 10 s after the one that saw no streams; dashes are samples the runner could not take while Sluice did not answer `/metrics`.

## Findings

1. **One issuer call per shape was the bottleneck.** A stream with 7 shapes made 7 calls, 4 at a time, and `POST /token` renewed them one after the other. In a reconnect of every stream that is about 46,000 calls; the test's issuer serves 32 at once at 5–20 ms. Calls queued at the issuer past `SLUICE_ISSUER_TIMEOUT` (2 s), timed out, and were retried.
2. **Each call that found no idle connection dialed a new one.** The CPU profile of the refill (`refill-allowance-prof`, CPU from 5 s after the cut for 30 s) had 14.1 of 28.6 s in `Transport.dialConn`, with a DNS lookup each; the transport kept 64 idle connections and closed the rest. The goroutine profile had 2,391 subscriptions waiting for one of the 8 database connections to check their holds, with PostgreSQL nearly idle: the goroutines holding those connections got no CPU. That run is void as a measurement (the goroutine dump and the execution trace landed in the refill).
3. **Capping the connections does not help on its own.** With 64 connections per host, a call's wait for a connection counts against the issuer timeout: most calls of the wave timed out inside Sluice without reaching the issuer (2.9% CPU), every one was retried, and Go memory reached 898 MiB. Reverted.
4. **Asking about every shape of a request in one call removes the queue.** With the issuer stub charging 5–20 ms per call plus 2 ms per shape: no `issuer_unavailable`, a third less CPU in the wave, opens at p50 0.24 s instead of 2.7 s, message p99 17.5 ms instead of 95 ms, and 96% back within 30 s with the 0.9 admission.
5. **The rest is admission, and the memory a reconnect lifts.** While the refill runs, streams being set up and garbage counted live during a collection lift the measured floor; after it, the node sits at the threshold with a little more memory per stream than before the cut. The 0.9 admission (5-point hysteresis, 10 s hold) stopped at 98.7%. Letting the streams that fit come back up to a ceiling of 0.6 took the node back to 99.9% within 10 s. Without the single issuer call it was fragile: one good run, one where the issuer queue blew memory past the limit (`refill-allowance-r2`, whose arrival phase also cost 11.4 ms of CPU per open against 7.2–7.3 in the others, so the host may have been busy).
6. **The count that fit must be taken when a stream is admitted.** Taken at collections below the threshold (`refill-batch-newadm-r2`), it lagged: a node fills to the threshold, so the collections after its last streams read at or past it, and the refill stopped at the count of the last collection below it (5,702 of 5,779). Taken at admission (`refill-batch-fit`), every one of the 5,773 streams was back within 10 s of the cut. 222 of them went to clients that had been waiting since the arrival phase, so as many clients that had a stream before the cut found the node full; the other 5,551 were back within 14.3 s.

## Not done

- Holds are still checked with one `EXISTS` per hold and shape. Deduplicating them per request needs every shape's watches installed before the check, so a `DELETE` between two installs is not missed.
- The first baseline (`refill-diag-baseline`) overlapped another project's load test on the same host and is void.
