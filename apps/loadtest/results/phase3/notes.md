# Phase 3 — notes kept while working

## Priorities (set by the user, 2026-10-10)

Raised to the top of phase 3, after 0.8.1:

1. **The hold's index column must not depend on the issuer knowing the order.** 0.8.1 keys a hold on the first of its equally good columns, and the README asks issuers to put the most selective first. Measured cost of getting it wrong (0.8.1, very high, first 10 min): 365 ms of CPU per 1,000 changes room-first against 95 ms user-first. Candidates:
   - adaptive: count distinct values per column in the index;
   - `pg_stats.n_distinct`;
   - indexing a hold under every equally good column and evaluating only the shortest list.

   Each one is measured before and after.
2. **Hold checks must not block the lock that stream opens and `/token` take.** In 0.8.0 the reader held `hold.Index`'s mutex while it evaluated every candidate. That was 86–93% of all lock waiting in the process, and `/token` p50 rose to 127 ms at 2 CPUs and 404 ms at 1 CPU (phase 2, mutex profiles). Candidates:
   - evaluate under a read lock and take the write lock only to remove a watch;
   - a copy-on-write index per relation.

3. **Admission control by memory (asked 2026-10-10, not in 0.8.1).**
   - `SLUICE_MAX_STREAMS=0` becomes the default and means automatic: Sluice refuses new streams when its live memory nears GOMEMLIMIT. The threshold is configurable, for example 85%, with hysteresis so that admission does not flap.
   - A value above 0 is an extra fixed cap on top of that.
   - Document the margin a reconnect storm needs, and measure it with the `reconnect` profile.
   - Motivation: `v081f-unicorn-1cpu-512m`. 12,000 streams in 512 MB with the cap raised to 15,000 was killed 11 times for memory. Some kills came at only 144–382 MiB of Go memory five seconds earlier: thousands of streams reconnecting at once spike memory between two samples.

Then the order agreed before:
- cheaper work per change on the reader;
- work off the reader thread (design only);
- degradation under saturation;
- B, A, F, E, G;
- the README and ROADMAP fix about `resumed: false`.

## The subscription index and UPDATEs without an old tuple (reviewed for 0.8.1)

`server.deliver` treats an UPDATE without an old tuple as a row that was already in the shape (`matchOld, unknownOld = matchNew, unknownNew`). So:
- it never makes up an `enter` or a `leave`;
- it never drops a subscription;
- routing (`registry.Candidates`) uses the new row only.

What it cannot see is a row leaving a shape through a column outside the replica identity. That is the documented limitation:
- `replica_identity_insufficient` warns of it at subscribe, when the shape wants `DELETE` or transitions;
- `SLUICE_REPLICA_IDENTITY=strict` refuses such shapes.

No change needed for 0.8.1.

## 0.8.1, second fix: how the hold rule is kept

The rule: an UPDATE without an old tuple cuts no hold. It rests on every hold filter reading only replica-identity columns. Where that is enforced:
- **At grant:** `oracle.parseHolds` refuses the hold.
- **On each catalog refresh:** the `RefreshLeases` loop drops the shape.
- **New in 0.8.1:** `OnRelation` drops it at once, on the reader goroutine, before the next change is dispatched. It runs on every Relation message, including the first one after a restart, using the identity the WAL carries rather than the catalog's.
