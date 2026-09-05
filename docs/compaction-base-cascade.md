# Design: eliminate the DB-sized base rewrite in leveled compaction

**Status:** proposal · **Scope:** `litestream-roci` (fork), file references at `v0.5.18-zero.3`
**Related:** upstream cascade issue (this repo's `litestream-compaction-cascade-issue.md`); upstream #1479 (serialize snapshot/compaction); upstream #1508 + our #29 (`lastPageMatch` O(LTX) scan).

## Problem

On a fresh generation the entire DB (~65 GB here) is written to the backup **~4–5 times**: as the L0 base (`0000/0001`), as the snapshot level (`0009/0001`), and then re-included in the first compaction of **every** ladder level (`0001/1-…`, `0002/1-…`, `0003/1-…`). Root cause: an empty destination level seeks its compaction source from TXID 1 ([compactor.go:111](../compactor.go#L111), `seekTXID = prevMaxInfo.MaxTXID + 1`), so it pulls the DB-sized base. Because our fork serializes snapshot+compaction under `compactionMu` ([store.go:788](../store.go#L788)), these full-DB rewrites run **sequentially and monopolize compaction** — ~1 hour per generation during which no other level makes progress and L0 files pile up (observed 55 → 172+). This repeats on **every** replication-manager restart, since each mints a fresh generation.

Empirically confirmed (2026-09-04, prod us-west-1 public, `zero-eldtg5tcvb3bn1io`): snapshot held the lock 18:56:12→19:13:18; L1 `[1–0x39]` 19:14→19:33; L2 `[1–0xb3]` 19:40→19:57; L3 `[1–0xb3]` from 20:00 — every level's first file spans `MinTXID = 1`.

## Leading proposal: write the base to the snapshot level, never to L0

**Redirect the single bootstrapping writer.** The initial full capture is produced by the sync path (`db.sync` with `info.snapshotting`, [db.go:2195](../db.go#L2195), writing `writeLTXFromDB` → `LTXPath(0, …)`). Change it to write the base to the **snapshot level (L9), locally and remotely**, and leave the increment path writing L0. Then:

- **The compaction ladder never sees the base.** L1 compacts L0, which now contains only increments (TXID ≥ 2), so no level re-materializes the DB. The cascade disappears — this *subsumes* Appendix A (no `seekTXID` tweak needed).
- **The snapshot-level monitor stands down automatically.** `CompactDB(SnapshotLevel)` already no-ops when the snapshot is current: `if dstInfo.MaxTXID >= pos.TXID { return ErrNoCompaction }` ([store.go:815](../store.go#L815)). With L9 already at TXID 1 and `pos == 1`, it skips — **no second full-DB read, no second upload.** This closes the concurrent-double-capture gap that PR #1479's mutex does *not* cover (the mutex serializes compaction-vs-compaction, not the sync-path snapshot vs. the L9 monitor).
- **Restore is unchanged and already compatible.** `CalcRestorePlan` seeds from the latest snapshot and extends contiguously ([replica.go:1522-1545](../replica.go#L1522)); with the base at L9 and increments at L0 `[2..]`, the plan is `snapshot[..1] + L0/ladder[2..]`, contiguous at the `snapMax+1` boundary ([replica.go:1662](../replica.go) contiguity check).

### Why not "make the L9 monitor the sole writer" (the obvious alternative)

There is a **circular dependency**. The L9 monitor rides on top of the position that the initial L0 capture establishes: today the sync path's first snapshot advances `pos` 0→1, and only then does the L9 monitor (which reads `db.Pos()`) have a valid TXID to snapshot. With `pos == 0`, `db.Snapshot` would call `WriteLTXFile(SnapshotLevel, 1, pos.TXID=0, …)` — a malformed `minTXID=1, maxTXID=0`. Only the sync path can mint TXID 1 from the live DB. So the fix is to **redirect** that existing bootstrap writer to L9, not to hand the job to the monitor.

### Required supporting change: make the position anchor level-aware

`MaxLTX`/`Pos` currently assume the newest TXID lives in a **local L0** file. That holds in steady state (increments enter via L0) but not if the base is at L9 with no L0 yet. Two pieces:

1. **`MaxLTX`** ([db.go:593](../db.go#L593)) — today `os.ReadDir(LTXLevelDir(0))` only. Generalize to return the global max across levels **with its level** (fast-path L0 first). This also makes the pure-reporting callers (`server.go:280/666`, `store.go:452/467`) *more* correct (they currently under-report once L0 is retention-pruned).
2. **Thread the level through the three consumers that open the position's file** (audit result — these are the *only* live level-0 assumptions):
   - `Pos()` — [db.go:639](../db.go#L639)
   - `verifyWithExecutor` — [db.go:1706](../db.go#L1706)
   - `snapshotWALEndOffset` — [db.go:2827](../db.go#L2827)

   (The `LTXPath(0,…)` at [db.go:1174](../db.go#L1174) is dead/commented code; the writes at 1646/2050 are increments = L0 by definition; 3127/3183 are L0 retention. All other `db.Pos()` callers use `pos.TXID` abstractly.)

The **load-bearing invariant** is that the file holding the current max TXID must be present **locally** (because `MaxLTX` is a local scan). Today that's automatic for L0; extend it so the L9 base is kept local while it is the anchor (i.e. until the first L0 increment supersedes it), and L9 retention must not delete the current-anchor snapshot (it already keeps ≥1 — [db.go:3033](../db.go#L3033)).

### Also required: level-aware sync output recording

The sync path records its output as L0 (`maxLTXFileInfos[0]`, `exec` state). The snapshotting write must record at L9 instead — same mechanical level-threading as above.

### Coupling to #1508 / #29

With the base at L9, the **first increment sync's `verify` scans the 65 GB L9 base** in `lastPageMatch` — the exact O(LTX) scan from #1508. Our `lastPageMatch` last-frame cache (#29, merged) already covers it, so it is coupled but solved.

### Net effect

One base write (to L9, local+remote). No L0 base, no ladder cascade, no concurrent double-read. Full-DB uploads per fresh generation: **~5 → 1**. Compaction is never monopolized by a full-DB rewrite.

### Risks / open questions

- **`Pos()` verifies the whole file** (`dec.Verify()`, reads all pages). Reading position from a 65 GB L9 base at bootstrap is as expensive as reading the 65 GB L0 base is today — no regression, but worth considering a header-only position decode as a follow-up.
- **Blast-radius audit is done for `Pos`/`verify`/`snapshotWALEndOffset`**, but the sync-output recording and the `MaxLTX` reporting callers should get a second read during implementation.
- **Interaction with restore-then-replicate**: after a restore (which reconstructs the local DB) the first sync currently writes an L0 base; under this proposal it writes an L9 base. Verify the restore→replicate handoff sets `pos` from the freshly written L9 via the generalized `MaxLTX`.

---

## Appendix: easier, less complete alternatives

These are smaller changes that fix part of the problem without the position-anchor work. They can be shipped independently and compose.

### A. Seek from the snapshot watermark (kills the cascade only) — **recommended ship-now**

Localized to the compactor, inside `Compactor.Compact` ([compactor.go:104](../compactor.go#L104)). When a destination level is empty, seek from the latest snapshot's max TXID instead of TXID 1 (the existing `seekTXID` line is [compactor.go:111](../compactor.go#L111)):

```go
seekTXID := prevMaxInfo.MaxTXID + 1
if prevMaxInfo.MaxTXID == 0 { // destination level never populated
    // Skip everything already captured in the snapshot. MaxLTXFileInfo is the
    // same helper used for dstLevel at :107; SnapshotLevel is already in scope.
    snapInfo, err := c.MaxLTXFileInfo(ctx, SnapshotLevel)
    if err != nil {
        return nil, fmt.Errorf("cannot determine snapshot max ltx file: %w", err)
    }
    if snapInfo.MaxTXID == 0 {
        // Snapshot not written yet — DEFER rather than fall back to seekTXID=1,
        // which would pull the DB-sized base into the ladder (the very cascade
        // we're preventing). See "ordering is a race" below.
        return nil, ErrNoCompaction
    }
    seekTXID = snapInfo.MaxTXID + 1
}
```

**Ordering is a race — this defer is load-bearing, not optional.** The snapshot (L9) and each ladder level (L1/L2/L3) run in **independent monitor goroutines** ([store.go:224-242](../store.go#L224)), all start their timers at `time.Nanosecond` ([store.go:575](../store.go#L575)) so they fire together on bootstrap, and `compactionMu` is a `TryLock` ([store.go:788](../store.go#L788)) that *serializes but does not order* them. So there is **no guarantee L9 is written before L1's first compaction** — we observed L9-first in production but nothing forces it. If L1 wins the lock first, `MaxLTXFileInfo(SnapshotLevel).MaxTXID == 0`; without the `ErrNoCompaction` defer above the seek would fall back to `1` and the base would be pulled into L1 anyway. The defer converts that timing coincidence into an actual invariant. It is **deadlock-free**: `db.Snapshot` reads the live DB and does not depend on the ladder, so "wait for L9" always resolves; the ladder simply skips a few ticks until L9 lands. (The leading proposal above is ordering-independent *by construction* — L0 never holds the base — which is why it needs no such guard.)
- **Fixes:** the L1/L2/L3 cascade (3× 65 GB). No touch to `Pos`/L0 machinery.
- **Leaves:** the duplicate L0+L9 base (2× 65 GB, concurrent) at startup.
- **Bigger role under D.** A also *is* the rule "a fresh ladder segment starts at the latest snapshot boundary." Standalone that only kills the bootstrap cascade; combined with D (where boundary snapshots live in L9 and leave a TXID gap in remote L0) it becomes a **correctness requirement** — it stops an empty ladder level from merging across a boundary gap. See "Boundary snapshots" under D.
- **Safety:** restore-compatible (seeds from snapshot; first ladder file now starts at `snapMax+1`, contiguous). Guard **only** the empty-destination case — do *not* use `max(snapMax, dstMax)` for non-empty levels, or a leapfrogging periodic snapshot could open an intra-level gap that trips `VerifyLevelConsistency`'s "TXID gap detected" check ([compactor.go:225](../compactor.go#L225), `info.MinTXID != prevInfo.MaxTXID+1`).
- **Tests:** first L1 file starts at `snapMax+1`; restore-plan contiguity across the snapshot↔ladder boundary; a leapfrog-snapshot case stays gap-free.

### B. Skip the *remote* upload of the L0 base (removes the 4th copy)

Keep writing the L0 base **locally** (needed by `Pos`/`verify`), but don't upload `0000/0001` to S3.

- **Fixes:** one redundant 65 GB upload + its concurrency with the L9 write.
- **Requires:** Appendix A as well — the compactor prefers local source files ([compactor.go:139](../compactor.go#L139)), so the local L0 base would still be pulled into L1 without the seek floor.
- **Needs:** a consumer audit of the remote `0000/0001` (restore uses L9; L0 retention deletes it in ~1h anyway — but confirm nothing else reads it).

### C. Snapshot level reuses the L0 base instead of re-reading the DB

Have `CompactDB(SnapshotLevel)` produce L9 by compacting the existing (local) L0 base — via `LocalFileOpener`, no second DB read — or by an S3 server-side copy, rather than `db.Snapshot`'s independent full re-read ([db.go:2995](../db.go#L2995)).

- **Fixes:** the concurrent double **read** and its double page-index memory spike (the #1479 gap), even without the position-anchor work.
- **Leaves:** two base *files* (L0 + L9) and the ladder cascade (needs A).

### D. Sync owns the base; the monitor owns periodic snapshots — **recommended ship**

The insight that supersedes B and C, and gets most of the leading proposal's payoff **without** the position-anchor refactor. Two moves:

**1. The snapshotting write path always persists local-L0 + remote-L9 — for *all* sync snapshots, not just the first.** The sync path writes a full-DB image whenever `info.snapshotting == true` ([db.go:2203](../db.go#L2203) → `writeLTXFromDB`). That is **not only the initial base**: the sync path also makes *boundary* snapshots mid-stream — "wal header salt reset" ([db.go:1781](../db.go#L1781), [:1795](../db.go#L1795)), "last page … wal overwritten by another process" ([:1806](../db.go#L1806)), "full or restart checkpoint detected" ([:1825](../db.go#L1825)). All are DB-sized full images tagged `MinTXID == MaxTXID == txID` ([db.go:2187-2188](../db.go#L2187)) and all flow through the same `writeLTXFromDB`. So the rule is: whenever `info.snapshotting`, write the file **locally to the L0 directory** (unchanged) but **upload it to remote L9** (instead of remote L0), and record `maxLTXFileInfos[SnapshotLevel]` (instead of `[0]`).

Redirecting *all* snapshotting writes (not just the first) is **necessary, not just tidy**: a boundary snapshot left in remote L0 is a DB-sized L0 file that the ladder would pull → the cascade again, now triggered *mid-stream* on every salt-reset/checkpoint/WAL-overwrite event. "The initial base" is the wrong scope; "the snapshotting write path" is the right one.

- **Anchor machinery is untouched.** `MaxLTX` is an `os.ReadDir` of the **local L0 dir** ([db.go:632](../db.go#L632)); `Pos`/`verify`/`snapshotWALEndOffset` open `LTXPath(0,…)`. The snapshot file physically stays in the local L0 dir, so all three still find it — **no level-aware `Pos` work needed.** This is the entire reason D is cheaper than the leading proposal.
- **The cascade is prevented structurally.** The compactor enumerates its source from the **remote** listing (`c.client.LTXFiles`, [compactor.go:113](../compactor.go#L113)); local files are only a read-through optimization ([compactor.go:139](../compactor.go#L139)). Snapshots are uploaded to remote L9, not remote L0, so `LTXFiles(L0,…)` returns only increments — the ladder physically cannot see a DB-sized image.

**2. The L9 monitor never makes the base; only periodic snapshots at pos > 1.** Add one guard to `CompactDB`'s snapshot shortcut ([store.go:810-825](../store.go#L810)):

```go
if pos.TXID <= 1 {
    return ErrNoCompaction // the base (TX1) is the sync path's responsibility
}
// existing guard: skip if the snapshot level is already at/beyond pos
if dstInfo.MaxTXID != 0 && dstInfo.MaxTXID >= pos.TXID {
    return ErrNoCompaction
}
```

- **The double-read is gone by construction** — the monitor never attempts the base, so nothing competes with the sync path's one initial read during its ~17-min L9 upload. This closes the concurrent-double-read (the #1479 memory gap) **without a lock**, avoiding the `execSem`↔`compactionMu` inversion that sinks the "put the sync snapshot under `compactionMu`" approach (sync holds `execSem` throughout its write; compaction takes `compactionMu`→`execSem`, so making sync take `compactionMu` after `execSem` deadlocks).
- **It subsumes the `pos==0` malformed-write guard.** At `pos == 0` (pre-base) and `pos == 1` (base only) the monitor stands down, so it can no longer attempt `WriteLTXFile(SnapshotLevel, 1, 0)` ([db.go:2995](../db.go#L2995)).
- **No near-double either.** Once the sync path records `maxLTXFileInfos[L9]` at TX1 with a fresh `CreatedAt`, `ErrCompactionTooEarly` ([store.go:805](../store.go#L805)) defers the monitor's next snapshot by a full `SnapshotInterval` — its first periodic snapshot lands one interval later at an advanced pos, not on the heels of TX1.
- **The monitor is still a *snapshotter*, deliberately.** Its periodic job stays a live-DB re-read ([db.go:2769](../db.go#L2769)) — the live DB is local and already open, so re-reading it is cheaper than the "purist" alternative of pulling the 65 GB prior-L9 from S3 to compact increments onto it. D carves TX1 out of the monitor; it does **not** stop the monitor from snapshotting.

**Safety:**
- **No data-loss if the sync-path L9 upload fails.** Any snapshot is a *full* DB image, so the monitor's first periodic snapshot (e.g. TX50000) subsumes the base; restore just seeds from the latest L9. The TX1 L9 is an early restore point, not a correctness dependency — so removing the monitor as a base "safety net" costs nothing.
- **Restore-then-replicate unaffected.** After a restore, pos resumes > 1 and the sync path writes increments (`info.snapshotting == false`), so "sync owns TX1" engages **only** on a fresh generation — exactly where the double-read occurs.
- **Local L0 base is subject to normal L0 retention** (~1h); by then increments exist locally and the anchor moves to the newest L0 increment. Same behavior as today, not a regression. Just don't *also* upload the base to remote L0.

**Boundary snapshots create an L0 gap — and this is why A is load-bearing, not belt-and-suspenders.** A boundary snapshot at TXID `k` now lives *only* in L9, so remote L0 has a **TXID gap** at `k` (increments `…k-1`, then `k+1…`). Two consequences:

- **Restore-to-latest is fine.** It seeds from the L9 boundary snapshot (a full image at `k`) and extends with L0 `k+1…`. The gap is covered by the snapshot.
- **The ladder must never merge across the gap** — and Appendix A is exactly the mechanism that prevents it. Without A, when retention later empties a ladder level, its next compaction takes the empty-destination path, seeks from **TXID 1**, and pulls *all* of remote L0 across the `k` gap — merging `[…k-1]` and `[k+1…]` into one file that is **missing TXID `k`'s pages**. At best that trips `VerifyLevelConsistency`; at worst it's a silently-wrong compaction (a page `k` modified, and nothing after `k` touched, reverts to its pre-`k` value on restore). With A, the empty level seeks from `snapMax+1 = k+1`, so the fresh ladder segment starts *at* the boundary and stays contiguous: `L9 boundary[…k] → ladder[k+1…]`.

So A's real meaning is **"a fresh ladder segment begins at the latest snapshot boundary."** That is redundant *only* against the bootstrap cascade (which D handles structurally); against the **boundary-snapshot + retention** interaction it is a **correctness requirement**. D and A are complementary: **D** keeps DB-sized images out of remote L0 (no cascade); **A** keeps the ladder from starting a segment below the latest snapshot (no cross-boundary merge). Ship both.

**Make `VerifyLevelConsistency` snapshot-aware.** Once boundary snapshots exist, a TXID gap in a ladder level that is *covered by an L9 snapshot* is legal, not a defect — the "TXID gap detected" check ([compactor.go:225](../compactor.go#L225)) must treat a gap whose missing range is spanned by a snapshot as valid, or legitimate post-boundary ladders will falsely flag.

**Tests:** snapshot file lands in local L0 dir + remote L9 (not remote L0); `MaxLTX`/`Pos` resolve from the local L0 snapshot; ladder's first L1 source is the TX2 increment (no MinTXID=1); monitor returns `ErrNoCompaction` at pos 0 and pos 1; monitor produces its first periodic L9 only after `SnapshotInterval` at pos > 1; restore chain `L9[..1] + L0/ladder[2..]` is contiguous; **a boundary snapshot at `k` leaves remote L0 gapped at `k`, an empty ladder level reseeds from `k+1` (not TXID 1), and restore across the boundary is byte-correct.**

**Purist end-state (later, not tomorrow):** make L9 a *true compaction* — roll new increments onto the prior L9 rather than re-reading the live DB — so the live DB is read exactly once per generation (by sync) and never again. Payoff isn't I/O (swaps a local read for a 65 GB remote one) but eliminating the ~17-min `chkMu.RLock` hold each snapshot interval, which otherwise can stall checkpointing on a hot DB. Bigger change, orthogonal to the double-read; out of scope here.

### Comparison

| approach | ladder cascade | duplicate L0/L9 base | concurrent double-read (memory) | touches `Pos`/anchor | size |
|---|---|---|---|---|---|
| A. seek from snapshot | ✅ removed | — | — | no | small |
| B. skip remote L0 upload (+A) | ✅ | ✅ upload removed | partial | no | small |
| C. L9 reuses L0 | — (needs A) | file remains | ✅ removed | no | medium |
| **D. sync owns base / monitor periodic (+A)** | ✅ removed | ✅ removed | ✅ removed (no lock) | no | small–medium |
| **Leading: base→L9** | ✅ removed | ✅ removed | ✅ removed | **yes** | larger |

**Sequencing recommendation:** ship **A** now (safe, tiny, kills the bootstrap cascade alone). Then ship **D** as the real fix — it supersedes B and C, closes both the cascade *and* the double-read with no lock and no anchor refactor, and needs the sync-path L0-local/L9-remote redirect (for *all* snapshotting writes), the one-line `pos <= 1` monitor guard, and the snapshot-aware `VerifyLevelConsistency` tweak. **A stays underneath D as a correctness requirement, not belt-and-suspenders** — with D's boundary snapshots living in L9, A is what stops an empty ladder level from merging across a boundary gap (see "Boundary snapshots" above). Together they are one rule: *snapshots live in L9; each ladder segment begins at the snapshot that precedes it.* Reserve the **leading proposal** (and the purist L9-by-compaction end-state) for "do it once, properly" if the `chkMu`/checkpoint-stall or single-DB-read invariants become worth the larger change.
