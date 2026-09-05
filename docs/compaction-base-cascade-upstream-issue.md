<!--- Please provide as much detail as possible to help us reproduce and fix the issue -->

## Bug Description

The **first** compaction into each destination level re-compacts the entire TXID history from TXID 1 — **including the DB-sized base snapshot** — so a freshly-replicated database is rewritten and re-uploaded **in full, once per compaction level**. On a large database each of these is an O(database size) read + merge + upload; there is no incremental benefit, because the level starts empty and pulls everything from the beginning.

This is independent of, and compounds with, snapshot/compaction serialization (#1479):

- **Without serialization:** the per-level base rewrites run concurrently, each holding a database-sized in-memory page index → memory blow-up / OOM on large DBs (the motivation for #1479).
- **With serialization (#1479):** they run sequentially, so compaction is **monopolized** by a chain of full-database rewrites (≈ *number-of-levels × base-upload-time*) during which no other compaction can make progress and incremental L0 files pile up unbounded.

Either way the root cause is the same: **the DB-sized base is re-included in every level's first compaction**, even though it is already fully captured at the snapshot level.

### Where it happens

`Compactor.Compact` chooses its source range from the **destination** level's high-water mark:

```go
// compactor.go
prevMaxInfo, err := c.MaxLTXFileInfo(ctx, dstLevel)   // dst level's max
seekTXID := prevMaxInfo.MaxTXID + 1                    // == 1 when dst is empty
itr, err := c.client.LTXFiles(ctx, srcLevel, seekTXID, false)
```

When a destination level is empty, `prevMaxInfo.MaxTXID == 0`, so `seekTXID == 1` and the compaction pulls **every** source-level file from TXID 1 onward — which, on a fresh generation, begins with the DB-sized base snapshot L0. The resulting L1 file therefore spans `[1 .. N]` and is itself DB-sized. When L2 first compacts, it pulls L1 from TXID 1 (again DB-sized) → L2 `[1 .. M]`; then L3 likewise. The base propagates up the entire ladder, one full-DB rewrite per level.

`CompactDB` (store.go) is the per-tick entry point; the snapshot level is handled as a shortcut (`dstLevel == SnapshotLevel` → `db.Snapshot`), and the incremental ladder falls through to `db.Compact` → `Compactor.Compact` above.

## Environment

**Litestream version:**
<!--- Line numbers below are from current main (v0.5.17-1-g4ed7a30). -->
```text
main @ v0.5.17-1-g4ed7a30
```

**Operating system & version:** Linux (container), arm64
**Installation method:** built from source
**Storage backend:** S3-compatible (reproduces on any backend; the cost is read+merge+upload of the whole DB per level)

## Steps to Reproduce

1. Replicate a large database (ours is ~65 GB) from empty, so the base snapshot L0 is DB-sized.
2. Keep the DB under steady write load so incremental L0 files accumulate.
3. Watch the remote store (or in-progress multipart uploads). As each ladder level (L1, then L2, then L3) is first populated, observe a **DB-sized** object whose key spans `MinTXID = 1` — i.e. the base is being re-written into that level.

**Expected:** the incremental ladder compacts only the increments *above the latest snapshot*. The DB-sized base is written once (to the snapshot level) and never re-materialized into L1/L2/L3. Per-level compaction cost is O(new data), not O(database size).

**Actual:** each level's first compaction re-reads/re-writes/re-uploads the entire database (spanning from TXID 1), because an empty destination seeks from TXID 1.

## Evidence

From one ~65 GB generation (object key = `<level>/<minTXID>-<maxTXID>.ltx`, size in bytes):

```text
# Base capture — the DB written twice at TXID 1 (level 0 and the snapshot level):
0000/0000000000000001-0000000000000001.ltx   65,390,548,046   # base snapshot L0 (~60.9 GiB)
0009/0000000000000001-0000000000000001.ltx   65,390,548,046   # snapshot level, identical size

# Then each ladder level's FIRST compaction re-includes the base (note MinTXID = 1),
# observed as successive DB-sized multipart uploads:
0001/0000000000000001-0000000000000039.ltx   ~65 GB   # L1  first compaction  (base + increments)
0002/0000000000000001-00000000000000b3.ltx   ~65 GB   # L2  first compaction  (pulls L1 from TXID 1)
0003/0000000000000001-00000000000000b3.ltx   ~65 GB   # L3  first compaction  (pulls L2 from TXID 1)
```

Timeline on our deployment (serialized build; each step held the compaction lock for its whole upload):

| step | key range | window | duration |
|---|---|---|---|
| snapshot (level 9) | `1–1` | 18:56:12 → 19:13:18 | ~17 min |
| L1 first compaction | `1–0x39` | 19:14:01 → ~19:33 | ~19 min |
| L2 first compaction | `1–0xb3` | ~19:40 → 19:57 | ~17 min |
| L3 first compaction | `1–0xb3` | 20:00 → … | ~17 min |

While these ran, **no other compaction could proceed** and incremental L0 files accumulated from 55 to 172+ over the same ~25 minutes. Net per fresh generation: **~4–5 sequential full-DB rewrites (≈ an hour of monopolized compaction) and hundreds of GB of redundant uploads**, all reconstructing a base that is already present at the snapshot level.

## Additional Context

**Suggested fix.** When compacting into an **empty** destination level, seek from the **latest snapshot watermark + 1** instead of TXID 1, so the ladder never re-includes the base:

```go
seekTXID := prevMaxInfo.MaxTXID + 1
if prevMaxInfo.MaxTXID == 0 {                 // destination level never populated
    if latestSnapshotMaxTXID == 0 {          // snapshot not written yet
        return nil, ErrNoCompaction          // defer; do NOT fall back to TXID 1
    }
    seekTXID = latestSnapshotMaxTXID + 1      // skip everything already in the snapshot
}
```

Note the empty-snapshot **defer**: the snapshot level and each ladder level are compacted by independent monitors with no ordering guarantee between them, so a ladder level's first compaction can fire before the snapshot exists. Falling back to `seekTXID = 1` in that window would re-pull the base — exactly the cascade this fixes. Deferring until the snapshot is present is deadlock-free (the snapshot is produced by reading the DB, not by compacting the ladder).

This is restore-safe with no other change, because restore already reconstructs from the snapshot and layers increments on top:

- `CalcRestorePlan` (replica.go) starts from the latest snapshot ≤ target ("Start with latest snapshot…", `infos = append(infos, snapshot)`), then greedily extends the contiguous TXID range from the other levels.
- The extension requires contiguity (`info.MinTXID > currentMax+1` disqualifies a candidate), which is preserved exactly: the first ladder file now begins at `snapshotMax + 1`, so it joins the snapshot with no gap. Today's plan merely *overlaps* the base (snapshot `[..1]` + L1 `[1..]`); the fix removes the redundant re-coverage.

Guard for the empty-destination case only (not `max(snapMax, dstMax)`): a non-empty level must keep seeking from `dstMax + 1` so a periodic snapshot that leapfrogs a lagging level cannot open an intra-level gap that trips `VerifyLevelConsistency` (the `prev.MaxTXID + 1 == curr.MinTXID` check).

**The base is also written twice — concurrently — at TXID 1.** On a fresh generation the full database is materialized as **both** an L0 file (`0000/0001`, the streaming initial capture) **and** a snapshot-level file (`0009/0001`). These are produced by two independent code paths — the sync/replication path and the snapshot-level compaction — under two different locks, so on a large DB they run **at the same time**: two full-DB reads, two DB-sized in-memory page indexes, and two DB-sized uploads, all at the worst possible moment (startup, before any compaction can help). In our trace both `0000/0001` and `0009/0001` were actively uploading in the same window (~18:56:12 → ~18:57:04+), and the snapshot level is an *independent full re-read* rather than a promotion of the L0 file.

We suspect this independence is deliberate (keeping the sync path and compaction decoupled), but the consequence scales with database size: it roughly doubles both the peak page-index memory (the very pressure #1479 targets, via a path that mutex doesn't cover) and the initial upload volume. It's a slightly separate issue from the cascade, but has the same "the DB-sized base gets processed more times than necessary" character, so we're flagging it here.

**A more complete alternative — we'd value your read on it.** Beyond the localized fix above, we're considering writing the base **once, directly to the snapshot level (local + remote), and starting the L0 stream at the first increment** (TXID = `snapshotMax + 1`) rather than writing a DB-sized L0 base at all. Because restore already seeds from the snapshot and layers increments on top, this would address **both** problems at once: no DB-sized L0 base (so the ladder — which pulls from L0 — never sees the base, making the cascade fix unnecessary), and no concurrent second capture (the snapshot-level monitor becomes a no-op when the snapshot is already current, so it doesn't re-read the DB).

The cost is that the position/anchor logic — which today derives the latest replicated position from the newest **L0** file — must become **level-aware**, so it can read the position from the snapshot level when no L0 increment exists yet, while preserving the invariant that the file holding the current max TXID is locally present. We're inclined toward this as our end state, but we'd very much appreciate the maintainers weighing in: is there something that fundamentally requires a level-0 file at the base TXID, or another pitfall we've missed? Happy to open a separate discussion/PR if that's a better venue.

**Relationship to other issues.**
- Complementary to #1479: #1479 serializes the concurrent DB-sized page indexes; this issue (and the alternative above) removes the *reason* several of them are DB-sized in the first place. With both, compaction is neither concurrent-and-OOM nor sequential-and-monopolizing.
- On our large base, the first incremental sync after the base also scans the whole base file in `lastPageMatch` (filed separately, #1508) — different code path, same "O(DB size) on a hot path" flavor.
