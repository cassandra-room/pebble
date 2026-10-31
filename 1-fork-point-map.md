# Fork-Point Map for Pebble master (a995e89)

This document maps every change in Part 1 of pebble-storage-design.md onto the Pebble source the fork is taken from: the master branch at commit a995e890c06ef86c583927906e16e7f313083a3f (21 October 2026), which was still the latest master commit on 31 October 2026. The module path on master is github.com/cockroachdb/pebble (without a /vN suffix; release branches add one, see section 13), and it needs Go 1.25.3 or newer.

For each change it says where the code is, what the code does there today, and what the fork has to change. It describes behaviour, not code. Names of new options, types and functions are left to the owner. Names that appear here are Pebble's own.

Line numbers are for a995e89 and approximate; search for the function name. "Verified" means read in the a995e89 source for this document. Where something was only checked by name, or not at all, it says so.

The design decisions are in pebble-storage-design.md. If anything here seems to contradict it, the design document wins, and the contradiction should be reported rather than resolved silently.

**Pin the commit.** Master has no stability promise between commits. Fork from exactly this commit, and move to a newer master commit only deliberately, running the full test suite each time. If the fork is moved, this map must be checked again.

---

## 1. The compaction filter option and its factory

**Where.** The Options struct in options.go.

**Today.** No such option exists. The nearest existing hooks are Options.Merger (around line 1041) and Options.BlockPropertyCollectors (around line 1160).

**Change.** Add an option (`Options.CompactionFilter`, of type `CompactionFilterFactory`) holding a factory that is called once per compaction and returns a filter object (`CompactionFilter`) for that compaction, or nothing. Per the design (section 2), the factory receives `CompactionFilterInfo`: the compaction's key bounds, its output level, whether it is a flush, whether it is the bottommost data layer, whether any snapshots are open (`HasSnapshots`), and a callback `NewExternalIter` to open an external lookup iterator (section 10). The factory is not called for flushes (design section 2: the filter never runs in flushes). The types (`CompactionFilterFactory`, `CompactionFilterInfo`, `CompactionFilter`, `CompactionFilterRecordInfo`, `CompactionFilterDecision`) live in `internal/base/compaction_filter.go` and are re-exported in `options.go`.

**Notes.**

- The compaction's bounds are in tableCompaction.bounds (compaction.go, around line 310), set in newCompaction (around line 573). Whether it is a flush is tableCompaction.IsFlush (around line 444). Whether it is the bottommost data layer is tableCompaction.isBottommostDataLayer (around line 974), which returns false for every flush.
- **Output level.** Master distinguishes outputLevel from eventualOutputLevel (around line 301): when the level below the output level has no data in the compaction's bounds, the outputs "will be later moved to eventualOutputLevel", and that level is used for target file sizes and compression. For the filter, the facts that matter are whether the compaction is the bottommost data layer and whether snapshots are open, not the level number.
- The filter object also needs to know whether the compaction has any open snapshots; see section 3, which matters for correctness.
- **Why flushes get no filter (design section 2).** A flush runs through the same path as a compaction (flush1 calls runCompaction, which calls compactAndWrite), so the factory sees IsFlush. Pebble's comment on IterConfig.IsBottommostDataLayer (internal/compact/iterator.go, around line 292) says "flushes always set this to false (because flushes almost never form the bottommost layer of data)", so Rule 2 could never apply in a flush.
- **The write stall that slower flushes would hit.** Options.MemTableStopWritesThreshold (options.go, around line 1035). When the queued memtables reach it, writes wait in DB.maybeInduceWriteStall (db.go, around line 2709; the threshold check is around line 2720) until a flush finishes.

---

## 2. Passing the filter into the compaction iterator

**Where.** compactAndWrite in compaction.go (around line 2764) builds compact.IterConfig (around line 2821) and creates the iterator with compact.NewIter.

**Today.** IterConfig (internal/compact/iterator.go, around line 272) carries the comparer, the merge function, the snapshot list (Snapshots), the tombstone elision settings, IsBottommostDataLayer, and callbacks for API misuse.

**Change.** Add fields to IterConfig for the filter object. The compaction's bounds are known to the caller; pass them to the factory rather than to the iterator, unless the hook needs them.

**Where the snapshot list comes from.** runCompaction (around line 2685) takes it from d.mu.snapshots.toSlice() (around line 2717) while holding the DB mutex, and passes it into compactAndWrite. toSlice returns nothing when no snapshot is open (snapshot.go; verified).

---

## 3. The hook in the compaction iterator

**Where.** Iter.Next in internal/compact/iterator.go (around line 439).

**Today (verified).** Next loops over input records. The two cases that matter:

- **SET and SETWITHDEL** (around line 628). It calls setNext (around line 853), which saves the key and value, sets the sequence number to zero when the record is in the bottommost snapshot stripe of a bottommost compaction (isBottommostSnapshotStripe, around line 1503), skips older records of the same key in the same stripe, and then Next returns the record.
- **MERGE** (around line 640). It records the current stripe index (origSnapshotIdx, around line 643), calls the configured merge function with the newest operand ("MERGE values are always stored in-place"), merges older operands of the same stripe (mergeNext), and, if the record is in the bottommost stripe of a bottommost compaction, sets the sequence number to zero and turns the kind into SET. It then calls finishValueMerger (around line 1511) with includesBase true for SET results and false for MERGE results. If the merger reports that the key should be deleted (DeletableValueMerger), the loop continues to the next key without returning anything; otherwise Next returns the record.

**Why the fork exists (design section 1).** The comment in the MERGE case (around line 650) says the kind is turned into SET "so that iteration and future compactions do not need to invoke the user's Merge operator". After that, nothing of ours runs for the row.

**Change.** Call the filter (`applyFilter`) just before each of these two returns, with the record's key, its value, its kind after merging, and the facts below (`CompactionFilterRecordInfo`). The filter answers keep (`CompactionFilterKeep`), replace the value (`CompactionFilterReplace`), or drop (`CompactionFilterDrop`). When dropping, if the iterator is still positioned on an unskipped older record in the same stripe (`i.pos == iterPosCurForward && i.skip`, which happens when `mergeNext` stops on a `SET` or `DEL` in the same stripe), the filter path calls `i.skipInStripe()` before continuing the loop so older records for the same key in the same stripe are not emitted.

**Note on stock `needDelete` and `skip` in `MERGE` (verified).** In stock Pebble, when `mergeNext` stops on an older `SET` or `DEL` in the same stripe (`i.pos == iterPosCurForward` and `i.skip == true`) and `DeletableValueMerger.DeletableFinish` returns `needDelete == true`, the `MERGE` case executes a bare `continue` without calling `i.skipInStripe()`. That re-enters the loop positioned at that older record rather than advancing past the stripe. The fork leaves stock Pebble's `needDelete` path untouched (`if needDelete { if i.closeValueCloser() != nil { return nil } continue }`) so that behaviour with `Options.CompactionFilter` unset is identical to stock Pebble, while `CompactionFilterDrop` calls `i.skipInStripe()` when `i.pos == iterPosCurForward && i.skip`.

**What the filter needs at the hook.**

- **The stripe index of the record.** i.curSnapshotIdx (around line 243). For the MERGE case, use origSnapshotIdx, because merging advances the iterator.
- **Whether the compaction has any open snapshots.** The length of i.cfg.Snapshots is zero.
- **Whether nothing older for this key exists below the compaction.** For a record in the bottommost stripe of a bottommost compaction (isBottommostSnapshotStripe), nothing older exists below. The per-key check Pebble uses for its own tombstones is i.delElider.ShouldElide(key) (around line 577).

**Snapshot semantics (verified, and important).** Snapshots.Index in internal/compact/snapshots.go (around line 50) returns the index of the first snapshot sequence number greater than the record's sequence number. So a record's stripe index is the number of open snapshots older than it; index 0 means every open snapshot can see it; with no snapshots there is only stripe 0. Pebble's own tombstone elision drops a DEL only in stripe 0. For this design the rules are (design section 3):

- Rule 1 inside one record: allowed in any stripe.
- Rule 1 for rows covered by a header deletion: allowed only if the header record is not newer than the row record, or both are in the same stripe.
- Rule 2 (purging): allowed only when the compaction has no open snapshots.

Reading this code is what showed that the design's earlier wording ("the filter acts only on records in the newest stripe") was unsafe for purging: a key can have records in several stripes of one compaction, and they are not merged with each other. The design has been corrected.

**Values.** With value separation off (design section 6; see section 11 for how to turn it off on master), SET values are in place too, and the filter can read them directly. With value separation on, SET values could be references into blob files (`!i.kv.V.IsInPlaceValue()`), where `saveValue` has cloned the blob handle into `i.valueBuf`. `applyFilter` fetches non-in-place values into a separate `i.filterBuf` so that if the filter returns `CompactionFilterKeep`, the blob handle in `i.valueBuf` is preserved intact.

**Key kinds the filter ignores.** Range deletions and range keys take separate paths in Next and are not passed to the filter. Data keys in this design never use them, except Pebble range deletes over whole tables or token ranges (DROP TABLE, TRUNCATE, ownership changes), which the filter must not interfere with.

---

## 4. The output splitter: cutting files only between partitions

**Where.** writeKeysToTable in internal/compact/run.go (around line 245) and OutputSplitter in internal/compact/splitting.go (NewOutputSplitter around line 133, ShouldSplitBefore around line 209, SplitKey around line 280, shouldSplitBasedOnSize around line 293). Split limits come from TableSplitLimit in run.go (around line 365).

**Today (verified).**

- writeKeysToTable computes a hard limit key as the smaller of the limit passed in and TableSplitLimit, creates the splitter, and defines equalPrev (around line 258) as "the writer's ComparePrev of the next key is zero", that is, the next user key equals the previous one.
- For each key it asks ShouldSplitBefore. That function splits at a reached hard limit (the limit key itself), or, when the size calls for it, at a grandparent boundary key, or before the next key provided equalPrev is false. If equalPrev is true it does not split and is asked again at the next key.
- TableSplitLimit returns the smallest of: the first grandparent file's smallest key at which the output would overlap more than MaxGrandparentOverlapBytes of grandparents, and the first L0 split key after the table's first key (L0SplitKeys, around line 91 of run.go, only set for flushes).
- compactAndWrite (around line 2900) also passes the end of the current span policy's key range as a limit. The default SpanPolicyFunc returns an empty policy (options.go, EnsureDefaults; verified), whose key range end is empty, so with the defaults there is no such limit. This design does not set span policies.

**Change (design section 5, "Keeping each partition in one file").**

- An option (`Options.PartitionPrefix`, of type `PartitionPrefixFunc`) holding the partition-prefix function. With the option unset, nothing changes.
- equalPrev becomes "the next key is in the same partition as the previous key". The writer only offers ComparePrev, which compares whole keys, so the splitter or runner will need to keep the previous key's partition prefix itself.
- A grandparent boundary key and any hard limit key (from TableSplitLimit and from the limit passed into writeKeysToTable) are moved back to the start of their partition before use. If that start is at or before the table's first key, the grandparent boundary or TableSplitLimit is ignored for this table (while an explicit `SpanPolicy.KeyRange.End` limit passed into `writeKeysToTable` is kept as-is when its partition start is at or before the table's first key, so span policy boundaries remain respected).
- The splitter's invariant checks (the assertion failures in ShouldSplitBefore and SplitKey: "boundary is behind the next key", "reached boundary ahead of the next user key", "splitKey <= startKey") must still hold after the change. They are active only in builds with invariants enabled, which Pebble's tests use.

**Further facts the design relies on (verified).**

- The splitter's comment (splitting.go, around line 53) says it splits "when the estimated file size is 0.5x-2x the target file size", and OutputSplitter "guarantees that we never split user keys between files" (around line 93). Inside the splitter, equalPrev is the parameter equalPrevFn of ShouldSplitBefore (around line 210); it is consulted before size-based splits only (around line 262), not at grandparent boundaries.
- The runner's MaxGrandparentOverlapBytes (run.go, around line 99) is set from maxGrandparentOverlapBytes in compaction.go (around line 75), ten times the target file size of the next level.
- Options.FlushSplitBytes (options.go, around line 911) sets how much each flush piece holds; its comment says "splitting sstables during flush allows increased compaction flexibility and concurrency when those tables are compacted to lower levels".
- expandedCompactionByteSizeLimit (compaction.go, around line 51) is 25 times the target file size (or less when disk space is short). It only limits optional growth of a compaction's input set; overlapping files in the output level are always included (design section 5, "Cost").
- Pebble's own splitter tests are in internal/compact/splitting_test.go (section 12).

**Flush split keys.** They are computed by calculateFlushSplitKeys in internal/manifest/l0_sublevels.go (around line 708) from interval start keys of existing L0 files, including keys taken from files' largest keys, and handed to the runner as L0SplitKeys (newFlush, compaction.go, around line 845). They reach the splitter only through TableSplitLimit, so the same "move back to the partition start" rule covers them.

**Other ways files are cut.** Excise and DeleteRange can produce virtual tables bounded by the excise or delete bounds. The design uses them only for whole tables or whole token ranges of a table, whose bounds are partition boundaries. Ingested files are written by our own code.

---

## 5. The property collector

**Where.** The collector interface is BlockPropertyCollector in sstable/block_property.go. It is registered through Options.BlockPropertyCollectors (options.go, around line 1160) and copied into the writer options by Options.MakeWriterOptions (around line 2839), which DB.makeWriterOptions (around line 2876) wraps.

**Today (verified).**

- A new collector is created for every sstable written, flushes included: compactAndWrite calls d.makeWriterOptions(c.eventualOutputLevel) for every output table (around line 2882), and flushes go through compactAndWrite (flush1, around line 1618, calls runCompaction, which calls compactAndWrite).
- When a table is finished, the writer calls each collector's FinishTable and stores the returned bytes in the table's user properties under the collector's Name (sstable/colblk_writer.go, around line 1160; the row-block writer does the same around line 1601). The first byte of the stored value is a short ID that Pebble puts there itself ("Place the shortID in the first byte").
- With value separation, collectors may be "provided a nil value" (block_property.go, around line 83). The design keeps value separation off.
- Custom table properties end up in sstable.Properties.UserProperties (sstable/properties.go, around line 134), which is not copied into memory (section 6).

**Changes.**

- **No collector in flushes (design section 5).** Add `Options.CompactionPropertyCollectorName` naming our collector. `Options.EnsureDefaults` precomputes `o.private.flushBlockPropertyCollectors` (filtering out the collector matching `CompactionPropertyCollectorName`), and in `compactAndWrite`, when `c.IsFlush()` is true, `wopts.BlockPropertyCollectors` is replaced with `d.opts.private.flushBlockPropertyCollectors`.
- **The collector itself is our own code** and needs no fork change: it is registered through BlockPropertyCollectors.

---

## 6. Copying the summary into in-memory file metadata

**Where.** TableBacking.PopulateProperties in internal/manifest/table_metadata.go (around line 481), which builds TableBackingProperties (around line 415). Its NumEntries field (around line 417) is the entry count the design's 0.2 share divides by (design section 5). Its callers:

- compaction outputs: compaction.go (around line 2997) calls maybeSetStatsFromProperties (table_stats.go, around line 826) with the writer's properties,
- ingested files: ingest.go (around line 356) calls the same function,
- files whose properties were not set at creation, including all files after a restart: loadTableStats (table_stats.go, around line 341) and estimateSizesBeneath (table_stats.go, around line 595), run by the background statistics job (collectTableStats, around line 87), read the properties block with ReadPropertiesBlock and call PopulateProperties.

**Today (verified).** PopulateProperties receives the table's full sstable.Properties, which include the user properties, but copies only fixed fields into TableBackingProperties: entry counts, raw key and value sizes, tombstone sizes and counts, range deletion and range key counts, value block size, value separation settings, and the tombstone-dense block ratio. Its comment says it "can be called at most once for a TableBacking". TableBacking.Properties (around line 472) reports whether the properties are present yet. Pebble's own elision-only picker treats a file whose properties or statistics are not present as not eligible, and does not cache that answer.

**Change.** Because every path goes through `PopulateProperties`, and it already receives the user properties, pass `collectorName string` (`d.opts.CompactionPropertyCollectorName`) into `PopulateProperties(props *sstable.Properties, collectorName string)` and copy only our collector's summary (the user property under `collectorName`, after stripping Pebble's 1-byte short-ID prefix) into `TableBackingProperties.CompactionPropertySummary` and `HasCompactionPropertySummary` **before** `b.propsValid.Swap(true)`, exposed via `TableBackingProperties.BlockPropertySummary()`. Storing only this single `[]byte` avoids keeping all `UserProperties` (such as `obsolete-key`) in memory. A file whose backing properties are not present yet (`m.TableBacking.Properties() == nil`) must be treated as "not known yet", never as "nothing due".

**Virtual tables.** Properties belong to the TableBacking, which virtual tables created by excise share with their backing file. Our summary therefore describes the whole backing file for each virtual table, which the design accepts as conservative.

---

## 7. Refusing trivial moves of files without the summary

**Where.** tableCompaction.maybeSwitchToMoveOrCopy in compaction.go (around line 667), called from newCompaction.

**Today (verified).** It considers only compactions with a single input file, no output-level files and no extra level data. It proceeds only for default compactions, and for tombstone-density compactions unless the output level is the lowest level ("the goal there is to actually remove the tombstones"). It refuses if the grandparent overlap is too large. Otherwise it turns the compaction into a move (local files; c.kind = compactionKindMove, around line 720) or a copy (shared storage).

**Change.** When `Options.CompactionPropertyCollectorName != ""`, refuse the move if the single input file's backing properties are not loaded yet or have no summary from our collector (`!hasSummary`; design section 5: files from flushes have none, and no file may reach L1 or below without one). The compaction then runs as a normal rewrite. When `Options.IsFileDue` (of type `IsFileDueFunc`, taking `DueFileInfo`) is set and reports that the input file is already due, refuse the move as well (design section 5, "What it does").

**Verified: `eventualOutputLevel` does not bypass `maybeSwitchToMoveOrCopy`.** Master's `eventualOutputLevel` (set in `newCompaction`, `compaction.go`) only configures target file sizes, compression, grandparent bounds, and value separation while the compaction writes its outputs into `outputLevel`. Any subsequent move of those outputs down toward `eventualOutputLevel` is scheduled as a separate compaction that goes through `newCompaction` and `maybeSwitchToMoveOrCopy` (and compaction outputs already carry a summary anyway).

---

## 8. The due-deletions picker step

**Where.** compactionPickerByScore.pickAutoNonScore in compaction_picker.go (around line 1500). DB.pickAnyCompaction (compaction.go, around line 2067) calls, in order: pickHighPrioritySpaceCompaction (compaction_picker.go, around line 1430; currently only high-priority blob file rewrites), pickAutoScore (the size-based step), any manual compaction, and finally pickAutoNonScore.

**Today (verified).** The non-score steps, in order: pickTombstoneDensityCompaction, pickElisionOnlyCompaction ("These are low-priority compactions because they don't help us keep up with writes, just reclaim disk space"), pickVirtualRewriteCompaction, pickBlobFileRewriteCompactionLowPriority, pickReadTriggeredCompaction, and pickRewriteCompaction for files marked for compaction.

**The template: pickElisionOnlyCompaction (around line 1687, verified).**

- It looks only at the bottom level, through an annotator (elisionOnlyAnnotator, around line 1602) built with manifest.MakePickFileAnnotator and manifest.PickFileAnnotatorFuncs (internal/manifest/annotator_table.go, around lines 228 and 247). The annotator keeps, per level, the best candidate file according to a Filter and a Compare function. Filter returns two answers: whether the file is eligible, and whether that answer is stable and may be cached.
- Its filter treats files being compacted as not eligible (cacheable), and files whose backing properties or statistics are missing as not eligible (not cacheable).
- It skips the candidate if its highest sequence number (SeqNums.High) is at or above env.earliestSnapshotSeqNum, which is d.mu.snapshots.earliest() (compaction.go, around line 2051). With no snapshot open, earliest returns the maximum sequence number (snapshot.go; verified), so nothing is skipped.
- It builds the compaction with pickedCompactionFromCandidateFile (around line 1642).

**Change.** Add `pickDueCompaction` to `pickAutoNonScore` (just after `pickElisionOnlyCompaction`) that picks a due file using `Options.IsFileDue` (`DueFileInfo`) and builds a compaction of dedicated kind `compactionKindFilter` (`"filter"`, scheduler priority 45, counted in `CompactMetrics.FilterCount`).

**Decisions recorded for the implementation:**

- **Due-ness is not cached.** Because whether a file is due changes as the clock moves and as per-range repair watermarks move without the file changing, `pickDueCompaction` does not use a cached `PickFileAnnotator`. Instead, it scans levels from `L6` down to `L1` (`>= baseLevel`) under `d.mu` on each `pickAutoNonScore` call, reading the in-memory `CompactionPropertySummary` from `TableBackingProperties` and calling `Options.IsFileDue`.
- **Level rule (`L6 -> L6`, or `l -> l+1` only when nothing overlaps two or more levels down).** For a due file in `L6`, the compaction runs `L6 -> L6`. For a due file in `l < 6` (`l >= baseLevel`), `pickedCompactionFromCandidateFile` requires `outputLevel` to be `defaultOutputLevel(l, p.baseLevel)` (`l + 1`), so `pickDueCompaction` considers `l -> l+1` only when no file in any level `>= l+2` overlaps. After `pickedCompactionFromCandidateFile` expands inputs and widens `pc.bounds`, `pickDueCompaction` re-checks `pc.bounds` against levels `>= l+2` and against `env.inProgressCompactions` with `outputLevel > l`. This guarantees the resulting compaction is always the bottommost data layer (`c.isBottommostDataLayer() == true`).

**Snapshots.** Because purging requires no open snapshots (design section 3), `pickDueCompaction` immediately returns `nil` when `env.earliestSnapshotSeqNum != base.SeqNumMax`.

---

## 9. "Pick again" and the timer

**Where.** DB.maybeScheduleCompaction (compaction.go, around line 1977), DB.Schedule (around line 2124), DB.GetWaitingCompaction (around line 2152), and pickedCompactionCache in compaction_scheduler.go (around line 190). The concurrency scheduler's periodicGranter (around line 412, started around line 306) runs every 100 ms.

**Today (verified).**

- maybeScheduleCompaction must be called with the DB mutex held. It runs delete-only and download compactions directly. Then, if the cache says a compaction is already waiting, it returns. Otherwise it calls pickAnyCompaction repeatedly and asks the scheduler for permission to run each result.
- Schedule and GetWaitingCompaction, called by the scheduler, pick again only if the cache says a compaction is waiting. When picking finds nothing, they call setNotWaiting (around line 223 of compaction_scheduler.go), after which nothing is picked until something changes.
- The cache is invalidated when a new version is installed (version_set.go, around line 318) and when compactions start (compaction.go, around lines 1994 and 2003).
- maybeScheduleCompactionAsync (around line 1958) takes the mutex and calls maybeScheduleCompaction. It calls Done on d.compactionSchedulers when it finishes, so whoever launches it must register it there first, as the read path does (iterator.go, around line 2539).

**Change.** Expose `DB.MaybeScheduleCompaction()` on `DB` (design section 5, "Pick again"), which returns early if the DB is closed, otherwise acquires `d.mu` and calls `d.maybeScheduleCompaction()`. The timer (once a minute by default) and the end of a repair (repair mode) make this call.

---

## 10. The lookup view for older records outside the compaction

**Where.** The read state (readState in read_state.go, around line 25: the current version and the memtables; loadReadState around line 75) and how Pebble builds a point iterator over it: Iterator.constructPointIter in db.go (around line 1382), and an alternative constructPointIterV2 (around line 1508) built on newer merging and level iterators, chosen when the iterator was allocated for it (db.go, around line 1236).

**Today (verified).** constructPointIter builds one merging iterator from the memtables, one level iterator per L0 sublevel (from current.L0SublevelFiles, newest first) and one per non-empty level from L1 down (from current.Levels[level]). Each level iterator takes a manifest.LevelIterator over that level's files.

**Change (design section 7).** Pass `CompactionFilterInfo.NewExternalIter` (implemented by `DB.newCompactionExternalIter`) to the compaction filter factory so the filter can open a `base.InternalIterator` over the memtables and the files outside the compaction, and use it to find the records of a purge candidate's key or range.

Notes for the implementer:

- **Which files to include.** Purging happens only in bottommost compactions, so nothing below the compaction matters. Above it: all memtables; every L0 file that is not one of the compaction's inputs (L0 files can overlap, so other L0 files can hold the key); and every level from L1 down to `c.startLevel.level`, excluding the compaction's own input files (`inCompaction` map). Levels below `c.startLevel.level` are skipped because non-input files in the output level cannot overlap the compaction's bounds and nothing exists below a bottommost compaction.
- **Raw records, not merged values.** The check needs every record's own timestamps. Pebble's public Iterator would run our merger over the records it finds. DB.ScanInternal (scan_internal.go) is not suitable either: its comment says it "collapses all point keys into one InternalKey; so only one internal key at most per user key is returned". The view must iterate internal keys and values directly, as the merging iterator inside constructPointIter does.
- **Standard point iterators so Bloom filters apply.** `newCompactionExternalIter` builds a `mergingIter` using standard point iterators (`internalIterOpts{readEnv: ...}` without `compaction: true`) and `manifest.LevelOptions{UseL6Filters: true}`, never `reader.NewCompactionIter` (which hardcodes `NeverUseFilterBlock` and sequential readahead). For an exact row key, `SeekPrefixGE` therefore consults each SSTable's Bloom filter (`seekPrefixGE` in `sstable/reader_iter_single_lvl.go`, around line 1016, calls `bloomFilterMayContain` around line 1041; verified). On master the filter is configured per level with `LevelOptions.TableFilterPolicy` (`options.go`, around line 498), for example `bloom.FilterPolicy(10)` from `sstable/tablefilters/bloom` (at 10 bits per key the false-positive rate is 1.14%). The default for L0 is no filter, and other levels inherit the previous level's setting. `Options.ApplyTableFilterPolicy` (around line 1638) sets all levels at once. The default comparer's `Split` returns the whole key (`DefaultSplit`, `internal/base/comparer.go`, around line 208), so the filter covers the whole key.
- **Lifetime and resource tracking.** `compactionExternalIter` pins a `readState` (`d.loadReadState()`), and with it memtables that Pebble would otherwise free, and releases it on `Close()` (guarded by `invariants.CloseChecker`). The design re-creates it regularly (design section 7).
- **Range deletions.** Records in the view may be covered by Pebble range deletes (DROP TABLE, TRUNCATE). Treating such records as present is conservative: it can only block a purge, never allow a wrong one.

---

## 11. Other facts the implementation relies on

- **Value separation is on by default on master.** EnsureDefaults (options.go, around line 1684) fills in a policy with Enabled set and a MinimumSize of 256 bytes when ValueSeparationPolicy (around line 841) is nil. The design needs it off, so the options must set a ValueSeparationPolicy whose Enabled is false.
- **Columnar blocks follow the format major version.** On master the table format comes from the format major version alone (DB.TableFormat in format_major_version.go, around line 480). Setting FormatMajorVersion to FormatColumnarBlocks or newer is enough (design section 25). The comment on ValueSeparationPolicy still mentions an EnableColumnarBlocks setting, which does not exist on master.
- **The merger interface.** base.ValueMerger and base.DeletableValueMerger in internal/base/merger.go. With the fork, the merger no longer needs DeletableValueMerger (design section 22).
- **Merged records become SET at the bottom** (section 3), which is why the fork exists (design section 1).
- **Merger name.** Stored on disk; opening with a different name fails (design section 25).
- **File-only snapshots for repair and streaming** (design section 24). DB.NewEventuallyFileOnlySnapshot (db.go, around line 1803) takes key ranges. The type's comment (snapshot.go, before the EventuallyFileOnlySnapshot type) says it prevents elision like a normal snapshot only until the overlapping memtables are flushed, then "transitions to a file-only snapshot state in which it pins zombies sstables like an open Iterator would". In the constructor, if no memtable overlaps the ranges, it pins the current version and is never added to the snapshot list; otherwise it adds a normal snapshot to the list, which transitionToFileOnlySnapshot later closes (snapshot.go, around lines 300 to 372). WaitForFileOnlySnapshot blocks until the transition, and can schedule a flush after a given delay. No fork change is needed.
- **Only SET values are separated.** On master, valsep/value_separator.go (around line 255) says "Only SET and SETWITHDEL can be separated"; Merge values stay in place. Whether a compaction writes new blob files or carries existing blob references forward is decided by shouldWriteBlobFiles (compaction_value_separation.go, around line 45; see the comment in compaction.go around line 262). On v2.1.7 the equivalent comment is in value_separation.go: "Merge and deletesized keys are never separated". This is background for design section 6; with value separation off it does not matter.
- **Bloom filter defaults.** NoFilterPolicy (options.go, around line 70) is the default for L0 (around line 529), and other levels inherit the previous level's setting (around line 496), so no level has a filter unless configured. The filter policy is bloom.FilterPolicy in sstable/tablefilters/bloom/bloom.go (around line 161). Point lookups in the view use a prefix seek, as the public Iterator.SeekPrefixGE (iterator.go, around line 1503) and Get do (section 10).
- **Version.Overlaps** (internal/manifest/version.go, around line 620) returns the files of one level overlapping given bounds. It was the planned tool for the rejected widening approach (design section 5, "Wide partitions: options considered"); aligned splitting does not need it.
- **Snapshot and zombie-file metrics.** Pebble's Metrics has a Snapshots section (metrics.go, around line 235) with Count, EarliestSeqNum, PinnedKeys and PinnedSize, and table metrics that count zombie files. They are what design section 24 exposes.

---

## 12. Where Pebble's own tests live

- The compaction iterator's data-driven tests: internal/compact/testdata, run by internal/compact/iterator_test.go.
- The splitter's tests: internal/compact/splitting_test.go and its testdata.
- The metamorphic tests: the metamorphic package, which runs random operations against many configurations and compares results.

With the filter option and the partition-prefix function unset, every existing test must pass unchanged (design section 9).

---

## 13. Module paths and versions, for reference

Checked in Pebble's git history on 31 October 2026:

- On master, go.mod has said "module github.com/cockroachdb/pebble" since September 2019, when the path was renamed from petermattis/pebble. It has no /vN suffix.
- The /v2 suffix was added only on the release branch pebble-release-2.1, in commit 451c7738 ("pebble-release-2.1: change path to v2", 27 August 2025). That commit is not on master. All v2.1.x tags, up to v2.1.7, come from that branch. The v2.0.0 tag still has the path without a suffix.
- CockroachDB's own release branches (crl-release-25.2 to crl-release-26.2) also exist.

A fork from master imports github.com/cockroachdb/pebble. Moving the fork to a release later would change the import path, which is a mechanical change.

**Differences from v2.1.7,** for anyone comparing with the earlier version of this map: the compaction iterator (Next, setNext, isBottommostSnapshotStripe), ShouldSplitBefore and TableSplitLimit are the same apart from three panic messages. v2.1.7 builds writer options with Options.MakeWriterOptions directly, keeps per-file statistics in TableMetadata.Stats rather than TableBackingProperties, configures Bloom filters with LevelOptions.FilterPolicy and the bloom package, has no high-priority space step, no virtual-table rewrite step and no constructPointIterV2, leaves value separation off by default, needs EnableColumnarBlocks for columnar blocks, and needs Go 1.23.
