# Cassandra-Style Storage Layer on Pebble — Design

This document describes how to store Cassandra-style data (partitions, clustering rows, regular columns, sets and maps, TTLs and deletes) in Pebble. It covers only the storage layer on one node. The distributed parts (token ring, replication, gossip, repair) are mentioned only where the storage layer has to support them.

It has two parts. Part 1 is the plan to fork Pebble and add a compaction filter, so that dead data is removed during compaction the way Cassandra removes it. Part 2 is the storage design itself (keys, values, writes, reads, deletes, merging), updated to assume the fork. The earlier cleanup design that works on unmodified Pebble (a GC index, a scheduler goroutine and PurgeOp merge records) is kept in the appendix as the fallback.

Throughout, "verified" means the behaviour was confirmed in the source code of Pebble, Cassandra, ScyllaDB, RocksDB, CockroachDB or Rocksandra, or by tests and measurements run against Pebble v2.1.7. Everything else is a design decision or an inference, and is marked as such. Decisions, including the few details still open, are summarised in section 10.

Terms such as partition header, local deletion time and grace period are explained in Part 2 and in the glossary.

---

# Part 1: Pebble fork and refactor plan

## 1. Why fork

Pebble's compaction already calls our merger, but only while a key still has more than one record. Once compaction merges a row at the bottom level, Pebble stores the result as a plain SET record "so that iteration and future compactions do not need to invoke the user's Merge operator" (Pebble's own source comment; verified, also by a test). From then on nothing of ours runs for that row. A row that received a tombstone and then no more writes would keep the tombstone forever.

Without a fork, the only way to make Pebble look at such a row again is to write something to it. That is what the fallback design does (appendix): it remembers every future cleanup in a GC index, wakes up when each is due, and writes a PurgeOp record to the row. It works and needs no lock, but it costs one extra index write per delete, one extra write per cleanup, a background goroutine, and its own crash-recovery reasoning.

Cassandra needs none of that, because its compaction looks at every cell it copies and simply does not copy dead ones (verified in PurgeFunction.java and CompactionController.java). RocksDB offers the same thing as a pluggable compaction filter, which Instagram's Rocksandra used. Pebble has no such hook. The fork adds one.

## 2. What the fork changes

The change is small in lines of code. The filter needs three places in Pebble, described here; their exact locations in Pebble master a995e89 are in handoff/1-fork-point-map.md, sections 1 to 3. Further changes support it: the compaction picker for due deletions, with its two interfaces to our code ("is this file due?" and "pick again"); three small changes for the property collector (leaving it out of flushes, never trivially moving a file without its summary, and copying the summary into Pebble's in-memory file metadata); and cutting output files only between partitions (all in section 5).

**A new option.** Pebble's options get a compaction-filter field (`Options.CompactionFilter`, of type `CompactionFilterFactory`). It holds a factory that Pebble calls once at the start of every compaction. The factory receives a description of the compaction (`CompactionFilterInfo`: its key range, its output level, whether it is a flush, whether it forms the bottommost layer of data, whether any snapshots are open, and a callback `NewExternalIter` to open an external lookup iterator) and returns a filter object (`CompactionFilter`) for that one compaction. Giving each compaction its own object lets the filter keep state while it streams through keys, which the partition-header rules in section 4 need, and lets it take a fixed copy of the clock and of the repair watermarks at the start (section 8).

**Passing it through.** Before a compaction starts, Pebble builds the configuration of its compaction iterator, the component that streams through the input records and decides what each output record will be. The fork adds the filter object to that configuration. The compaction already knows whether it is the bottommost data layer, and the configuration already carries that fact, so nothing new has to be computed.

**The hook.** The hook sits in the compaction iterator's step to the next output record. For a SET it keeps the key's newest record in its snapshot stripe and returns it. For a run of MERGE records it calls our merger, and when the merge includes the key's oldest data in the bottom snapshot stripe, it turns the result into a SET. The fork calls the filter at these two points, just before the record is returned, with `CompactionFilterRecordInfo`: the key, the value, whether nothing older for this key exists below the compaction, the record's snapshot stripe (which open snapshots can see it), and whether the compaction has any open snapshots at all. The last two are needed for the snapshot rules in section 3; Pebble already knows them, and the fork only passes them on. Pebble already answers the question of whether anything older exists below for its own tombstones, so the filter reuses that answer.

The filter answers one of three things (`CompactionFilterDecision`): keep the record as it is (`CompactionFilterKeep`), write it with a new (smaller) value (`CompactionFilterReplace`), or drop it (`CompactionFilterDrop`). Dropping continues the loop to the next key instead of returning; if the iterator is still positioned on an unskipped older record in the same stripe (`i.pos == iterPosCurForward && i.skip`, which occurs when `mergeNext` stops on a `SET` or `DEL` in the same stripe), the drop path calls `i.skipInStripe()` first so older records in the stripe do not reappear (whereas stock Pebble's `DeletableValueMerger` `needDelete` path is left unchanged so stock behaviour is preserved when `Options.CompactionFilter` is unset; see `1-fork-point-map.md`, section 3).

Decision: the filter runs only in compactions of sstables, never in flushes. A flush runs through the same code as a compaction and builds the same compaction iterator, so the factory checks whether the compaction is a flush and attaches no filter if it is. The reasons:

- A flush can never purge anything. Pebble's own comment on the bottommost-layer flag says "flushes always set this to false (because flushes almost never form the bottommost layer of data)", so Rule 2 could never apply. The most a filter could do in a flush is Rule 1, which the merger already does within a row.
- A flush has to finish before its frozen memtable is freed. Making each flush slower holds memtables longer, so writes reach Pebble's memtable stall limit (the MemTableStopWritesThreshold option) at a lower write rate. While stalled, writes wait until a flush finishes.

RocksDB makes the same choice by default: its CompactionFilterFactory applies the filter only "to files generated by compaction" unless ShouldFilterTableFileCreation is overridden (verified in include/rocksdb/compaction_filter.h).

## 3. The filter rules

The filter applies two rules. Both rely only on what is inside the value, the clock copied at compaction start, and the check that nothing older exists outside the compaction.

**Rule 1: drop hidden data.** Anything a deletion inside the same row hides is dropped at once: a cell or element older than the row deletion, an element older than its collection's clear time, a cell older than a newer tombstone for it. The deletion itself stays, so this is always safe, whatever level the compaction is at. The merger already does this when it merges several records, so for rows Rule 1 mostly matters for rows hidden by the partition header (section 4).

Also under Rule 1, an expiring cell or element whose expiry has passed is rewritten as a tombstone. Its data bytes go, and the tombstone keeps hiding older versions until it may be purged. Cassandra does the same during compaction.

**Rule 2: purge.** A tombstone (a deleted cell or element, a row deletion, a collection clear time, or an expired cell) is removed entirely when all of the following hold:

- its local deletion time is earlier than the purge cutoff. The cutoff is now minus the grace period in the default mode, or the repair watermark in repair mode (section 8),
- nothing older for this key exists below the compaction, which Pebble's check tells us,
- nothing older for this key exists outside the compaction, in a memtable or a level above, which is the lookup in section 7,
- the compaction has no open snapshots (below).

If after this nothing is left of the row, neither live data nor tombstones, the filter drops the record and the row is gone from disk once the compaction's input files are deleted.

Apart from the snapshot rule, these are the same conditions Cassandra uses: its local deletion time must be before gcBefore (now minus gc_grace_seconds), and no SSTable outside the compaction may hold older data for the key (verified in PurgeFunction.java and CompactionController.java).

**Snapshots.** A Pebble snapshot must keep returning exactly what it saw when it was taken. Compaction respects this by splitting a key's records into snapshot stripes and never combining records across a stripe boundary (verified in Pebble's source). A record's stripe index is the number of open snapshots older than it: index 0 means every open snapshot can see it, and the highest index means none can. With no snapshots open there is one stripe.

The rules differ in what they need:

- Rule 1 inside one record (dropping data the record's own deletions hide, and turning expired cells into tombstones) is allowed in any stripe. Every reader that sees the record sees the same result before and after, because reads apply the same deletions and hide expired data by the clock.
- Rule 1 for rows covered by a header deletion (section 4) is allowed only if every snapshot that can see the row record can also see the header record. That holds when the header record is not newer than the row record, or when both are in the same stripe. Otherwise a snapshot taken between the two would still have to see the row's data.
- Rule 2 (purging) is allowed only when the compaction has no open snapshots at all. With snapshots open, a key can have records in several stripes of the same compaction, and they are not merged with each other. A purged tombstone in one stripe could then let an older record in another stripe, or a newer record carrying an older timestamp, reappear. Pebble's own tombstone elision only checks lower levels and the lowest stripe, which is enough for sequence-number deletes but not for timestamp-based ones.

An earlier version of this document said the filter acts only on records in the newest stripe. That was not safe for Rule 2, for the reason in the last point, and was replaced by these rules.

In this design snapshots are used only by repair and streaming (section 24). A normal Pebble snapshot covers the whole database, so while one is open, purging pauses in every compaction (Rule 1 continues). To keep that pause short, repair and streaming use Pebble's file-only snapshots, which leave the list of snapshots that compaction respects as soon as the memtables they depend on have been flushed (section 24). The picker also skips cleanup compactions while any snapshot is open, as Pebble's own cleanup of its tombstones does.

**Example.** Row A was inserted with name "Kim" at 100 and deleted at 200, with a local deletion time of day 1. The grace period is ten days. A compaction on day 5 at a middle level sees both records. The merger combines them, and Rule 1 drops "Kim", leaving only the row deletion. A bottom-level compaction on day 12 sees the row as a SET: the local deletion time is before the cutoff (day 2), nothing is below, nothing older is above, and no snapshot is open. Rule 2 removes the deletion, nothing is left, and the record is dropped. If a client had inserted "Lee" at 300 on day 11, the row would hold "Lee" and the deletion would still be removed, because "Lee" is newer than it. No lock is involved: compaction only ever looks at records that already exist, and a write arriving during the compaction is a new record above it.

## 4. The partition header in the filter

Range deletes and whole-partition deletes live in the partition header, a key that sorts just before the partition's rows (section 18). Rows they hide are separate keys, and a row's value never says that the header hides it. The filter solves this by using the order in which compaction emits keys: a partition's header always comes out before its rows.

**Remembering the header.** When the filter sees a header, it keeps that header's deletions (the partition deletion and the range list) in memory, after applying its own rules to the header. It then checks every following row of the same partition against them, and forgets them when the first key of the next partition arrives. This needs memory for one header at a time.

**Rule 1 for covered rows.** Any cell, element or row marker of a covered row with a timestamp at or below the covering deletion's timestamp is dropped, subject to the snapshot condition in section 3 (every snapshot that can see the row record can also see the header record). If nothing is left, the row record is dropped. Data written after the delete has a newer timestamp and stays. This is safe at any level, because the deletion stays in the header until Rule 2 removes it, and Rule 2 never removes it while older covered data might exist elsewhere.

**Which rows the filter decodes.** For each row, the filter decodes the value only if the row is covered by a deletion in the header it is remembering, or the row's flag bit says it contains a deleted or expiring item (section 5, "The property collector"). Any other row is copied unchanged without being decoded. In a partition with no range or partition deletion, unflagged rows are never decoded; in a partition with "everything after 900" deleted, only the rows after 900 are decoded, plus any flagged ones.

**Rule 2 for header deletions.** A range entry or partition deletion is removed from the header only when, in addition to the conditions of section 3:

- the compaction's key range contains the whole range the deletion covers, so every covered row in the compaction's levels passes through it and is cleaned by Rule 1 in the same pass,
- the compaction is the bottommost data layer for that range, so no covered row exists below it,
- the lookup in section 7 finds no row in that range, in a memtable or a level above, with data at or below the deletion's timestamp,
- Rule 1 was applied to every covered row in the compaction, which always holds when no snapshot is open.

If a compaction includes the header but not the whole range it covers, the deletion stays. Because the fork keeps each partition in one file per level (section 5, "Keeping each partition in one file"), a compaction that brings the header into the bottom level includes the whole partition there, so this case is temporary. If a compaction includes covered rows but not the header, it does not know about the deletion and copies those rows unchanged. They are cleaned in a later compaction that includes both. Reads are unaffected either way, because the read path checks the header.

Decision: the filter does not look up a missing header with a read of its own during compaction. The case arises when a compaction rewrites rows of a partition whose header is not among its inputs, for example rows compacted to the bottom long ago while today's range delete still sits in a header record in an upper level. Those rows are copied unchanged, and are cleaned later, when the header compacts down to their level and one compaction includes both; with partition-aligned files that compaction includes the whole partition (section 5). Looking the header up instead would cost an extra random read for every partition that has rows but no header in the compaction, which is most partitions in most compactions, and would make compaction issue reads into the database it is changing, which is a source of slowdowns and hard-to-test behaviour. The cost of not looking it up is only that hidden rows take disk space a while longer; reads are always correct, because every read checks the header first. The lookup in section 7 also reads during compaction, but only for partitions with something to purge, in bottommost compactions past the purge cutoff, which is far fewer.

**Example.** Chat c1 has rows at 1000, 1002 and 1005, written at time 100. At time 400 the client deletes everything after 1000, with a local deletion time of day 1. A bottom-level compaction on day 12 covers all of c1. The filter sees the header first and remembers "after 1000, open-ended, timestamp 400". It then sees row 1000 and keeps it, and rows 1002 and 1005 and drops both, because all their data is at 100. The range entry is past the cutoff, the whole open-ended range lies inside the compaction, nothing is below, and nothing older is above, so it is removed from the header. If row 1005 had been re-inserted at 500, its new data would survive and only its data at 100 would be dropped.

## 5. A compaction picker for due deletions

The filter only runs when Pebble decides to compact a file. Two things can keep a file with dead data from being rewritten for a long time:

- the file sits at the bottom level and nothing new lands on top of it, so the normal picker, which is driven by level sizes, never chooses it,
- the file is moved to a lower level without being rewritten (a trivial move), so the filter never sees its records. A test confirmed this for the merger (section 27).

Pebble already has a picker for a similar case: its elision-only step finds bottom-level files that hold many deletions and rewrites them on their own, using an annotator that keeps track of the best candidate file per level. There is also a step for files dense with tombstones. The fork adds a third, modelled on the first (locations in handoff/1-fork-point-map.md, section 8).

**What it needs to know.** A table property collector, which sees every key and value as a file is written, records per file when its deletions become purgeable: their local deletion times, or expiry times for TTL data, including header deletions. What exactly it records, and how it is kept cheap, is described under "The property collector" below. Whether a file is due is not decided by the fork but by our code (next paragraph): in the default mode a deletion is due at its local deletion time plus the grace period, and in repair mode when the repair watermark for its token range passes that time.

**Interfaces between the fork and our code.** The fork does not hardcode the grace period or any GC mode. It has two small interfaces (plus `Options.CompactionPropertyCollectorName` to identify our collector's property), none of which mentions repair:

- **"Is this file due?"** (`Options.IsFileDue`, of type `IsFileDueFunc`). The picker hands our code `DueFileInfo` (what the collector recorded for the file, the file's entry count, creation time, level, and key bounds), and our code answers whether the file is worth compacting now. Our code applies the grace period, the repair watermarks or the table's GC mode (section 8), and the rule described under "The property collector" below (the share of purgeable items, the minimum file age, and the rule for header deletions). Because the answer comes from our code, adding repair mode or changing the rules later needs no fork change.
- **"Pick again."** (`DB.MaybeScheduleCompaction()`). A call our code makes to tell Pebble that something may have become due and it should run its compaction picking again. Our code calls it when the timer below fires, and in repair mode after a repair finishes and moves a watermark, because files then become due at once rather than as time passes.

**Why "pick again" is needed: Pebble only re-checks when files change.** Pebble calls its pickers whenever something changes the set of files (after a flush, a compaction or an ingest), and its compaction scheduler checks every 100 ms whether a compaction is waiting. But when the pickers find nothing to do, Pebble records that nothing is waiting, and the 100 ms check skips picking until a file change resets that (verified in Pebble's source; handoff/1-fork-point-map.md, section 9). Whether a file is due depends on the clock and on the watermarks, which change without any file changing. In a busy database a flush or compaction soon triggers picking anyway; in a quiet table nothing would, and a due file could wait indefinitely.

**The timer.** A timer in our code makes the "pick again" call. Two ways were considered:

- wake exactly when the earliest due time across all files passes, which every file's recorded times allow our code to compute,
- check at a fixed interval, as Cassandra does. Cassandra runs a task every minute, starting five minutes after startup, that asks every table's compaction strategy whether there is anything to do, tombstone compactions included (scheduleWithFixedDelay with getBackgroundCompactionTaskSubmitter in CassandraDaemon.java and ColumnFamilyStore.java; verified). It also checks after every flush and after every background compaction finishes (submitBackground, called from complete in CompactionManager.java; verified). ScyllaDB's compaction manager uses a timer of one hour (periodic_compaction_submission_interval, 3600 seconds) for tables that have had no regular compaction in that hour (compaction_manager.cc and compaction_manager.hh; verified).

Decision: a fixed interval, as in Cassandra, once a minute by default and configurable. It is simpler, and with a grace period of days a delay of a minute does not matter. The timer does nothing but make the call: it writes nothing, decides nothing, and needs no recovery after a restart, because the recorded times live in the files.

**What it does.** When a file is due, the picker (`pickDueCompaction`) schedules a compaction of dedicated kind `compactionKindFilter` (`"filter"`, counted in `CompactMetrics.FilterCount`). When the normal picker would trivially move a file that is already due, the fork rewrites it instead.

- **Decision: level rule (`L6 -> L6`, or `l -> l+1` only when nothing overlaps two or more levels down).** For a due file at the bottom level (`L6`), the picker schedules an `L6 -> L6` rewrite. For a due file in an upper level `l < 6` (`l >= baseLevel`), Pebble's `pickedCompactionFromCandidateFile` requires the output level to be the adjacent level `l + 1` (`defaultOutputLevel(l, p.baseLevel)`). The picker therefore picks `l -> l+1` only when no file in any level `>= l+2` overlaps the candidate, and after `pickedCompactionFromCandidateFile` widens the compaction's bounds (`pc.bounds`), it re-checks `pc.bounds` against all levels `>= l+2` as well as in-progress compactions outputting to levels `> l`. This guarantees that every due-deletions compaction is the bottommost data layer for its key range (`c.isBottommostDataLayer() == true`).
- **Decision: being due is not cached.** Because whether a file is due changes with the clock and with per-range repair watermarks without the file changing, the picker does not cache due-ness in a B-tree annotator; instead, it scans every file in `L6` down to `L1` (`>= baseLevel`) under `d.mu` on each `pickAutoNonScore` call, reading the in-memory summary from `TableBackingProperties` and calling `Options.IsFileDue`.

Where it runs: Pebble picks by level size first, and only when no level is over its target does it try a list of extra steps: files dense with tombstones, then elision-only compactions, then our due-deletions filter compaction (`pickDueCompaction`), then virtual sstable rewrites, blob file rewrites and read-triggered compactions. Pebble's comment calls these "low-priority compactions because they don't help us keep up with writes, just reclaim disk space." The new step is added to that list, so the compactions that keep writes flowing always go first.

Cassandra has the same safety net: when an SSTable holds enough droppable tombstones (tombstone_threshold, default 0.2), it compacts that SSTable on its own.

**Example.** A bottom-level file holds a thousand rows, 300 of them deleted with local deletion times on day 1. Its summary says "300 items purgeable from day 1". On day 11, with a grace period of ten days, our code sees that 0.3 of the file is purgeable, above the 0.2 threshold, and the file is older than a day, so the picker rewrites it. The filter removes the 300 deleted rows and copies the other 700. The new file's summary shows no deletions, so it is not picked again. A file with only one deleted row out of a thousand would not be picked for this reason alone; that row is removed whenever a normal compaction next rewrites the file (the rule is under "The property collector" below).

### The property collector

**What a collector is.** Pebble lets an application record its own summary of each sstable while Pebble writes it. This is a normal Pebble feature (block property collectors; verified), not part of the fork. The application registers it in Pebble's options, and a new collector "is created for each sstable built and lives for the lifetime of writing that table". Pebble hands the collector every key and value written, in key order; asks it for a result at the end of each data and index block, which is stored as a block property; and asks it for a result when the file is finished, which is stored in the file's properties. Pebble never interprets those bytes. The feature was built for skipping: a reader can pass a filter that skips blocks or files whose properties cannot match. Pebble's comments also note that with value separation, "block properties must only apply to the key, and will be provided a nil value", which is one more reason value separation stays off (section 6).

This design uses only the table-level part: one summary per file, returned when the file is finished. It is what the picker's "is this file due?" question is answered from.

**Decision: what it records.** Per file:

- a histogram of when the file's deletions become purgeable. Each deleted cell, removed element, row deletion, collection clear, expiring cell or element, and each partition deletion or range entry in a header counts as one item, at its local deletion time (or expiry time for TTL data),
- the earliest due time among the file's header deletions, kept separately and exactly.

It does not record how much data an item removes: a row deletion counts as one item even if it hides fifty cells, and a range entry as one even if it hides thousands of rows. The separate header time exists for that reason (see the rule below).

**Decision: the histogram's size.** At most about 100 buckets, each a point in time with a count. A new time adds to a bucket with exactly that time, or adds a new bucket; when that makes more than the limit, the two buckets nearest in time are merged into one, with the combined count, at a time between them weighted by their counts. Times are rounded to the minute first, which cuts the number of distinct points. The summary therefore stays small whatever the file's time spread, the total count stays exact, and only the position in time of nearby items is blurred. This is Cassandra's design (verified): StreamingTombstoneHistogramBuilder.java merges the "nearest points p1 and p2" into "one weighted point" when the collection grows "larger than maxBinSize", and SSTable.java sets TOMBSTONE_HISTOGRAM_BIN_SIZE to 100, a temporary buffer TOMBSTONE_HISTOGRAM_SPOOL_SIZE of 100000 so that it does not merge constantly while a file is written, and rounds times by STREAMING_HISTOGRAM_ROUND_SECONDS, 60 by default. The limit is tunable. Blurring does not affect correctness: the filter decides each deletion exactly when it runs.

**Decision: the rule our "is this file due?" code applies.**

- At least 0.2 of the file is purgeable. The purgeable count is the sum of the histogram buckets whose time is past the purge cutoff (now minus the table's grace period, or in repair mode the watermark of the table and token range the file covers; a file spanning several ranges uses the earliest of their watermarks).
- The share's denominator is the file's number of entries, which Pebble already records for every file, so it costs nothing. The share therefore means purgeable items per row, slightly different from Cassandra, which divides by an estimated cell count; counting cells would require decoding every row.
- The file is older than a minimum age, by default one day, so that a file whose purges were blocked by the lookup in section 7 is not picked again at once.
- A file whose earliest header-deletion time is past the cutoff is always worth compacting, whatever the share, because one header deletion may hide a whole partition's rows. Neither Cassandra nor ScyllaDB has this extra rule.

The 0.2 and the one day are Cassandra's and ScyllaDB's defaults (tombstone_threshold and tombstone_compaction_interval); both are configurable.

**Example.** A file holds 10,000 entries. Its histogram says 500 items become purgeable on day 3, 1,800 on day 5 and 300 on day 9, and it holds no header deletions. On day 6, with a cutoff of day 5 and the file older than a day, 500 plus 1,800 is 2,300 purgeable items, a share of 0.23, so the file is picked.

**Decision: a flag bit in the row header.** The row header's flags byte (section 15) gets one more bit meaning "this row contains at least one deleted or expiring item": a deleted cell, a removed element, a row deletion, a collection clear, or a cell or element with a TTL. Three places keep it correct:

- the write path sets it when it builds an operand for a DELETE, an element removal, a collection overwrite, or any write with a TTL, and leaves it off for a plain INSERT or UPDATE,
- the merger sets it on its output only if the output still contains such an item,
- the compaction filter clears it when, after purging, nothing deleted or expiring is left.

For each key the collector checks whether it is a partition header (kind byte 0x00 in the key) or a row with the bit set. Headers exist only to hold deletions, so they are always decoded and need no flag. Rows without the bit are skipped after reading one byte; most rows in most tables carry no deletion and no TTL. The filter uses the same bit: a row that is neither flagged nor covered by a deletion in its partition's header is copied without being decoded (section 4).

Example: a file holds 10,000 rows of a users table and 2 partition headers. 9,950 rows were only inserted and updated and are skipped after one byte each; 40 have a deleted column and 10 have a TTL on a cell, and are decoded with the 2 headers. The collector decodes 52 values instead of 10,002.

**Decision: no collection in flushes.** The collector is not created for files written by flushes. Pebble builds the writer options, which include the collectors, for each output file in one place that flushes also go through (verified), so the fork leaves our collector (named by `Options.CompactionPropertyCollectorName`) out there when the compaction is a flush (handoff/1-fork-point-map.md, section 5). The collector interface itself does not change.

Files written by flushes therefore have no record. To keep every file below L0 recorded, the fork never trivially moves a file that lacks the record: it is rewritten through a normal compaction instead, which gives the new file a record. The decision whether a compaction may become a move is made in one place (`tableCompaction.maybeSwitchToMoveOrCopy`; handoff/1-fork-point-map.md, section 7), which already refuses moves in some cases; for example, it does not turn a tombstone-density compaction into a move into the lowest level, because "the goal there is to actually remove the tombstones" (verified). The fork adds this refusal there (and it was verified that `eventualOutputLevel` does not bypass `maybeSwitchToMoveOrCopy`: outputs are written into `outputLevel` and only move down in later compactions that pass through `maybeSwitchToMoveOrCopy`).

The result is an invariant: files in L0 may lack the record, and every file in L1 to L6 has one. Flushes only write L0; files leave L0 only by being rewritten; files this layer ingests are written by our own code with the collector; and virtual files created by excise share their backing file's properties, so they keep a record that describes the whole original file, which is conservative but safe. Purging happens only at the bottom, so nothing is missed. A test checks the invariant directly after random workloads.

Why: it removes all collector work from flushes, which matters most in tables where nearly every write has a TTL, since nearly every row would then be flagged. The cost is the occasional rewrite of an L0 file that could have been moved; because within each table data keys start with the token, a flushed L0 file usually covers a wide key range and overlaps the next level, so such moves are rare anyway. The alternative considered was to collect in flushes too, which needs no extra fork changes and cannot leave a file unrecorded, but adds decoding to flushes.

**Decision: the summary is copied into Pebble's in-memory file metadata.** Pebble keeps a description of every file in memory, but only fixed fields such as entry counts, deletion counts and sizes (verified). A custom property stays in the file's properties block and is not copied into memory (handoff/1-fork-point-map.md, section 6). The picker cannot open files to read it, because it runs while holding Pebble's main lock, so the fork copies only our collector's summary (with Pebble's 1-byte short-ID prefix stripped) into `TableBackingProperties` (`CompactionPropertySummary` / `HasCompactionPropertySummary`, exposed via `BlockPropertySummary()`) before publishing `propsValid`, when a file is created or its properties are loaded.

**ScyllaDB and Cassandra, for comparison.** ScyllaDB's per-file summary holds statistics only, no keys (verified in sstables/sstables.cc and compaction/compaction_strategy.cc). Each SSTable stores a histogram of tombstone drop times and an estimated cell count. ScyllaDB computes the purge cutoff for the SSTable's token range, in repair mode from the repair history, estimates the share of droppable tombstones, and compacts the SSTable on its own when that share is at least tombstone_threshold and the SSTable is older than tombstone_compaction_interval (one day). Cassandra's worthDroppingTombstones (AbstractCompactionStrategy.java) applies the same threshold and minimum age, plus an estimate of how much of the SSTable does not overlap other SSTables. Both run these compactions only when no regular compaction is needed.

### Keeping each partition in one file

Decision: the fork changes where Pebble cuts output files, so that a file never ends in the middle of a partition. In every level from L1 to L6, a partition is then in at most one file, with its header and all its rows together, as in Cassandra. This replaces an earlier plan to widen cleanup compactions to all files of a partition.

**Why it is needed.** A range entry can only be removed in a compaction that covers its whole range (section 4). Stock Pebble cuts files by size, not by partition, so a large partition can be spread over several files of the same level. Take chat c1 with rows from 1000 to 9000 in three bottom-level files: A holds the header and rows 1000 to 3000, B holds 3000 to 6000, C holds 6000 to 9000. The entry "after 1000" covers all three.

- A compaction of A alone covers only A. The hidden rows in A are dropped, but the entry stays.
- A compaction of B or C alone does not include the header, so the filter does not know their rows are hidden and copies them unchanged.
- A compaction from L5 into L6 takes only the L6 files overlapping the incoming file. To take A, B and C together, that file's key range would have to span all of c1, which no file can do once c1 is larger than a file.

So for a partition larger than one file, the entry would never be removed on its own. Reads would stay correct, because the header keeps hiding the rows, but the hidden rows would keep taking disk space and every read of the partition would still step past them.

**How Cassandra and ScyllaDB avoid this.** Cassandra's compaction writes whole partitions and switches to a new output file only between them: in CompactionAwareWriter.java, append receives one whole partition and calls maybeSwitchWriter with its partition key before writing it (verified). Its flushes split only at disk or shard boundaries, which are token positions: flushRunnables in db/memtable/Flushing.java, and ShardedMultiWriter, which "splits the output sstable at the partition boundaries of the compaction shards" (verified). So in Cassandra a range tombstone always sits in the same file as the part of the partition next to it, and a single-SSTable compaction at the bottom level sees the whole partition. ScyllaDB keeps separate memtables per compaction group, so a flush only contains whole partitions of that group (replica/compaction_group.hh, verified).

**Where Pebble cuts files today** (Pebble master a995e89, verified; locations in handoff/1-fork-point-map.md, section 4). Every flush and compaction writes its output through one function, which asks Pebble's output splitter before each key whether to start a new file. There is no other splitting path; master does not split one compaction into parallel parts by key range. A file ends for one of three reasons:

1. **Size.** The splitter splits "when the estimated file size is 0.5x-2x the target file size", preferring the boundaries of files two levels down (grandparents), and always at twice the target. Before a size-based split it checks whether the next key equals the previous key, and if it does, it does not split there but tries again at the next key. This is how Pebble guarantees that "we never split user keys between files".
2. **A grandparent boundary.** When the size is close to the target and the next key is where a grandparent file starts, it splits there, without that check.
3. **A hard limit.** The file must end before a given key. The limits are the start of a grandparent file once the output would overlap more than ten times the target size of the next level, the L0 split keys during flushes, and span policies, which this design does not use.

Flushes go through the same splitter. Pebble deliberately cuts a flush into several L0 files: FlushSplitBytes, by default twice the L0 target file size, sets how much each piece holds, so that "splitting sstables during flush allows increased compaction flexibility and concurrency when those tables are compacted to lower levels" (Pebble's comment on that option). The L0 split keys are taken from the boundaries of existing L0 files, including files' last keys, so one can fall in the middle of a partition.

**What the fork changes.**

- **A partition-prefix function.** A new option (`Options.PartitionPrefix`, of type `PartitionPrefixFunc`) holds a function, supplied by us, that tells Pebble where a key's partition prefix ends. For data keys this is the 0x01 byte, the table ID, the 8-byte token and the encoded partition key, which is prefix-free and ends with its terminator, so the length can be found by reading the key. Keys with any other prefix byte (hints, Paxos state, schemas, node-local metadata) are each treated as their own unit and split as in stock Pebble.
- **Size-based splits.** The splitter's check "the next key equals the previous key" becomes "the next key is in the same partition as the previous key". The splitter then waits until the next partition starts, exactly as it already waits for the next distinct key.
- **Grandparent boundaries and hard limits.** Before splitting at a grandparent boundary or a limit key, the fork moves that key back to the start of its partition. If that start is at or before the first key of the file being written, so the whole file so far is one partition, the boundary is ignored and the file grows. This also covers the L0 split keys used by flushes, because they reach the splitter as hard limits.
- **Off unless configured.** With no partition-prefix function set, Pebble behaves exactly as stock, so Pebble's own tests (the splitter, compaction and metamorphic tests) keep passing unchanged. New tests cover our function.

**Why the rule holds in every level.** Compaction outputs start and end at partition boundaries, so the files of each level do too, and they are the grandparents of later compactions. Trivial moves move whole files and keep them aligned. That includes L0 files moved into the next level without rewriting, which is why flush split keys must be aligned too. Ingested files are written by this layer and are cut at partitions. Excise and DeleteRange can cut files at their bounds, but this design uses them only for whole tables or for whole token ranges of a table, and because the table ID and the token are the first parts of every data key, those bounds are partition boundaries. L0 files may overlap each other, so one partition in several L0 files from different flushes is normal, as in Cassandra.

**What it gives the cleanup design.**

- When a header's file is compacted into L6, Pebble includes every L6 file overlapping it, which is the single L6 file of that partition. So the whole partition is inside the compaction, and the first of the extra conditions for header deletions in section 4 (the whole range inside the compaction) holds without any special effort.
- The picker compacts single due files (with the overlapping files below them when the file is not yet at the bottom), as Cassandra and ScyllaDB do. It does not need to know which header in a file is due or gather a partition's files.

**Cost.** A partition is never cut, so a large partition becomes a large file. The default target file size is 2 MB at L0, doubling with each level, and stock Pebble always cuts at twice the target; with this change a 1 GB partition is a 1 GB file. Such a file hurts in three ways:

- every compaction that touches it rewrites all of it, even when only a few rows changed and nothing is deleted,
- a compaction needs room for the new copy before the old one is deleted, so a 10 GB file needs another 10 GB of free disk while it is compacted,
- a file being compacted cannot be picked by any other compaction, so one long compaction of a huge file holds up that part of the key space.

Pebble's cap on how far a compaction may grow (25 times the target file size) only limits optional growth: overlapping files in the output level are always included, so such a compaction happens anyway. For partitions well below the target file size, files simply end a little earlier or later than they otherwise would.

Cassandra has exactly this cost. Its compaction never splits a partition, whatever its size (CompactionAwareWriter.java, verified), and no size exception was found. Its answer is to warn about large partitions and to tell users to keep partitions small through data modelling, not to protect the storage.

**Decision: no size cap, for now.** Every partition stays in one file per level, whatever its size, as in Cassandra. Keeping partitions small is the data model's job. A cap can be added later if very wide partitions turn out to be common; the options considered are below.

**Not verified yet.** All of the above comes from reading Pebble's source. A prototype on v2.1.7 should confirm that Pebble's tests still pass with the function unset, that with it set no partition ever spans two files in L1 to L6, and how much write amplification changes with realistic partition sizes.

### Wide partitions: options considered

These were investigated and discussed, and are recorded here so the decision can be revisited.

**A size cap with manual cleanup.** Above a hard cap, for example a few times the target file size of the level being written, the fork would let Pebble split the partition as it normally would. One badly modelled partition could then not produce a huge file. The price: the header is a single Pebble key, and a key can be in only one file of a level, so it would go into the first piece only, since it sorts before all the partition's rows. The other pieces would hold only rows, and a compaction of one of them would not know which rows are hidden. Range deletions in those partitions would be cleaned only when a manual compaction covers all the pieces, the fallback Cassandra and ScyllaDB rely on in their own awkward cases (ScyllaDB's knowledge-base article on flushing old tombstones uses nodetool compact). Reads would stay correct, because a read of the partition always reads the header first. A metric reporting oversized partitions would show operators when the data model is the problem.

**A size cap with widening for oversized partitions only.** Instead of manual cleanup, the picker would widen the compaction for a due header deletion in an oversized partition. With aligned splitting this is much simpler than widening in general:

- a file can only end in the middle of a partition if that partition is above the cap, which the picker can see from file metadata alone: a file's last key and the next file's first key in the same level share a partition prefix,
- the collector needs at most one extra bit per file: whether the due header deletion belongs to the file's last partition, the one that runs past the file's end,
- gathering is local: the picker takes the due file and adds its neighbours in the same level, one by one, as long as the boundary between them falls inside the same partition.

It would still rewrite the whole oversized partition at once, and still has to wait and retry when a neighbouring file is already being compacted. It is a second mechanism next to aligned splitting.

**Copying the deletions into every piece, as ScyllaDB can.** ScyllaDB has a mechanism for splitting a large partition across files (verified in compaction/compaction.cc, master 5aea8fb). Its compaction writer, compacted_fragments_writer, has split_large_partition(). It is used when the compaction allows it (can_split_large_partition, described in compaction_descriptor.hh as "Can split large partitions at clustering boundary") and the output reaches the size limit (max_sstable_bytes) in the middle of a partition. When it splits, it:

1. closes any open range tombstone in the current file at the split point,
2. ends the partition in that file and starts the same partition again in a new file,
3. copies the partition tombstone into the new file ("Replicate partition tombstone to every fragment, allowing the SSTable run reader to open a single fragment during the read"),
4. reopens the range tombstone in the new file just after the split point, which the comment calls "very important for not losing information on the range tombstone".

Every piece therefore carries the deletions that cover its own rows. can_split_large_partition defaults to false, and in the parts of the source searched (compaction/, replica/, sstables/ and the boost tests) the only place that sets it to true is a test in test/boost/sstable_compaction_test.cc. Other parts of the repository were not searched, so another caller may exist; but by default ScyllaDB, like Cassandra, does not split partitions.

Doing the same here would need more than in ScyllaDB, because the header is a separate key:

- a copy of the header needs a key of its own that sorts just before the first row of the next piece. One way is the previous piece's last row key with an extra byte appended: because the clustering encoding is prefix-free, that key sorts after that row and before the next one,
- the splitter would have to write this key, holding the partition deletion and the range entries that cover the following rows, at the start of each new file. Pebble's compaction cannot add keys that nobody wrote, so this is a larger fork change,
- the read path, the merger and the filter would have to understand these copies, and they would have to be removed when the pieces are joined again.

**Widening for every partition.** This was the first plan: the collector records the key of the due header, and the picker turns it into the partition's key range and gathers every file overlapping it in every level down to L6, then compacts them together. It changes nothing in Pebble's file layout, but it needs header keys in the property, a gather across levels, and retries when files are busy. It was replaced by aligned splitting, which makes the gathering unnecessary for every partition below any cap. The widened compaction and aligned splitting cost the same when a large partition's range delete is cleaned, since both rewrite the whole partition; the difference is that aligned splitting also rewrites a large partition whole in ordinary compactions.

**Pebble range keys.** Range deletes could be stored as Pebble range keys instead of in the header. Pebble cuts range keys at file boundaries itself: its range-keys design document (docs/RFCS/20211018_range_keys.md) says they are "physically fragmented as an artifact of the log-structured merge tree structure and internal sstable boundaries", which "allows a range key to be split across many sstables, while preserving locality between range keys and point keys". Each file would then hold the piece of the delete that covers its own rows, and no file-cutting rule would be needed. Not chosen, for the reasons in section 18: the timestamp would have to be the suffix, the default comparer's handling of suffixed range keys next to suffix-less row keys is not verified, range keys take a separate path in compaction so the filter would need a second hook, and the read path would change.

If very wide partitions become common, the suggested order is the cap with manual cleanup first, then ScyllaDB-style copies only if manual cleanup is not acceptable.

## 6. Value separation

Decision: value separation is turned off for the data tables.

### What it is

Value separation stores large values in separate blob files and leaves only a small reference next to the key in the sstable. Compaction then mostly rewrites keys and references, while the values stay where they are, which lowers write amplification. The costs are an extra lookup into the blob file on reads, and extra disk space, because blob files keep values that are no longer referenced until Pebble rewrites them. RocksDB's BlobDB works the same way.

On Pebble master it is on by default whenever columnar blocks are enabled, which this design needs: Pebble's defaults fill in a policy with a minimum size of 256 bytes, so any value of 256 bytes or more is a candidate. The option is marked "Experimental". In v2.1.7 it is off unless requested.

### Why it is turned off

- **It hardly applies to this design.** Pebble separates only SET values. Merge values are never separated (v2.1.7: "Merge and deletesized keys are never separated"; master: "Only SET and SETWITHDEL can be separated"). Every write here is a Merge. A row's value becomes a SET only when a compaction merges it at the bottom, and most compactions carry existing blob references forward instead of separating new values. So most rows are never separated.
- **What little it does costs reads**, as the measurements below show.
- **It would complicate the filter.** For a separated value the compaction iterator sees only a reference, so the filter, and the property collector in section 5, would have to fetch the value from the blob file before they could read its timestamps.

Turning it on would pay off only if the design wrote whole rows with Set instead of Merge, which would give up the merge-based, lock-free write path. That trade is not worth it.

### Measurements

Both settings were measured on Pebble v2.1.7 with columnar blocks, a 64 MB block cache, 32 MB memtables, and keys shaped like the data keys in section 13 (about 30 bytes). One run per setting, on a 2-CPU cloud machine with the data in the operating system's page cache. Write amplification is the bytes written by flushes and compactions (sstables and blob files) divided by the bytes the application wrote. The "scan reading values" column is a stand-in for the work a compaction filter would do, since the filter does not exist yet. A simple merger stood in for the row merger: each operand overwrites the start of the row.

**Test 1: the workload of this design.** Every write is a Merge. Each row is inserted once with a full value, then receives three times as many small updates (40 bytes) as there are rows, to random rows. There were a million rows for 128 and 512 bytes, and 250,000 rows for 2048 bytes.

| Row size | Separation | Write time | Write amplification | Values in blob files | Disk | Get p50 / p99 | Scan, keys only | Scan reading values |
|---|---|---|---|---|---|---|---|---|
| 128 B | off | 22.2 s | 3.57 | 0 MB | 239 MB | 22.5 / 89 µs | 877 ms | 916 ms |
| 128 B | on | 22.6 s | 3.49 | 0 MB | 240 MB | 19.5 / 84 µs | 1117 ms | 1040 ms |
| 512 B | off | 28.8 s | 3.41 | 0 MB | 467 MB | 27.6 / 112 µs | 2372 ms | 2072 ms |
| 512 B | on | 30.4 s | 3.35 | 32 MB | 479 MB | 31.5 / 126 µs | 2545 ms | 2646 ms |
| 2048 B | off | 15.5 s | 3.08 | 0 MB | 421 MB | 22.3 / 121 µs | 1518 ms | 1418 ms |
| 2048 B | on | 15.8 s | 3.13 | 35 MB | 427 MB | 29.3 / 140 µs | 1635 ms | 1686 ms |

Only about 32 to 35 MB of roughly 500 MB of values reached blob files. Write time and write amplification did not change by more than the noise. Point reads and scans were somewhat slower with separation on. At 128 bytes nothing was separated, because no value reached the 256-byte minimum, so that pair only shows the noise between runs.

**Test 2: whole rows written with Set.** For comparison, the workload value separation is built for: 250,000 rows of 2048 bytes, each written once with Set, then 250,000 more Sets of whole rows to random rows.

| Separation | Write time | Write amplification | Values in blob files | Disk | Get p50 / p99 | Scan, keys only | Scan reading values |
|---|---|---|---|---|---|---|---|
| off | 17.6 s | 2.95 | 0 MB | 483 MB | 15.5 / 64 µs | 718 ms | 728 ms |
| on | 12.9 s | 2.72 | 506 MB live | 643 MB | 25.9 / 109 µs | 94 ms | 852 ms |

Here separation works as designed: writes were 27% faster, write amplification was lower, and a scan that reads only keys was about eight times faster. In exchange, point reads were about 65% slower, reading values in a scan was 17% slower, and disk use was a third higher.

Limits: single runs, so small differences are noise. The datasets of about half a gigabyte give a shallow tree, which understates the write-amplification saving in test 2. The tests ran on v2.1.7 rather than master, though master has the same SET-only rule.

If value separation is ever wanted, the filter and the property collector have to fetch separated values, and the measurements should be repeated with the real row merger and the filter.

## 7. Older records outside the compaction

### The problem

Pebble's check that nothing older exists below a compaction is not enough on its own. A record for the same key can sit outside the compaction, in a memtable or in a level above, and still carry an older timestamp, because timestamps come from coordinators and clients, not from Pebble's write order. If the filter purged the deletion at the bottom, that older record would later be compacted down, meet nothing that hides it, and reappear.

For a single row key, Pebble orders records by arrival, so an older-timestamp record above a tombstone means it arrived later than the tombstone. This happens when:

- a hint delivers an old write late. A hint is a write that a coordinator stored for a replica that was down, and replays later with its original timestamp. Cassandra calls this hinted handoff: it is on by default (hinted_handoff_enabled), hints are created for at most max_hint_window (3 hours) of downtime, and they are kept in local files under hints_directory (verified in conf/cassandra.yaml). In this design hints live under prefix 0x03 (section 24),
- repair streams old data into this node. Ingested files get a newer sequence number than everything already present, so they land above existing data even when their rows carry old timestamps,
- coordinators' clocks differ, or clients supply their own timestamps.

For range and partition deletions the problem is more common, because the header and the rows are different keys: rows written before the delete can easily still sit in higher levels when the header reaches the bottom.

### What the check must guarantee

Before the filter purges a deletion D, no record outside the compaction may hold the keys D covers with a timestamp at or below D's timestamp. For a row, cell or element tombstone, or an expired cell, that is one row key. For a range entry or partition deletion in a header, it is the rows in the deleted range.

- "Outside the compaction" means every memtable, including those waiting to be flushed, and every file in the levels above, but not the compaction's own input files. Their records pass through the filter, which drops the old ones by Rule 1. Purging only happens in a bottommost compaction, so nothing is below.
- Newer records do not block: a row re-inserted after the delete has a larger timestamp and stays correct after the purge.
- Writes that arrive after the check are out of scope, as in Cassandra: the deletion is past its purge cutoff, so an old write arriving now is the late write that the grace period, repair and the hint rules exist to prevent (see "Repair mode" below).

### How Cassandra checks it

Cassandra's check is per partition key and includes memtables (verified in getPurgeEvaluator in CompactionController.java). For each SSTable outside the compaction that overlaps the key, it asks the SSTable's Bloom filter whether it may contain the partition (mayContainAssumingKeyIsInRange); only those that may contain it contribute their minimum timestamp. For every memtable that holds the partition (memtable.rowIterator(key) is not null), it adds the memtable's minimum timestamp. A tombstone may be purged if its timestamp is below the smallest of these; if nothing outside the compaction holds the key, it may always be purged.

These minimums cost Cassandra almost nothing, because its format needs them anyway. Every memtable tracks the smallest timestamp written to it (minTimestamp in AbstractMemtable.java, updated on every write in SkipListMemtable.java and TrieMemtable.java, together with the minimum local deletion time). Every SSTable records its minimum and maximum timestamps while it is written, flushes included (updateTimestamp in MetadataCollector.java), and timestamps in the SSTable format are stored as deltas from a per-file minimum. Every SSTable also has a Bloom filter over its partition keys.

### Decision: look up the actual records

The filter checks each purge candidate against the records that actually exist outside the compaction.

**Finding candidates.** Candidates are not searched for across the levels. A cleanup compaction reads its input files in key order, and the filter decodes every partition header and every row whose flag bit says it holds a deleted or expiring item (section 4), which are exactly the records that can hold a deletion. A purge candidate is a deletion in one of those records that passes the other Rule 2 conditions (past the cutoff, bottommost compaction, no snapshot). Deletions still in upper levels or memtables are never candidates; they become candidates when they reach a bottommost compaction.

**The view.** Every Pebble read starts from a read state: the current list of files and the current memtables, obtained cheaply (verified; handoff/1-fork-point-map.md, section 10). Through `CompactionFilterInfo.NewExternalIter`, the fork builds an internal iterator (`base.InternalIterator`) over such a read state that leaves out the compaction's own input files, using standard point iterators (not compaction iterators, so SSTable Bloom filters including L6 are consulted on `SeekPrefixGE`). Leaving the compaction's input files out is essential: otherwise the check would find the old records being compacted and never purge.

**The lookup.**

- For a row, cell or element tombstone, or an expired cell, the filter seeks the view to that row key with a prefix seek (on the whole key, as Pebble's own Get does).
- For a range entry or partition deletion, it seeks to the start of the deleted range and scans the partition's rows in it.
- For each record found, it decodes the smallest timestamp with the normal row decoder. The deletion is purged only if every record found is newer than it, or nothing is found.

**Ordering and freshness.** Compaction emits keys in order, so one forward-moving iterator over the view serves every candidate in a compaction. It is re-created regularly, for example every few seconds or every few thousand candidates, because Pebble keeps a memtable in memory for as long as any iterator uses it, and one iterator held for a long compaction would hold old memtables in memory. Each re-created view is a consistent picture: data moving from a memtable into an L0 file is found in one or the other.

**Bloom filters.** Pebble's Bloom filters keep the exact-key lookups cheap. Pebble has them (at 10 bits per key the false-positive rate is 1.14%), but they are off by default: L0 has no filter and the other levels inherit that. With the default comparer the filter covers the whole key, which is exactly the question a row-tombstone check asks, and the sstable iterator consults it on prefix seeks and skips the file on a definite no. All of this is verified in Pebble's source (handoff/1-fork-point-map.md, sections 10 and 11). Decision: turn Bloom filters on for L1 to L6, for example at 10 bits per key, and leave L0 without them, which is Pebble's default.

Why L0 is left out for now: L0 files are written by flushes, and building a filter means hashing every key of the file while it is written and storing about 10 bits per key, which adds work to every flush; L0 files are also short-lived, because Pebble compacts L0 more eagerly than any other level. What a filter would save there: L0 files can overlap and often cover wide key ranges, so an exact-key lookup, whether the purge check's or an ordinary point read, usually has to check every L0 sublevel, reading each file's index block. Those index blocks belong to recent files and are usually in the block cache, which makes the checks fairly cheap. Because the other levels inherit L0's setting unless they set their own, L1 to L6 must each be set explicitly (for example with ApplyTableFilterPolicy) so that L0 stays without a filter. Whether L0 should get filters is decided later by measurement: how many L0 files purge lookups and point reads actually check. It is a configuration change and needs no redesign.

Pebble's whole-key filters cannot answer "does this file hold any row of partition c1 in this range?", so header lookups do not use them. They do not need to: with partition-aligned files (section 5) a partition is in at most one file per level, so the view finds that file from file metadata and reads one index block in it. That is about as cheap as a Bloom check that says "maybe", and header deletions are much rarer than ordinary tombstones. Bloom filters on the partition prefix would need a custom Comparer.Split, which changes how Pebble treats keys elsewhere, so that route is not taken.

**No re-picking loop.** If the check blocks a purge, the deletion stays in the rewritten file, and the picker must not choose that file again at once. A file is picked for cleanup only once it is older than a minimum age, as Cassandra and ScyllaDB do with tombstone_compaction_interval (one day by default). The newer upper data then has time to come down first.

**A cheaper first version.** The decoding in the last lookup step can start as a simple existence check: block the purge if any record for the key exists outside the compaction. It is still safe. The cost is that rows updated constantly keep a small row-deletion field longer, because there is always a fresh record above.

**Example.** Row X was deleted at timestamp 200 and is past its cutoff. Outside the compaction, an L5 file U1 holds other partitions with rows written at 50; the memtable holds a late hint for X at 100 and an unrelated hint for row Y at 20; an L3 file U2 holds a re-insert of X at 300. The seek for X finds the hint at 100 and the re-insert at 300. 100 is not newer than 200, so the tombstone is kept. U1 and Y are never looked at. Without the hint for X, only the re-insert at 300 would be found and the tombstone would be purged.

**Costs.** One seek, or a short scan, into the upper levels per partition with a purge candidate, only in bottommost compactions past the cutoff. These compactions are low priority (section 5), so they give way under heavy writes. This is a read during compaction, which section 4 avoids for header lookups; the difference is scale, since that lookup would happen for every partition with hidden rows, and this one only for partitions with something to purge. Nothing extra is recorded: no timestamp property per file, no tracking per memtable, and no collector work in flushes for this check. The fork has to build an iterator over a read state that excludes specific files. Nothing here is measured yet; a prototype should measure how many candidates a cleanup compaction has and how long their lookups take.

### Repair mode

The check is the same in repair mode: it compares timestamps of records, and the mode only changes the time condition (section 8). It matters more there. Repair streams old-timestamp data into this node, and in repair mode purging becomes possible right after a repair finishes, which is exactly when that streamed data is still in the upper levels. Looking up the actual records means only keys that really have old data above are blocked, rather than everything overlapping a freshly ingested repair file.

Late arrivals after the check need their own rules in repair mode, because there is no fixed grace period that is longer than the hint window:

- Hints created before the watermark carry writes that repair has already synchronised, and the receiving replica drops them (the hint rule, section 8).
- Writes that arrive after the watermark but carry an older timestamp than the tombstone, through clock skew or client-supplied timestamps, can still bring data back after a purge. The check only sees what is present when it runs. How ScyllaDB's repair mode handles this has not been verified.

### Alternatives considered

**A minimum timestamp per file and per memtable.** A property collector records the smallest cell timestamp in every file, flushes included, and the fork tracks a minimum per memtable, which Pebble does not do today. At compaction start the fork takes the minimum over every upper file overlapping the compaction and every memtable, and purges only deletions older than it. It reads nothing during compaction, but it blocks far too much: old data anywhere in an overlapping upper file blocks every purge in that range, and one old hint for any key in the memtable blocks every purge on the node until it has been compacted down. In repair mode a freshly ingested repair file would block almost everything in its range. It also adds work to every flush, because Pebble treats values as opaque bytes, so our collector would have to decode them.

**The same, per partition, filtered by Bloom filters (Cassandra's approach).** Only upper files whose Bloom filter says they may contain the partition, and memtables that hold it, contribute their minimum. Better, but a file or memtable that does contain the partition still contributes its overall minimum, not the partition's, so unrelated old data in the same file or memtable can still block. It needs everything from the first alternative plus Bloom filters on the partition prefix, either through a custom Comparer.Split (not verified, and it changes how Pebble treats keys elsewhere) or through our own filter stored in each file's properties.

## 8. Repair mode

ScyllaDB offers a "repair" tombstone GC mode: a tombstone may be purged only once a repair that started after it was written has finished for its range. Cassandra has something close: with only_purge_repaired_tombstones, a tombstone is purged only if it is in repaired data and also past the grace period. The fork can support the same.

**The watermark.** For each table and each token range this node holds, the node records the start time of the last full repair of that table's range that finished successfully. That time is the repair watermark of that table and range. Repair works table by table, as in Cassandra and ScyllaDB, so the watermark is kept per table. The watermark exists only for repair mode; the default mode purges by now minus the grace period and never needs one.

- What it promises: everything this node had for that range before the watermark has been compared with every other replica and copied where it was missing. So a deletion this node applied before the watermark is on every replica, and may be purged.
- Why the start time and not the end time: a repair takes a while, and writes that arrive while it runs may or may not have been included in the comparison. Only what existed when the repair began is certain to have been synchronised, so the watermark is set to the start time, once the repair has finished.
- It only moves forward. Each successful full repair of a range sets it to that repair's start time. A repair that fails does not move it, so purging waits for the next successful one.
- It is per table, per token range and per node, because tables and ranges are repaired at different times.
- It is stored as node-local metadata (prefix 0x05) and survives restarts.

ScyllaDB keeps the same information as a repair time per table and token range: when a repair finishes, repair/row_level.cc calls update_repair_time for that table and range on every shard and saves an entry to the system table repair_history, and its repair mode takes the purge cutoff from it (get_gc_before_for_key in tombstone_gc.hh; verified). "Watermark" is this document's name for it. The idea is the same as an event-time watermark in stream processing such as Flink: a progress marker saying that everything before it is complete, which only moves forward.

**Example.** Range R is repaired successfully on Monday, starting at 10:00, so R's watermark becomes Monday 10:00. A row deleted on Sunday at 18:00 can now be purged; a row deleted on Monday at 11:00 must wait for the next successful repair of R. If Tuesday's repair fails, the watermark stays at Monday 10:00; if Wednesday's repair, starting at 09:00, succeeds, it moves to Wednesday 09:00.

**In the filter.** At the start of each compaction the filter takes a copy of the watermarks, so a repair that finishes during the compaction does not change the rules halfway through. The purge cutoff in Rule 2 becomes the watermark of the key's table and token range. Because the table ID and the token are the first parts of every data key (section 13), the filter finds the table, and so its GC mode and grace period, and the token range from the key directly.

**Modes.** The mode is chosen per table:

- default: purge after the grace period, as in Cassandra,
- repair: purge once the watermark has passed, with no fixed waiting time,
- combined: purge only when both hold, like Cassandra's only_purge_repaired_tombstones,
- immediate: purge at expiry, for tables where every write has the same TTL and nothing is deleted otherwise (ScyllaDB's "immediate" mode),
- disabled: never purge.

Choosing the mode per table requires the filter to know which table a key belongs to. Every data key carries a table ID (section 13), so the filter reads it from the key.

**The hint rule.** In repair mode, a hint created before the watermark of its table and token range is dropped, not delivered. Such a hint carries a write that repair has already copied to the replica, so delivering it again is unnecessary at best, and harmful if the write is older than a tombstone that has meanwhile been purged.

Example: on day 1 at timestamp 100 a client writes "row X = Kim"; replica R is down, so the coordinator stores a hint for R. At timestamp 200 the client deletes row X, and R, now back, receives the delete. On day 3 a full repair of X's range finishes, every replica agrees that X is deleted, and the watermark becomes day 3. On day 4 R's compaction purges X's tombstone, which repair mode allows. On day 5 the coordinator delivers the old hint "X = Kim at 100". Without the rule, nothing on R hides it any more, Kim comes back, and the next repair would copy it to the other replicas. With the rule, the hint was created on day 1, before the watermark, so it is dropped.

The rule is needed only in repair mode. In the default mode a tombstone is not purged until the grace period (ten days by default) has passed, and the hint window is far shorter (three hours in Cassandra), so every hint has been delivered or given up long before the purge.

Decision: the rule runs at the receiving replica. When a hint arrives, the replica compares the hint's creation time with its own watermark for the hint's table and token range, and discards the hint if it is older. A coordinator that is not itself a replica of that range does not know the range's watermark, so it cannot apply the rule reliably; the replica always knows its own watermarks. Two things are needed from the start, so that the rule can be added together with repair mode later without changing formats:

- a delivered hint carries its creation time. Hints are already keyed by target node and time (section 24), so the time exists; it has to be sent with the hint on delivery,
- the receiver can tell that a write is a hint and not a normal write, through the delivery message. Cassandra delivers hints with their own message type, as far as is known here; not verified in its source.

**Writes the rules cannot catch.** A write that arrives after the watermark but carries an older timestamp than a tombstone, through clock skew between coordinators or client-supplied timestamps, can still bring data back after a purge. The lookup in section 7 only sees what is present when it runs. How ScyllaDB's repair mode handles this has not been verified.

**Bursts of due files.** In repair mode files do not become due gradually: many become due at once, when a repair of their range finishes and the watermark moves. After moving a watermark, our code makes the "pick again" call (section 5), since no file has changed and Pebble would not otherwise re-check. The picker should rate-limit cleanup compactions after a watermark change. The minimum-age rule from section 7 still applies, so a file whose purge was blocked is not picked again straight away.

**Example.** A row is deleted on day 1. A full repair of its range starts on day 3 and finishes on day 4, so the watermark becomes day 3. A bottom-level compaction on day 5 purges the row deletion, six days earlier than the default mode would. If that repair had failed, the watermark would have stayed earlier and the deletion would stay until a later repair succeeds.

## 9. Keeping the fork healthy

- **Fork from master, pinned at commit a995e89** (section 10), and move to a newer master commit only deliberately, with a full test run each time. Master needs Go 1.25.3 or newer. The earlier prototype tests ran on v2.1.7 with Go 1.24.
- **Run Pebble's own tests** on every rebase, in particular the compaction iterator's data-driven tests (internal/compact/testdata) and the metamorphic tests, which run random operations against many configurations and compare results. Add filter cases to both: with a filter that does nothing, every existing result must stay the same.
- **Keep the patch small.** Only the compaction-filter option (`Options.CompactionFilter`, `CompactionFilterInfo` with `NewExternalIter`, `CompactionFilter`, `CompactionFilterRecordInfo`, `CompactionFilterDecision`), the new fields in the compaction iterator's configuration (`IterConfig.Filter`), the hook in the compaction iterator (`applyFilter`), the due-deletions picker step (`pickDueCompaction`, `compactionKindFilter`, `CompactMetrics.FilterCount`) and its two interfaces (`Options.IsFileDue` / `DueFileInfo` and `DB.MaybeScheduleCompaction`), leaving our collector (`Options.CompactionPropertyCollectorName`) out of flushes (`flushBlockPropertyCollectors`), refusing trivial moves of files without our summary or already due (`maybeSwitchToMoveOrCopy`), copying only our collector's summary into in-memory file metadata before publishing `propsValid` (`TableBacking.PopulateProperties`, `TableBackingProperties.BlockPropertySummary`), and the output splitter's partition rule change (`Options.PartitionPrefix`). The collector itself is our own code, registered through Pebble's existing option. The partition rule is off unless a partition-prefix function is set, so stock behaviour and Pebble's existing test results are unchanged without it. Everything Cassandra-specific stays in our own code behind the filter interface.
- **Write our own tests** for the rules above: purging only at the bottom, an older record in a memtable or an upper level blocking a purge (and a newer one not blocking it), the compaction's own inputs never blocking it, snapshots blocking a purge in every stripe while Rule 1 inside a record continues, a covered row not cleaned when a snapshot sits between it and the header, a header deletion kept when its range is only partly inside a compaction, a due file being rewritten instead of moved, and no partition ever spanning two files in L1 to L6, including after flushes and trivial moves.

## 10. What changes in the rest of the design

Changes to the key and value formats:

- The row header no longer has a purge cutoff field, because there is no PurgeOp.
- The row header's flags byte gains the "contains a deleted or expiring item" bit (section 5, "The property collector").
- Every data key carries a table ID after the 0x01 prefix (section 13). This is not caused by the fork; it was missing from the design.

Behaviour that changes:

- **The GC index, the scheduler goroutine and PurgeOp are not needed.** Writes no longer add a GC index entry to their batch (section 16). Cleanup is described in section 20, and the fallback in the appendix.
- **The merger only combines.** It still merges records by timestamp and drops data hidden by a deletion it can see, but it no longer purges tombstones and no longer needs to tell Pebble a row has disappeared. Purging is the filter's job (section 22).
- **Range-delete cleanup moves to the filter.** Covered rows are no longer converted into per-row deletions by a background job (section 18).
- **Files are cut only between partitions.** Each partition is in at most one file per level from L1 to L6 (section 5). Large partitions therefore make large files (section 26).
- **The grace period and GC mode are applied only by our own cleanup code**: per deletion by the filter, and per file by the answer to the picker's "is this file due?" question, both using the same purge cutoff (sections 5 and 19). Before, they were spread over the GC index's due times and the merger's cutoff.

### Open decisions

- **A time limit for repair and streaming operations** (section 24): whether to close stuck operations after a set time, as Cassandra does, or rely on the metrics and an operator, as ScyllaDB appears to.

The decisions below were open for a while and are recorded with their outcome.

### Settled after discussion

- **The table ID is 4 bytes, big-endian** (section 13).
- **The fork is taken from Pebble master, pinned at commit a995e89** (21 October 2026, still the latest master commit on 31 October 2026), not from a release. handoff/1-fork-point-map.md maps every fork point onto this commit. Master has no stability promise between commits, so the fork moves to a newer commit only deliberately, with a full test run (section 9). Master's module path is github.com/cockroachdb/pebble, without a /vN suffix: in Pebble's history the /v2 suffix was added only on the release branch pebble-release-2.1 (commit 451c7738, 27 August 2025), never on master. Master needs Go 1.25.3, turns value separation on by default (so it must be switched off explicitly, section 25), and takes columnar blocks from the format major version alone. An earlier plan was to fork from the newest release tag (v2.1.7 at the time); it was replaced at the owner's choice.
- **A table ID in every data key.** Without one, two tables could not share one Pebble database: their keys would collide (section 13). The alternative considered was one Pebble database per table. Chosen: a table ID right after the 0x01 prefix, before the token.
- **Range deletes are supported.** Clustering-range deletes and whole-partition deletes are both stored in the partition header (section 18). The alternative considered was to support only whole-partition deletes, as Rocksandra does, and reject range deletes explicitly. That would have simplified the header and section 4, but it is not taken.
- **The fallback is kept.** The appendix keeps the GC-index cleanup as a path that works on unmodified Pebble, for reference and in case the fork has to be set aside.

---

# Part 2: The storage design

## 11. The big picture

Pebble is a sorted key-value store. It knows nothing about rows, columns, timestamps, TTLs or tombstones. It gives us four things we build on: keys kept in sorted order, atomic batches, a merge operator, which is a hook that lets our code decide how several writes to the same key are combined, and, with the fork, a compaction filter that lets our code decide what compaction keeps.

Our layer adds everything Cassandra-specific on top of that:

- an encoding that turns a table ID, partition key and clustering key into one sortable Pebble key,
- a compact row value that carries, for every cell, its write timestamp, deletion state and expiry,
- a merger that combines writes by timestamp (last write wins, per cell),
- a read path that hides deleted and expired data,
- a compaction filter that eventually removes dead data from disk (Part 1).

The single most important design decision is this: every write to a row is a Pebble merge, never a plain set. A merge does not read or overwrite anything when it is written. It simply adds a small record. Our merger later combines those records by timestamp. Because the outcome depends only on timestamps and never on the order in which writes arrived, concurrent writers never need locks, and neither does cleanup, which only works on records that already exist.

---

## 12. Keyspace layout

Every Pebble key starts with one byte that says what kind of key it is. This keeps unrelated data apart and lets each kind be scanned on its own.

- Prefix 0x01 holds table data: the rows themselves, and the partition headers. Each data key carries the ID of its table (section 13).
- Prefix 0x02 holds lightweight-transaction (Paxos) state, one key per partition, keyed by table ID, token and partition key like the data keys. This is local to the node.
- Prefix 0x03 holds hints, which are writes waiting for a node that was down. Local.
- Prefix 0x04 holds table schemas, keyed by table ID.
- Prefix 0x05 holds node-local metadata, including the repair watermarks (section 8), and the GC index if the fallback is used (appendix).

---

## 13. Data keys

A data key is built from, in order: the prefix byte 0x01, the table ID, the token, the partition key, a one-byte kind marker, and the clustering key.

The table ID identifies the table the row belongs to. It is a 4-byte number stored big-endian, so that byte order matches numeric order. 4 bytes allow about four billion tables, enough even though IDs are never reused; 8 bytes and a variable-length order-preserving encoding were considered and not needed. Table IDs are assigned when a table is created, recorded in the schema (section 14), and never reused.

The token is the 8-byte hash of the partition key, stored big-endian with its sign bit flipped so that byte order matches numeric order. Putting the token right after the table ID means every token range of a table is one contiguous range of Pebble keys, which makes streaming, repair and dropping a range we no longer own simple range operations, one per table. It also lets the compaction filter find a key's table and token range, and so its schema, GC mode, grace period and repair watermark, from the key alone.

The partition key and clustering key use an order-preserving encoding, meaning that comparing the encoded bytes gives the same answer as comparing the original values. Integers are stored big-endian with the sign bit flipped. Strings and byte arrays escape any zero byte and end with a two-byte terminator, which guarantees that one key can never be a prefix of a different key (so "c1" and "c10" never mix). Clustering columns declared DESC have their bytes inverted so they sort in reverse.

The kind marker is 0x00 for the partition header and 0x01 for a row. The partition header therefore sorts before every row of its partition. It holds partition-level deletions (section 18).

### Why the table ID

Decision: every data key carries a table ID, right after the 0x01 prefix and before the token. The reasons:

- **Without it, keys of different tables collide.** Take a table users with primary key id and a table orders with primary key id. The rows users id 42 and orders id 42 would have the same token (the hash of 42) and the same encoded partition key, and so exactly the same Pebble key. Their merge records would be combined as if they were one row, and their column IDs collide too, since each table numbers its own columns from 1.
- **Every component that decodes values needs the row's table.** The merger, the filter, the collector and the read path need the table's schema, for example to skip a fixed-size column by its size, and the filter needs the table's grace period and GC mode (section 8).
- **Per-table operations need each table's keys together.** With the table ID first, each table's keys are one contiguous range, so DROP TABLE and TRUNCATE are one Pebble range delete each, which Pebble can carry out by dropping whole files without rewriting them, and per-table repair and streaming are range scans.

The cost is a few bytes per key, which prefix compression mostly removes, since neighbouring keys share their table ID. Moving a token range to another node becomes one range delete per table instead of one in total.

**Alternatives considered.**

- **One Pebble database per table.** Each table would get its own instance, with its own write-ahead log, memtables and compactions. Tables would be isolated and dropping one would be trivial, but many tables would mean many write-ahead logs and memtables sharing memory, more compaction work competing, and no atomic batch across tables, which matters for secondary indexes stored as tables of their own.
- **The table ID after the token.** This keeps each token range contiguous across all tables, so an ownership change is one range delete, but it scatters each table across the whole key space, which makes drops, truncates and per-table repair harder.

For comparison, Cassandra keeps every table in its own SSTables, in a directory per table, while sharing one commit log and one memtable memory pool. CockroachDB puts the table ID at the front of its keys (from the earlier look at its key encoding; not rechecked here).

**Example.** For the table users, with table ID 7 and primary key id, the row id = 42 has the data key: 0x01, then table ID 7, then the token of 42, then the encoded 42, then 0x01 (row). There is no clustering key because the table has none. For a table messages, with table ID 9, partition key chat_id and clustering key sent_at, the message sent at 1002 in chat c1 has the key: 0x01, table ID 9, the token of "c1", the encoded "c1", 0x01, then the encoded 1002. A row orders id = 42 in a table with ID 8 has a different key from users id = 42, because the table IDs differ.

Because the partition key comes before the clustering key, all rows of one partition sit next to each other. Reading a whole partition is one range scan, and reading a slice of clustering keys is a narrower range scan (section 17).

### Where the layout came from

Cassandra stores a partition as one continuous record in its SSTable: the partition key once, then each row with its clustering key once. We cannot do exactly that in Pebble, because Pebble stores independent keys, so the partition key is repeated in every row key. Pebble's block prefix compression stores the shared beginning of neighbouring keys only once per group, which removes most of that repetition on disk. With Pebble's columnar block format (FormatColumnarBlocks, which must be switched on explicitly), keys are stored with one shared prefix per block and one per bundle of 16 keys (verified in Pebble's source).

---

## 14. Schema and column IDs

Tables are identified by numeric table IDs, never by name. Creating a table assigns the next unused table ID, and a table ID is never reused, for the same reason as column IDs: a new table with an old ID would see the old table's rows. Dropping or truncating a table removes its keys with one Pebble range delete over its table ID's key range, and the schema records the drop.

Columns are identified by numeric IDs, never by name, numbered per table. The schema, stored under prefix 0x04 and keyed by table ID, maps each column name to an ID and a type, and records dropped columns together with the time they were dropped.

Adding a column assigns the next unused ID. Nothing is rewritten: old rows simply have no cell for that ID, and reads treat it as null. Dropping a column records the drop time. Reads ignore cells of that column written before the drop, and the compaction filter drops them when it next rewrites the row. A column ID is never reused, otherwise old cells would appear under the new column.

Every node must use the same schema. Schema changes travel through gossip and carry a version number. A request built against a different schema version waits or is rejected. (Database Internals, chapter 8, describes a real Cassandra bug caused by schema changes reaching nodes at different times.)

---

## 15. The row value

Each row is one Pebble key and one value. The value holds the whole row: every regular column and every element of every set or map. It is written in a fixed order, and that fixed order is what makes merging cheap.

### Layout

The value starts with a small row header containing a flags byte, the row timestamp, and the row deletion time if the row was deleted. One bit of the flags byte means "this row contains at least one deleted or expiring item" (a deleted cell, a removed element, a row deletion, a collection clear, or a cell or element with a TTL). It lets the property collector and the compaction filter skip ordinary rows without decoding them (section 5, "The property collector"). The write path sets it, and the merger and the filter keep it correct.

After the header come the columns, sorted by column ID. A regular column is a single cell. A set or map column starts with its own small header, followed by its elements sorted by name. The collection header holds two things, as in Cassandra:

- the time the whole collection was last cleared, if it ever was,
- the number of elements, which tells a decoder how many elements to read and lets it size its output up front.

There is no byte size for the collection as a whole. Each element carries its own lengths (its name, and its value if the value has a variable size), so a reader finds the end of the collection by stepping over its elements. This is the same as Cassandra, which stores a count per collection and lengths per element.

The element count is always recomputed when a new value is written, never combined from older values, because merging and filtering drop duplicate and dead elements. The merger and the compaction filter therefore count what they actually write: they reserve a fixed-size slot for the count, write the elements, then fill the slot in. A writer creating a new operand knows its elements up front and fills it in directly.

### What one cell contains

Every cell starts with a flags byte. The flags say which of the optional fields follow, so ordinary cells stay small.

- The flags byte is always present. It records whether the cell is deleted, whether it expires, whether it is empty, and whether it uses the row's timestamp instead of its own.
- The timestamp is the write time, stored as a small varint delta. It is omitted when it equals the row timestamp. It is used for last-write-wins.
- The local deletion time is present only for deleted or expiring cells. It records when this node saw the cell die, or when it expires. It is used for the grace period (section 19).
- The TTL is present only for expiring cells.
- The length is present only for variable-size types such as text or blob. It is a varint byte count that tells the reader where the data ends. Fixed-size types such as int or bigint need no length because the schema says how big they are.
- The data is the value itself, absent for deleted or empty cells.

A set or map element is a cell with one extra field in front: the element name (for a set) or map key (for a map), stored as a length followed by the bytes. Cassandra calls this the cell path. For a set, the element has no data, so the empty flag is set.

### Why lengths make streaming possible

The reader never has to guess where a string ends. Before each variable-size value the writer stores its length. Reading "05 apple" means: read the length 5, take the next five bytes as the name, then continue reading the timestamp and flags. The cursor then sits exactly at the start of the next element. This is what lets the merger and the filter read one element at a time without decoding the whole value.

### Worked example

Take the table users(id, name, tags set of text) and these statements:

- At time 100: insert id 42 with name "Kim" and tags {"a", "b"}.
- At time 200: add "c" to tags with a TTL of 30.

After both, row 42's value contains a header with row timestamp 100. Then column 1 (name) holds one cell: "Kim", using the row timestamp. Then column 3 (tags) holds a collection header with no clear time and three elements in name order: "a" written at 100, "b" written at 100, and "c" written at 200 that expires at 230.

### Compared with Cassandra's SSTable format

This layout copies Cassandra's 3.0 row format closely: a flags byte per cell, timestamps shared with the row where possible, deletion time and TTL only when needed, lengths only for variable-size data, and a cell path for collection elements. The differences are on the Pebble side. Every row carries an 8-byte internal trailer (sequence number and kind), and timestamps are deltas against a fixed per-table base rather than the SSTable's minimum, because a single Pebble value cannot know the minimum of the file it ends up in. For a three-row example, a rough estimate put Cassandra at about 90 bytes, this layout at about 130 bytes, and a one-key-per-cell layout at about 380 bytes. Those numbers are estimates, not measurements.

---

## 16. Write path

Every statement becomes one merge operand for the row key. An operand has the same layout as a stored row value, but contains only what the statement changes. Writes never read the existing row.

- INSERT writes the row timestamp (marking the row as existing) plus the given cells.
- UPDATE writes only the given cells. Unlike INSERT it does not mark the row as existing, the same distinction Cassandra makes.
- Deleting one column writes that cell with the deleted flag and the local deletion time.
- Deleting a row writes the row deletion time in the header.
- Adding a set element writes that element. With a TTL it carries an expiry.
- Removing a set element writes that element with the deleted flag. Removal is a mark, not an erase: a newer version that says "deleted".
- Overwriting a whole set writes a collection clear time of "timestamp minus one", plus the new elements at the timestamp. The new elements are therefore newer than the clear and survive it.
- Deleting a clustering range, or a whole partition, writes into the partition header key instead of a row key (section 18).

Every operand that deletes something or carries a TTL has the row header's "contains a deleted or expiring item" bit set (section 15); plain inserts and updates leave it off.

The operand and any secondary-index entries are written together in one atomic batch. Nothing extra is written for later cleanup: the compaction filter finds dead data in the values themselves. Timestamps come from the coordinator, or from the client if it supplies them.

**Example.** "Remove 'a' from tags at time 210" writes, for row 42, an operand containing only the tags column with one element: "a", deleted, timestamp 210, local deletion time equal to this node's current time.

---

## 17. Read path

To read a partition, open a Pebble iterator bounded to that partition's key range. The lower bound is the prefix byte, table ID, token and partition key, and nothing more. The upper bound is the same prefix with its last byte increased by one. Because the partition-key encoding is prefix-free, this range contains exactly that partition. For a clustering slice, narrow the bounds using the encoded clustering values. For reverse order, start from the last key and move backwards. For paging, remember the last clustering key returned and start just after it.

For each key, Pebble returns the merged value, because our merger runs during reads. The read path then applies what depends on the current time:

- it hides any cell or element whose expiry has passed,
- it hides anything deleted, or older than the row deletion time, the collection clear time, or a partition or range deletion from the partition header,
- it decodes only the columns the query asked for.

The read path never depends on the compaction filter. The filter only removes data the read path would hide anyway, so a read gives the same answer before and after any compaction.

### Reading only part of a row

Pebble always returns a whole value. There is no way to ask for part of one, and it would save little, because Pebble reads from disk in blocks of a few kilobytes anyway. The value returned by a point lookup points into Pebble's block cache, so getting it does not copy it.

What the read path controls is how much of the value it decodes. It starts after the row header and moves through the columns in order. A fixed-size column the query does not need is skipped by its known size from the schema. A variable-size column is skipped by reading its flags and length and moving past that many bytes without looking at them. A collection the query does not need is skipped by reading its element count and stepping over each element using that element's lengths. Only the columns the query asked for are decoded. Cassandra reads cells within a row the same way, stepping past unneeded cells by their lengths. Its jump-ahead structures exist only at higher levels: a body size per row to skip whole rows, and an index of clustering ranges for large partitions.

One limit applies regardless. If a row still has several merge records that compaction has not combined, Pebble runs the merger over all of them before returning the value, and the merger processes the whole row, because Pebble does not tell it which columns the query wants. Partial decoding saves work only once the row is a single merged value. Pebble keeps the number of separate records per row small on its own: a flush combines all records of a key in the memtable into one, and each compaction combines them again. What lets records pile up is a backlog of files at the top level, when writes outpace compaction, and long-lived snapshots, which prevent records on either side of the snapshot from being combined.

**Example.** For row 42 after the writes in sections 15 and 16, a read at time 220 returns name "Kim" and tags {"b", "c"}: "a" is marked deleted. A read at time 240 returns tags {"b"}, because "c" expired at 230. Neither read waited for any cleanup. Expired and deleted data disappear from query results at exactly the right moment, regardless of when it is physically removed.

---

## 18. Deletes at every level

Cassandra supports deletes at several levels, and each has a place in this design.

- A deleted cell is a cell with the deleted flag in the row value.
- A removed set or map element is an element with the deleted flag.
- A cleared or overwritten collection is the collection clear time in the collection header.
- A deleted row is the row deletion time in the row header.
- A deleted clustering range (for example, every message after time 1000) is a range deletion with start, end and timestamp, stored in the partition header key.
- A deleted partition is the partition deletion time, stored in the partition header key.

Because the partition header sorts before the rows, a partition scan reads it first and then applies its deletions to every row it returns. Each deletion records two times: the write timestamp, used to decide what it hides, and the local deletion time, used to decide when it may be purged (section 19).

Only the partition header and the rows themselves are keys. No delete ever creates a key of its own: row, cell and element deletes are recorded in the row's value, and range and partition deletes in the header's value.

### Range and partition deletes: the partition header

Decision: clustering-range deletes and whole-partition deletes are stored in the partition header. The row key format does not change. (Supporting only whole-partition deletes, as Rocksandra does, was considered and not taken; section 10.)

**The header key.** It is the partition's prefix (0x01, table ID, token, partition key) followed by the kind byte 0x00, with no clustering key. There is one per partition, and it sorts immediately before the partition's first row, so a partition scan meets it first without a separate lookup, and compaction emits it before the partition's rows (section 4).

**The header value.** It holds:

- the partition deletion, if any: a write timestamp and a local deletion time,
- a list of range deletes. Each entry has a start bound and an end bound (each a clustering value plus whether it is inclusive, or open-ended), a write timestamp, and a local deletion time.

The list is kept **fragmented and sorted**, following the design RocksDB uses for its own range deletions: no two entries overlap, and entries are ordered by their start bound. Where several deletes cover the same clustering values, that stretch becomes one entry carrying the newest write timestamp among them (and the matching local deletion time). This keeps the list small and lets readers search it.

**Writing a range delete.** "Delete every message in chat c1 with sent_at greater than 1000, at time 400" becomes one merge operand for c1's header key, containing a single range entry: start after 1000, no end, timestamp 400, local deletion time equal to this node's current time. A whole-partition delete is the same, with a partition deletion instead of a range entry. Neither scans or touches any row, so the cost is constant however many rows are covered.

**Merging the header.** The merger knows from the key's kind byte whether it is merging a row or a header (Pebble passes the key to the merger). For a header it keeps the newest partition deletion and merges the range lists: it walks the sorted lists of all records together, cuts entries where they overlap, and gives each resulting piece the newest timestamp covering it. Because the outcome depends only on timestamps, a range delete that arrives late with an older timestamp cannot override a newer one.

**Reading.** A row is hidden, or partly hidden, by the header when its data is older than a deletion covering it. Concretely, any cell, element or row marker whose timestamp is at or below the partition deletion timestamp, or at or below the timestamp of the range entry covering the row's clustering key, is treated as deleted. Data written after the delete has a newer timestamp and stays visible.

- A partition scan reads the header first, then walks the sorted range list side by side with the rows, which arrive in the same clustering order. Each row is checked against the one entry covering it, so nothing has to be searched or held beyond the current entry.
- A single-row read also reads the header, then binary-searches the range list for the entry covering that row. This is one extra lookup per read, usually in the same block as the row, because the header sorts just before the partition's rows.

**Several clustering columns.** With more than one clustering column, every range delete CQL allows is still one continuous stretch of rows in clustering order, and therefore one continuous stretch of encoded keys. Take a table messages with partition key chat_id and clustering columns day, then sent_at. CQL lets the earlier clustering columns be fixed with equality and a range be put on the next one: all messages of one day; part of one day (day equal to a value, sent_at below 1000); or everything before a given day, whatever its sent_at. It does not allow a range on sent_at without fixing day, such as "sent_at below 1000 on any day", because that would be a slice from every day rather than one stretch.

Each bound of a range entry is therefore stored as a clustering prefix (zero, one or more leading clustering values) plus a kind: start or end, inclusive or exclusive. This is how Cassandra stores its own range markers. "Every message of one day" has an inclusive start and an inclusive end, both on the one-value prefix of that day, where inclusive on a prefix means including every row that begins with it. "Part of a day, sent_at below 1000" has an inclusive start on the day and an exclusive end on the two-value prefix of the day and 1000. "Everything before a day" has no start (the beginning of the partition) and an exclusive end on that day.

Comparing a row with a bound uses the key encoding directly. Every encoded clustering value is prefix-free, so a row belongs to a prefix exactly when its encoded clustering key starts with the encoded prefix. If it does, the bound's kind decides: an inclusive bound keeps the row inside the range at that edge, an exclusive one keeps it outside. If it does not, plain byte comparison decides which side the row is on. This single comparison is used everywhere a bound is involved: when the merger fragments and sorts the range list, when the read path checks a row, and when the compaction filter checks a row against the header it is remembering. The filter also uses it to decide whether a range lies wholly inside a compaction's key range (section 4): the range's keys run from the encoded start prefix to just past every key that begins with the encoded end prefix (for an inclusive end).

Clustering columns declared DESC are stored with inverted bytes, so byte order still matches clustering order and the comparison does not change. The only difference is in turning a CQL condition into bounds: for a DESC column, "less than" means later in clustering order, so it becomes the end side of the range rather than the start. Cassandra performs the same translation when it builds a slice. This happens in the write path and does not affect storage.

**Example.** Chat c1 has rows at 1000, 1002 and 1005, all written at time 100. At time 400 the client deletes everything after 1000. The header now holds one entry: after 1000, open-ended, timestamp 400. A read of c1 returns only row 1000. At time 500 the client re-inserts row 1005. A read now returns rows 1000 and 1005, because the new data at 500 is newer than the delete at 400, while row 1002 stays hidden.

### Cleaning up covered rows

A range or partition delete hides rows, but those rows are separate keys whose values never mention the header. Pebble's own DeleteRange cannot be used to remove them: it removes everything written before it by sequence number, so a row re-inserted after the CQL delete but before the cleanup would be lost.

With the fork, the compaction filter cleans covered rows as described in section 4: it remembers the header while compaction streams through the partition, drops covered data older than the deletion, and removes the deletion from the header once the whole range has passed through a bottommost compaction and nothing older exists above it. Nothing is written by a background job, and the cost is part of compaction, which rewrites those rows anyway.

### Read performance with many range deletes

Many range deletes slow reads in two ways until compaction catches up. First, every read of the partition decodes the header's range list, so a partition with thousands of separate ranges pays for that on each read; fragmentation keeps the list as small as possible. Second, and more important, covered rows stay on disk until a compaction that includes the header passes over them, and a scan must still step past each of them, because any of them may have been written again after the delete and must have its timestamp checked. With the fork, Rule 1 drops covered data at the first compaction that sees both the header and the rows, which can be long before the grace period ends. Only the header's own entry must wait for its purge cutoff. This is the same problem as tombstone buildup in Cassandra. It is reduced further by data modelling that turns mass deletes into whole-partition deletes (for example, putting a time bucket in the partition key).

### Alternatives considered

- **Markers inside the partition, as in Cassandra.** Cassandra writes a range delete as a start marker and an end marker placed among the rows at the clustering positions where the range begins and ends, so a reader meets each marker exactly where it applies and keeps at most one open deletion. The equivalent here would be extra keys among the rows, which needs more work, especially to merge overlapping ranges written at different times.
- **Pebble range keys.** Pebble can store a range as a range key, kept outside the normal keys and fragmented by Pebble itself. The deletion timestamp would have to be the range key's suffix, because range keys with the same suffix overwrite each other where they overlap. Pebble's automatic hiding of covered keys would additionally require row keys to carry a version suffix, which would replace the merge-based design with a CockroachDB-style one. Using range keys without that hiding remains an option if headers grow too large. How the default comparer handles suffixed range keys next to suffix-less row keys has not been verified.
- **Pebble's DeleteRange.** It works by sequence number, not by timestamp, so it cannot represent a delete that replicas compare under last-write-wins. It is used only for local physical cleanup (section 24).
- **Converting range deletes into per-row deletes.** This is how the fallback cleans covered rows without a fork (appendix).

For comparison, Instagram's Rocksandra stored whole-partition deletes in a separate RocksDB column family for partition metadata, and did not support clustering-range deletes: its streaming code rejects them and a comparison tool notes they are not supported (verified in its source).

---

## 19. Grace period and local deletion time

A tombstone or expired cell cannot be removed as soon as it exists. If one replica missed the delete and the others had already thrown their tombstone away, repair would copy the old value back and the deleted data would reappear. Cassandra therefore keeps tombstones for gc_grace_seconds (default 864000 seconds, ten days), and operators must run repair within that window.

Cassandra stores two times in every tombstone (verified in its source, DeletionTime.java). The first is markedForDeleteAt, the write timestamp, used to decide what the tombstone hides. The second is localDeletionTime, this node's own clock when the delete was applied, or the expiry time for TTL cells. Purging compares the local time, not the write timestamp (verified in ColumnFamilyStore.java and PurgeFunction.java): a tombstone may be purged when its local deletion time is earlier than now minus gc_grace_seconds, and no older data for the same key exists outside the current compaction. An optional setting additionally requires the data to have been repaired.

Using the local time matters because write timestamps come from clients and can be skewed, while the grace period is really about how long this node has had the delete.

Our design follows the same rule. Every deleted or expiring cell, and every header deletion, stores its local deletion time. The compaction filter compares that time with the purge cutoff it computed at the start of the compaction: now minus the grace period, or the repair watermark in repair mode (section 8).

**Example.** A cell with a TTL of 30 seconds written at 00:00 expires at 00:30. Reads hide it from 00:30. The first compaction after 00:30 may turn it into a tombstone (Rule 1). It stays on disk as a tombstone until 00:30 plus the grace period, and is removed by the first bottom-level compaction after that which meets the other conditions of Rule 2.

For tables where every write has the same TTL and nothing is deleted or overwritten without one, purging right at expiry is safe. ScyllaDB offers this as its "immediate" tombstone GC mode, and it is one of the filter's modes (section 8).

---

## 20. Cleanup

Cleanup is done entirely by the compaction filter (Part 1). In summary:

- every compaction drops data hidden by a deletion and turns expired cells into tombstones (Rule 1),
- a bottom-level compaction removes tombstones past the purge cutoff when nothing older exists outside it and no snapshot is open (Rule 2),
- range and partition deletions in the header are applied to the rows that follow it and removed once their whole range has been through a bottommost compaction (section 4),
- a picker rewrites files whose dead data has become due, including files that would otherwise be trivially moved (section 5),
- output files are cut only between partitions, so a partition's header and rows are always in the same file of a level (section 5).

There is no index of future cleanups, no background goroutine, and no extra write per delete. The work happens while compaction is copying the data anyway, which is how Cassandra does it.

### Why cleanup needs no lock

Compaction only reads records that already exist and writes new files. A write that arrives during a compaction is a new record in the memtable, above the compaction, and the compaction never sees it. When that record is later compacted down onto the filtered result, the merger combines them by timestamp. So the filter can never erase a concurrent write, and the outcome is the same whichever happens first.

### When the bytes actually leave disk

The filter removes data from the files a compaction writes. The bytes leave disk when the compaction's input files are deleted, which Pebble does once no open iterator or snapshot still uses them. How soon a dead row is rewritten depends on compaction: normal compactions reach most data regularly, and the due-deletions picker (section 5) catches files that would otherwise sit untouched. If disk space must come back at a particular moment, a manual compaction over a table's token range forces every overlapping file to be rewritten. It is expensive, so it should be used per range and rarely.

### How CockroachDB does it, for comparison

CockroachDB is also built on Pebble and also has no compaction filter. It runs a separate GC pass (verified in its source: mvcc_gc_queue.go, gc/gc.go, cmd_gc.go). It stores every version as a separate key ending in its timestamp, and deletes are versions with empty values. Each range has a GC threshold of now minus gc.ttlseconds. A queue checks every range and processes it only when its garbage score, computed from per-range statistics of how much data is dead and for how long, exceeds 1. There is a cooldown of up to two hours after the last GC of that range, shrinking to none as the score reaches 2. The queue waits one second between ranges (kv.mvcc_gc.queue_interval), and a single run times out after ten minutes. A run scans the range, then deletes exact old versions, using a Pebble range delete when at least 2000 consecutive keys are dead. It needs no row locks because it only ever deletes specific old versions, and a concurrent write creates a different key. Its row-level TTL feature is a separate SQL job that issues ordinary DELETE statements.

---

## 21. Sets and maps, including element TTL

A non-frozen set or map is stored like Cassandra's "complex cell": one cell per element, each with its own timestamp, deletion flag, local deletion time and expiry, plus one clear time for the whole collection. A set element is a map entry whose value is empty.

Adding an element writes only that element. Removing one writes a newer version of it marked deleted. Overwriting the whole collection writes a clear time of the timestamp minus one, plus the new elements at the timestamp. Each element with a TTL expires on its own. Reads hide it after its expiry, and the compaction filter turns it into a tombstone and finally removes it, exactly like a regular cell.

**Example.** Tags start as {"a", "b"} at 100. At 200, "c" is added with a TTL of 30, so it expires at 230. At 210, "a" is removed. A read at 220 returns {"b", "c"}. A read at 240 returns {"b"}. A compaction after 230 turns "c" into a tombstone. After the grace period, the next bottom-level compaction of the row removes the deleted "a" and the expired "c", and the stored set contains only "b". If someone re-adds "a" at 300, that is a new record above the compaction; when it is merged with the filtered row, "a" at 300 is the only version of "a" and survives.

### Large collections

Decision: every collection is stored inside its row's value. There are no separate Pebble keys per element. This matches Cassandra, which also stores a whole collection with its row.

The consequence is that every read, every merge and every filtered compaction of the row handles every element of its collections. Cassandra has the same limit. Its documentation says it "reads collections in their entirety", that "collections are not paged internally", and to "keep collections small", and it sets hard limits such as at most 65,535 keys in a map. This design does not enforce any limit on the number of elements; keeping collections small is left to the application. The streaming merger (section 22) keeps the cost of merging a large collection to a single pass over its bytes, with no maps, and the filter can work the same way. It is the first optimization to build.

Splitting elements into separate keys was considered and rejected. It would have allowed reading and purging single elements of very large collections, at the cost of more keys and a separate collection-clear key that reads must check against every element.

Lists are not covered. They use time-based element IDs and positional operations, and need their own design. Frozen collections are a single regular cell and need nothing special.

---

## 22. How the merger works

Pebble hands the merger all the records stored for one row, as byte strings, one at a time. The merger never edits stored bytes. Pebble's files are immutable. It reads the records and produces one new byte string.

With the fork, the merger only combines. It never removes a tombstone, because whether a tombstone may go depends on the clock, the grace period, the repair watermark and what exists outside the merge, and all of that is the filter's business. As a result the merger no longer needs Pebble's optional interface for deleting a key from a merge: it never has to tell Pebble that a row has disappeared.

It proceeds in this order:

1. **Headers.** It reads the header of each record and keeps the largest row timestamp and the largest row deletion time, and writes these into the new header first, because the deletion time decides what counts as hidden in the following steps.
2. **Columns.** It keeps a cursor in each record and looks at the next column ID in each. It takes the lowest. If several records have that column, it keeps the cell with the newest timestamp, with a fixed tie rule (the tombstone wins, then the larger value) so every replica picks the same winner. If the winner is older than the row deletion time, it is dropped. Otherwise it is copied to the output. The cursors that were used move forward, and it repeats.
3. **Collections.** For a set or map it does the same one level down: it takes the newest collection clear time, then walks the elements by name across all records, keeps the newest version of each name, drops it if it is older than the collection or row deletion time, and copies it otherwise.
4. **Finish.** It fills in the element counts, sets the row header's "contains a deleted or expiring item" bit only if the output still holds such an item, and returns the new bytes.

The merger never reads the clock. Its result depends only on the records, so every read, every compaction and every replica computes the same answer. Hiding expired data by the current time is the read path's job, and removing it is the filter's.

A cell flagged "uses the row timestamp" has no timestamp of its own and inherits the timestamp of the record it came from. When copying it, the merger converts it to an absolute timestamp using its own record's row timestamp, then writes it relative to the output's row timestamp, or writes it explicitly if they differ.

For a partition header, the merger keeps the newest partition deletion and merges the range lists as described in section 18.

### Streaming instead of building maps

Every record keeps its columns sorted by ID and its elements sorted by name. The merger can therefore walk all records side by side and write each winner straight to the output, the same lockstep merge of sorted lists described in Database Internals, chapter 7, applied inside one row. It decodes one element at a time from the bytes, using the length prefixes to find where each element ends, and never builds a decoded copy of the whole row.

Peak memory is about the raw bytes of the records plus the output. Pebble gives each record to the merger one call at a time and the merger may not hold on to Pebble's buffers, so it keeps a copy of each record's raw bytes until the end. That is still much less than decoding every record into maps and building a result map. The first implementation used maps and JSON for readability. The streaming binary version is the target.

The compaction filter reads a single value and writes a single value, so it is one sorted pass over one record: copy what stays, skip what goes, fill in the counts.

**Example.** Three records for the tags of row 42 hold, in name order: the first "a" at 100 and "b" at 100, the second "c" at 200, the third "a" deleted at 210. The merger sees "a" at the front of the first and third records at the same moment, keeps the deleted version because 210 is newer, then takes "b", then "c". The output holds "a" deleted at 210, "b" at 100 and "c" at 200, and a read shows {"b", "c"}.

### When the merger runs

Writing a merge only appends a record. The merger runs in two places. During a read, Pebble gathers the key's records from the memtable and every level and merges them with the oldest data included. The result is returned and not stored. During flush and compaction, Pebble merges only the records in the files being compacted. If that includes the oldest data, the result is written as a plain SET. Otherwise it is written as a combined merge record. With the fork, in a compaction (not a flush), the compaction filter then looks at that result before it is written (section 2). After a compaction there is one value again, so later reads decode only that one until new writes arrive.

Cassandra does the same kind of work, merging a row's versions from several SSTables during reads and compactions. Two differences: Cassandra's memtable combines writes to a row immediately, so it holds one version per row, while Pebble keeps each merge record separate until it is merged. And Cassandra's merge streams over sorted data, which the streaming merger above copies.

---

## 23. Lightweight transactions

Conditional statements (IF NOT EXISTS, IF column = value) use Paxos per partition, as in Cassandra. Each partition has one key under prefix 0x02, keyed by table ID, token and partition key, holding the promised ballot, the accepted ballot with its proposed change, and the last committed ballot. This state is local, so it is updated with plain set operations, and every change must be written with a synchronous commit, because a replica that forgets a promise after a crash breaks Paxos. The compaction filter does not touch keys outside prefix 0x01.

The rounds are prepare, read, propose and commit. One-key-per-row makes the read cheap: a replica can return the row together with its promise, saving a round trip, which is the same idea as ScyllaDB's merged prepare-and-read and Cassandra's Paxos v2. The commit is an ordinary merge of the change, with cell timestamps taken from the ballot, so it needs no special write path. Ballot timestamps only increase per partition, so each conditional write wins under last-write-wins. As in Cassandra and ScyllaDB, conditional and unconditional writes should not be mixed on the same data. When coordinators compete, the loser retries with a higher ballot after a random backoff.

---

## 24. Other local data

- **Hints.** Writes for a node that is down are stored under prefix 0x03, keyed by target node and creation time, replayed when it returns, and deleted with plain Pebble deletes once delivered or too old. A delivered hint carries its creation time and is marked as a hint, so that in repair mode the receiving replica can drop hints older than its watermark for their table and range (section 8).
- **Ownership changes.** When a token range moves to another node, the old owner removes it with one Pebble range delete per table, since each table's part of the range is contiguous. Pebble can drop whole files covered by a range delete without rewriting them (delete-only compactions, listed in Pebble's README).
- **Bulk loading and streaming.** Data received in bulk is written into sstable files directly and ingested, which skips the write-ahead log, memtable and most compaction. Ingested files need the same per-file properties as compacted ones (the due times of their deletions), so they are written with the same property collector. Because ingested files land above existing data even when their rows are old, they are also what the lookup in section 7 most often finds after a repair.
- **Repair.** Merkle trees over a table's token range are built by iterating that range on a file-only snapshot (below). A successful full repair moves that table and range's watermark (section 8).

In all of these, Pebble's own delete is the right tool, because the data is local and not subject to last-write-wins between replicas.

### Snapshots for repair and streaming

**What streaming is.** Streaming means copying whole token ranges between nodes in bulk. It is not how normal writes reach replicas: a normal write is a single message to each replica, applied as one merge, with no snapshot. Streaming happens only when a node joins (bootstrap), leaves (decommission), is rebuilt or replaced, and when a repair finds differences between replicas. Each streaming operation starts, copies its ranges and ends; in a stable cluster with no nodes joining or leaving, streaming only happens during repairs. Repair and streaming are the only users of snapshots in this design.

**How snapshots are closed.** A Pebble snapshot stays open until the code that opened it closes it. Pebble never closes one by itself and has no timeout. Snapshots exist only in memory, so a restart closes them all. Normal Pebble iterators are not snapshots and do not appear in the list of snapshots that compaction respects (as far as is known; not checked in the source).

**Decision: repair and streaming use file-only snapshots.** Pebble has a second kind of snapshot, the eventually-file-only snapshot, limited to given key ranges. Pebble's documentation says it behaves like a normal snapshot only until the memtables holding keys in its ranges have been flushed, and then turns into a file-only snapshot that keeps the files it reads alive, the way an open iterator does, without holding back compaction (verified in Pebble's source). If no memtable holds keys in its ranges when it is taken, it is file-only from the start. A caller can wait until it has become file-only, and can ask Pebble to flush the memtables after a set delay so that this happens soon.

With these snapshots, purging pauses only for the short time until the relevant memtables are flushed, not for the whole repair or stream. The cost moves from paused cleanup to disk space: files that compaction has replaced stay on disk until the snapshot reading them is closed. This is how Cassandra and ScyllaDB work. Cassandra's repair and streaming take references to the set of SSTable files they read, and ScyllaDB builds its repair and streaming readers over the table's memtables and its current set of SSTables held by a shared reference (both verified in their source). Their compaction carries on meanwhile and purges in the new files, while the old files stay on disk for the reader still using them.

**Decision: safeguards for long operations.**

- Every repair and streaming operation closes its snapshot on every path, including errors and cancellation.
- The storage layer exposes Pebble's existing snapshot numbers: the number of open snapshots, the oldest one, the keys and bytes that flushes and compactions had to keep only because snapshots were open, and the number and size of files kept alive only for open iterators and file-only snapshots. Pebble gives no age in time.
- The storage layer adds one number of its own: when each running repair or streaming operation started, so the age of the oldest running one can be reported.

**What these numbers are for.** They are for the people running the database, collected by a monitoring system; the database itself never reads them. They turn slow, silent problems into visible ones: a repair whose progress stops moving, or a stream running far longer than usual, points to a stuck operation; disk use growing while the data does not points to files kept alive by an old snapshot, and the numbers above say which; bytes streamed and rows repaired show whether operations do what they should. An example alert: the oldest running repair or stream on a node started more than six hours ago. An operator looks, finds a stream to a node that went away, cancels it, the snapshot closes and the files are freed.

For comparison, Cassandra exposes metrics for the operations rather than for snapshots, such as the number of active outbound streams and the bytes streamed, including repair bytes. ScyllaDB exposes a finished percentage for each kind of operation (bootstrap, replace, rebuild, decommission, node removal and repair), bytes streamed in and out, and repair rows, bytes and hashes exchanged. Neither reports snapshots (verified in their source).

**Not decided: a time limit for repair and streaming.** Cassandra has two: streams send keep-alive messages and are closed if those fail, so stalled streams are detected within about ten minutes by default, and its automatic repair scheduler retries stuck repair sessions after three hours. In ScyllaDB no time limit was found: its old streaming socket timeout setting is kept only for compatibility and unused, and its stream sessions read without a timeout (only the configuration and the stream session were searched). With file-only snapshots a stuck operation costs disk space rather than paused cleanup, so a limit is a safeguard, not a requirement. Listed in section 10.

---

## 25. Pebble configuration

- Use the forked Pebble, with the compaction filter, the due-deletions picker, the property collector for due times, and the partition-prefix function registered.
- Turn Bloom filters on for L1 to L6, for example bloom.FilterPolicy(10), set explicitly per level so that L0 keeps Pebble's default of no filter (section 7). Pebble enables none by default. The lookup in section 7 relies on them, and they also speed up ordinary point reads.
- Set the format major version to FormatColumnarBlocks or newer, so keys get columnar prefix compression. It is off unless requested.
- Turn value separation off (section 6). On master it has to be switched off explicitly, because the default policy enables it.
- Use the default key schema. A custom KeySchema is a supported public extension (Pebble's source says "users may define their own key schema"), but it only covers keys, needs a key writer and a key seeker, and once used must be kept forever. It is only worth it if timestamps are ever moved into keys.
- Register the merger with a fixed name. The name is stored on disk, and opening the database with a different merger name fails.
- The fork is based on Pebble master at commit a995e89, which requires Go 1.25.3 or newer (section 10). The earlier prototype tests ran on Pebble v2.1.7 with Go 1.24.

---

## 26. Efficiency compared with Cassandra

- **Disk size.** Cassandra is somewhat smaller: it stores the partition key once per partition and uses timestamp deltas against the SSTable minimum. This design comes close with one key per row, block prefix compression and the Cassandra-style row value. The remaining gap is about the 8-byte Pebble trailer and slightly larger timestamps per row.
- **Writes.** Both are blind writes with no read. Equivalent. With the fork there is no extra index write for cleanup.
- **Reads.** Comparable, provided the merger streams instead of building maps.
- **Cleanup.** With the fork, both purge inside compaction, under the same conditions. Without it, the fallback pays one GC index entry and one PurgeOp per cleaned row.
- **Compaction strategies.** Cassandra offers leveled, size-tiered and time-window compaction. Pebble offers leveled only. For TTL-heavy time-series tables, time-bucketed keys and whole-partition deletes give much of the benefit of time-window compaction.
- **Large partitions.** Because files are cut only between partitions (section 5), a large partition is one large file per level and is rewritten whole whenever a compaction touches it. Cassandra behaves the same way, and the same advice applies: keep partitions small. There is no size cap for now (section 5, "Wide partitions: options considered").
- **Maintenance.** The fork is the cost: it has to be rebased on each Pebble release and tested (section 9). The alternative with a compaction filter, RocksDB as used by Rocksandra, needs cgo.

---

## 27. What has been verified, and what has not

Verified by tests against Pebble v2.1.7 (ten tests, written for the fallback design in the appendix):

- a tombstone stays on disk until a PurgeOp arrives, and after compaction the row is gone with no sstables left,
- a write racing with a PurgeOp survives in either order,
- without the row's oldest data, tombstones and the PurgeOp are kept,
- deleting one column leaves the others,
- an expired cell is removed after its PurgeOp,
- after a row is merged at the bottom level, Pebble stops calling the merger until a new merge record arrives (this is the reason for the fork),
- a set element with a TTL expires on its own while other elements and columns stay,
- overwriting a map hides old elements immediately, and its clear time is later purged,
- a removed and re-added element survives a racing PurgeOp,
- a trivially moved file keeps its bytes until a later rewriting compaction, which then removes them (this is the reason for the picker in section 5).

Verified in source code: Pebble's merger interface and its "includes the oldest data" flag, conversion of merged rows to SET at the bottom level, the optional interface for deleting a key from a merge, block property collectors and filters, columnar blocks and key schemas, and the fork points (the compaction iterator's configuration, its SET and MERGE cases, the check for older data below, the bottommost-layer flag, the trivial-move decision, the elision-only picker, value separation being on by default, and only SET values being separated; locations in handoff/1-fork-point-map.md).

Also verified in Pebble's source, for the fork's design: where output files are cut (the output splitter, its size, grandparent and limit rules, and the flush split keys), the read state used by the lookup view, the Bloom filter policy and its use on prefix seeks, the block property collector interface and how writer options are built per output, the in-memory file metadata fields, the picker's order of steps and Pebble's "nothing waiting" state, and the stall logic for memtables and L0. Each is marked verified where it is described in Part 1.

Verified in other systems' source: Cassandra's DeletionTime fields, its gcBefore computation and purge condition, its overlap check per key with Bloom filters and memtables, its tombstone histogram, its tombstone-compaction rule, its periodic compaction check, and how its compaction and flush writers never cut a partition; ScyllaDB's repair-based purge cutoff, tombstone histogram, compaction timer and its optional splitting of large partitions; RocksDB's compaction filter default for flushes; CockroachDB's GC queue scoring, intervals and GC request handling; Rocksandra's handling of partition and range deletes.

Measured on Pebble v2.1.7: value separation on and off, for this design's merge-only workload and for a Set workload (section 6).

Not yet verified or built:

- the fork itself: the option, the hook, the filter rules, the header handling in the filter, the lookup of older records outside the compaction (the view without the compaction's inputs, and its use of Bloom filters), the snapshot rule, the picker with its "is this file due?" and "pick again" interfaces, the timer that makes the "pick again" call, the property collector (histogram, header due time, flag bit, no collection in flushes, no trivial moves of unrecorded files, the copy into in-memory metadata), and the partition-aligned output splitting,
- file-only snapshots for repair and streaming, closing them on every path, and the start time of running operations as a metric,
- repair watermarks, the GC modes, the receiver-side hint rule (with hints carrying their creation time and a hint marker on delivery), and rate-limiting cleanup after a watermark change,
- Bloom filter settings: whether L0 should get filters, decided by measuring how many L0 files lookups check,
- removing the purge logic from the prototype merger,
- the table ID in data keys, Paxos keys and schema keys, with DROP TABLE and TRUNCATE as one range delete per table,
- the local-deletion-time field (the current code compares write timestamps),
- the compact binary row format and the streaming merger (the current code uses JSON and maps),
- partition headers with range and partition deletions: the fragmented range list, its merging and the read-path check,
- the element count in the collection header.

---

## 28. Glossary

- **Compaction filter.** The hook the fork adds to Pebble's compaction. It sees each record compaction is about to write and keeps it, shrinks it, or drops it.
- **Merge operand (merge record).** A record written with Pebble's Merge. It is combined with the key's other records by our merger when the key is read or compacted.
- **Oldest-data flag.** Pebble's flag telling the merger that the merge includes the key's oldest data, so nothing older exists anywhere.
- **Tombstone.** A record saying something was deleted. It hides older versions until it is purged.
- **Local deletion time.** This node's clock when it applied a delete, or the expiry for TTL data. Compared with the purge cutoff.
- **Grace period.** How long a tombstone must be kept before purging, so repair can spread deletes to every replica.
- **Purge cutoff.** The time before which a tombstone's local deletion time must lie for it to be purged: now minus the grace period, or the repair watermark.
- **Repair watermark.** Per table, token range and node, the start time of the last successful full repair of that table's range. Deletions applied before it are on every replica. Used only in repair mode.
- **Hint rule.** In repair mode, the receiving replica drops a delivered hint created before its watermark for the hint's table and range, because repair has already delivered that write.
- **Streaming.** Copying whole token ranges between nodes in bulk, when nodes join, leave or are rebuilt, and when repair finds differences. Not the path of normal writes.
- **File-only snapshot.** Pebble's eventually-file-only snapshot: after the memtables it depends on are flushed, it keeps the files it reads alive instead of holding back compaction. Used by repair and streaming (section 24).
- **Snapshot stripe.** The records of a key between two open snapshots. Compaction never combines records across stripes. The filter purges only when no snapshot is open (section 3).
- **Bottommost data layer.** A compaction with no older data below it for its key range.
- **Hint.** A write a coordinator stores for a replica that did not acknowledge it, and replays with its original timestamp once the replica is back (Cassandra's hinted handoff).
- **Property collector.** Our code that Pebble calls for every key and value while writing an sstable (not during flushes, in this design). It records per file a histogram of when its deletions become purgeable and the earliest header-deletion due time, from which the picker decides whether the file is due.
- **Bloom filter.** A small per-file summary of keys that answers "definitely not here" or "maybe here", used to skip files on point lookups. Pebble's filters cover whole keys and are off by default.
- **Output splitter.** The part of Pebble that decides where one output file of a flush or compaction ends and the next begins. The fork makes it cut only between partitions.
- **Trivial move.** A compaction that moves a file to a lower level without rewriting it, so neither the merger nor the filter sees its records.
- **Table ID.** A number identifying a table, assigned at creation and never reused. It follows the 0x01 prefix in every data key, so each table's keys are one contiguous range.
- **Complex cell.** Cassandra's term for a non-frozen collection stored as one cell per element.
- **PurgeOp, GC index.** Parts of the fallback design (appendix).

---

## Appendix: cleanup without a fork (fallback)

This is the earlier cleanup design. It works on unmodified Pebble and is what the prototype and its ten tests implement. It is kept as a fallback (section 10). It replaces Part 1; the rest of Part 2 is unchanged, except that the row header gets back a purge cutoff field and writes add a GC index entry to their batch.

**The GC index.** Whenever a statement creates something that will later need cleaning (a tombstone, an expiring cell or element, a collection clear, a dropped column, a header deletion), the same batch writes one extra key under prefix 0x05. That key contains the due time followed by the row key, and its value is empty. The due time is the local deletion time plus the grace period. Because the due time comes first, Pebble keeps the entries sorted by due time, and "all entries due by now" is one iterator with an upper bound of now. The index lives in Pebble, so it survives restarts.

**The scheduler.** One background goroutine per node reads the first entry, sleeps until it is due, and wakes sooner if a writer adds an earlier one. It also wakes once a minute as a safety net. For each due entry it writes a PurgeOp to the row and deletes the entry, in one batch. After a crash it finds the overdue entries and carries on; writing the same PurgeOp twice is harmless.

**The PurgeOp.** A tiny merge operand containing only a purge cutoff (now minus the grace period). Writing it gives Pebble a reason to call the merger for that row again, and the cutoff tells the merger what it may drop. When a merge includes the row's oldest data (includesBase), the merger drops tombstones and expired data covered by the cutoff, drops the PurgeOp, and tells Pebble the row is gone if nothing is left (DeletableValueMerger). Without the oldest data it keeps every tombstone and the PurgeOp. It must be written only once the entry is due: written earlier, either it would allow an early purge, or the row could be compacted into a plain SET first and the merger would never see it again.

**Why it needs no lock.** A PurgeOp never erases anything. It is just another record, and the merger decides by timestamps, so a write that races with it survives in either order (tested).

**Range deletes.** Covered rows never see the header, so when a header entry is due the goroutine scans the covered rows, writes each a merge operand with a row deletion at the range's timestamp plus a purge cutoff, and then writes a PurgeOp to the header.

**Its costs.** One extra index write per delete, one PurgeOp per cleanup, a goroutine, and bytes that leave disk only when a compaction rewrites the file; a trivially moved file keeps them (tested). Alternatives looked at for this path were a lock-read-delete job (needs row locks), block property filters to find candidate blocks (scans nearly everything when tombstones are spread out), and per-range garbage statistics as in CockroachDB (scans live rows too, and only estimates with blind writes).

---

## Sources

- Pebble source code (github.com/cockroachdb/pebble): options.go, compaction.go, compaction_picker.go, db.go, internal/compact/run.go, internal/compact/splitting.go, internal/manifest/l0_sublevels.go, read_state.go, compaction_scheduler.go, internal/base/comparer.go, sstable/reader_iter_single_lvl.go, sstable/tablefilters/bloom/bloom.go, internal/manifest/table_metadata.go, valsep/value_separator.go (master), value_separation.go (v2.1.7), internal/base/merger.go, internal/compact/iterator.go, sstable/block_property.go, sstable/colblk/data_block.go, format_major_version.go, cockroachkvs/cockroachkvs.go. Master as of 21 Oct 2026 (a995e89), and v2.1.7 for tests.
- RocksDB source code (github.com/facebook/rocksdb, b6582c8): include/rocksdb/compaction_filter.h.
- Cassandra source code (github.com/apache/cassandra, trunk as of 27 Oct 2026): db/DeletionTime.java, db/ColumnFamilyStore.java, db/partitions/PurgeFunction.java, db/compaction/CompactionController.java, db/compaction/AbstractCompactionStrategy.java, db/compaction/writers/CompactionAwareWriter.java, db/compaction/unified/ShardedMultiWriter.java, db/memtable/Flushing.java, db/memtable/AbstractMemtable.java, db/memtable/SkipListMemtable.java, db/memtable/TrieMemtable.java, io/sstable/metadata/MetadataCollector.java, io/sstable/SSTable.java, utils/streamhist/StreamingTombstoneHistogramBuilder.java, db/compaction/CompactionManager.java, service/CassandraDaemon.java, conf/cassandra.yaml.
- CockroachDB source code (github.com/cockroachdb/cockroach, 23 Sep 2026): pkg/kv/kvserver/mvcc_gc_queue.go, pkg/kv/kvserver/gc/gc.go, pkg/kv/kvserver/batcheval/cmd_gc.go, pkg/storage/mvcc.go.
- ScyllaDB source code (github.com/scylladb/scylladb, 5aea8fb): sstables/sstables.cc, compaction/compaction_strategy.cc, compaction/compaction_strategy_impl.hh, tombstone_gc.hh, repair/row_level.cc, replica/compaction_group.hh, compaction/compaction_manager.cc, compaction/compaction_manager.hh, compaction/compaction.cc, compaction/compaction_descriptor.hh, test/boost/sstable_compaction_test.cc.
- ScyllaDB, SSTables 3.0 Data File Format: https://docs.scylladb.com/manual/master/architecture/sstable/sstable3/sstables-3-data-file-format.md
- Apache Cassandra, compaction and tombstones: https://cassandra.apache.org/doc/latest/cassandra/managing/operating/compaction/overview.html
- Apache Cassandra, collections: https://cassandra.apache.org/doc/latest/cassandra/developing/cql/collections/collection-create.html
- ScyllaDB, repair-based tombstone garbage collection: https://www.scylladb.com/2022/06/30/preventing-data-resurrection-with-repair-based-tombstone-garbage-collection/
- Instagram Engineering, Rocksandra: https://instagram-engineering.com/open-sourcing-a-10x-reduction-in-apache-cassandra-tail-latency-d64f86b43589
- Rocksandra source code (github.com/Instagram/cassandra, branch rocks_3.0, November 2019): rocksdb/RocksDBEngine.java, rocksdb/RocksDBCF.java, rocksdb/streaming/RocksDBSStableWriter.java, rocksdb/tools/RowIteratorSanityCheck.java.
- RocksDB, DeleteRange: A New Native RocksDB Operation: https://rocksdb.org/blog/2018/11/21/delete-range.html
- Pebble design documents: docs/range_deletions.md and docs/RFCS/20211018_range_keys.md.
- Alex Petrov, Database Internals (O'Reilly, 2019), chapters 7, 8, 11, 12 and 14.
