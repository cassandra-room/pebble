// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/crlib/testutils/leaktest"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/sstable/tablefilters/bloom"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func disableValueSeparation(opts *Options) {
	opts.ValueSeparationPolicy = func() ValueSeparationPolicy {
		return ValueSeparationPolicy{Enabled: false}
	}
}

type testCallbackFilter struct {
	fn      func(key, value []byte, info CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error)
	closeFn func() error
}

func (f *testCallbackFilter) Filter(
	key, value []byte, info CompactionFilterRecordInfo,
) (CompactionFilterDecision, []byte, error) {
	if f.fn == nil {
		return CompactionFilterKeep, nil, nil
	}
	return f.fn(key, value, info)
}

func (f *testCallbackFilter) Close() error {
	if f.closeFn != nil {
		return f.closeFn()
	}
	return nil
}

// TestCompactionFilterMergeAndDecisions verifies that:
// 1. Flushes do not invoke CompactionFilter.
// 2. Bottommost compactions convert MERGE chains into SET and invoke
//    CompactionFilter with Keep, Replace, and Drop decisions.
// 3. Non-bottommost compactions pass IsBottommostDataLayer=false so bottom-only
//    purges do not run above the bottom layer.
func TestCompactionFilterMergeAndDecisions(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var flushFilterCalls atomic.Int32
	var closeCalls atomic.Int32

	opts := &Options{
		FS: vfs.NewMem(),
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			if info.IsFlush {
				flushFilterCalls.Add(1)
			}
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					switch string(key) {
					case "drop-tombstone":
						// Only purge tombstones at the bottommost stripe when no snapshots are open.
						if rInfo.IsBottommostStripe && !rInfo.HasOpenSnapshots && bytes.Equal(value, []byte("TOMBSTONE")) {
							return CompactionFilterDrop, nil, nil
						}
					case "replace-me":
						// Rule 1: strip ":dead" suffix from merged value in any compaction.
						if cleaned, ok := bytes.CutSuffix(value, []byte(":dead")); ok {
							return CompactionFilterReplace, cleaned, nil
						}
					}
					return CompactionFilterKeep, nil, nil
				},
				closeFn: func() error {
					closeCalls.Add(1)
					return nil
				},
			}
		},
	}
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Write records using Merge across two flushes so L0 has two overlapping
	// files and compaction rewrites rather than trivially moving.
	require.NoError(t, d.Merge([]byte("keep-me"), []byte("v1"), nil))
	require.NoError(t, d.Merge([]byte("replace-me"), []byte("live"), nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.Merge([]byte("replace-me"), []byte(":dead"), nil))
	require.NoError(t, d.Merge([]byte("drop-tombstone"), []byte("TOMBSTONE"), nil))
	require.NoError(t, d.Flush())

	// Flush must not call the compaction filter.
	require.Equal(t, int32(0), flushFilterCalls.Load())

	// Compact to bottommost level.
	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false))
	require.GreaterOrEqual(t, closeCalls.Load(), int32(1))

	// "keep-me" is unchanged.
	val, closer, err := d.Get([]byte("keep-me"))
	require.NoError(t, err)
	require.Equal(t, "v1", string(val))
	require.NoError(t, closer.Close())

	// "replace-me" had ":dead" stripped by CompactionFilterReplace.
	val, closer, err = d.Get([]byte("replace-me"))
	require.NoError(t, err)
	require.Equal(t, "live", string(val))
	require.NoError(t, closer.Close())

	// "drop-tombstone" was dropped by CompactionFilterDrop.
	if _, closer, err := d.Get([]byte("drop-tombstone")); err == nil {
		_ = closer.Close()
		t.Fatal("expected drop-tombstone to be dropped")
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
}

// TestCompactionFilterSnapshotsAndStripes verifies:
// 1. Open snapshots block Rule 2 (tombstone purging) in every stripe while
//    Rule 1 (in-record cleanup via CompactionFilterReplace) still runs per stripe.
// 2. A covered row in an older snapshot stripe is not dropped by a partition
//    header in a newer stripe when a snapshot sits between them.
func TestCompactionFilterSnapshotsAndStripes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	opts := &Options{
		FS: vfs.NewMem(),
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			var headerStripe int = -1
			var hasHeader bool
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					// Track partition header for "p1/".
					if string(key) == "p1/00_header" {
						hasHeader = true
						headerStripe = rInfo.StripeIndex
						// Purge header deletion only if bottommost and no snapshots are open.
						if rInfo.IsBottommostStripe && !rInfo.HasOpenSnapshots {
							return CompactionFilterDrop, nil, nil
						}
						return CompactionFilterKeep, nil, nil
					}
					// Covered row "p1/01_row": only drop if covered by header in the SAME
					// snapshot stripe (no snapshot between header and row).
					if string(key) == "p1/01_row" && hasHeader && rInfo.StripeIndex == headerStripe {
						return CompactionFilterDrop, nil, nil
					}
					// Rule 1 interior cleanup on "p1/02_clean": strip ":expired" in any stripe.
					if cleaned, ok := bytes.CutSuffix(value, []byte(":expired")); ok {
						return CompactionFilterReplace, cleaned, nil
					}
					return CompactionFilterKeep, nil, nil
				},
			}
		},
	}
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Write initial row and flush.
	require.NoError(t, d.Merge([]byte("p1/01_row"), []byte("old-row"), nil))
	require.NoError(t, d.Merge([]byte("p1/02_clean"), []byte("v1:expired"), nil))
	require.NoError(t, d.Flush())

	// Take a snapshot between the row write and the partition header deletion.
	snap := d.NewSnapshot()

	// Write partition header deletion and a newer version of p1/02_clean after the snapshot.
	require.NoError(t, d.Merge([]byte("p1/00_header"), []byte("DEL_PARTITION"), nil))
	require.NoError(t, d.Merge([]byte("p1/02_clean"), []byte("v2:expired"), nil))
	require.NoError(t, d.Flush())

	// Compact while snapshot is open.
	require.NoError(t, d.Compact(context.Background(), []byte("p1/"), []byte("p2/"), false))

	// Snapshot must still see "p1/01_row" (not dropped because snapshot sat between
	// header and row) and "p1/02_clean" with Rule 1 applied ("v1").
	val, closer, err := snap.Get([]byte("p1/01_row"))
	require.NoError(t, err)
	require.Equal(t, "old-row", string(val))
	require.NoError(t, closer.Close())

	val, closer, err = snap.Get([]byte("p1/02_clean"))
	require.NoError(t, err)
	require.Equal(t, "v1", string(val))
	require.NoError(t, closer.Close())

	// Latest view must still see "p1/00_header" (not purged while snapshot is open)
	// and "p1/02_clean" with Rule 1 applied ("v1v2").
	val, closer, err = d.Get([]byte("p1/00_header"))
	require.NoError(t, err)
	require.Equal(t, "DEL_PARTITION", string(val))
	require.NoError(t, closer.Close())

	val, closer, err = d.Get([]byte("p1/02_clean"))
	require.NoError(t, err)
	require.Equal(t, "v1v2", string(val))
	require.NoError(t, closer.Close())

	// Close the snapshot, flush an overlapping key in the range, and compact again:
	// now header and row are in the same bottommost stripe with no open snapshots,
	// so both are purged!
	require.NoError(t, snap.Close())
	require.NoError(t, d.Merge([]byte("p1/01_z_keep"), []byte("alive"), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte("p1/"), []byte("p2/"), false))

	if _, closer, err := d.Get([]byte("p1/00_header")); err == nil {
		_ = closer.Close()
		t.Fatal("expected p1/00_header to be purged")
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
	if _, closer, err := d.Get([]byte("p1/01_row")); err == nil {
		_ = closer.Close()
		t.Fatal("expected p1/01_row to be purged")
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}
}

// TestCompactionFilterExternalIterLookup verifies Section 10:
// - An older record (by application timestamp encoded in the value) in a memtable
//   or upper level blocks purging a tombstone in a bottommost compaction.
// - A newer record in a memtable or upper level does NOT block purging.
// - The compaction's own input files are excluded from NewExternalIter and never
//   block purging.
func TestCompactionFilterExternalIterLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Encode values as 1-byte kind ('D' for delete, 'W' for write) + 8-byte timestamp.
	encodeVal := func(kind byte, ts uint64) []byte {
		b := make([]byte, 9)
		b[0] = kind
		binary.BigEndian.PutUint64(b[1:], ts)
		return b
	}
	decodeVal := func(b []byte) (byte, uint64) {
		return b[0], binary.BigEndian.Uint64(b[1:])
	}

	var checkExternal atomic.Bool
	var filterActive atomic.Bool
	var filterDone sync.WaitGroup
	filterDone.Add(1)
	var filterOnce sync.Once

	opts := &Options{
		FS: vfs.NewMem(),
		Merger: &Merger{
			Name: "ts-merger",
			Merge: func(key, value []byte) (ValueMerger, error) {
				return &tsValueMerger{buf: slices.Clone(value)}, nil
			},
		},
		IsFileDue: func(info DueFileInfo) bool {
			return info.Level == 6 && checkExternal.CompareAndSwap(true, false)
		},
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			var extIter base.InternalIterator
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					if !filterActive.Load() || !rInfo.IsBottommostStripe || rInfo.HasOpenSnapshots {
						return CompactionFilterKeep, nil, nil
					}
					kind, delTS := decodeVal(value)
					if kind != 'D' {
						return CompactionFilterKeep, nil, nil
					}
					if extIter == nil {
						var err error
						extIter, err = info.NewExternalIter()
						if err != nil {
							return CompactionFilterKeep, nil, err
						}
					}
					prefix := DefaultComparer.Split(key)
					for kv := extIter.SeekPrefixGE(key[:prefix], key, base.SeekGEFlagsNone); kv != nil && bytes.Equal(kv.K.UserKey, key); kv = extIter.Next() {
						valBytes, _, err := kv.V.Value(nil)
						if err != nil {
							return CompactionFilterKeep, nil, err
						}
						_, otherTS := decodeVal(valBytes)
						// An older record outside the compaction blocks the purge!
						if otherTS < delTS {
							return CompactionFilterKeep, nil, nil
						}
					}
					return CompactionFilterDrop, nil, nil
				},
				closeFn: func() error {
					var err error
					if extIter != nil {
						err = extIter.Close()
					}
					if filterActive.Load() {
						filterOnce.Do(filterDone.Done)
					}
					return err
				},
			}
		},
	}
	opts.ApplyTableFilterPolicy(func() DBTableFilterPolicy {
		var p DBTableFilterPolicy
		for i := range p {
			p[i] = bloom.FilterPolicy(10)
		}
		return p
	})
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Place three deletions at timestamp 100 into L6 (with checkExternal=false so they
	// are not purged during setup).
	require.NoError(t, d.Merge([]byte("k-self"), encodeVal('D', 100), nil))
	require.NoError(t, d.Merge([]byte("k-older-mem"), encodeVal('D', 100), nil))
	require.NoError(t, d.Merge([]byte("k-newer-mem"), encodeVal('D', 100), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false))

	// Now write to the memtable (without flushing, so it stays outside the L6 compaction):
	// 1. "k-older-mem" gets a late-arriving write with timestamp 50 (< 100) -> MUST BLOCK purge!
	// 2. "k-newer-mem" gets a write with timestamp 150 (> 100) -> MUST NOT BLOCK purge!
	// 3. "k-self" has only its own tombstone in L6 (at ts 100) -> compaction input excluded, MUST BE PURGED!
	require.NoError(t, d.Merge([]byte("k-older-mem"), encodeVal('W', 50), nil))
	require.NoError(t, d.Merge([]byte("k-newer-mem"), encodeVal('W', 150), nil))

	filterActive.Store(true)
	checkExternal.Store(true)
	d.MaybeScheduleCompaction()
	filterDone.Wait()
	require.Eventually(t, func() bool {
		if _, closer, err := d.Get([]byte("k-self")); err == nil {
			_ = closer.Close()
			return false
		} else {
			return errors.Is(err, ErrNotFound)
		}
	}, 5*time.Second, 10*time.Millisecond)

	// Verify L6 contents directly by scanning L6 table(s):
	// - "k-self" was purged from L6 (so Get returns ErrNotFound).
	if _, closer, err := d.Get([]byte("k-self")); err == nil {
		_ = closer.Close()
		t.Fatal("expected k-self to be purged")
	} else {
		require.ErrorIs(t, err, ErrNotFound)
	}

	// - "k-older-mem" had an older record (ts 50) in the memtable, so the L6 deletion
	//   (ts 100) was KEPT; merging ts 50 and ts 100 yields the deletion at ts 100!
	val, closer, err := d.Get([]byte("k-older-mem"))
	require.NoError(t, err)
	gotKind, gotTS := decodeVal(val)
	require.Equal(t, byte('D'), gotKind)
	require.Equal(t, uint64(100), gotTS)
	require.NoError(t, closer.Close())

	// - "k-newer-mem" had a newer record (ts 150) in the memtable, so the L6 deletion
	//   was purged and Get returns the write at ts 150.
	val, closer, err = d.Get([]byte("k-newer-mem"))
	require.NoError(t, err)
	gotKind, gotTS = decodeVal(val)
	require.Equal(t, byte('W'), gotKind)
	require.Equal(t, uint64(150), gotTS)
	require.NoError(t, closer.Close())
}

type tsValueMerger struct {
	buf []byte
}

func (m *tsValueMerger) MergeNewer(value []byte) error {
	if len(m.buf) == 0 || binary.BigEndian.Uint64(value[1:]) >= binary.BigEndian.Uint64(m.buf[1:]) {
		m.buf = append(m.buf[:0], value...)
	}
	return nil
}

func (m *tsValueMerger) MergeOlder(value []byte) error {
	if len(m.buf) == 0 || binary.BigEndian.Uint64(value[1:]) > binary.BigEndian.Uint64(m.buf[1:]) {
		m.buf = append(m.buf[:0], value...)
	}
	return nil
}

func (m *tsValueMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	return m.buf, nil, nil
}

// TestPropertyCollectorFlushSkipTrivialMoveAndDuePicker verifies Sections 5, 6, 7, 8, and 9:
// - The collector named by CompactionPropertyCollectorName is skipped on flushes.
// - Trivial moves of flush tables (which lack the summary) are refused.
// - Compacted tables record the collector's summary in TableBackingProperties.
// - Due-deletions picker skips files while a snapshot is open, and picks due files
//   when MaybeScheduleCompaction is called after watermarks/clock advance.
func TestPropertyCollectorFlushSkipTrivialMoveAndDuePicker(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const collectorName = "gc-summary-collector"
	var watermarkTS atomic.Uint64
	watermarkTS.Store(50) // Initially below deletion timestamp 100.

	var mu sync.Mutex
	var movedTables int

	opts := &Options{
		FS:                              vfs.NewMem(),
		CompactionPropertyCollectorName: collectorName,
		BlockPropertyCollectors: []func() BlockPropertyCollector{
			func() BlockPropertyCollector {
				return &testMaxTSCollector{name: collectorName}
			},
		},
		IsFileDue: func(info DueFileInfo) bool {
			if len(info.Summary) < 8 {
				return false
			}
			maxDelTS := binary.BigEndian.Uint64(info.Summary[:8])
			return maxDelTS > 0 && maxDelTS <= watermarkTS.Load()
		},
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			cutoff := watermarkTS.Load()
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					if rInfo.IsBottommostStripe && !rInfo.HasOpenSnapshots && len(value) == 9 && value[0] == 'D' {
						ts := binary.BigEndian.Uint64(value[1:])
						if ts <= cutoff {
							return CompactionFilterDrop, nil, nil
						}
					}
					return CompactionFilterKeep, nil, nil
				},
			}
		},
		EventListener: &EventListener{
			TableCreated: func(info TableCreateInfo) {
				if info.Reason == "compacting" {
					// Track rewrites vs moves.
				}
			},
			CompactionEnd: func(info CompactionInfo) {
				mu.Lock()
				if len(info.Output.Tables) == 1 && len(info.Input) > 0 && len(info.Input[0].Tables) == 1 {
					if info.Output.Tables[0].FileNum == info.Input[0].Tables[0].FileNum {
						movedTables++
					}
				}
				mu.Unlock()
			},
		},
	}
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Write a deletion at ts=100 and flush to L0.
	delVal := make([]byte, 9)
	delVal[0] = 'D'
	binary.BigEndian.PutUint64(delVal[1:], 100)
	require.NoError(t, d.Set([]byte("row1"), delVal, nil))
	require.NoError(t, d.Flush())

	// Verify the L0 flush file does NOT have the collector summary.
	d.mu.Lock()
	l0Iter := d.mu.versions.currentVersion().Levels[0].Iter()
	l0File := l0Iter.First()
	require.NotNil(t, l0File)
	l0Props, ok := l0File.TableBacking.Properties()
	require.True(t, ok)
	_, hasSummary := l0Props.BlockPropertySummary()
	require.False(t, hasSummary, "flush table must not have compaction property collector summary")
	d.mu.Unlock()

	// Compact L0 -> L6. Because the L0 file has no summary, trivial move must be
	// refused and the file must be rewritten with a summary!
	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false))
	mu.Lock()
	require.Equal(t, 0, movedTables, "flush table without summary must not be trivially moved")
	mu.Unlock()

	// Verify the L6 file now has the summary with maxDelTS == 100.
	d.mu.Lock()
	l6Iter := d.mu.versions.currentVersion().Levels[6].Iter()
	l6File := l6Iter.First()
	require.NotNil(t, l6File)
	l6Props, ok := l6File.TableBacking.Properties()
	require.True(t, ok)
	summary, hasSummary := l6Props.BlockPropertySummary()
	require.True(t, hasSummary, "compacted L6 table must have summary")
	require.Equal(t, uint64(100), binary.BigEndian.Uint64(summary[:8]))
	d.mu.Unlock()

	// Hold an open snapshot, advance the watermark to 200 (making the file due),
	// and call MaybeScheduleCompaction. Because a snapshot is open, the due picker
	// must NOT compact the file yet.
	snap := d.NewSnapshot()
	watermarkTS.Store(200)
	d.MaybeScheduleCompaction()
	time.Sleep(50 * time.Millisecond)

	val, closer, err := d.Get([]byte("row1"))
	require.NoError(t, err)
	require.Equal(t, delVal, val)
	require.NoError(t, closer.Close())

	// Close the snapshot and call MaybeScheduleCompaction(). Now the due-deletions
	// picker schedules a filter compaction of the L6 file and drops "row1"!
	require.NoError(t, snap.Close())
	d.MaybeScheduleCompaction()

	require.Eventually(t, func() bool {
		if _, closer, err := d.Get([]byte("row1")); err == nil {
			_ = closer.Close()
			return false
		} else {
			return errors.Is(err, ErrNotFound)
		}
	}, 5*time.Second, 10*time.Millisecond, "expected due L6 file to be compacted and row1 purged")
}

type testMaxTSCollector struct {
	name  string
	maxTS uint64
}

func (c *testMaxTSCollector) Name() string { return c.name }

func (c *testMaxTSCollector) AddPointKey(key InternalKey, value []byte) error {
	if len(value) == 9 && value[0] == 'D' {
		ts := binary.BigEndian.Uint64(value[1:])
		if ts > c.maxTS {
			c.maxTS = ts
		}
	}
	return nil
}

func (c *testMaxTSCollector) AddRangeKeys(span sstable.Span) error {
	return nil
}

func (c *testMaxTSCollector) FinishDataBlock(buf []byte) ([]byte, error) {
	return buf, nil
}

func (c *testMaxTSCollector) AddPrevDataBlockToIndexBlock() {}

func (c *testMaxTSCollector) FinishIndexBlock(buf []byte) ([]byte, error) {
	return buf, nil
}

func (c *testMaxTSCollector) FinishTable(buf []byte) ([]byte, error) {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], c.maxTS)
	return append(buf, b[:]...), nil
}

func (c *testMaxTSCollector) AddCollectedWithSuffixReplacement(
	oldProp []byte, oldSuffix, newSuffix []byte,
) error {
	return nil
}

func (c *testMaxTSCollector) SupportsSuffixReplacement() bool { return false }

// TestPartitionAlignedSplittingEndToEnd verifies Section 4:
// With PartitionPrefix configured and small TargetFileSizes, no partition ever
// spans two SSTables in any level L1..L6 across flushes and compactions.
func TestPartitionAlignedSplittingEndToEnd(t *testing.T) {
	defer leaktest.AfterTest(t)()

	partitionPrefix := func(key []byte) int {
		if idx := bytes.IndexByte(key, '/'); idx >= 0 {
			return idx + 1
		}
		return len(key)
	}

	opts := &Options{
		FS:              vfs.NewMem(),
		PartitionPrefix: partitionPrefix,
		FlushSplitBytes: 512,
	}
	opts.WithFSDefaults()
	for i := range opts.TargetFileSizes {
		opts.TargetFileSizes[i] = 512
	}
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Write 12 partitions, each with 10 rows of 100-byte values (so several
	// partitions exceed TargetFileSizes[i] = 512 bytes), across multiple flushes.
	val := bytes.Repeat([]byte("x"), 100)
	for batchIdx := 0; batchIdx < 3; batchIdx++ {
		for p := 0; p < 12; p++ {
			for r := 0; r < 10; r++ {
				key := fmt.Sprintf("part%02d/row%02d_%d", p, r, batchIdx)
				require.NoError(t, d.Set([]byte(key), val, nil))
			}
		}
		require.NoError(t, d.Flush())
	}

	// Compact across the keyspace.
	require.NoError(t, d.Compact(context.Background(), []byte("part00/"), []byte("part99/"), false))

	// Check every level L1..L6: no two adjacent SSTables in the same level may
	// share the same partition prefix!
	checkPartitionInvariant := func() int {
		d.mu.Lock()
		defer d.mu.Unlock()
		vers := d.mu.versions.currentVersion()
		totalTables := 0
		for level := 1; level < numLevels; level++ {
			iter := vers.Levels[level].Iter()
			var prevLargestPrefix []byte
			for f := iter.First(); f != nil; f = iter.Next() {
				totalTables++
				sm := f.Smallest().UserKey
				lg := f.Largest().UserKey
				smPrefix := sm[:partitionPrefix(sm)]
				lgPrefix := lg[:partitionPrefix(lg)]
				if len(prevLargestPrefix) > 0 {
					require.NotEqual(t, string(prevLargestPrefix), string(smPrefix),
						"level %d split partition %q across two tables", level, smPrefix)
				}
				prevLargestPrefix = slices.Clone(lgPrefix)
			}
		}
		return totalTables
	}
	require.Greater(t, checkPartitionInvariant(), 1, "expected multiple output tables")

	// Run a random workload of writes, updates, deletes, flushes, and compactions
	// across partitions and verify that the "no partition spans two files in
	// L1 to L6" invariant holds throughout.
	rng := rand.New(rand.NewPCG(42, 99))
	for step := 0; step < 8; step++ {
		for op := 0; op < 60; op++ {
			p := rng.IntN(20)
			r := rng.IntN(15)
			key := []byte(fmt.Sprintf("part%02d/row%02d", p, r))
			if rng.IntN(5) == 0 {
				require.NoError(t, d.Delete(key, nil))
			} else {
				v := bytes.Repeat([]byte{byte('a' + (op % 26))}, 40+rng.IntN(120))
				require.NoError(t, d.Set(key, v, nil))
			}
		}
		require.NoError(t, d.Flush())
		if step%2 == 1 {
			p1 := rng.IntN(10)
			p2 := p1 + 1 + rng.IntN(10)
			start := []byte(fmt.Sprintf("part%02d/", p1))
			end := []byte(fmt.Sprintf("part%02d/", p2))
			require.NoError(t, d.Compact(context.Background(), start, end, false))
		}
	}
	require.NoError(t, d.Compact(context.Background(), []byte("part00/"), []byte("part99/"), false))
	require.Greater(t, checkPartitionInvariant(), 1, "expected multiple output tables after random workload")
}

// TestCompactionFilterBoundsAndUpperLevelLookup verifies:
// - A partition header deletion whose partition range is not fully contained
//   within CompactionFilterInfo.Bounds is kept (not purged).
// - An older SSTable record in an upper level (L0) blocks a purge in L6 via
//   NewExternalIter, whereas a newer L0 SSTable record does not block it.
func TestCompactionFilterBoundsAndUpperLevelLookup(t *testing.T) {
	defer leaktest.AfterTest(t)()

	encodeVal := func(kind byte, ts uint64) []byte {
		b := make([]byte, 9)
		b[0] = kind
		binary.BigEndian.PutUint64(b[1:], ts)
		return b
	}
	decodeVal := func(b []byte) (byte, uint64) {
		return b[0], binary.BigEndian.Uint64(b[1:])
	}

	var checkPurge atomic.Bool
	var filterActive atomic.Bool
	var filterDone sync.WaitGroup
	filterDone.Add(1)
	var filterOnce sync.Once

	opts := &Options{
		FS: vfs.NewMem(),
		Merger: &Merger{
			Name: "ts-merger",
			Merge: func(key, value []byte) (ValueMerger, error) {
				return &tsValueMerger{buf: slices.Clone(value)}, nil
			},
		},
		IsFileDue: func(info DueFileInfo) bool {
			return info.Level == 6 && checkPurge.CompareAndSwap(true, false)
		},
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			var extIter base.InternalIterator
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					if !filterActive.Load() || !rInfo.IsBottommostStripe || rInfo.HasOpenSnapshots {
						return CompactionFilterKeep, nil, nil
					}
					// If this is a partition header for partition "p_partial/", check
					// whether the entire partition range ["p_partial/", "p_partial0") is
					// contained within the compaction's bounds.
					if bytes.HasPrefix(key, []byte("p_partial/")) {
						partBounds := base.UserKeyBoundsEndExclusive([]byte("p_partial/"), []byte("p_partial0"))
						if !info.Bounds.ContainsBounds(DefaultComparer.Compare, partBounds) {
							return CompactionFilterKeep, nil, nil
						}
					}
					kind, delTS := decodeVal(value)
					if kind != 'D' {
						return CompactionFilterKeep, nil, nil
					}
					if extIter == nil {
						var err error
						extIter, err = info.NewExternalIter()
						if err != nil {
							return CompactionFilterKeep, nil, err
						}
					}
					prefix := DefaultComparer.Split(key)
					for kv := extIter.SeekPrefixGE(key[:prefix], key, base.SeekGEFlagsNone); kv != nil && bytes.Equal(kv.K.UserKey, key); kv = extIter.Next() {
						valBytes, _, err := kv.V.Value(nil)
						if err != nil {
							return CompactionFilterKeep, nil, err
						}
						_, otherTS := decodeVal(valBytes)
						if otherTS < delTS {
							return CompactionFilterKeep, nil, nil
						}
					}
					return CompactionFilterDrop, nil, nil
				},
				closeFn: func() error {
					var err error
					if extIter != nil {
						err = extIter.Close()
					}
					if filterActive.Load() {
						filterOnce.Do(filterDone.Done)
					}
					return err
				},
			}
		},
	}
	opts.ApplyTableFilterPolicy(func() DBTableFilterPolicy {
		var p DBTableFilterPolicy
		for i := range p {
			p[i] = bloom.FilterPolicy(10)
		}
		return p
	})
	disableValueSeparation(opts)

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	// Write deletions to L6:
	// - "k-older-l0" at ts=100
	// - "k-newer-l0" at ts=100
	// - "p_partial/00_hdr" at ts=100 (where the L6 file ends at "p_partial/00_hdr",
	//   so its bounds do NOT cover all of ["p_partial/", "p_partial0")!).
	require.NoError(t, d.Merge([]byte("k-newer-l0"), encodeVal('D', 100), nil))
	require.NoError(t, d.Merge([]byte("k-older-l0"), encodeVal('D', 100), nil))
	require.NoError(t, d.Merge([]byte("p_partial/00_hdr"), encodeVal('D', 100), nil))
	require.NoError(t, d.Flush())
	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z"), false))

	// Flush an L0 SSTable with an older record (ts=40) for "k-older-l0" and a
	// newer record (ts=140) for "k-newer-l0".
	require.NoError(t, d.Merge([]byte("k-older-l0"), encodeVal('W', 40), nil))
	require.NoError(t, d.Merge([]byte("k-newer-l0"), encodeVal('W', 140), nil))
	require.NoError(t, d.Flush())

	// Trigger the due compaction on L6.
	filterActive.Store(true)
	checkPurge.Store(true)
	d.MaybeScheduleCompaction()
	filterDone.Wait()

	// Wait for the L6 compaction to finish.
	d.mu.Lock()
	for d.mu.compact.compactingCount > 0 {
		d.mu.compact.cond.Wait()
	}
	d.mu.Unlock()

	// 1. "p_partial/00_hdr" was KEPT because the compaction's bounds ended at
	//    "p_partial/00_hdr" and did not cover ["p_partial/", "p_partial0").
	val, closer, err := d.Get([]byte("p_partial/00_hdr"))
	require.NoError(t, err)
	gotKind, gotTS := decodeVal(val)
	require.Equal(t, byte('D'), gotKind)
	require.Equal(t, uint64(100), gotTS)
	require.NoError(t, closer.Close())

	// 2. "k-older-l0" had an older record in L0 (ts=40), blocking the L6 purge,
	//    so the deletion at ts=100 was KEPT and wins over ts=40.
	val, closer, err = d.Get([]byte("k-older-l0"))
	require.NoError(t, err)
	gotKind, gotTS = decodeVal(val)
	require.Equal(t, byte('D'), gotKind)
	require.Equal(t, uint64(100), gotTS)
	require.NoError(t, closer.Close())

	// 3. "k-newer-l0" had only a newer record in L0 (ts=140), so the L6 deletion
	//    was purged and Get returns the write at ts=140.
	val, closer, err = d.Get([]byte("k-newer-l0"))
	require.NoError(t, err)
	gotKind, gotTS = decodeVal(val)
	require.Equal(t, byte('W'), gotKind)
	require.Equal(t, uint64(140), gotTS)
	require.NoError(t, closer.Close())

	// Verify that NewExternalIter used Bloom filters on the L0 SSTable and that
	// the due compaction was counted in CompactMetrics.FilterCount.
	m := d.Metrics()
	require.Greater(t, m.Filter.Hits+m.Filter.Misses, int64(0),
		"expected NewExternalIter SeekPrefixGE to consult SSTable Bloom filters")
	require.GreaterOrEqual(t, m.Compact.FilterCount, int64(1),
		"expected due compaction to increment CompactMetrics.FilterCount")
}

// TestCompactionFilterValueSeparationBlobRef verifies that when value
// separation is enabled, invoking CompactionFilter on blob-referenced values
// does not corrupt the blob handle in valueBuf when the filter returns Keep.
func TestCompactionFilterValueSeparationBlobRef(t *testing.T) {
	defer leaktest.AfterTest(t)()

	largeVal1 := bytes.Repeat([]byte("A"), 512)
	largeVal2 := append(bytes.Repeat([]byte("B"), 512), []byte(":strip")...)

	opts := &Options{
		FS:                 vfs.NewMem(),
		FormatMajorVersion: FormatNewest,
		ValueSeparationPolicy: func() ValueSeparationPolicy {
			return ValueSeparationPolicy{
				Enabled:                  true,
				MinimumSize:              64,
				MinimumMVCCGarbageSize:   64,
				MaxBlobReferenceDepth:    5,
				RewriteMinimumAge:        time.Minute,
				GarbageRatioLowPriority:  0.10,
				GarbageRatioHighPriority: 0.30,
			}
		},
		CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
			return &testCallbackFilter{
				fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
					if cleaned, ok := bytes.CutSuffix(value, []byte(":strip")); ok {
						return CompactionFilterReplace, cleaned, nil
					}
					return CompactionFilterKeep, nil, nil
				},
			}
		},
	}

	d, err := Open("", opts)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, d.Close())
	}()

	require.NoError(t, d.Set([]byte("blob-keep"), largeVal1, nil))
	require.NoError(t, d.Set([]byte("z-overlap"), []byte("1"), nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.Set([]byte("a-overlap"), []byte("2"), nil))
	require.NoError(t, d.Set([]byte("blob-replace"), largeVal2, nil))
	require.NoError(t, d.Flush())

	require.NoError(t, d.Compact(context.Background(), []byte("a"), []byte("z-overlap\x00"), true))
	require.NoError(t, d.CheckLevels(nil))

	val, closer, err := d.Get([]byte("blob-keep"))
	require.NoError(t, err)
	got1 := slices.Clone(val)
	require.NoError(t, closer.Close())
	require.Equal(t, largeVal1, got1)

	val, closer, err = d.Get([]byte("blob-replace"))
	require.NoError(t, err)
	got2 := slices.Clone(val)
	require.NoError(t, closer.Close())
	require.Equal(t, bytes.Repeat([]byte("B"), 512), got2)
}

// TestDuePickerUpperLevelCases verifies the l -> l+1 due-picker rules:
// 1. A due L4 file with direct overlap in L6 is not picked.
// 2. A due L4 file whose bounds do not directly overlap L6, but whose
//    overlapping L5 input expands pc.bounds into an L6 file, is rejected.
// 3. A due L5 file overlapping L6 data IS picked (L5 -> L6), and in an
//    end-to-end DB run its output in L6 is purged.
func TestDuePickerUpperLevelCases(t *testing.T) {
	defer leaktest.AfterTest(t)()

	makeMeta := func(cmp Compare, fileNum base.TableNum, smallest, largest string) *manifest.TableMetadata {
		m := &manifest.TableMetadata{
			TableNum: fileNum,
			Size:     1024,
		}
		m.InitPhysicalBacking()
		m.TableBacking.PopulateProperties(&sstable.Properties{NumEntries: 10}, "")
		m.PopulateStats(&manifest.TableStats{})
		m.ExtendPointKeyBounds(cmp,
			base.ParseInternalKey(smallest),
			base.ParseInternalKey(largest),
		)
		m.SeqNums.Low = m.Smallest().SeqNum()
		m.SeqNums.High = m.Largest().SeqNum()
		m.LargestSeqNumAbsolute = m.SeqNums.High
		m.InitAllowedSeeks = 1 << 30
		m.AllowedSeeks.Store(1 << 30)
		return m
	}

	t.Run("L4-direct-L6-overlap-rejected", func(t *testing.T) {
		opts := DefaultOptions()
		opts.IsFileDue = func(info DueFileInfo) bool {
			return info.Level == 4
		}
		opts.WithFSDefaults()
		defer opts.private.fsCloser.Close()

		var files [numLevels][]*manifest.TableMetadata
		files[4] = []*manifest.TableMetadata{makeMeta(opts.Comparer.Compare, 1, "a#2,SET", "m#2,SET")}
		files[6] = []*manifest.TableMetadata{makeMeta(opts.Comparer.Compare, 2, "f#1,SET", "z#1,SET")}
		vers, latest := newVersionWithLatest(opts, files)
		picker := newCompactionPickerByScore(vers, latest, opts, nil)

		pc := picker.pickDueCompaction(compactionEnv{
			diskAvailBytes:         1 << 30,
			earliestSnapshotSeqNum: base.SeqNumMax,
		})
		require.Nil(t, pc, "due L4 file overlapping L6 must not be picked")
	})

	t.Run("L4-expanded-bounds-L6-overlap-rejected", func(t *testing.T) {
		opts := DefaultOptions()
		opts.IsFileDue = func(info DueFileInfo) bool {
			return info.Level == 4
		}
		opts.WithFSDefaults()
		defer opts.private.fsCloser.Close()

		// L4 [a, d] does not directly overlap L6 [m, z], but overlaps L5 [c, p]
		// which widens pc.bounds to [a, p], overlapping L6 [m, z].
		var files [numLevels][]*manifest.TableMetadata
		files[4] = []*manifest.TableMetadata{makeMeta(opts.Comparer.Compare, 1, "a#3,SET", "d#3,SET")}
		files[5] = []*manifest.TableMetadata{makeMeta(opts.Comparer.Compare, 2, "c#2,SET", "p#2,SET")}
		files[6] = []*manifest.TableMetadata{makeMeta(opts.Comparer.Compare, 3, "m#1,SET", "z#1,SET")}
		vers, latest := newVersionWithLatest(opts, files)
		picker := newCompactionPickerByScore(vers, latest, opts, nil)

		pc := picker.pickDueCompaction(compactionEnv{
			diskAvailBytes:         1 << 30,
			earliestSnapshotSeqNum: base.SeqNumMax,
		})
		require.Nil(t, pc, "due L4 file whose widened L5 bounds overlap L6 must be rejected")
	})

	t.Run("L5-over-L6-picked-and-purged", func(t *testing.T) {
		var allowDue atomic.Bool
		opts := &Options{
			FS:                       vfs.NewMem(),
			LBaseMaxBytes:            1,
			DisableIngestAsFlushable: func() bool { return true },
			IsFileDue: func(info DueFileInfo) bool {
				return allowDue.Load() && info.Level == 5 && info.Bounds.ContainsUserKey(DefaultComparer.Compare, []byte("k1"))
			},
			CompactionFilter: func(info CompactionFilterInfo) CompactionFilter {
				return &testCallbackFilter{
					fn: func(key, value []byte, rInfo CompactionFilterRecordInfo) (CompactionFilterDecision, []byte, error) {
						if rInfo.IsBottommostStripe && !rInfo.HasOpenSnapshots && bytes.Equal(value, []byte("TOMBSTONE")) {
							return CompactionFilterDrop, nil, nil
						}
						return CompactionFilterKeep, nil, nil
					},
				}
			},
		}
		disableValueSeparation(opts)

		d, err := Open("", opts)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, d.Close())
		}()

		writeSST := func(name string, kvs ...string) {
			f, err := opts.FS.Create(name, vfs.WriteCategoryUnspecified)
			require.NoError(t, err)
			w := sstable.NewWriter(objstorageprovider.NewFileWritable(f), d.opts.MakeWriterOptions(5, d.TableFormat()))
			for i := 0; i < len(kvs); i += 2 {
				require.NoError(t, w.Set([]byte(kvs[i]), []byte(kvs[i+1])))
			}
			require.NoError(t, w.Close())
			require.NoError(t, d.Ingest(context.Background(), []string{name}))
		}

		// Place live data in L6; with LBaseMaxBytes=1, baseLevel drops below L6 so
		// the next overlapping ingest lands directly in L5.
		writeSST("l6.sst", "k1", "old-live", "k2", "stay-live")
		writeSST("l5-due.sst", "k1", "TOMBSTONE")

		// Verify l5-due.sst is in L5 and L6 has overlapping data.
		d.mu.Lock()
		l5Len := d.mu.versions.currentVersion().Levels[5].Len()
		l6Len := d.mu.versions.currentVersion().Levels[6].Len()
		d.mu.Unlock()
		require.Equal(t, 1, l5Len)
		require.Equal(t, 1, l6Len)

		// Enable due picking on L5 and schedule compaction.
		allowDue.Store(true)
		d.MaybeScheduleCompaction()

		require.Eventually(t, func() bool {
			if _, closer, err := d.Get([]byte("k1")); err == nil {
				_ = closer.Close()
				return false
			} else {
				return errors.Is(err, ErrNotFound)
			}
		}, 5*time.Second, 10*time.Millisecond, "expected due L5 file over L6 to be compacted to L6 and purged")

		val, closer, err := d.Get([]byte("k2"))
		require.NoError(t, err)
		require.Equal(t, "stay-live", string(val))
		require.NoError(t, closer.Close())
	})
}



