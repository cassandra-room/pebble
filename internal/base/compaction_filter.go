// Copyright 2026 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package base

// CompactionFilterDecision indicates the action the compaction iterator should
// take for a record passed to a CompactionFilter.
type CompactionFilterDecision int8

const (
	// CompactionFilterKeep keeps the record unchanged.
	CompactionFilterKeep CompactionFilterDecision = iota
	// CompactionFilterReplace keeps the record with a replaced value.
	CompactionFilterReplace
	// CompactionFilterDrop drops the record from the compaction output.
	//
	// WARNING: Dropping a record in a non-bottommost compaction
	// (!info.IsBottommostDataLayer) or when snapshots are open
	// (info.StripeIndex > 0 or info.HasOpenSnapshots) can make older records for
	// the same key in lower levels or older snapshot stripes visible again.
	// Callers purging deletions must only return CompactionFilterDrop when
	// info.IsBottommostStripe && !info.HasOpenSnapshots (or when dropping a
	// record that is already covered by a retained deletion in the same
	// snapshot stripe).
	CompactionFilterDrop
)

// CompactionFilterRecordInfo provides metadata about the record being
// inspected by a CompactionFilter.
type CompactionFilterRecordInfo struct {
	// Kind is the record's InternalKeyKind after any merge (e.g. SET,
	// SETWITHDEL, MERGE).
	Kind InternalKeyKind
	// SeqNum is the record's original sequence number before bottommost
	// sequence-number zeroing.
	SeqNum SeqNum
	// StripeIndex is the snapshot stripe index of the record (the number of
	// open snapshots older than the record; 0 is the bottommost stripe).
	StripeIndex int
	// HasOpenSnapshots is true if the compaction has any open snapshots.
	HasOpenSnapshots bool
	// IsBottommostDataLayer is true if the compaction forms the bottommost
	// layer of data for the compaction's key range.
	IsBottommostDataLayer bool
	// IsBottommostStripe is true if IsBottommostDataLayer is true and
	// StripeIndex == 0.
	IsBottommostStripe bool
	// NothingOlderBelow is true if StripeIndex == 0 and no older data for
	// this key exists in levels below the compaction.
	NothingOlderBelow bool
}

// CompactionFilter filters point records (SET, SETWITHDEL, and merged MERGE
// records) during compaction.
type CompactionFilter interface {
	// Filter inspects a record about to be emitted by the compaction iterator
	// and returns whether to keep it, replace its value, or drop it.
	Filter(key, value []byte, info CompactionFilterRecordInfo) (decision CompactionFilterDecision, newValue []byte, err error)

	// Close releases any resources held by the filter when the compaction
	// finishes.
	Close() error
}

// CompactionFilterInfo describes the compaction for which a CompactionFilter
// is being created.
type CompactionFilterInfo struct {
	// Bounds are the user key bounds of the compaction.
	Bounds UserKeyBounds
	// OutputLevel is the immediate output level of the compaction.
	OutputLevel int
	// EventualOutputLevel is the level to which output files may later be
	// moved if no overlapping data exists in intermediate levels.
	EventualOutputLevel int
	// IsFlush is true if this compaction is a memtable/ingest flush.
	IsFlush bool
	// IsBottommostDataLayer is true if the compaction forms the bottommost
	// layer of data for its key range.
	IsBottommostDataLayer bool
	// HasOpenSnapshots is true if any snapshots are open.
	HasOpenSnapshots bool
	// Snapshots holds the open snapshot sequence numbers for this compaction.
	Snapshots []SeqNum
	// NewExternalIter creates a fresh internal iterator over the current
	// memtables and sstables outside the compaction (above the compaction's
	// output). The caller must call Close() on the returned iterator when done
	// or before creating a replacement view.
	NewExternalIter func() (InternalIterator, error)
}

// CompactionFilterFactory is called once at the start of a compaction to
// create a CompactionFilter for that compaction, or return nil if no filter
// should run.
type CompactionFilterFactory func(info CompactionFilterInfo) CompactionFilter

// PartitionPrefixFunc returns the byte length of the partition prefix of key.
// If the returned length n satisfies 0 < n < len(key), key[:n] is the start of
// key's partition; otherwise key is treated as its own partition.
//
// Note: Partition-aligned splitting applies to target file size, grandparent
// overlap, and L0 split limits. If a SpanPolicyFunc specifies a bounded
// KeyRange.End, that explicit span boundary takes precedence.
type PartitionPrefixFunc func(key []byte) int

// DueFileInfo describes an sstable candidate for a due-deletions compaction.
type DueFileInfo struct {
	// Summary is the table-level summary recorded by the configured
	// BlockPropertyCollector (with Pebble's 1-byte shortID prefix removed).
	Summary []byte
	// Bounds are the user key bounds of the file.
	Bounds UserKeyBounds
	// NumEntries is the number of entries in the file.
	NumEntries uint64
	// CreationTime is the file's creation time in Unix seconds.
	CreationTime int64
	// Size is the file's size in bytes.
	Size uint64
	// Level is the LSM level containing the file.
	Level int
}

// IsFileDueFunc reports whether the described sstable is due for a cleanup
// compaction.
type IsFileDueFunc func(info DueFileInfo) bool

