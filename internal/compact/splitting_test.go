// Copyright 2024 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package compact

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/cockroachdb/crlib/crstrings"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/internal/testkeys"
)

func TestOutputSplitter(t *testing.T) {
	var s *OutputSplitter
	var grandparents manifest.LevelMetadata
	datadriven.RunTest(t, "testdata/output_splitter", func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "init-grandparents":
			// We create a version with all tables in L1.
			var files [manifest.NumLevels][]*manifest.TableMetadata
			if d.Input != "" {
				for l := range crstrings.LinesSeq(d.Input) {
					f, err := manifest.ParseTableMetadataDebug(l)
					if err != nil {
						d.Fatalf(t, "error parsing %q: %v", l, err)
					}
					files[1] = append(files[1], f)
				}
			}
			l0Organizer := manifest.NewL0Organizer(base.DefaultComparer, 64*1024 /* flushSplitBytes */)
			v := manifest.NewVersionForTesting(base.DefaultComparer, l0Organizer, files)
			if err := v.CheckOrdering(); err != nil {
				d.Fatalf(t, "%v", err)
			}
			grandparents = v.Levels[1]

		case "run":
			var startKey, limitKey string
			var targetFileSize uint64
			f := &Frontiers{cmp: base.DefaultComparer.Compare}
			d.ScanArgs(t, "start-key", &startKey)
			d.MaybeScanArgs(t, "limit-key", &limitKey)
			d.ScanArgs(t, "target-size", &targetFileSize)
			s = NewOutputSplitter(
				base.DefaultComparer.Compare, []byte(startKey), []byte(limitKey),
				targetFileSize, grandparents.Iter(), f,
			)
			var last string
			for i, l := range strings.Split(d.Input, "\n") {
				var key string
				var estimatedSize uint64
				fmt.Sscanf(l, "%s %d", &key, &estimatedSize)
				// Advance the frontier, except (sometimes) for the first key where the
				// splitter allows for the frontier to already be at the next user key.
				if i > 0 || rand.IntN(2) == 0 {
					f.Advance([]byte(key))
				}
				if s.ShouldSplitBefore([]byte(key), estimatedSize, func(k []byte) bool { return f.cmp(k, []byte(last)) == 0 }) {
					return fmt.Sprintf("%s %d: split at %q", key, estimatedSize, s.SplitKey())
				}
				last = key
			}
			return fmt.Sprintf("split at %q", s.SplitKey())

		default:
			d.Fatalf(t, "unknown command: %s", d.Cmd)
		}
		return ""
	})
}

func TestFrontiers(t *testing.T) {
	cmp := testkeys.Comparer.Compare
	var keySets [][][]byte
	datadriven.RunTest(t, "testdata/frontiers", func(t *testing.T, td *datadriven.TestData) string {
		switch td.Cmd {
		case "init":
			// Init configures a frontier per line of input. Each line should
			// contain a sorted whitespace-separated list of keys that the
			// frontier will use.
			//
			// For example, the following input creates two separate monitored
			// frontiers: one that sets its key successively to 'd', 'e', 'j'
			// and one that sets its key to 'a', 'p', 'n', 'z':
			//
			//    init
			//    b e j
			//    a p n z

			keySets = keySets[:0]
			for line := range crstrings.LinesSeq(td.Input) {
				keySets = append(keySets, bytes.Fields([]byte(line)))
			}
			return ""
		case "scan":
			f := &Frontiers{cmp: cmp}
			for _, keys := range keySets {
				initTestFrontier(f, keys...)
			}
			var buf bytes.Buffer
			for kStr := range strings.FieldsSeq(td.Input) {
				k := []byte(kStr)
				f.Advance(k)
				fmt.Fprintf(&buf, "%s : { %s }\n", kStr, f.String())
			}
			return buf.String()
		default:
			return fmt.Sprintf("unrecognized command %q", td.Cmd)
		}
	})
}

// initTestFrontiers adds a new frontier to f that iterates through the provided
// keys. The keys slice must be sorted.
func initTestFrontier(f *Frontiers, keys ...[]byte) *frontier {
	ff := &frontier{}
	var key []byte
	if len(keys) > 0 {
		key, keys = keys[0], keys[1:]
	}
	reached := func(k []byte) (nextKey []byte) {
		if len(keys) > 0 {
			nextKey, keys = keys[0], keys[1:]
		}
		return nextKey
	}
	ff.Init(f, key, reached)
	return ff
}

func TestOutputSplitterWithPartitionPrefix(t *testing.T) {
	cmp := base.DefaultComparer.Compare
	partitionPrefix := func(k []byte) int {
		if idx := bytes.IndexByte(k, '/'); idx >= 0 {
			return idx + 1
		}
		return len(k)
	}
	partitionStart := func(k []byte) []byte {
		n := partitionPrefix(k)
		if n <= 0 || n >= len(k) {
			return k
		}
		return k[:n]
	}

	t.Run("size-split-waits-for-partition-boundary", func(t *testing.T) {
		f := &Frontiers{cmp: cmp}
		var gp manifest.LevelMetadata
		s := NewOutputSplitterWithPartitionPrefix(
			cmp, []byte("p1/a"), nil, 100, gp.Iter(), f, partitionPrefix,
		)
		var prevPrefix []byte
		equalPrev := func(k []byte) bool {
			return len(prevPrefix) > 0 && cmp(partitionStart(k), prevPrefix) == 0
		}
		keys := []struct {
			key  string
			size uint64
			want bool
		}{
			{"p1/a", 0, false},
			{"p1/b", 80, false},
			// Exceeds targetFileSize (100), but still in partition "p1/".
			{"p1/c", 150, false},
			{"p1/d", 250, false},
			// Next partition "p2/" -> splits at "p2/".
			{"p2/a", 250, true},
		}
		for _, tc := range keys {
			f.Advance([]byte(tc.key))
			got := bool(s.ShouldSplitBefore([]byte(tc.key), tc.size, equalPrev))
			if got != tc.want {
				t.Fatalf("key %s: got split=%v, want %v", tc.key, got, tc.want)
			}
			if got {
				if string(s.SplitKey()) != "p2/" {
					t.Fatalf("got splitKey=%q, want %q", s.SplitKey(), "p2/")
				}
				break
			}
			prevPrefix = partitionStart([]byte(tc.key))
		}
	})

	t.Run("grandparent-boundary-moved-to-partition-start", func(t *testing.T) {
		var files [manifest.NumLevels][]*manifest.TableMetadata
		// Grandparent 1 starts mid-partition at "p2/m", Grandparent 2 starts at "p3/b".
		gp1, err := manifest.ParseTableMetadataDebug("000001:[p2/m#1,SET-p2/z#1,SET]")
		if err != nil {
			t.Fatal(err)
		}
		gp2, err := manifest.ParseTableMetadataDebug("000002:[p3/b#1,SET-p3/z#1,SET]")
		if err != nil {
			t.Fatal(err)
		}
		files[1] = []*manifest.TableMetadata{gp1, gp2}
		l0Org := manifest.NewL0Organizer(base.DefaultComparer, 64*1024)
		v := manifest.NewVersionForTesting(base.DefaultComparer, l0Org, files)
		gp := v.Levels[1]

		f := &Frontiers{cmp: cmp}
		s := NewOutputSplitterWithPartitionPrefix(
			cmp, []byte("p1/a"), nil, 100, gp.Iter(), f, partitionPrefix,
		)
		var prevPrefix []byte
		equalPrev := func(k []byte) bool {
			return len(prevPrefix) > 0 && cmp(partitionStart(k), prevPrefix) == 0
		}

		// At 60 bytes (>= 50% of 100), reaching "p2/a" triggers the grandparent
		// frontier that was moved from "p2/m" to "p2/".
		steps := []struct {
			key  string
			size uint64
			want bool
		}{
			{"p1/a", 0, false},
			{"p1/b", 60, false},
			{"p2/a", 60, true},
		}
		for _, tc := range steps {
			f.Advance([]byte(tc.key))
			got := bool(s.ShouldSplitBefore([]byte(tc.key), tc.size, equalPrev))
			if got != tc.want {
				t.Fatalf("key %s: got split=%v, want %v", tc.key, got, tc.want)
			}
			if got {
				if string(s.SplitKey()) != "p2/" {
					t.Fatalf("got splitKey=%q, want %q", s.SplitKey(), "p2/")
				}
				break
			}
			prevPrefix = partitionStart([]byte(tc.key))
		}
	})

	t.Run("limit-moved-to-partition-start", func(t *testing.T) {
		f := &Frontiers{cmp: cmp}
		var gp manifest.LevelMetadata
		// Limit is "p2/m", which moves back to "p2/".
		s := NewOutputSplitterWithPartitionPrefix(
			cmp, []byte("p1/a"), []byte("p2/m"), 1000, gp.Iter(), f, partitionPrefix,
		)
		var prevPrefix []byte
		equalPrev := func(k []byte) bool {
			return len(prevPrefix) > 0 && cmp(partitionStart(k), prevPrefix) == 0
		}
		f.Advance([]byte("p1/a"))
		if s.ShouldSplitBefore([]byte("p1/a"), 0, equalPrev) {
			t.Fatal("unexpected split at p1/a")
		}
		prevPrefix = partitionStart([]byte("p1/a"))

		f.Advance([]byte("p2/a"))
		if !s.ShouldSplitBefore([]byte("p2/a"), 10, equalPrev) {
			t.Fatal("expected split at p2/a due to limit moved from p2/m to p2/")
		}
		if string(s.SplitKey()) != "p2/" {
			t.Fatalf("got splitKey=%q, want %q", s.SplitKey(), "p2/")
		}
	})
}
