// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sort

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/require"
	"os"
	"strings"
	"testing"
)

// todo test merging (two files combine to one with expected size and same values on read)
// todo test compact (2*n files compact to n, size doubles, reads back to same values)
// todo test sort (make sure comparison works correctly)
// todo test mem iter (keys in memory all come back)
// todo test file round-trip (flush mem, read back through file iter)

// helpers -> tempdir provider, tuples and sort comparison

func TestMemSort(t *testing.T) {
	tests := []struct {
		td  val.TupleDesc
		cnt int
	}{
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			cnt: 100,
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Int64Enc, Nullable: false},
			),
			cnt: 100,
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			cnt: 100,
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Int64Enc, Nullable: false},
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			cnt: 100,
		},
	}

	name := func(td val.TupleDesc, cnt int) string {
		b := strings.Builder{}
		sep := ""
		for _, t := range td.Types {
			fmt.Fprintf(&b, "%s%s", sep, string(t.Enc))
			sep = ", "
		}
		sep = "_"
		fmt.Fprintf(&b, "%s%d", sep, cnt)
		return b.String()
	}

	tmpProv := mustNewProv(t)
	defer os.Remove(tmpProv.GetTempDir())
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	for _, tt := range tests {
		t.Run(name(tt.td, tt.cnt), func(t *testing.T) {
			km := newKeyMem(mustNewFile(t, tmpProv), tt.cnt)

			keys := testTuples(ns, tt.td, tt.cnt)
			expSize := 0
			for _, k := range keys {
				expSize += len(k)
				km.insert(k)
			}

			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(l, r) <= 0
			}
			km.sort(keyCmp)
			ensureSorted(t, km.keys, keyCmp)

			cnt, size := drainIterCntSize(km)
			require.Equal(t, tt.cnt, cnt)
			require.Equal(t, expSize, size)
		})
	}
}

func TestMemMerge(t *testing.T) {
	tests := []struct {
		td     val.TupleDesc
		counts []int
	}{
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			counts: []int{100},
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			counts: []int{100, 100},
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			counts: []int{100, 100, 100, 100},
		},

		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			counts: []int{1000, 10000, 10, 100000, 100000},
		},
	}

	name := func(td val.TupleDesc, counts []int) string {
		b := strings.Builder{}
		sep := ""
		for _, t := range td.Types {
			fmt.Fprintf(&b, "%s%s", sep, string(t.Enc))
			sep = ", "
		}
		sep = "_"
		for _, c := range counts {
			fmt.Fprintf(&b, "%s%d", sep, c)

		}
		return b.String()
	}

	tmpProv := mustNewProv(t)
	defer os.Remove(tmpProv.GetTempDir())
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	batchSize := 100

	for _, tt := range tests {
		t.Run(name(tt.td, tt.counts), func(t *testing.T) {
			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(l, r) <= 0
			}

			s := NewTupleSorter(batchSize, 0, keyCmp)

			expSize := 0
			expCnt := 0
			for _, cnt := range tt.counts {
				km := newKeyMem(mustNewFile(t, tmpProv), batchSize)
				keys := testTuples(ns, tt.td, cnt)
				for _, k := range keys {
					expSize += len(k)
					expCnt++
					km.insert(k)
				}
				km.sort(keyCmp)
				s.files = append(s.files, km)
			}

			target := newKeyFile(mustNewFile(t, tmpProv), batchSize)

			ctx := context.Background()
			m := newFileMerger(ctx, keyCmp, target, s.files...)
			m.run(ctx)

			cnt, size := drainIterCntSize(target)
			require.Equal(t, expCnt, cnt)
			require.Equal(t, expSize, size)
		})
	}
}

func TestMemCompact(t *testing.T) {
	// run compact until there's only 1 file
	// check at each iteration that we halved the file count, cnt and size is still the same
}

func TestFileRoundtrip(t *testing.T) {
	// write keyMem, same iter count checking
}

func TestFileMerge(t *testing.T) {
	// same as mem merge
}

func TestCompact(t *testing.T) {
	// same as mem compact
}

func TestFileCompact(t *testing.T) {
	// same as mem compact
}

func TestFileE2E(t *testing.T) {
	// simulate full lifecycle
	// vary batch size and file count so multiple compacts/merges
	// make the batch size and file size small enough that
	// we have to spill to disk and compact several times
}

func testTuples(ns tree.NodeStore, kd val.TupleDesc, cnt int) []val.Tuple {
	keyBuilder := val.NewTupleBuilder(kd)

	var keys []val.Tuple
	for i := 0; i < cnt; i++ {
		keys = append(keys, tree.RandomTuple(keyBuilder, ns))
	}

	return keys
}

func testStringTuples() ([]val.Tuple, func(val.Tuple, val.Tuple) bool) {

}

func testkeyFile() *keyFile {

}

func ensureSorted(t *testing.T, keys []val.Tuple, cmp func(val.Tuple, val.Tuple) bool) {
	for i := 0; i < len(keys)-1; i += 2 {
		require.True(t, cmp(keys[i], keys[i+1]))
	}
}

func mustNewProv(t *testing.T) *tempfiles.TempFileProviderAt {
	tmpDir := os.TempDir()
	err := os.Mkdir(tmpDir, os.ModeDir)
	require.NoError(t, err)

	return tempfiles.NewTempFileProviderAt(tmpDir)
}

func mustNewFile(t *testing.T, prov tempfiles.TempFileProvider) *os.File {
	f, err := prov.NewFile("", "external_sort_test_*")
	if err != nil {
		require.NoError(t, err)
	}
	return f
}

func drainIterCntSize(ki keyIterable) (cnt int, size int) {
	ctx := context.Background()
	iter := ki.IterAll(ctx)
	for {
		k, err := iter.Next(ctx)
		if err != nil {
			break
		}
		cnt++
		size += len(k)
	}
	return cnt, size
}
