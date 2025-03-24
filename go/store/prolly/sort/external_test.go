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
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/util/tempfiles"
	"github.com/dolthub/dolt/go/store/val"
)

func TestFlush(t *testing.T) {
	ctx := sql.NewEmptyContext()
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

	tmpProv := newProv(t)
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	keySize := 100

	for _, tt := range tests {
		t.Run(name(tt.td, tt.cnt), func(t *testing.T) {
			km := newKeyMem(tt.cnt * keySize)

			keys := testTuples(ns, tt.td, tt.cnt)
			expSize := 0
			for _, k := range keys {
				expSize += len(k)
				require.True(t, km.insert(k))
			}

			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(ctx, l, r) <= 0
			}

			t.Run("sorting", func(t *testing.T) {
				km.sort(keyCmp)
				ensureSorted(t, km.keys, keyCmp)
			})

			t.Run("mem iter", func(t *testing.T) {
				cnt, size := drainIterCntSize(t, km)
				require.Equal(t, tt.cnt, cnt)
				require.Equal(t, expSize, size)
			})

			t.Run("file iter", func(t *testing.T) {
				kf, err := km.flush(mustNewFile(t, tmpProv), keyCmp)
				require.NoError(t, err)
				cnt, size := drainIterCntSize(t, kf)
				require.Equal(t, tt.cnt, cnt)
				require.Equal(t, expSize, size)
			})

		})
	}
}

func TestMerge(t *testing.T) {
	ctx := sql.NewEmptyContext()
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

	tmpProv := newProv(t)
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	batchSize := 4096
	keySize := 100

	for _, tt := range tests {
		t.Run(name(tt.td, tt.counts), func(t *testing.T) {
			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(ctx, l, r) <= 0
			}

			var keyMems []keyIterable
			var keyFiles []keyIterable
			expSize := 0
			expCnt := 0
			for _, cnt := range tt.counts {
				km := newKeyMem(cnt * keySize)
				keys := testTuples(ns, tt.td, cnt)
				for _, k := range keys {
					expSize += len(k)
					expCnt++
					require.True(t, km.insert(k))
				}
				kf, err := km.flush(mustNewFile(t, tmpProv), keyCmp)
				require.NoError(t, err)
				keyFiles = append(keyFiles, kf)
				keyMems = append(keyMems, km)
			}

			t.Run("mem merge", func(t *testing.T) {
				target := newKeyFile(mustNewFile(t, tmpProv), batchSize)

				ctx := sql.NewEmptyContext()
				m, _ := newFileMerger(ctx, keyCmp, target, keyMems...)
				m.run(ctx)

				cnt, size := drainIterCntSize(t, target)
				require.Equal(t, expCnt, cnt)
				require.Equal(t, expSize, size)
			})

			t.Run("file merge", func(t *testing.T) {
				target := newKeyFile(mustNewFile(t, tmpProv), batchSize)

				m, _ := newFileMerger(ctx, keyCmp, target, keyFiles...)
				m.run(ctx)

				cnt, size := drainIterCntSize(t, target)
				require.Equal(t, expCnt, cnt)
				require.Equal(t, expSize, size)
			})
		})
	}
}

func TestCompact(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// run compact until there's only 1 file
	// check at each iteration that we halved the file count, cnt and size is still the same
	tests := []struct {
		td      val.TupleDesc
		fileCnt int
	}{
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			fileCnt: 16,
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			fileCnt: 64,
		},
		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			fileCnt: 128,
		},

		{
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			fileCnt: 128,
		},
	}

	name := func(td val.TupleDesc, fileCnt int) string {
		b := strings.Builder{}
		sep := ""
		for _, t := range td.Types {
			fmt.Fprintf(&b, "%s%s", sep, string(t.Enc))
			sep = ", "
		}
		sep = "_"
		fmt.Fprintf(&b, "%s%d", sep, fileCnt)

		return b.String()
	}

	tmpProv := newProv(t)
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	batchSize := 10
	keySize := 100

	for _, tt := range tests {
		t.Run(name(tt.td, tt.fileCnt), func(t *testing.T) {
			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(ctx, l, r) <= 0
			}

			var keyFiles []keyIterable
			expSize := 0
			expCnt := 0
			for i := 0; i < tt.fileCnt; i++ {
				km := newKeyMem(batchSize * keySize)
				keys := testTuples(ns, tt.td, batchSize)
				for _, k := range keys {
					expSize += len(k)
					expCnt++
					require.True(t, km.insert(k))
				}
				kf, err := km.flush(mustNewFile(t, tmpProv), keyCmp)
				require.NoError(t, err)
				keyFiles = append(keyFiles, kf)
			}

			ctx := sql.NewEmptyContext()

			t.Run("file compact", func(t *testing.T) {
				s := NewTupleSorter(batchSize, tt.fileCnt, keyCmp, tmpProv)
				defer s.Close()
				s.files = append(s.files, keyFiles)
				err := s.compact(ctx, 0)

				require.NoError(t, err)
				require.Equal(t, 0, len(s.files[0]))
				require.Equal(t, 1, len(s.files[1]))
				require.Equal(t, 2, len(s.files))

				cnt, size := drainIterCntSize(t, s.files[1][0])
				require.Equal(t, expCnt, cnt)
				require.Equal(t, expSize, size)

			})
		})
	}
}

func TestFileE2E(t *testing.T) {
	ctx := sql.NewEmptyContext()
	// simulate full lifecycle
	// vary batch size and file count so multiple compacts/merges
	// make the batch size and file size small enough that
	// we have to spill to disk and compact several times
	tests := []struct {
		name      string
		rows      int
		batchSize int
		fileMax   int
		td        val.TupleDesc
	}{
		{
			name: "uint32",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			rows:      10_000,
			batchSize: 10_000,
			fileMax:   4,
		},
		{
			name: "uint32",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			rows:      10_000,
			batchSize: 1000,
			fileMax:   4,
		},
		{
			name: "uint32",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Uint32Enc, Nullable: false},
			),
			rows:      20_000,
			batchSize: 500,
			fileMax:   16,
		},
		{
			name: "int64",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.Int64Enc, Nullable: false},
			),
			rows:      7_777,
			batchSize: 1000,
			fileMax:   4,
		},
		{
			name: "(string)",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			rows:      10_000,
			batchSize: 100,
			fileMax:   32,
		},
		{
			name: "(string)",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			rows:      10_000,
			batchSize: 483,
			fileMax:   31,
		},
		{
			name: "(string)",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			rows:      1,
			batchSize: 100,
			fileMax:   30,
		},
		{
			name: "(string)",
			td: val.NewTupleDescriptor(
				val.Type{Enc: val.StringEnc, Nullable: false},
			),
			rows:      0,
			batchSize: 100,
			fileMax:   30,
		},
	}

	tmpProv := newProv(t)
	defer tmpProv.Clean()

	ns := tree.NewTestNodeStore()

	for _, tt := range tests {
		t.Run(fmt.Sprintf("%s %d-rows %d-batch %d-files", tt.name, tt.rows, tt.batchSize, tt.fileMax), func(t *testing.T) {
			keyCmp := func(l, r val.Tuple) bool {
				return tt.td.Compare(ctx, l, r) <= 0
			}

			ctx := sql.NewEmptyContext()
			keys := testTuples(ns, tt.td, tt.rows)
			s := NewTupleSorter(tt.batchSize, tt.fileMax, keyCmp, tmpProv)
			defer s.Close()
			expSize := 0
			for _, k := range keys {
				err := s.Insert(ctx, k)
				require.NoError(t, err)
				expSize += len(k)
			}

			iterable, err := s.Flush(ctx)
			require.NoError(t, err)
			var cnt, size int
			iter, err := iterable.IterAll(ctx)
			require.NoError(t, err)
			defer iter.Close()
			var lastKey val.Tuple
			for {
				k, err := iter.Next(ctx)
				if err != nil {
					break
				}
				if lastKey != nil {
					require.True(t, keyCmp(lastKey, k))
				}
				cnt++
				size += len(k)
				lastKey = k
			}

			require.Equal(t, tt.rows, cnt)
			require.Equal(t, expSize, size)
		})
	}

}

func testTuples(ns tree.NodeStore, kd val.TupleDesc, cnt int) []val.Tuple {
	keyBuilder := val.NewTupleBuilder(kd, ns)

	var keys []val.Tuple
	for i := 0; i < cnt; i++ {
		keys = append(keys, tree.RandomTuple(keyBuilder, ns))
	}

	return keys
}

func ensureSorted(t *testing.T, keys []val.Tuple, cmp func(val.Tuple, val.Tuple) bool) {
	for i := 0; i < len(keys)-1; i += 2 {
		require.True(t, cmp(keys[i], keys[i+1]))
	}
}

func newProv(t *testing.T) *tempfiles.TempFileProviderAt {
	tmpDir := t.TempDir()
	return tempfiles.NewTempFileProviderAt(tmpDir)
}

func mustNewFile(t *testing.T, prov tempfiles.TempFileProvider) *os.File {
	f, err := prov.NewFile("", "external_sort_test_*")
	if err != nil {
		require.NoError(t, err)
	}
	return f
}

func drainIterCntSize(t *testing.T, ki keyIterable) (cnt int, size int) {
	ctx := sql.NewEmptyContext()
	iter, err := ki.IterAll(ctx)
	require.NoError(t, err)
	defer iter.Close()
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
