// Copyright 2025 Dolthub, Inc.
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

package tree

import (
	"context"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"testing"
)

func TestPatchGeneratorFromRoots(t *testing.T) {
	t.Run("OneChangedTuple", func(t *testing.T) {
		// Changing one tuple, we expect a single Range for the key in the root.
		// If we split the range diff, we expect a single ModifiedDiff for the key in the leaf.
		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, len(fromTups))
		// Copy elements from the original slice to the new slice
		copy(toTups, fromTups)
		bld := val.NewTupleBuilder(desc, ns)
		bld.PutUint32(0, uint32(42))
		toTups[23][1], err = bld.Build(sharedPool) // modify value at index 23.
		assert.NoError(t, err)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		// Range and then EOF
		dif, diffType, err := dfr.Next(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dif)
		assert.Equal(t, ModifiedDiff, diffType)

		dif, diffType, err = dfr.Next(ctx)
		require.ErrorIs(t, err, io.EOF)

		// Range and split visits the one different tuple.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		_, _, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, diffType, err = dfr.split(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dif)
		assert.Equal(t, ModifiedDiff, diffType)
		assert.Equal(t, fromTups[23][0], val.Tuple(dif.EndKey))
		assert.Equal(t, fromTups[23][1], val.Tuple(dif.From))
		assert.Equal(t, toTups[23][1], val.Tuple(dif.To))

		dif, diffType, err = dfr.Next(ctx)
		assert.Equal(t, io.EOF, err)
	})
	t.Run("FromHasSuffix", func(t *testing.T) {
		// One or more rows at the end were deleted.
		// We expect that to manifest as a ModifiedDiff for the new final node,
		// Followed by one or more RangeRemovedDiffs

		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		chunkBoundaries := []uint32{414, 718, 830}
		maxKey := uint32(1023)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 512)
		copy(toTups, fromTups)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		dif, diffType, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, ModifiedDiff, diffType)
		require.NotNil(t, dif.To)

		// One or more RangeRemovedDiff and then EOF
		dif, diffType, err = dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, diffType)
			require.Nil(t, dif.To)
			dif, diffType, err = dfr.Next(ctx)
		}

		require.ErrorIs(t, err, io.EOF)

		// Range splits into level 0 removes until the next chunk boundary, then level 1 removes.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		_, _, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, diffType, err = dfr.split(ctx)
		for i := uint32(512); i <= chunkBoundaries[1]; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif, "%d", i)
			require.Equal(t, RemovedDiff, diffType)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.EndKey), "%d", i)
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From), "%d", i)
			assert.Nil(t, dif.To)
			dif, diffType, err = dfr.Next(ctx)
		}
		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Equal(t, 1, dif.Level)
		endKey, ok := desc.GetUint32(0, val.Tuple(dif.EndKey))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[2], endKey)
		startKey, ok := desc.GetUint32(0, val.Tuple(dif.KeyBelowStart))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[1], startKey)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Equal(t, 1, dif.Level)
		endKey, ok = desc.GetUint32(0, val.Tuple(dif.EndKey))
		require.True(t, ok)
		require.Equal(t, maxKey, endKey)
		startKey, ok = desc.GetUint32(0, val.Tuple(dif.KeyBelowStart))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[2], startKey)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		assert.Equal(t, io.EOF, err)
	})
	t.Run("FromHasPrefix", func(t *testing.T) {
		// Some rows were deleted from the beginning.
		// We expect this to present as a single ModifiedDiff at the start that covers all the removed rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 512)
		copy(toTups, fromTups[512:])
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		// Range and then EOF
		dif, diffType, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, ModifiedDiff, diffType)
		assert.Equal(t, dif.KeyBelowStart, Item(nil))
		assert.GreaterOrEqual(t, dif.EndKey, toTups[0][0])

		dif, diffType, err = dfr.Next(ctx)
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		_, _, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, diffType, err = dfr.split(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, diffType)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.EndKey))
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From))
			assert.Nil(t, dif.To)
			dif, diffType, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToHasSuffix", func(t *testing.T) {
		// Some rows were added to the end.
		// We expect this to present as one or more ModifiedDiffs at the end that covers all the added rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		toTups, desc := AscendingUintTuples(1024)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		fromTups := make([][2]val.Tuple, 512)
		copy(fromTups, toTups)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		require.NoError(t, err)

		// At least one ModifiedDiff. The previous key is less than the first added key, and the end key is greater.
		dif, diffType, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, ModifiedDiff, diffType)
		assert.NotNil(t, dif.KeyBelowStart)
		assert.Equal(t, dfr.order.Compare(ctx, val.Tuple(dif.KeyBelowStart), fromTups[511][0]), -1)
		assert.Equal(t, dfr.order.Compare(ctx, val.Tuple(dif.EndKey), fromTups[511][0]), 1)

		KeyBelowStart := dif.EndKey
		dif, diffType, err = dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, AddedDiff, diffType)
			assert.Equal(t, KeyBelowStart, dif.KeyBelowStart)

			KeyBelowStart = dif.EndKey
			dif, diffType, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		dif, diffType, err = dfr.Next(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			for dif.Level > 0 {
				dif, diffType, err = dfr.split(ctx)
				require.NoError(t, err)
			}
			require.NotNil(t, dif)
			if !assert.Equal(t, AddedDiff, diffType) {
				panic(1)
			}
			assert.Equal(t, toTups[512+i][0], val.Tuple(dif.EndKey))
			assert.Equal(t, toTups[512+i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
			dif, diffType, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToHasPrefix", func(t *testing.T) {
		// Some rows were added to the beginning.
		// We expect this to present as one or more ModifiedDiffs at the start that covers all the added rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		toTups, desc := AscendingUintTuples(1024)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		fromTups := make([][2]val.Tuple, 512)
		copy(fromTups, toTups[512:])
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		require.NoError(t, err)

		// At least one ModifiedDiff. The previous key is nil because it comes before any other observed keys, and the end key is greater.
		dif, diffType, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, ModifiedDiff, diffType)
		assert.Nil(t, dif.KeyBelowStart, Item(nil))

		KeyBelowStart := dif.EndKey
		dif, diffType, err = dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, ModifiedDiff, diffType)
			assert.Equal(t, KeyBelowStart, dif.KeyBelowStart)

			KeyBelowStart = dif.EndKey
			dif, diffType, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		dif, diffType, err = dfr.Next(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			for dif.Level > 0 {
				dif, diffType, err = dfr.split(ctx)
				require.NoError(t, err)
			}
			require.NotNil(t, dif)
			if !assert.Equal(t, AddedDiff, diffType) {
				panic(1)
			}
			assert.Equal(t, toTups[i][0], val.Tuple(dif.EndKey))
			assert.Equal(t, toTups[i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
			dif, diffType, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToIsEmpty", func(t *testing.T) {
		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		chunkBoundaries := []uint32{414, 718, 830}
		maxKey := uint32(1023)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 0)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		require.NoError(t, err)

		var KeyBelowStart Item
		dif, diffType, err := dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, diffType)
			assert.Nil(t, dif.To)
			assert.Equal(t, KeyBelowStart, dif.KeyBelowStart)
			KeyBelowStart = dif.EndKey
			dif, diffType, err = dfr.Next(ctx)
		}

		assert.Equal(t, KeyBelowStart, Item(fromTups[1023][0]))
		assert.Equal(t, io.EOF, err)

		// Range splits into level 0 removes until the next chunk boundary, then level 1 removes.
		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		_, _, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, diffType, err = dfr.split(ctx)
		for i := uint32(0); i <= chunkBoundaries[0]; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif, "%d", i)
			require.Equal(t, RemovedDiff, diffType)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.EndKey), "%d", i)
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From), "%d", i)
			assert.Nil(t, dif.To)
			dif, diffType, err = dfr.Next(ctx)
		}

		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Equal(t, 1, dif.Level)
		endKey, ok := desc.GetUint32(0, val.Tuple(dif.EndKey))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[1], endKey)
		startKey, ok := desc.GetUint32(0, val.Tuple(dif.KeyBelowStart))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[0], startKey)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Equal(t, 1, dif.Level)
		endKey, ok = desc.GetUint32(0, val.Tuple(dif.EndKey))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[2], endKey)
		startKey, ok = desc.GetUint32(0, val.Tuple(dif.KeyBelowStart))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[1], startKey)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		require.NotNil(t, dif)
		require.Equal(t, RemovedDiff, diffType)
		require.Equal(t, 1, dif.Level)
		endKey, ok = desc.GetUint32(0, val.Tuple(dif.EndKey))
		require.True(t, ok)
		require.Equal(t, maxKey, endKey)
		startKey, ok = desc.GetUint32(0, val.Tuple(dif.KeyBelowStart))
		require.True(t, ok)
		require.Equal(t, chunkBoundaries[2], startKey)
		require.Nil(t, dif.To)
		dif, diffType, err = dfr.Next(ctx)

		assert.Equal(t, io.EOF, err)
	})

	t.Run("FromIsEmpty", func(t *testing.T) {
		ctx := context.Background()
		ns := NewTestNodeStore()

		toTups, desc := AscendingUintTuples(1024)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		fromTups := make([][2]val.Tuple, 0)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		dfr := PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)

		var KeyBelowStart Item

		// Range and then EOF
		dif, diffType, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, dif.KeyBelowStart)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, AddedDiff, diffType)
			assert.Equal(t, KeyBelowStart, dif.KeyBelowStart)
			KeyBelowStart = dif.EndKey
			dif, diffType, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		dfr = PatchGeneratorFromRoots(ctx, ns, ns, fromRoot, toRoot, desc)
		for i := 0; i < 1024; i++ {
			dif, diffType, err = dfr.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, dif)
			for dif.Level > 0 {
				dif, diffType, err = dfr.split(ctx)
				require.NoError(t, err)
				require.NotNil(t, dif)
			}
			require.Equal(t, AddedDiff, diffType)
			assert.Equal(t, toTups[i][0], val.Tuple(dif.EndKey))
			assert.Equal(t, toTups[i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
		}

		_, _, err = dfr.Next(ctx)
		assert.Equal(t, io.EOF, err)
	})
}
