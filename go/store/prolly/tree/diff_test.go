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

package tree

import (
	"context"
	"github.com/stretchr/testify/require"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/val"
)

// TestDifferFromRoots tests the DifferFromRoots function, very minimally. We don't have any direct tests of this
// method, and when developing the layerDifferFromRoots method I wanted to verify some assumptions.
// TODO - test DifferFromRoots more thoroughly.
func TestDifferFromRoots(t *testing.T) {
	ctx := sql.NewEmptyContext()
	ns := NewTestNodeStore()

	fromTups, desc := AscendingUintTuples(1234)
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

	dfr, err := DifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
	assert.NoError(t, err)

	dif, err := dfr.Next(ctx)
	assert.NoError(t, err)
	assert.NotNil(t, dif)
	assert.Equal(t, fromTups[23][0], val.Tuple(dif.Key))
	assert.Equal(t, fromTups[23][1], val.Tuple(dif.From))
	assert.Equal(t, toTups[23][1], val.Tuple(dif.To))
	assert.Equal(t, ModifiedDiff, dif.Type)

	dif, err = dfr.Next(ctx)
	assert.Equal(t, io.EOF, err)
}

func TestRangeDifferFromRoots(t *testing.T) {
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

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		assert.NoError(t, err)

		// Range and then EOF
		dif, err := dfr.Next(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dif)
		assert.Equal(t, RangeDiff, dif.Type)

		dif, err = dfr.Next(ctx)
		require.ErrorIs(t, err, io.EOF)

		// Range and split visits the one different tuple.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)
		_, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, err = dfr.split(ctx)
		assert.NoError(t, err)
		assert.NotNil(t, dif)
		assert.Equal(t, ModifiedDiff, dif.Type)
		assert.Equal(t, fromTups[23][0], val.Tuple(dif.Key))
		assert.Equal(t, fromTups[23][1], val.Tuple(dif.From))
		assert.Equal(t, toTups[23][1], val.Tuple(dif.To))

		dif, err = dfr.Next(ctx)
		assert.Equal(t, io.EOF, err)
	})
	t.Run("FromHasSuffix", func(t *testing.T) {
		// One or more rows at the end were deleted.
		// We expect that to manifest as a RangeDiff for the new final node,
		// Followed by one or more RangeRemovedDiffs

		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 512)
		copy(toTups, fromTups)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		dif, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RangeDiff, dif.Type)
		require.NotNil(t, dif.To)

		// One or more RangeRemovedDiff and then EOF
		dif, err = dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RangeRemovedDiff, dif.Type)
		require.Nil(t, dif.To)
		dif, err = dfr.Next(ctx)

		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RangeRemovedDiff, dif.Type)
			require.Nil(t, dif.To)
			dif, err = dfr.Next(ctx)
		}

		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)
		_, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, err = dfr.split(ctx)
		for i := 512; i < 1024; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, dif.Type)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.Key))
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From))
			assert.Nil(t, dif.To)
			dif, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("FromHasPrefix", func(t *testing.T) {
		// Some rows were deleted from the beginning.
		// We expect this to present as a single RangeDiff at the start that covers all the removed rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 512)
		copy(toTups, fromTups[512:])
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		// Range and then EOF
		dif, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RangeDiff, dif.Type)
		assert.Equal(t, dif.PreviousKey, Item(nil))
		assert.GreaterOrEqual(t, dif.Key, toTups[0][0])

		dif, err = dfr.Next(ctx)
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)
		_, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, err = dfr.split(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, dif.Type)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.Key))
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From))
			assert.Nil(t, dif.To)
			dif, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToHasSuffix", func(t *testing.T) {
		// Some rows were added to the end.
		// We expect this to present as one or more RangeDiffs at the end that covers all the added rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		toTups, desc := AscendingUintTuples(1024)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		fromTups := make([][2]val.Tuple, 512)
		copy(fromTups, toTups)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		// At least one RangeDiff. The previous key is less than the first added key, and the end key is greater.
		dif, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RangeDiff, dif.Type)
		assert.NotNil(t, dif.PreviousKey)
		assert.Equal(t, dfr.order.Compare(ctx, val.Tuple(dif.PreviousKey), fromTups[511][0]), -1)
		assert.Equal(t, dfr.order.Compare(ctx, val.Tuple(dif.Key), fromTups[511][0]), 1)

		previousKey := dif.Key
		dif, err = dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RangeDiff, dif.Type)
			assert.Equal(t, previousKey, dif.PreviousKey)

			previousKey = dif.Key
			dif, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		dif, err = dfr.Next(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			for dif.Level > 0 {
				dif, err = dfr.split(ctx)
				require.NoError(t, err)
			}
			require.NotNil(t, dif)
			if !assert.Equal(t, AddedDiff, dif.Type) {
				panic(1)
			}
			assert.Equal(t, toTups[512+i][0], val.Tuple(dif.Key))
			assert.Equal(t, toTups[512+i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
			dif, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToHasPrefix", func(t *testing.T) {
		// Some rows were added to the beginning.
		// We expect this to present as one or more RangeDiffs at the start that covers all the added rows.
		ctx := context.Background()
		ns := NewTestNodeStore()

		toTups, desc := AscendingUintTuples(1024)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		fromTups := make([][2]val.Tuple, 512)
		copy(fromTups, toTups[512:])
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		// At least one RangeDiff. The previous key is nil because it comes before any other observed keys, and the end key is greater.
		dif, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.NotNil(t, dif)
		require.Equal(t, RangeDiff, dif.Type)
		assert.Nil(t, dif.PreviousKey, Item(nil))

		previousKey := dif.Key
		dif, err = dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RangeDiff, dif.Type)
			assert.Equal(t, previousKey, dif.PreviousKey)

			previousKey = dif.Key
			dif, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		// Range splits into all 512 removes.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		dif, err = dfr.Next(ctx)
		for i := 0; i < 512; i++ {
			require.NoError(t, err)
			for dif.Level > 0 {
				dif, err = dfr.split(ctx)
				require.NoError(t, err)
			}
			require.NotNil(t, dif)
			if !assert.Equal(t, AddedDiff, dif.Type) {
				panic(1)
			}
			assert.Equal(t, toTups[i][0], val.Tuple(dif.Key))
			assert.Equal(t, toTups[i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
			dif, err = dfr.Next(ctx)
		}

		assert.Equal(t, io.EOF, err)
	})
	t.Run("ToIsEmpty", func(t *testing.T) {
		ctx := context.Background()
		ns := NewTestNodeStore()

		fromTups, desc := AscendingUintTuples(1024)
		fromRoot, err := MakeTreeForTest(fromTups)
		assert.NoError(t, err)

		toTups := make([][2]val.Tuple, 0)
		toRoot, err := MakeTreeForTest(toTups)
		assert.NoError(t, err)

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		var previousKey Item
		dif, err := dfr.Next(ctx)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RangeRemovedDiff, dif.Type)
			assert.Nil(t, dif.To)
			assert.Equal(t, previousKey, dif.PreviousKey)
			previousKey = dif.Key
			dif, err = dfr.Next(ctx)
		}

		assert.Equal(t, previousKey, Item(fromTups[1023][0]))
		assert.Equal(t, io.EOF, err)

		// Range splits into all 1024 removes.
		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)
		_, err = dfr.Next(ctx)
		require.NoError(t, err)

		dif, err = dfr.split(ctx)
		for i := 0; i < 1024; i++ {
			require.NoError(t, err)
			require.NotNil(t, dif)
			require.Equal(t, RemovedDiff, dif.Type)
			assert.Equal(t, fromTups[i][0], val.Tuple(dif.Key))
			assert.Equal(t, fromTups[i][1], val.Tuple(dif.From))
			assert.Nil(t, dif.To)
			dif, err = dfr.Next(ctx)
		}

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

		dfr, err := RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)

		var previousKey Item

		// Range and then EOF
		dif, err := dfr.Next(ctx)
		require.NoError(t, err)
		require.Nil(t, dif.PreviousKey)
		for err == nil {
			require.NotNil(t, dif)
			require.Equal(t, RangeDiff, dif.Type)
			assert.Equal(t, previousKey, dif.PreviousKey)
			previousKey = dif.Key
			dif, err = dfr.Next(ctx)
		}
		require.ErrorIs(t, err, io.EOF)

		dfr, err = RangeDifferFromRoots(ctx, ns, ns, fromRoot, toRoot, desc, false)
		require.NoError(t, err)
		for i := 0; i < 1024; i++ {
			dif, err = dfr.Next(ctx)
			require.NoError(t, err)
			require.NotNil(t, dif)
			for dif.Level > 0 {
				dif, err = dfr.split(ctx)
				require.NoError(t, err)
				require.NotNil(t, dif)
			}
			require.Equal(t, AddedDiff, dif.Type)
			assert.Equal(t, toTups[i][0], val.Tuple(dif.Key))
			assert.Equal(t, toTups[i][1], val.Tuple(dif.To))
			assert.Nil(t, dif.From)
		}

		_, err = dfr.Next(ctx)
		assert.Equal(t, io.EOF, err)
	})
}
