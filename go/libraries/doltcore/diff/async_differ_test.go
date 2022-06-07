// Copyright 2021 Dolthub, Inc.
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

package diff

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dtu "github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/types"
)

func TestAsyncDiffer(t *testing.T) {
	ctx := context.Background()
	storage := &chunks.MemoryStorage{}
	vrw := types.NewValueStore(storage.NewView())

	vals := []types.Value{
		types.Uint(0), types.String("a"),
		types.Uint(1), types.String("b"),
		types.Uint(3), types.String("d"),
		types.Uint(4), types.String("e"),
		types.Uint(6), types.String("g"),
		types.Uint(7), types.String("h"),
		types.Uint(9), types.String("j"),
		types.Uint(10), types.String("k"),
		types.Uint(12), types.String("m"),
		types.Uint(13), types.String("n"),
		types.Uint(15), types.String("p"),
		types.Uint(16), types.String("q"),
		types.Uint(18), types.String("s"),
		types.Uint(19), types.String("t"),
		types.Uint(21), types.String("v"),
		types.Uint(22), types.String("w"),
		types.Uint(24), types.String("y"),
		types.Uint(25), types.String("z"),
	}

	m1, err := types.NewMap(ctx, vrw, vals...)
	require.NoError(t, err)

	vals = []types.Value{
		types.Uint(0), types.String("a"), // unchanged
		//types.Uint(1), types.String("b"),		// deleted
		types.Uint(2), types.String("c"), // added
		types.Uint(3), types.String("d"), // unchanged
		//types.Uint(4), types.String("e"),		// deleted
		types.Uint(5), types.String("f"), // added
		types.Uint(6), types.String("g"), // unchanged
		//types.Uint(7), types.String("h"),		// deleted
		types.Uint(8), types.String("i"), // added
		types.Uint(9), types.String("j"), // unchanged
		//types.Uint(10), types.String("k"),	// deleted
		types.Uint(11), types.String("l"), // added
		types.Uint(12), types.String("m2"), // changed
		//types.Uint(13), types.String("n"),	// deleted
		types.Uint(14), types.String("o"), // added
		types.Uint(15), types.String("p2"), // changed
		//types.Uint(16), types.String("q"),	// deleted
		types.Uint(17), types.String("r"), // added
		types.Uint(18), types.String("s2"), // changed
		//types.Uint(19), types.String("t"),	// deleted
		types.Uint(20), types.String("u"), // added
		types.Uint(21), types.String("v2"), // changed
		//types.Uint(22), types.String("w"),	// deleted
		types.Uint(23), types.String("x"), // added
		types.Uint(24), types.String("y2"), // changed
		//types.Uint(25), types.String("z"),	// deleted
	}
	m2, err := types.NewMap(ctx, vrw, vals...)
	require.NoError(t, err)

	tests := []struct {
		name           string
		createdStarted func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer
		expectedStats  map[types.DiffChangeType]uint64
	}{
		{
			name: "iter all",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				ad.Start(ctx, m1, m2)
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 5,
				types.DiffChangeAdded:    8,
				types.DiffChangeRemoved:  9,
			},
		},

		{
			name: "iter range starting with nil",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				ad.StartWithRange(ctx, m1, m2, nil, func(ctx context.Context, value types.Value) (bool, bool, error) {
					return true, false, nil
				})
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 5,
				types.DiffChangeAdded:    8,
				types.DiffChangeRemoved:  9,
			},
		},

		{
			name: "iter range staring with Null Value",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				ad.StartWithRange(ctx, m1, m2, types.NullValue, func(ctx context.Context, value types.Value) (bool, bool, error) {
					return true, false, nil
				})
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 5,
				types.DiffChangeAdded:    8,
				types.DiffChangeRemoved:  9,
			},
		},

		{
			name: "iter range less than 17",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				end := types.Uint(27)
				ad.StartWithRange(ctx, m1, m2, types.NullValue, func(ctx context.Context, value types.Value) (bool, bool, error) {
					valid, err := value.Less(m1.Format(), end)
					return valid, false, err
				})
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 5,
				types.DiffChangeAdded:    8,
				types.DiffChangeRemoved:  9,
			},
		},

		{
			name: "iter range less than 15",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				end := types.Uint(15)
				ad.StartWithRange(ctx, m1, m2, types.NullValue, func(ctx context.Context, value types.Value) (bool, bool, error) {
					valid, err := value.Less(m1.Format(), end)
					return valid, false, err
				})
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 1,
				types.DiffChangeAdded:    5,
				types.DiffChangeRemoved:  5,
			},
		},

		{
			name: "iter range 10 < 15",
			createdStarted: func(ctx context.Context, m1, m2 types.Map) *AsyncDiffer {
				ad := NewAsyncDiffer(4)
				start := types.Uint(10)
				end := types.Uint(15)
				ad.StartWithRange(ctx, m1, m2, start, func(ctx context.Context, value types.Value) (bool, bool, error) {
					valid, err := value.Less(m1.Format(), end)
					return valid, false, err
				})
				return ad
			},
			expectedStats: map[types.DiffChangeType]uint64{
				types.DiffChangeModified: 1,
				types.DiffChangeAdded:    2,
				types.DiffChangeRemoved:  2,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			ad := test.createdStarted(ctx, m1, m2)
			err := readAll(ad)
			require.NoError(t, err)
			require.Equal(t, test.expectedStats, ad.diffStats)
		})
	}

	t.Run("can close without reading all", func(t *testing.T) {
		ad := NewAsyncDiffer(1)
		ad.Start(ctx, m1, m2)
		res, more, err := ad.GetDiffs(1, -1)
		require.NoError(t, err)
		assert.True(t, more)
		assert.Len(t, res, 1)
		err = ad.Close()
		assert.NoError(t, err)
	})

	t.Run("can filter based on change type", func(t *testing.T) {
		ad := NewAsyncDiffer(20)
		ad.Start(ctx, m1, m2)
		res, more, err := ad.GetDiffs(10, -1)
		require.NoError(t, err)
		assert.True(t, more)
		assert.Len(t, res, 10)
		err = ad.Close()
		assert.NoError(t, err)

		ad = NewAsyncDiffer(20)
		ad.Start(ctx, m1, m2)
		res, more, err = ad.GetDiffsWithFilter(10, 20*time.Second, types.DiffChangeModified)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Len(t, res, 5)
		err = ad.Close()
		assert.NoError(t, err)

		ad = NewAsyncDiffer(20)
		ad.Start(ctx, m1, m2)
		res, more, err = ad.GetDiffsWithFilter(6, -1, types.DiffChangeAdded)
		require.NoError(t, err)
		assert.True(t, more)
		assert.Len(t, res, 6)
		err = ad.Close()
		assert.NoError(t, err)
	})

	k1Row1Vals := []types.Value{c1Tag, types.Uint(3), c2Tag, types.String("d")}
	k1Vals, err := getKeylessRow(ctx, k1Row1Vals)
	assert.NoError(t, err)
	k1, err := types.NewMap(ctx, vrw, k1Vals...)
	assert.NoError(t, err)

	// Delete one row, add two rows
	k2Row1Vals := []types.Value{c1Tag, types.Uint(4), c2Tag, types.String("d")}
	k2Vals1, err := getKeylessRow(ctx, k2Row1Vals)
	assert.NoError(t, err)
	k2Row2Vals := []types.Value{c1Tag, types.Uint(1), c2Tag, types.String("e")}
	k2Vals2, err := getKeylessRow(ctx, k2Row2Vals)
	assert.NoError(t, err)
	k2Vals := append(k2Vals1, k2Vals2...)
	k2, err := types.NewMap(ctx, vrw, k2Vals...)
	require.NoError(t, err)

	t.Run("can diff and filter keyless tables", func(t *testing.T) {
		kd := &keylessDiffer{AsyncDiffer: NewAsyncDiffer(20)}
		kd.Start(ctx, k1, k2)
		res, more, err := kd.GetDiffs(10, 20*time.Second)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Len(t, res, 3)
		err = kd.Close()
		assert.NoError(t, err)

		kd = &keylessDiffer{AsyncDiffer: NewAsyncDiffer(20)}
		kd.Start(ctx, k1, k2)
		res, more, err = kd.GetDiffsWithFilter(10, 20*time.Second, types.DiffChangeModified)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Len(t, res, 0)
		err = kd.Close()
		assert.NoError(t, err)

		kd = &keylessDiffer{AsyncDiffer: NewAsyncDiffer(20)}
		kd.Start(ctx, k1, k2)
		res, more, err = kd.GetDiffsWithFilter(6, -1, types.DiffChangeAdded)
		require.NoError(t, err)
		assert.False(t, more)
		assert.Len(t, res, 2)
		err = kd.Close()
		assert.NoError(t, err)
	})
}

func readAll(ad *AsyncDiffer) error {
	for {
		_, more, err := ad.GetDiffs(10, -1)

		if err != nil {
			return err
		}

		if !more {
			break
		}
	}

	return nil
}

var c1Tag = types.Uint(1)
var c2Tag = types.Uint(2)
var cardTag = types.Uint(schema.KeylessRowCardinalityTag)
var rowIdTag = types.Uint(schema.KeylessRowIdTag)

func getKeylessRow(ctx context.Context, vals []types.Value) ([]types.Value, error) {
	nbf, err := types.GetFormatForVersionString(constants.FormatDefaultString)
	if err != nil {
		return []types.Value{}, err
	}

	id1, err := types.UUIDHashedFromValues(nbf, vals...)
	if err != nil {
		return []types.Value{}, err
	}

	prefix := []types.Value{
		cardTag,
		types.Uint(1),
	}
	vals = append(prefix, vals...)

	return []types.Value{
		dtu.MustTuple(rowIdTag, id1),
		dtu.MustTuple(vals...),
	}, nil
}
