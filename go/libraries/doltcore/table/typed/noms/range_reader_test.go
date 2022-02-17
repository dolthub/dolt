// Copyright 2019 Dolthub, Inc.
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

package noms

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

var rangeReaderTests = []struct {
	name       string
	ranges     []*ReadRange
	expectKeys []int64
}{
	{
		"test range ending at",
		[]*ReadRange{NewRangeEndingAt(mustTuple(10), greaterThanCheck(2))},
		[]int64{10, 8, 6, 4},
	},
	{
		"test range ending before",
		[]*ReadRange{NewRangeEndingBefore(mustTuple(10), greaterThanCheck(2))},
		[]int64{8, 6, 4},
	},
	{
		"test range starting at",
		[]*ReadRange{NewRangeStartingAt(mustTuple(10), lessThanCheck(20))},
		[]int64{10, 12, 14, 16, 18},
	},
	{
		"test range starting after",
		[]*ReadRange{NewRangeStartingAfter(mustTuple(10), lessThanCheck(20))},
		[]int64{12, 14, 16, 18},
	},
	{
		"test range iterating to the end",
		[]*ReadRange{NewRangeStartingAt(mustTuple(100), lessThanCheck(200))},
		[]int64{100},
	},
	{
		"test multiple ranges",
		[]*ReadRange{
			NewRangeEndingBefore(mustTuple(10), greaterThanCheck(2)),
			NewRangeStartingAt(mustTuple(10), lessThanCheck(20)),
		},
		[]int64{8, 6, 4, 10, 12, 14, 16, 18},
	},
	{
		"test empty range starting after",
		[]*ReadRange{NewRangeStartingAfter(mustTuple(100), lessThanCheck(200))},
		[]int64(nil),
	},
	{
		"test empty range starting at",
		[]*ReadRange{NewRangeStartingAt(mustTuple(101), lessThanCheck(200))},
		[]int64(nil),
	},
	{
		"test empty range ending before",
		[]*ReadRange{NewRangeEndingBefore(mustTuple(0), greaterThanCheck(-100))},
		[]int64(nil),
	},
	{
		"test empty range ending at",
		[]*ReadRange{NewRangeEndingAt(mustTuple(-1), greaterThanCheck(-100))},
		[]int64(nil),
	},
}

func mustTuple(id int64) types.Tuple {
	t, err := types.NewTuple(types.Format_Default, types.Uint(pkTag), types.Int(id))

	if err != nil {
		panic(err)
	}

	return t
}

func TestRangeReader(t *testing.T) {
	ctx := context.Background()
	colColl := schema.NewColCollection(
		schema.NewColumn("id", pkTag, types.IntKind, true),
		schema.NewColumn("val", valTag, types.IntKind, false))

	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	storage := &chunks.MemoryStorage{}
	vrw := types.NewValueStore(storage.NewView())
	m, err := types.NewMap(ctx, vrw)
	assert.NoError(t, err)

	me := m.Edit()
	for i := 0; i <= 100; i += 2 {
		k, err := types.NewTuple(vrw.Format(), types.Uint(pkTag), types.Int(i))
		require.NoError(t, err)

		v, err := types.NewTuple(vrw.Format(), types.Uint(valTag), types.Int(100-i))
		require.NoError(t, err)

		me.Set(k, v)
	}

	m, err = me.Map(ctx)
	assert.NoError(t, err)

	for _, test := range rangeReaderTests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rd := NewNomsRangeReader(sch, m, test.ranges)

			var keys []int64
			for {
				r, err := rd.ReadRow(ctx)

				if err == io.EOF {
					break
				}

				assert.NoError(t, err)
				col0, ok := r.GetColVal(0)
				assert.True(t, ok)

				keys = append(keys, int64(col0.(types.Int)))
			}

			err = rd.Close(ctx)
			assert.NoError(t, err)

			assert.Equal(t, test.expectKeys, keys)
		})
	}
}

func TestRangeReaderOnEmptyMap(t *testing.T) {
	ctx := context.Background()
	colColl := schema.NewColCollection(
		schema.NewColumn("id", pkTag, types.IntKind, true),
		schema.NewColumn("val", valTag, types.IntKind, false))

	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	storage := &chunks.MemoryStorage{}
	vrw := types.NewValueStore(storage.NewView())
	m, err := types.NewMap(ctx, vrw)
	assert.NoError(t, err)

	for _, test := range rangeReaderTests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rd := NewNomsRangeReader(sch, m, test.ranges)

			r, err := rd.ReadRow(ctx)
			assert.Equal(t, io.EOF, err)
			assert.Nil(t, r)
		})
	}
}

type greaterThanCheck int64

func (n greaterThanCheck) Check(ctx context.Context, k types.Tuple) (valid bool, skip bool, err error) {
	col0, err := k.Get(1)

	if err != nil {
		panic(err)
	}

	return int64(col0.(types.Int)) > int64(n), false, nil
}

type lessThanCheck int64

func (n lessThanCheck) Check(ctx context.Context, k types.Tuple) (valid bool, skip bool, err error) {
	col0, err := k.Get(1)

	if err != nil {
		panic(err)
	}

	return int64(col0.(types.Int)) < int64(n), false, nil
}
