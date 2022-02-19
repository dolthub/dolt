// Copyright 2020 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	pkTag uint64 = iota
	valTag
)

func TestReaderForKeys(t *testing.T) {
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

	tests := []struct {
		name     string
		keys     []int
		expected []int
	}{
		{
			name:     "tens",
			keys:     []int{10, 20, 30, 40, 50, 60, 70, 80, 90},
			expected: []int{10, 20, 30, 40, 50, 60, 70, 80, 90},
		},
		{
			name:     "fives",
			keys:     []int{5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65, 70, 75, 80, 85, 90, 95},
			expected: []int{10, 20, 30, 40, 50, 60, 70, 80, 90},
		},
		{
			name:     "empty",
			keys:     []int{},
			expected: []int{},
		},
		{
			name:     "no keys that are in the map",
			keys:     []int{-5, -3, -1, 1, 3, 5, 102, 104, 106},
			expected: []int{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			rd := NewNomsMapReaderForKeys(m, sch, intKeysToTupleKeys(t, vrw.Format(), test.keys))

			var rows []row.Row
			for {
				r, err := rd.ReadRow(ctx)

				if err == io.EOF {
					break
				}

				assert.NoError(t, err)
				rows = append(rows, r)
			}

			testAgainstExpected(t, rows, test.expected)
			rd.Close(ctx)
		})
	}
}

func intKeysToTupleKeys(t *testing.T, nbf *types.NomsBinFormat, keys []int) []types.Tuple {
	tupleKeys := make([]types.Tuple, len(keys))

	for i, key := range keys {
		tuple, err := types.NewTuple(nbf, types.Uint(pkTag), types.Int(key))
		require.NoError(t, err)

		tupleKeys[i] = tuple
	}

	return tupleKeys
}

func testAgainstExpected(t *testing.T, rows []row.Row, expected []int) {
	assert.Equal(t, len(expected), len(rows))
	for i, r := range rows {
		k, ok := r.GetColVal(pkTag)
		require.True(t, ok)
		v, ok := r.GetColVal(valTag)
		require.True(t, ok)

		kn := int(k.(types.Int))
		vn := int(v.(types.Int))

		expectedK := expected[i]
		expectedV := 100 - expectedK

		assert.Equal(t, expectedK, kn)
		assert.Equal(t, expectedV, vn)
	}
}
