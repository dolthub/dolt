// Copyright 2020 Liquidata, Inc.
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

package table

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

func TestCompositeTableReader(t *testing.T) {
	const (
		numReaders        = 7
		numItemsPerReader = 7
	)

	ctx := context.Background()

	coll, err := schema.NewColCollection(
		schema.NewColumn("id", 0, types.UintKind, true, "", false, "", schema.NotNullConstraint{}),
		schema.NewColumn("val", 1, types.IntKind, false, "", false, ""),
	)
	require.NoError(t, err)
	sch := schema.SchemaFromCols(coll)

	var readers []TableReadCloser
	var expectedKeys []uint64
	var expectedVals []int64
	for i := 0; i < numReaders; i++ {
		var rows []row.Row
		for j := 0; j < numItemsPerReader; j++ {
			idx := j + (i * numItemsPerReader)
			expectedKeys = append(expectedKeys, uint64(idx))
			expectedVals = append(expectedVals, int64(idx))
			r, err := row.New(types.Format_Default, sch, row.TaggedValues{
				0: types.Uint(uint64(idx)),
				1: types.Int(idx),
			})
			require.NoError(t, err)
			rows = append(rows, r)
		}
		imt := NewInMemTableWithData(sch, rows)
		rd := NewInMemTableReader(imt)
		readers = append(readers, rd)
	}

	compositeRd, err := NewCompositeTableReader(readers)
	require.NoError(t, err)

	var keys []uint64
	var vals []int64
	for {
		r, err := compositeRd.ReadRow(ctx)

		if err == io.EOF {
			break
		}

		assert.NoError(t, err)
		val0, ok := r.GetColVal(0)
		assert.True(t, ok)
		val1, ok := r.GetColVal(1)
		assert.True(t, ok)

		keys = append(keys, uint64(val0.(types.Uint)))
		vals = append(vals, int64(val1.(types.Int)))
	}

	assert.Equal(t, expectedKeys, keys)
	assert.Equal(t, expectedVals, vals)

	err = compositeRd.Close(ctx)
	assert.NoError(t, err)
}
