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

package merge

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type RowMergeTest struct {
	name                  string
	row, mergeRow, ancRow types.Value
	sch                   schema.Schema
	expectedResult        types.Value
	expectCellMerge       bool
	expectConflict        bool
}

func TestRowMerge(t *testing.T) {
	tests := []RowMergeTest{
		createRowMergeStruct(
			"add same row",
			[]types.Value{types.String("one"), types.Int(2)},
			[]types.Value{types.String("one"), types.Int(2)},
			nil,
			[]types.Value{types.String("one"), types.Int(2)},
			false,
			false,
		),
		createRowMergeStruct(
			"add diff row",
			[]types.Value{types.String("one"), types.String("two")},
			[]types.Value{types.String("one"), types.String("three")},
			nil,
			nil,
			false,
			true,
		),
		createRowMergeStruct(
			"both delete row",
			nil,
			nil,
			[]types.Value{types.String("one"), types.Uint(2)},
			nil,
			false,
			false,
		),
		createRowMergeStruct(
			"one delete one modify",
			nil,
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(2)},
			nil,
			false,
			true,
		),
		createRowMergeStruct(
			"modify rows without overlap",
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(3)},
			[]types.Value{types.String("one"), types.Uint(2)},
			[]types.Value{types.String("two"), types.Uint(3)},
			true,
			false,
		),
		createRowMergeStruct(
			"modify rows with equal overlapping changes",
			[]types.Value{types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))},
			[]types.Value{types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			true,
			false,
		),
		createRowMergeStruct(
			"modify rows with differing overlapping changes",
			[]types.Value{types.String("two"), types.Uint(2), types.UUID(uuid.MustParse("99999999-9999-9999-9999-999999999999"))},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("00000000-0000-0000-0000-000000000000"))},
			nil,
			false,
			true,
		),
		createRowMergeStruct(
			"modify rows where one adds a column",
			[]types.Value{types.String("two"), types.Uint(2)},
			[]types.Value{types.String("one"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2)},
			[]types.Value{types.String("two"), types.Uint(3), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			true,
			false,
		),
		createRowMergeStruct(
			"modify row where values added in different columns",
			[]types.Value{types.String("one"), types.Uint(2), types.String(""), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff"))},
			[]types.Value{types.String("one"), types.Uint(2), types.UUID(uuid.MustParse("ffffffff-ffff-ffff-ffff-ffffffffffff")), types.String("")},
			[]types.Value{types.String("one"), types.Uint(2), types.NullValue, types.NullValue},
			nil,
			false,
			true,
		),
		createRowMergeStruct(
			"modify row where initial value wasn't given",
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.String("a")))},
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.String("b")))},
			[]types.Value{mustTuple(types.NewTuple(types.Format_Default, types.String("one"), types.Uint(2), types.NullValue))},
			nil,
			false,
			true,
		),
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowMergeResult, err := pkRowMerge(context.Background(), types.Format_Default, test.sch, test.row, test.mergeRow, test.ancRow)
			assert.NoError(t, err)
			assert.Equal(t, test.expectedResult, rowMergeResult.mergedRow,
				"expected "+mustString(types.EncodedValue(context.Background(), test.expectedResult))+
					"got "+mustString(types.EncodedValue(context.Background(), rowMergeResult.mergedRow)))
			assert.Equal(t, test.expectCellMerge, rowMergeResult.didCellMerge)
			assert.Equal(t, test.expectConflict, rowMergeResult.isConflict)
		})
	}
}

func valsToTestTupleWithoutPks(vals []types.Value) types.Value {
	return valsToTestTuple(vals, false)
}

func valsToTestTupleWithPks(vals []types.Value) types.Value {
	return valsToTestTuple(vals, true)
}

func valsToTestTuple(vals []types.Value, includePrimaryKeys bool) types.Value {
	if vals == nil {
		return nil
	}

	tplVals := make([]types.Value, 0, 2*len(vals))
	for i, val := range vals {
		if !types.IsNull(val) {
			tag := i
			// Assume one primary key tag, add 1 to all other tags
			if includePrimaryKeys {
				tag++
			}
			tplVals = append(tplVals, types.Uint(tag))
			tplVals = append(tplVals, val)
		}
	}

	return mustTuple(types.NewTuple(types.Format_Default, tplVals...))
}

func createRowMergeStruct(name string, vals, mergeVals, ancVals, expected []types.Value, expectCellMrg bool, expectCnf bool) RowMergeTest {
	longest := vals

	if len(mergeVals) > len(longest) {
		longest = mergeVals
	}

	if len(ancVals) > len(longest) {
		longest = ancVals
	}

	cols := make([]schema.Column, len(longest)+1)
	// Schema needs a primary key to be valid, but all the logic being tested works only on the non-key columns.
	cols[0] = schema.NewColumn("primaryKey", 0, types.IntKind, true)
	for i, val := range longest {
		tag := i + 1
		cols[tag] = schema.NewColumn(strconv.FormatInt(int64(tag), 10), uint64(tag), val.Kind(), false)
	}

	colColl := schema.NewColCollection(cols...)
	sch := schema.MustSchemaFromCols(colColl)

	tpl := valsToTestTupleWithPks(vals)
	mergeTpl := valsToTestTupleWithPks(mergeVals)
	ancTpl := valsToTestTupleWithPks(ancVals)
	expectedTpl := valsToTestTupleWithPks(expected)
	return RowMergeTest{name, tpl, mergeTpl, ancTpl, sch, expectedTpl, expectCellMrg, expectCnf}
}
