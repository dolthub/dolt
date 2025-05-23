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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type nomsRowMergeTest struct {
	name                  string
	row, mergeRow, ancRow types.Value
	sch                   schema.Schema
	expectedResult        types.Value
	expectCellMerge       bool
	expectConflict        bool
}

type rowMergeTest struct {
	name                                  string
	row, mergeRow, ancRow                 val.Tuple
	mergedSch, leftSch, rightSch, baseSch schema.Schema
	expectedResult                        val.Tuple
	expectCellMerge                       bool
	expectConflict                        bool
}

type testCase struct {
	name                     string
	row, mergeRow, ancRow    []*int
	rowCnt, mRowCnt, aRowCnt int
	expectedResult           []*int
	expectCellMerge          bool
	expectConflict           bool
}

// 0 is nil, negative value is invalid
func build(ints ...int) []*int {
	out := make([]*int, len(ints))
	for i, v := range ints {
		if v < 0 {
			panic("invalid")
		}
		if v == 0 {
			continue
		}
		t := v
		out[i] = &t
	}
	return out
}

var convergentEditCases = []testCase{
	{
		"add same row",
		build(1, 2),
		build(1, 2),
		nil,
		2, 2, 2,
		build(1, 2),
		false,
		false,
	},
	{
		"both delete row",
		nil,
		nil,
		build(1, 2),
		2, 2, 2,
		nil,
		false,
		false,
	},
	{
		"modify row to equal value",
		build(2, 2),
		build(2, 2),
		build(1, 1),
		2, 2, 2,
		build(2, 2),
		false,
		false,
	},
}

var testCases = []testCase{
	{
		"insert different rows",
		build(1, 2),
		build(2, 3),
		nil,
		2, 2, 2,
		nil,
		false,
		true,
	},
	{
		"delete a row in one, and modify it in other",
		nil,
		build(1, 3),
		build(1, 2),
		2, 2, 2,
		nil,
		false,
		true,
	},
	{
		"modify rows without overlap",
		build(2, 1),
		build(1, 2),
		build(1, 1),
		2, 2, 2,
		build(2, 2),
		true,
		false,
	},
	{
		"modify rows with equal overlapping changes",
		build(2, 2, 255),
		build(2, 3, 255),
		build(1, 2, 0),
		3, 3, 3,
		build(2, 3, 255),
		true,
		false,
	},
	{
		"modify rows with differing overlapping changes",
		build(2, 2, 128),
		build(1, 3, 255),
		build(1, 2, 0),
		3, 3, 3,
		nil,
		false,
		true,
	},
	{
		"modify rows where one adds a column",
		build(2, 2),
		build(1, 3, 255),
		build(1, 2),
		2, 3, 2,
		build(2, 3, 255),
		true,
		false,
	},
	{
		"modify rows where one drops a column",
		build(1, 2, 1),
		build(2, 1),
		build(1, 1, 1),
		3, 2, 3,
		build(2, 2),
		true,
		false,
	},
	// TODO (dhruv): need to fix this test case for new storage format
	//{
	//	"add rows but one holds a new column",
	//	build(1, 1),
	//	build(1, 1, 1),
	//	nil,
	//	2, 3, 2,
	//	build(1, 1, 1),
	//	true,
	//	false,
	//},
	{
		"Delete a row in one, set all null in the other",
		build(0, 0, 0), // build translates zeros into NULL values
		nil,
		build(1, 1, 1),
		3, 3, 3,
		nil,
		false,
		true,
	},
}

func TestRowMerge(t *testing.T) {
	if types.Format_Default != types.Format_DOLT {
		t.Skip()
	}

	ctx := sql.NewEmptyContext()

	tests := make([]rowMergeTest, len(testCases))
	for i, t := range testCases {
		tests[i] = createRowMergeStruct(t)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := NewValueMerger(test.mergedSch, test.leftSch, test.rightSch, test.baseSch, syncPool, nil)

			merged, ok, err := v.TryMerge(ctx, test.row, test.mergeRow, test.ancRow)
			assert.NoError(t, err)
			assert.Equal(t, test.expectConflict, !ok)
			vD := test.mergedSch.GetValueDescriptor(v.ns)
			assert.Equal(t, vD.Format(ctx, test.expectedResult), vD.Format(ctx, merged))
		})
	}
}

func TestNomsRowMerge(t *testing.T) {
	if types.Format_Default == types.Format_DOLT {
		t.Skip()
	}

	testCases := append(testCases, convergentEditCases...)
	tests := make([]nomsRowMergeTest, len(testCases))
	for i, t := range testCases {
		tests[i] = createNomsRowMergeStruct(t)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			rowMergeResult, err := nomsPkRowMerge(context.Background(), types.Format_Default, test.sch, test.row, test.mergeRow, test.ancRow)
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

func createRowMergeStruct(t testCase) rowMergeTest {
	mergedSch := calcMergedSchema(t)
	leftSch := calcSchema(t.rowCnt)
	rightSch := calcSchema(t.mRowCnt)
	baseSch := calcSchema(t.aRowCnt)

	tpl := buildTup(leftSch, t.row)
	mergeTpl := buildTup(rightSch, t.mergeRow)
	ancTpl := buildTup(baseSch, t.ancRow)
	expectedTpl := buildTup(mergedSch, t.expectedResult)
	return rowMergeTest{
		t.name,
		tpl, mergeTpl, ancTpl,
		mergedSch, leftSch, rightSch, baseSch,
		expectedTpl,
		t.expectCellMerge,
		t.expectConflict}
}

func createNomsRowMergeStruct(t testCase) nomsRowMergeTest {
	sch := calcMergedSchema(t)

	tpl := valsToTestTupleWithPks(toVals(t.row))
	mergeTpl := valsToTestTupleWithPks(toVals(t.mergeRow))
	ancTpl := valsToTestTupleWithPks(toVals(t.ancRow))
	expectedTpl := valsToTestTupleWithPks(toVals(t.expectedResult))
	return nomsRowMergeTest{t.name, tpl, mergeTpl, ancTpl, sch, expectedTpl, t.expectCellMerge, t.expectConflict}
}

func calcMergedSchema(t testCase) schema.Schema {
	longest := t.rowCnt
	if t.mRowCnt > longest {
		longest = t.mRowCnt
	}
	if t.aRowCnt > longest {
		longest = t.aRowCnt
	}

	return calcSchema(longest)
}

func calcSchema(nCols int) schema.Schema {
	cols := make([]schema.Column, nCols+1)
	// Schema needs a primary key to be valid, but all the logic being tested works only on the non-key columns.
	cols[0] = schema.NewColumn("primaryKey", 0, types.IntKind, true)
	for i := 0; i < nCols; i++ {
		tag := i + 1
		cols[tag] = schema.NewColumn(strconv.FormatInt(int64(tag), 10), uint64(tag), types.IntKind, false)
	}

	colColl := schema.NewColCollection(cols...)
	sch := schema.MustSchemaFromCols(colColl)
	return sch
}

func buildTup(sch schema.Schema, r []*int) val.Tuple {
	if r == nil {
		return nil
	}

	vD := sch.GetValueDescriptor(nil)
	vB := val.NewTupleBuilder(vD, nil)
	for i, v := range r {
		if v != nil {
			vB.PutInt64(i, int64(*v))
		}
	}
	tup, err := vB.Build(syncPool)
	if err != nil {
		panic(err)
	}
	return tup
}

func toVals(ints []*int) []types.Value {
	if ints == nil {
		return nil
	}

	v := make([]types.Value, len(ints))
	for i, d := range ints {
		if d == nil {
			v[i] = types.NullValue
			continue
		}

		v[i] = types.Int(*d)
	}
	return v
}
