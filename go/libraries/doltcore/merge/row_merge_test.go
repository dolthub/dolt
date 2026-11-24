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

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

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
	// {
	//	"add rows but one holds a new column",
	//	build(1, 1),
	//	build(1, 1, 1),
	//	nil,
	//	2, 3, 2,
	//	build(1, 1, 1),
	//	true,
	//	false,
	// },
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
	ctx := newOwnedMergeContext()

	tests := make([]rowMergeTest, len(testCases))
	for i, t := range testCases {
		tests[i] = createRowMergeStruct(t)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			v := NewValueMerger(ctx, test.mergedSch, test.leftSch, test.rightSch, test.baseSch, syncPool, nil, "")

			merged, ok, err := v.TryMerge(ctx, test.row, test.mergeRow, test.ancRow)
			assert.NoError(t, err)
			assert.Equal(t, test.expectConflict, !ok)
			vD := test.mergedSch.GetValueDescriptor(v.ns)
			assert.Equal(t, vD.Format(ctx, test.expectedResult), vD.Format(ctx, merged))
		})
	}
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
	tup, err := vB.Build(context.Background(), syncPool)
	if err != nil {
		panic(err)
	}
	return tup
}

// intPtr returns a pointer to |v|, for building tuples with buildTup, which treats nil as NULL.
func intPtr(v int) *int { return &v }

// TestRowMergeConvergentAddColumn tests merging a column that both sides added with the same definition. In rows that
// existed before the column was added, both sides backfill the column with its default, so a side whose value still
// matches the default has not changed it.
func TestRowMergeConvergentAddColumn(t *testing.T) {
	ctx := newOwnedMergeContext()
	baseSch := calcSchema(1)
	baseCol := schema.NewColumn("c1", 1, types.IntKind, false)

	tests := []struct {
		name           string
		defaultValue   string
		anc            []*int
		left, right    []*int
		expected       []*int
		expectConflict bool
		noTableName    bool
	}{
		{
			name:         "left has the default, right changed it",
			defaultValue: "0",
			anc:          []*int{intPtr(1)},
			left:         []*int{intPtr(1), intPtr(0)},
			right:        []*int{intPtr(1), intPtr(11)},
			expected:     []*int{intPtr(1), intPtr(11)},
		},
		{
			name:         "right has the default, left changed it",
			defaultValue: "0",
			anc:          []*int{intPtr(1)},
			left:         []*int{intPtr(1), intPtr(7)},
			right:        []*int{intPtr(1), intPtr(0)},
			expected:     []*int{intPtr(1), intPtr(7)},
		},
		{
			name:         "both changed it to the same value",
			defaultValue: "0",
			anc:          []*int{intPtr(1)},
			left:         []*int{intPtr(1), intPtr(5)},
			right:        []*int{intPtr(1), intPtr(5)},
			expected:     []*int{intPtr(1), intPtr(5)},
		},
		{
			name:           "both changed it to different values",
			defaultValue:   "0",
			anc:            []*int{intPtr(1)},
			left:           []*int{intPtr(1), intPtr(7)},
			right:          []*int{intPtr(1), intPtr(11)},
			expectConflict: true,
		},
		{
			name:           "row inserted on both sides has no base",
			defaultValue:   "0",
			left:           []*int{intPtr(1), intPtr(0)},
			right:          []*int{intPtr(1), intPtr(11)},
			expectConflict: true,
		},
		{
			name:         "without a default, left has NULL, right changed it",
			defaultValue: "",
			anc:          []*int{intPtr(1)},
			left:         []*int{intPtr(1), nil},
			right:        []*int{intPtr(1), intPtr(11)},
			expected:     []*int{intPtr(1), intPtr(11)},
		},
		{
			name:           "non-deterministic default is not a base",
			defaultValue:   "(FLOOR(RAND() * 0))",
			anc:            []*int{intPtr(1)},
			left:           []*int{intPtr(1), intPtr(0)},
			right:          []*int{intPtr(1), intPtr(11)},
			expectConflict: true,
		},
		{
			name:           "default that reads another column is not a base",
			defaultValue:   "(`c1` * 2)",
			anc:            []*int{intPtr(1)},
			left:           []*int{intPtr(1), intPtr(2)},
			right:          []*int{intPtr(1), intPtr(11)},
			expectConflict: true,
		},
		{
			name:           "without a table name, the default is not resolved",
			defaultValue:   "0",
			anc:            []*int{intPtr(1)},
			left:           []*int{intPtr(1), intPtr(0)},
			right:          []*int{intPtr(1), intPtr(11)},
			expectConflict: true,
			noTableName:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			addedCol, err := schema.NewColumnWithTypeInfo("c2", 2, baseCol.TypeInfo, false, test.defaultValue, false, "")
			require.NoError(t, err)
			sch := schema.MustSchemaFromCols(schema.NewColCollection(
				schema.NewColumn("primaryKey", 0, types.IntKind, true),
				baseCol,
				addedCol,
			))

			tableName := "t"
			if test.noTableName {
				tableName = ""
			}
			m := NewValueMerger(ctx, sch, sch, sch, baseSch, syncPool, nil, tableName)

			merged, ok, err := m.TryMerge(ctx, buildTup(sch, test.left), buildTup(sch, test.right), buildTup(baseSch, test.anc))
			require.NoError(t, err)
			require.Equal(t, test.expectConflict, !ok)
			if !test.expectConflict {
				vD := sch.GetValueDescriptor(m.ns)
				require.Equal(t, vD.Format(ctx, buildTup(sch, test.expected)), vD.Format(ctx, merged))
			}
		})
	}
}
