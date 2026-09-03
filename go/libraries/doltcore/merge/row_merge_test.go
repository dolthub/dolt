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
	ctx := sql.NewEmptyContext()

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

// intPtr returns a pointer to v, letting test code represent the integer 0 in a []*int slice
// (the build() helper treats 0 as NULL, so explicit pointers are needed for zero values).
func intPtr(v int) *int { return &v }

// TestRowMergeConvergentAddColumn tests that when both branches add the same column with an
// identical definition (convergent DDL), TryMerge uses the column default as a synthetic base
// so that a schema-backfill value is not treated as an intentional write.
func TestRowMergeConvergentAddColumn(t *testing.T) {
	defaultValue := "0"
	defaultValueInt, err := strconv.Atoi(defaultValue)
	require.NoError(t, err)

	ctx := sql.NewEmptyContext()

	// baseSch: table before either branch adds "extra"
	baseSch := calcSchema(1) // PK + 1 non-pk col (tag=1)

	// extSch: same first column plus "extra INTEGER DEFAULT 0" (tag=2)
	baseCol := schema.NewColumn("1", 1, types.IntKind, false)
	extraCol, err := schema.NewColumnWithTypeInfo("2", 2, baseCol.TypeInfo, false, defaultValue, false, "")
	require.NoError(t, err)
	extSch := schema.MustSchemaFromCols(schema.NewColCollection(
		schema.NewColumn("primaryKey", 0, types.IntKind, true),
		baseCol,
		extraCol,
	))

	// bv builds a base-schema value tuple (1 non-pk field).
	bv := func(v int) val.Tuple { return buildTup(baseSch, []*int{intPtr(v)}) }
	// ev builds an ext-schema value tuple (2 non-pk fields).
	ev := func(v1, v2 int) val.Tuple { return buildTup(extSch, []*int{intPtr(v1), intPtr(v2)}) }

	tests := []struct {
		name           string
		left, right    val.Tuple
		anc            val.Tuple
		expected       val.Tuple
		expectConflict bool
		noTableName    bool // if true, leave tableName unset to test fallback conflict behaviour
	}{
		{
			name:           "left has backfill default, right has explicit value — right wins",
			anc:            bv(1),
			left:           ev(1, defaultValueInt), // extra=0 from schema backfill
			right:          ev(1, 11),              // extra=11 from explicit UPDATE
			expected:       ev(1, 11),
			expectConflict: false,
		},
		{
			name:           "right has backfill default, left has explicit value — left wins",
			anc:            bv(1),
			left:           ev(1, 7),               // extra=7 from explicit UPDATE
			right:          ev(1, defaultValueInt), // extra=0 from schema backfill
			expected:       ev(1, 7),
			expectConflict: false,
		},
		{
			name:           "both have same explicit non-default value — converge",
			anc:            bv(1),
			left:           ev(1, 5),
			right:          ev(1, 5),
			expected:       ev(1, 5),
			expectConflict: false,
		},
		{
			name:           "both have different explicit non-default values — genuine conflict",
			anc:            bv(1),
			left:           ev(1, 7),
			right:          ev(1, 11),
			expected:       nil,
			expectConflict: true,
		},
		{
			name:           "without table name, differing values conflict (fallback behaviour)",
			anc:            bv(1),
			left:           ev(1, defaultValueInt),
			right:          ev(1, 11),
			expected:       nil,
			expectConflict: true,
			noTableName:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tableName string
			if !test.noTableName {
				tableName = "t"
			}
			m := NewValueMerger(ctx, extSch, extSch, extSch, baseSch, syncPool, nil, tableName)

			merged, ok, err := m.TryMerge(ctx, test.left, test.right, test.anc)
			assert.NoError(t, err)
			assert.Equal(t, test.expectConflict, !ok, "conflict mismatch")
			vD := extSch.GetValueDescriptor(m.ns)
			assert.Equal(t, vD.Format(ctx, test.expected), vD.Format(ctx, merged))
		})
	}
}
