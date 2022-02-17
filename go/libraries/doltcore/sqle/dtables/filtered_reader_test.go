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

package dtables

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	pk0Name = "pk0"
	pk0Tag  = 0
	pk1Name = "pk1"
	pk1Tag  = 1
	c1Name  = "c1"
	c1Tag   = 2
	c2Name  = "c2"
	c2Tag   = 3
)

var noPKSch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn(c1Name, c1Tag, types.IntKind, false),
	schema.NewColumn(c2Name, c2Tag, types.IntKind, false)))

var oneIntPKSch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn(pk0Name, pk0Tag, types.IntKind, true),
	schema.NewColumn(c1Name, c1Tag, types.IntKind, false)))

var twoIntPKSch = schema.MustSchemaFromCols(schema.NewColCollection(
	schema.NewColumn(pk0Name, pk0Tag, types.IntKind, true),
	schema.NewColumn(pk1Name, pk1Tag, types.IntKind, true),
	schema.NewColumn(c1Name, c1Tag, types.IntKind, false)))

func int64Range(start, end, stride int64) []int64 {
	vals := make([]int64, 0, end-start)
	for i := start; i < end; i += stride {
		vals = append(vals, i)
	}

	return vals
}

func genNoPKRows(cols ...int64) []row.Row {
	rows := make([]row.Row, len(cols))

	var err error
	for i, col := range cols {
		taggedVals := row.TaggedValues{c1Tag: types.Int(col), c2Tag: types.Int(col)}
		rows[i], err = row.New(types.Format_Default, noPKSch, taggedVals)

		if err != nil {
			panic(err)
		}
	}

	return rows
}

func genOneIntPKRows(pks ...int64) []row.Row {
	rows := make([]row.Row, len(pks))

	var err error
	for i, pk := range pks {
		taggedVals := row.TaggedValues{pk0Tag: types.Int(pk), c1Tag: types.Int(pk)}
		rows[i], err = row.New(types.Format_Default, oneIntPKSch, taggedVals)

		if err != nil {
			panic(err)
		}
	}

	return rows
}

func int64TupleGen(pk0ToPK1s map[int64][]int64) [][2]int64 {
	var tuples [][2]int64
	for pk0, pk1s := range pk0ToPK1s {
		for _, pk1 := range pk1s {
			tuples = append(tuples, [2]int64{pk0, pk1})
		}
	}

	return tuples
}

func genTwoIntPKRows(pks ...[2]int64) []row.Row {
	rows := make([]row.Row, len(pks))

	var err error
	for i, pk := range pks {
		taggedVals := row.TaggedValues{pk0Tag: types.Int(pk[0]), pk1Tag: types.Int(pk[1]), c1Tag: types.Int(pk[0])}
		rows[i], err = row.New(types.Format_Default, twoIntPKSch, taggedVals)

		if err != nil {
			panic(err)
		}
	}

	return rows
}

func mapFromRows(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema, rows ...row.Row) (types.Map, error) {
	resMap, err := types.NewMap(ctx, vrw)

	if err != nil {
		return types.EmptyMap, err
	}

	me := resMap.Edit()
	for _, r := range rows {
		me = me.Set(r.NomsMapKey(sch), r.NomsMapValue(sch))
	}

	return me.Map(ctx)
}

func TestFilteredReader(t *testing.T) {
	tests := []struct {
		name         string
		sch          schema.Schema
		filters      []sql.Expression
		rowData      []row.Row
		expectedRows []row.Row
	}{
		{
			"unfiltered test no pks",
			noPKSch,
			nil,
			genNoPKRows(int64Range(0, 20, 1)...),
			genNoPKRows(int64Range(0, 20, 1)...),
		},
		{
			// When there are no PKs to use to filter, FilteredReader should
			// return all table data, without throwing any errors.
			"no pks equality",
			noPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, c1Name, false),
				expression.NewLiteral(int64(10), sql.Int64))},
			genNoPKRows(int64Range(0, 20, 1)...),
			genNoPKRows(int64Range(0, 20, 1)...),
		},
		{
			"unfiltered test one pk",
			oneIntPKSch,
			nil,
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(int64Range(0, 20, 1)...),
		},
		{
			"one pk equality",
			oneIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewLiteral(int64(10), sql.Int64))},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(10),
		},
		{
			"one pk with explicitly false filter",
			oneIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewLiteral(int64(0), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64))},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(),
		},
		{
			"one pk equal to value which doesnt exist",
			oneIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewLiteral(int64(100), sql.Int64))},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(),
		},
		{
			"one pk inequality",
			oneIntPKSch,
			[]sql.Expression{expression.NewGreaterThanOrEqual(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewLiteral(int64(10), sql.Int64))},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(int64Range(10, 20, 1)...),
		},
		{
			"one pk in filter",
			oneIntPKSch,
			[]sql.Expression{expression.NewInTuple(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewTuple(
					expression.NewLiteral(int64(0), sql.Int64),
					expression.NewLiteral(int64(5), sql.Int64),
					expression.NewLiteral(int64(10), sql.Int64),
					expression.NewLiteral(int64(15), sql.Int64),
				))},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(0, 5, 10, 15),
		},
		{
			// iteration is only based on the primary key. Even though there are no rows with c1 == 10, the row with
			// pk == 5 will be returned and the column filtering will happen later.
			"one pk equals 5 and c1 equals 10",
			oneIntPKSch,
			[]sql.Expression{
				expression.NewAnd(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, c1Name, false),
						expression.NewLiteral(int64(10), sql.Int64),
					),
				),
			},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(5),
		},
		{
			// same as above
			"two filters same as f1 && f2",
			oneIntPKSch,
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetField(1, sql.Int64, c1Name, false),
					expression.NewLiteral(int64(10), sql.Int64),
				),
			},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(5),
		},
		{
			// iteration is only based on the primary key. Even though there are no rows with c1 == 10, all rows will be
			// returned as none of the rows can be eliminated based on their primary key alone due to the || c1 == 10
			// clause
			"one pk equals 5 and c1 equals 10",
			oneIntPKSch,
			[]sql.Expression{
				expression.NewOr(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, c1Name, false),
						expression.NewLiteral(int64(10), sql.Int64),
					),
				),
			},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(int64Range(0, 20, 1)...),
		},
		{
			"one pk multiple ranges and a discreet value",
			oneIntPKSch,
			[]sql.Expression{
				expression.NewOr(
					expression.NewOr(
						expression.NewAnd(
							expression.NewGreaterThan(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(0), sql.Int64),
							),
							expression.NewLessThanOrEqual(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(4), sql.Int64),
							),
						),
						expression.NewAnd(
							expression.NewGreaterThanOrEqual(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(10), sql.Int64),
							),
							expression.NewLessThan(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(14), sql.Int64),
							),
						),
					),
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(19), sql.Int64),
					),
				),
			},
			genOneIntPKRows(int64Range(0, 20, 1)...),
			genOneIntPKRows(1, 2, 3, 4, 10, 11, 12, 13, 19),
		},
		{
			"two pk no filters",
			twoIntPKSch,
			nil,
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
		},
		{
			"two pk with explicitly false filter",
			twoIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewLiteral(int64(0), sql.Int64),
				expression.NewLiteral(int64(1), sql.Int64))},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(),
		},
		{
			"two pk, pk0 equal to existing value",
			twoIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewLiteral(int64(2), sql.Int64))},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{2: {4, 5}})...),
		},
		{
			"two pk, pk0 equal to missing value",
			twoIntPKSch,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewLiteral(int64(-25), sql.Int64))},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(),
		},
		{
			"two pk in filter on first pk",
			twoIntPKSch,
			[]sql.Expression{expression.NewInTuple(
				expression.NewGetField(0, sql.Int64, pk0Name, false),
				expression.NewTuple(
					expression.NewLiteral(int64(0), sql.Int64),
					expression.NewLiteral(int64(2), sql.Int64),
				))},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 2: {4, 5}})...),
		},
		{
			"two pk, pk0 equal to existing value anded with pk1 equality",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewAnd(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(2), sql.Int64)),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, pk1Name, false),
						expression.NewLiteral(int64(2), sql.Int64)),
				),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{2: {4, 5}})...),
		},
		{
			"two pk, pk0 equal to existing value ored with pk1 equality",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewOr(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(2), sql.Int64)),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, pk1Name, false),
						expression.NewLiteral(int64(2), sql.Int64)),
				),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
		},
		{
			"two pk inequality",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(0), sql.Int64)),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{1: {0}, 2: {4, 5}})...),
		},
		{
			"two pks in exclusive range",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewGreaterThan(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(1), sql.Int64)),
				expression.NewLessThan(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(3), sql.Int64)),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}, 3: {0, 1}, 4: {7, 8, 9}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{2: {4, 5}})...),
		},
		{
			"two pks in inclusive range",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewGreaterThanOrEqual(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(1), sql.Int64)),
				expression.NewLessThanOrEqual(
					expression.NewGetField(0, sql.Int64, pk0Name, false),
					expression.NewLiteral(int64(3), sql.Int64)),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{0: {0, 1, 2, 3}, 1: {0}, 2: {4, 5}, 3: {0, 1}, 4: {7, 8, 9}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{1: {0}, 2: {4, 5}, 3: {0, 1}})...),
		},
		{
			"two pk multiple ranges and a discreet value",
			twoIntPKSch,
			[]sql.Expression{
				expression.NewOr(
					expression.NewOr(
						expression.NewAnd(
							expression.NewGreaterThanOrEqual(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(-5), sql.Int64),
							),
							expression.NewLessThan(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(5), sql.Int64),
							),
						),
						expression.NewAnd(
							expression.NewGreaterThan(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(10), sql.Int64),
							),
							expression.NewLessThanOrEqual(
								expression.NewGetField(0, sql.Int64, pk0Name, false),
								expression.NewLiteral(int64(20), sql.Int64),
							),
						),
					),
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, pk0Name, false),
						expression.NewLiteral(int64(7), sql.Int64),
					),
				),
			},
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{
				0:  {0, 1, 2, 3},
				1:  {0},
				2:  {4, 5},
				3:  {0, 1},
				4:  {7, 8, 9},
				5:  {0, 1, 2, 3},
				6:  {0},
				7:  {4, 5},
				8:  {0, 1},
				9:  {7, 8, 9},
				10: {0, 1, 2, 3},
				11: {0},
				12: {4, 5},
				13: {0, 1},
				14: {7, 8, 9}})...),
			genTwoIntPKRows(int64TupleGen(map[int64][]int64{
				0:  {0, 1, 2, 3},
				1:  {0},
				2:  {4, 5},
				3:  {0, 1},
				4:  {7, 8, 9},
				7:  {4, 5},
				11: {0},
				12: {4, 5},
				13: {0, 1},
				14: {7, 8, 9}})...),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			_, vrw, err := dbfactory.MemFactory{}.CreateDB(ctx, types.Format_Default, nil, nil)
			require.NoError(t, err)

			createFunc, err := CreateReaderFuncLimitedByExpressions(types.Format_Default, test.sch, test.filters)
			require.NoError(t, err)

			tblData, err := mapFromRows(ctx, vrw, test.sch, test.rowData...)
			require.NoError(t, err)
			rd, err := createFunc(ctx, tblData)
			require.NoError(t, err)

			resMap, err := types.NewMap(ctx, vrw)
			require.NoError(t, err)

			me := resMap.Edit()
			for {
				r, err := rd.ReadRow(ctx)
				if err == io.EOF {
					break
				}

				require.NoError(t, err)
				me = me.Set(r.NomsMapKey(test.sch), r.NomsMapValue(test.sch))
			}

			resMap, err = me.Map(ctx)
			require.NoError(t, err)
			assert.Equal(t, uint64(me.NumEdits()), resMap.Len())

			expectedMap, err := mapFromRows(ctx, vrw, test.sch, test.expectedRows...)
			require.NoError(t, err)

			assert.Equal(t, expectedMap.Len(), resMap.Len())
			assert.True(t, expectedMap.Equals(resMap))
		})
	}
}
