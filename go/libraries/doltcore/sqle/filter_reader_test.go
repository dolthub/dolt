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

package sqle

import (
	"context"
	"io"
	"testing"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	pkTag = 0
	c1Tag = 1
)

var oneIntPKCol = schema.SchemaFromCols(mustColColl(schema.NewColCollection(
	schema.NewColumn("pk", pkTag, types.IntKind, true),
	schema.NewColumn("c1", c1Tag, types.IntKind, false))))

func int64Range(start, end, stride int64) []int64 {
	vals := make([]int64, 0, end-start)
	for i := start; i < end; i += stride {
		vals = append(vals, i)
	}

	return vals
}

func genOnePKRows(pks ...int64) []row.Row {
	rows := make([]row.Row, len(pks))

	var err error
	for i, pk := range pks {
		taggedVals := row.TaggedValues{pkTag: types.Int(pk), c1Tag: types.Int(pk)}
		rows[i], err = row.New(types.Format_Default, oneIntPKCol, taggedVals)

		if err != nil {
			panic(err)
		}
	}

	return rows
}

func mustColColl(coll *schema.ColCollection, err error) *schema.ColCollection {
	if err != nil {
		panic(err)
	}

	return coll
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
			"unfiltered test one pk",
			oneIntPKCol,
			nil,
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(int64Range(0, 20, 1)...),
		},
		{
			"one pk equality",
			oneIntPKCol,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, "pk", false),
				expression.NewLiteral(int64(10), sql.Int64))},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(10),
		},
		{
			"one pk equal to value which doesnt exist",
			oneIntPKCol,
			[]sql.Expression{expression.NewEquals(
				expression.NewGetField(0, sql.Int64, "pk", false),
				expression.NewLiteral(int64(100), sql.Int64))},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(),
		},
		{
			"one pk inequality",
			oneIntPKCol,
			[]sql.Expression{expression.NewGreaterThanOrEqual(
				expression.NewGetField(0, sql.Int64, "pk", false),
				expression.NewLiteral(int64(10), sql.Int64))},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(int64Range(10, 20, 1)...),
		},
		{
			"one pk in filter",
			oneIntPKCol,
			[]sql.Expression{expression.NewIn(
				expression.NewGetField(0, sql.Int64, "pk", false),
				expression.NewTuple(
					expression.NewLiteral(int64(0), sql.Int64),
					expression.NewLiteral(int64(5), sql.Int64),
					expression.NewLiteral(int64(10), sql.Int64),
					expression.NewLiteral(int64(15), sql.Int64),
				))},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(0, 5, 10, 15),
		},
		{
			// iteration is only based on the primary key. Even though there are no rows with c1 == 10, the row with
			// pk == 5 will be returned and the column filtering will happen later.
			"one pk equals 5 and c1 equals 10",
			oneIntPKCol,
			[]sql.Expression{
				expression.NewAnd(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, "pk", false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, "c1", false),
						expression.NewLiteral(int64(10), sql.Int64),
					),
				),
			},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(5),
		},
		{
			// same as above
			"two filters same as f1 && f2",
			oneIntPKCol,
			[]sql.Expression{
				expression.NewEquals(
					expression.NewGetField(0, sql.Int64, "pk", false),
					expression.NewLiteral(int64(5), sql.Int64),
				),
				expression.NewEquals(
					expression.NewGetField(1, sql.Int64, "c1", false),
					expression.NewLiteral(int64(10), sql.Int64),
				),
			},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(5),
		},
		{
			// iteration is only based on the primary key. Even though there are no rows with c1 == 10, all rows will be
			// returned as none of the rows can be eliminated based on their primary key alone due to the || c1 == 10
			// clause
			"one pk equals 5 and c1 equals 10",
			oneIntPKCol,
			[]sql.Expression{
				expression.NewOr(
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, "pk", false),
						expression.NewLiteral(int64(5), sql.Int64),
					),
					expression.NewEquals(
						expression.NewGetField(1, sql.Int64, "c1", false),
						expression.NewLiteral(int64(10), sql.Int64),
					),
				),
			},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(int64Range(0, 20, 1)...),
		},

		{
			"one pk multiple ranges and a discreet value",
			oneIntPKCol,
			[]sql.Expression{
				expression.NewOr(
					expression.NewOr(
						expression.NewAnd(
							expression.NewGreaterThan(
								expression.NewGetField(0, sql.Int64, "pk", false),
								expression.NewLiteral(int64(0), sql.Int64),
							),
							expression.NewLessThanOrEqual(
								expression.NewGetField(0, sql.Int64, "pk", false),
								expression.NewLiteral(int64(4), sql.Int64),
							),
						),
						expression.NewAnd(
							expression.NewGreaterThanOrEqual(
								expression.NewGetField(0, sql.Int64, "pk", false),
								expression.NewLiteral(int64(10), sql.Int64),
							),
							expression.NewLessThan(
								expression.NewGetField(0, sql.Int64, "pk", false),
								expression.NewLiteral(int64(14), sql.Int64),
							),
						),
					),
					expression.NewEquals(
						expression.NewGetField(0, sql.Int64, "pk", false),
						expression.NewLiteral(int64(19), sql.Int64),
					),
				),
			},
			genOnePKRows(int64Range(0, 20, 1)...),
			genOnePKRows(1, 2, 3, 4, 10, 11, 12, 13, 19),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			memDB, err := dbfactory.MemFactory{}.CreateDB(ctx, types.Format_Default, nil, nil)
			require.NoError(t, err)

			createFunc, err := CreateReaderFuncLimitedByExpressions(types.Format_Default, test.sch, test.filters)
			require.NoError(t, err)

			tblData, err := mapFromRows(ctx, memDB, test.sch, test.rowData...)
			require.NoError(t, err)
			rd, err := createFunc(ctx, tblData)
			require.NoError(t, err)

			resMap, err := types.NewMap(ctx, memDB)
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

			expectedMap, err := mapFromRows(ctx, memDB, test.sch, test.expectedRows...)
			require.NoError(t, err)

			assert.Equal(t, expectedMap.Len(), resMap.Len())
			assert.True(t, expectedMap.Equals(resMap))
		})
	}
}
