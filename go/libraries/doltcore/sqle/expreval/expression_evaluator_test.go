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

package expreval

import (
	"errors"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetComparisonType(t *testing.T) {
	getId := expression.NewGetField(0, gmstypes.Int64, "id", false)
	getMedian := expression.NewGetField(1, gmstypes.Int64, "median", false)
	getAverage := expression.NewGetField(2, gmstypes.Float64, "average", false)
	litOne := expression.NewLiteral(int64(1), gmstypes.Int64)
	litTwo := expression.NewLiteral(int64(1), gmstypes.Int64)
	litThree := expression.NewLiteral(int64(1), gmstypes.Int64)

	tests := []struct {
		name             string
		binExp           expression.BinaryExpression
		expectedNumGFs   int
		expectedNumLits  int
		expectedCompType ComparisonType
		expectErr        bool
	}{
		{
			"id = 1",
			expression.NewEquals(getId, litOne),
			1,
			1,
			VariableConstCompare,
			false,
		},
		{
			"1 = 1",
			expression.NewEquals(litOne, litOne),
			0,
			2,
			ConstConstCompare,
			false,
		},
		{
			"average > float(median)",
			expression.NewGreaterThan(getAverage, expression.NewConvert(getMedian, "float")),
			2,
			0,
			VariableVariableCompare,
			false,
		},
		{
			" > float(median)",
			expression.NewInTuple(getId, expression.NewTuple(litOne, litTwo, litThree)),
			1,
			3,
			VariableInLiteralList,
			false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			gfs, lits, compType, err := GetComparisonType(test.binExp)
			assertOnUnexpectedErr(t, test.expectErr, err)

			assert.Equal(t, test.expectedNumGFs, len(gfs))
			assert.Equal(t, test.expectedNumLits, len(lits))
			assert.Equal(t, test.expectedCompType, compType)
		})
	}
}

var errFunc = func(ctx *sql.Context, row sql.Row) (b bool, err error) {
	return false, errors.New("")
}

func TestNewAndAndOrFuncs(t *testing.T) {
	tests := []struct {
		name         string
		f1           ExpressionFunc
		f2           ExpressionFunc
		expectedOr   bool
		expectedAnd  bool
		expectOrErr  bool
		expectAndErr bool
	}{
		{
			"false false",
			falseFunc,
			falseFunc,
			false,
			false,
			false,
			false,
		},
		{
			"true false",
			trueFunc,
			falseFunc,
			true,
			false,
			false,
			false,
		},
		{
			"false true",
			falseFunc,
			trueFunc,
			true,
			false,
			false,
			false,
		},
		{
			"true true",
			trueFunc,
			trueFunc,
			true,
			true,
			false,
			false,
		},
		{
			"false err",
			falseFunc,
			errFunc,
			false,
			false,
			true,
			false, // short circuit avoids err
		},
		{
			"err false",
			errFunc,
			falseFunc,
			false,
			false,
			true,
			true,
		},
		{
			"err true",
			errFunc,
			trueFunc,
			false,
			false,
			true,
			true,
		},
		{
			"true err",
			trueFunc,
			errFunc,
			true,
			false,
			false, // short circuit avoids err
			true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := sql.NewEmptyContext()
			or := newOrFunc(test.f1, test.f2)
			and := newAndFunc(test.f1, test.f2)

			actualOr, err := or(ctx, nil)
			assertOnUnexpectedErr(t, test.expectOrErr, err)

			if err == nil {
				assert.Equal(t, test.expectedOr, actualOr)
			}

			actualAnd, err := and(ctx, nil)
			assertOnUnexpectedErr(t, test.expectAndErr, err)

			if err != nil {
				assert.Equal(t, test.expectedAnd, actualAnd)
			}
		})
	}
}

func TestNewComparisonFunc(t *testing.T) {
	ctx := sql.NewEmptyContext()

	// Schema: col0 (Int64, idx 0), col1 (Int64, idx 1), date (Datetime, idx 2)
	testSch := sql.Schema{
		{Name: "col0", Type: gmstypes.Int64},
		{Name: "col1", Type: gmstypes.Int64},
		{Name: "date", Type: gmstypes.Datetime},
	}

	const (
		eq  string = "eq"
		gt         = "gt"
		gte        = "gte"
		lt         = "lt"
		lte        = "lte"
	)

	ops := make(map[string]CompareOp)
	ops[eq] = EqualsOp{}
	ops[gt] = GreaterOp{}
	ops[gte] = GreaterEqualOp{}
	ops[lt] = LessOp{}
	ops[lte] = LessEqualOp{}

	type funcTestVal struct {
		name      string
		row       sql.Row
		expectRes map[string]bool
		expectErr map[string]bool
	}

	tests := []struct {
		name         string
		sch          sql.Schema
		be           expression.BinaryExpression
		expectNewErr bool
		testVals     []funcTestVal
	}{
		{
			name: "compare int literals -1 and -1",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewLiteral(int8(-1), gmstypes.Int8),
				expression.NewLiteral(int64(-1), gmstypes.Int64),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col0=-1 and col1=-1",
					row:  sql.Row{int64(-1), int64(-1), nil},
					//expectedRes based on comparison of the literals -1 and -1
					expectRes: map[string]bool{eq: true, gt: false, gte: true, lt: false, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=0 and col1=100",
					row:  sql.Row{int64(0), int64(100), nil},
					//expectedRes based on comparison of the literals -1 and -1
					expectRes: map[string]bool{eq: true, gt: false, gte: true, lt: false, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare int literals -5 and 5",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewLiteral(int8(-5), gmstypes.Int8),
				expression.NewLiteral(uint8(5), gmstypes.Uint8),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col0=-1 and col1=-1",
					row:  sql.Row{int64(-1), int64(-1), nil},
					//expectedRes based on comparison of the literals -5 and 5
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: true, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=0 and col1=100",
					row:  sql.Row{int64(0), int64(100), nil},
					//expectedRes based on comparison of the literals -5 and 5
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: true, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare string literals b and a",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewLiteral("b", gmstypes.Text),
				expression.NewLiteral("a", gmstypes.Text),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col0=-1 and col1=-1",
					row:  sql.Row{int64(-1), int64(-1), nil},
					//expectedRes based on comparison of the literals "b" and "a"
					expectRes: map[string]bool{eq: false, gt: true, gte: true, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=0 and col1=100",
					row:  sql.Row{int64(0), int64(100), nil},
					//expectedRes based on comparison of the literals "b" and "a"
					expectRes: map[string]bool{eq: false, gt: true, gte: true, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare int value to numeric string literals",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(0, gmstypes.Int64, "col0", false),
				expression.NewLiteral("1", gmstypes.Text),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col0=0 and col1=-1",
					row:  sql.Row{int64(0), int64(-1), nil},
					//expectedRes based on comparison of col0=0 to "1"
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: true, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=1 and col1=100",
					row:  sql.Row{int64(1), int64(100), nil},
					//expectedRes based on comparison of col0=1 to "1"
					expectRes: map[string]bool{eq: true, gt: false, gte: true, lt: false, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=2 and col1=-10",
					row:  sql.Row{int64(2), int64(-10), nil},
					//expectedRes based on comparison of col0=2 to "1"
					expectRes: map[string]bool{eq: false, gt: true, gte: true, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare date value to date string literals",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(2, gmstypes.Datetime, "date", false),
				expression.NewLiteral("2000-01-01", gmstypes.Text),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col0=0 and col1=-1 and date=2000-01-01",
					row:  sql.Row{int64(0), int64(-1), time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)},
					//expectedRes based on comparison of date=2000-01-01 and "2000-01-01"
					expectRes: map[string]bool{eq: true, gt: false, gte: true, lt: false, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=1 and col1=100 and date=2000-01-02",
					row:  sql.Row{int64(0), int64(-1), time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC)},
					//expectedRes based on comparison of date=2000-01-02 and "2000-01-01"
					expectRes: map[string]bool{eq: false, gt: true, gte: true, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col0=2 and col1=-10 and date=1999-12-31",
					row:  sql.Row{int64(0), int64(-1), time.Date(1999, 12, 31, 0, 0, 0, 0, time.UTC)},
					//expectedRes based on comparison of date=1999-12-31 and "2000-01-01"
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: true, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare col1 and col0",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(1, gmstypes.Int64, "col1", false),
				expression.NewGetField(0, gmstypes.Int64, "col0", false),
			),
			expectNewErr: false,
			testVals: []funcTestVal{
				{
					name: "col1=0 and col0=0",
					row:  sql.Row{int64(0), int64(0), nil},
					//expectedRes based on comparison of col1=0 and col0=0
					expectRes: map[string]bool{eq: true, gt: false, gte: true, lt: false, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col1=0 and col0=1",
					row:  sql.Row{int64(1), int64(0), nil},
					//expectedRes based on comparison of col1=0 and col0=1
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: true, lte: true},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col1=1 and col0=0",
					row:  sql.Row{int64(0), int64(1), nil},
					//expectedRes based on comparison of col1=1 and col0=0
					expectRes: map[string]bool{eq: false, gt: true, gte: true, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
				{
					name: "col1=null and col0=0",
					row:  sql.Row{int64(0), nil, nil},
					//expectedRes based on comparison of col1=null and col0=0
					expectRes: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
					expectErr: map[string]bool{eq: false, gt: false, gte: false, lt: false, lte: false},
				},
			},
		},
		{
			name: "compare const and unknown column variable",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(0, gmstypes.Int64, "unknown", false),
				expression.NewLiteral("1", gmstypes.Text),
			),
			expectNewErr: true,
			testVals:     []funcTestVal{},
		},
		{
			name: "compare variables with first unknown",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(0, gmstypes.Int64, "unknown", false),
				expression.NewGetField(1, gmstypes.Int64, "col1", false),
			),
			expectNewErr: true,
			testVals:     []funcTestVal{},
		},
		{
			name: "compare variables with second unknown",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(1, gmstypes.Int64, "col1", false),
				expression.NewGetField(0, gmstypes.Int64, "unknown", false),
			),
			expectNewErr: true,
			testVals:     []funcTestVal{},
		},
		{
			name: "variable with literal that can't be converted",
			sch:  testSch,
			be: expression.NewEquals(
				expression.NewGetField(0, gmstypes.Int64, "col0", false),
				expression.NewLiteral("not a number", gmstypes.Text),
			),
			expectNewErr: true,
			testVals:     []funcTestVal{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			for opId := range ops {
				t.Run(opId, func(t *testing.T) {
					op := ops[opId]
					f, err := newComparisonFunc(ctx, op, test.be, test.sch)

					if test.expectNewErr {
						assert.Error(t, err)
					} else {
						require.NoError(t, err)
					}

					for i := range test.testVals {
						testVal := test.testVals[i]
						t.Run(testVal.name, func(t *testing.T) {
							actual, err := f(ctx, testVal.row)
							expected := testVal.expectRes[opId]
							expectErr := testVal.expectErr[opId]

							assertOnUnexpectedErr(t, expectErr, err)

							if err == nil {
								assert.Equal(t, expected, actual)
							}
						})
					}
				})
			}
		})
	}
}
