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

package expeval

import (
	"testing"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/expression"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/store/types"
)

func getMustBool(t *testing.T) func(bool, error) bool {
	return func(b bool, e error) bool {
		require.NoError(t, e)
		return b
	}
}

var jan11990 = time.Date(1990, 1, 1, 0, 0, 0, 0, time.UTC)

func TestCompareNomsValues(t *testing.T) {
	tests := []struct {
		name string
		v1   types.Value
		v2   types.Value
		gt   bool
		gte  bool
		lt   bool
		lte  bool
		eq   bool
	}{
		{
			name: "int 1 and int 1",
			v1:   types.Int(1),
			v2:   types.Int(1),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "int -1 and int -1",
			v1:   types.Int(-1),
			v2:   types.Int(1),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		},
		{
			name: "int 0 int -5",
			v1:   types.Int(0),
			v2:   types.Int(-5),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
	}

	eqOp := equalsOp{}
	gtOp := greaterOp{}
	gteOp := greaterEqualOp{}
	ltOp := lessOp{}
	lteOp := lessEqualOp{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustBool := getMustBool(t)
			resEq := mustBool(eqOp.compareNomsValues(test.v1, test.v2))
			resGt := mustBool(gtOp.compareNomsValues(test.v1, test.v2))
			resGte := mustBool(gteOp.compareNomsValues(test.v1, test.v2))
			resLt := mustBool(ltOp.compareNomsValues(test.v1, test.v2))
			resLte := mustBool(lteOp.compareNomsValues(test.v1, test.v2))

			assert.True(t, resEq == test.eq, "equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGt == test.gt, "greater failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGte == test.gte, "greater equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLt == test.lt, "less than failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLte == test.lte, "less than equals failure. Expected: %t Actual %t", test.lte, resLte)
		})
	}
}

func assertOnUnexpectedErr(t *testing.T, expectErr bool, err error) {
	if expectErr {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func TestCompareLiterals(t *testing.T) {
	tests := []struct {
		name string
		l1   *expression.Literal
		l2   *expression.Literal
		gt   bool
		gte  bool
		lt   bool
		lte  bool
		eq   bool
	}{
		{
			name: "int 1 and int 1",
			l1:   expression.NewLiteral(int8(1), sql.Int8),
			l2:   expression.NewLiteral(1, sql.Int32),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "int 9 and int 10",
			l1:   expression.NewLiteral(int32(9), sql.Int32),
			l2:   expression.NewLiteral(int64(10), sql.Int64),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		},
		{
			name: "uint 10 and int -20",
			l1:   expression.NewLiteral(int8(10), sql.Uint8),
			l2:   expression.NewLiteral(int16(-20), sql.Int16),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
		{
			name: "int 1 and string 1",
			l1:   expression.NewLiteral(int8(1), sql.Int8),
			l2:   expression.NewLiteral("1", sql.Text),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "int 9 and string 10",
			l1:   expression.NewLiteral(int32(9), sql.Int32),
			l2:   expression.NewLiteral("10", sql.Text),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		},
		{
			name: "uint 10 and string -20",
			l1:   expression.NewLiteral(uint64(10), sql.Uint64),
			l2:   expression.NewLiteral("-20", sql.Text),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
		{
			name: "equal uints",
			l1:   expression.NewLiteral(uint64(0xAAAAAAAAAAAAAAAA), sql.Uint64),
			l2:   expression.NewLiteral(uint64(0xAAAAAAAAAAAAAAAA), sql.Uint64),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "uints",
			l1:   expression.NewLiteral(uint64(0xAAAAAAAAAAAAAAAA), sql.Uint64),
			l2:   expression.NewLiteral(uint64(0xBBBBBBBBBBBBBBBB), sql.Uint64),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		},
		{
			name: "uint and int",
			l1:   expression.NewLiteral(uint64(0xBBBBBBBBBBBBBBBB), sql.Uint64),
			l2:   expression.NewLiteral(int8(77), sql.Int8),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
		{
			name: "strings",
			l1:   expression.NewLiteral("b", sql.Text),
			l2:   expression.NewLiteral("a", sql.Text),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
		{
			name: "string dates",
			l1:   expression.NewLiteral("1999-12-31", sql.Text),
			l2:   expression.NewLiteral("2000-01-01", sql.Text),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		},
		{
			name: "equal dates",
			l1:   expression.NewLiteral("2000-01-01", sql.Datetime),
			l2:   expression.NewLiteral("1999-12-31", sql.Datetime),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
		{
			name: "equal dates",
			l1:   expression.NewLiteral("2000-01-01", sql.Datetime),
			l2:   expression.NewLiteral("2000-01-01", sql.Text),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "string float 1.5 int 1",
			l1:   expression.NewLiteral("1.5", sql.Text),
			l2:   expression.NewLiteral(1, sql.Int32),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		}, {
			name: "float 1.5 float 2.5",
			l1:   expression.NewLiteral(1.5, sql.Float64),
			l2:   expression.NewLiteral(float32(2.5), sql.Float32),
			gt:   false,
			gte:  false,
			lt:   true,
			lte:  true,
			eq:   false,
		}, {
			name: "float 1.5 float 1.5",
			l1:   expression.NewLiteral(1.5, sql.Float64),
			l2:   expression.NewLiteral(float32(1.5), sql.Float32),
			gt:   false,
			gte:  true,
			lt:   false,
			lte:  true,
			eq:   true,
		},
		{
			name: "string b int 0",
			l1:   expression.NewLiteral("b", sql.TinyText),
			l2:   expression.NewLiteral(0, sql.Int8),
			gt:   true,
			gte:  true,
			lt:   false,
			lte:  false,
			eq:   false,
		},
	}

	eqOp := equalsOp{}
	gtOp := greaterOp{}
	gteOp := greaterEqualOp{}
	ltOp := lessOp{}
	lteOp := lessEqualOp{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resEq, err := eqOp.compareLiterals(test.l1, test.l2)
			assert.NoError(t, err)
			resGt, err := gtOp.compareLiterals(test.l1, test.l2)
			assert.NoError(t, err)
			resGte, err := gteOp.compareLiterals(test.l1, test.l2)
			assert.NoError(t, err)
			resLt, err := ltOp.compareLiterals(test.l1, test.l2)
			assert.NoError(t, err)
			resLte, err := lteOp.compareLiterals(test.l1, test.l2)
			assert.NoError(t, err)

			assert.True(t, resEq == test.eq, "equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGt == test.gt, "greater failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGte == test.gte, "greater equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLt == test.lt, "less than failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLte == test.lte, "less than equals failure. Expected: %t Actual %t", test.lte, resLte)
		})
	}
}

func TestCompareToNull(t *testing.T) {
	tests := []struct {
		name string
		v    types.Value
		gt   bool
		gte  bool
		lt   bool
		lte  bool
		eq   bool
	}{
		{
			name: "nil",
			v:    types.NullValue,
			gt:   false,
			gte:  false,
			lt:   false,
			lte:  false,
			eq:   true,
		},
		{
			name: "not nil",
			v:    types.Int(5),
			gt:   false,
			gte:  false,
			lt:   false,
			lte:  false,
			eq:   false,
		},
	}

	eqOp := equalsOp{}
	gtOp := greaterOp{}
	gteOp := greaterEqualOp{}
	ltOp := lessOp{}
	lteOp := lessEqualOp{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustBool := getMustBool(t)
			resEq := mustBool(eqOp.compareToNull(test.v))
			resGt := mustBool(gtOp.compareToNull(test.v))
			resGte := mustBool(gteOp.compareToNull(test.v))
			resLt := mustBool(ltOp.compareToNull(test.v))
			resLte := mustBool(lteOp.compareToNull(test.v))

			assert.True(t, resEq == test.eq, "equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGt == test.gt, "greater failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGte == test.gte, "greater equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLt == test.lt, "less than failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLte == test.lte, "less than equals failure. Expected: %t Actual %t", test.lte, resLte)
		})
	}
}
