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

package expreval

import (
	"testing"
	"time"

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

	eqOp := EqualsOp{}
	gtOp := GreaterOp{}
	gteOp := GreaterEqualOp{}
	ltOp := LessOp{}
	lteOp := LessEqualOp{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustBool := getMustBool(t)
			resEq := mustBool(eqOp.CompareNomsValues(test.v1, test.v2))
			resGt := mustBool(gtOp.CompareNomsValues(test.v1, test.v2))
			resGte := mustBool(gteOp.CompareNomsValues(test.v1, test.v2))
			resLt := mustBool(ltOp.CompareNomsValues(test.v1, test.v2))
			resLte := mustBool(lteOp.CompareNomsValues(test.v1, test.v2))

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
			name: "nil not equal to nil",
			v:    types.NullValue,
			gt:   false,
			gte:  false,
			lt:   false,
			lte:  false,
			eq:   false,
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

	eqOp := EqualsOp{}
	gtOp := GreaterOp{}
	gteOp := GreaterEqualOp{}
	ltOp := LessOp{}
	lteOp := LessEqualOp{}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mustBool := getMustBool(t)
			resEq := mustBool(eqOp.CompareToNil(test.v))
			resGt := mustBool(gtOp.CompareToNil(test.v))
			resGte := mustBool(gteOp.CompareToNil(test.v))
			resLt := mustBool(ltOp.CompareToNil(test.v))
			resLte := mustBool(lteOp.CompareToNil(test.v))

			assert.True(t, resEq == test.eq, "equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGt == test.gt, "greater failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resGte == test.gte, "greater equals failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLt == test.lt, "less than failure. Expected: %t Actual %t", test.lte, resLte)
			assert.True(t, resLte == test.lte, "less than equals failure. Expected: %t Actual %t", test.lte, resLte)
		})
	}
}
