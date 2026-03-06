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
	"testing"

	"github.com/stretchr/testify/assert"
)

func assertOnUnexpectedErr(t *testing.T, expectErr bool, err error) {
	if expectErr {
		assert.Error(t, err)
	} else {
		assert.NoError(t, err)
	}
}

func TestCompareToNil(t *testing.T) {
	eqOp := EqualsOp{}
	gtOp := GreaterOp{}
	gteOp := GreaterEqualOp{}
	ltOp := LessOp{}
	lteOp := LessEqualOp{}

	// When both values are null (otherIsNull = true): only equals returns true
	assert.True(t, eqOp.CompareToNil(true), "null == null should be true")
	assert.False(t, gtOp.CompareToNil(true), "null > null should be false")
	assert.False(t, gteOp.CompareToNil(true), "null >= null should be false")
	assert.False(t, ltOp.CompareToNil(true), "null < null should be false")
	assert.False(t, lteOp.CompareToNil(true), "null <= null should be false")

	// When only the column value is null (otherIsNull = false): all return false
	assert.False(t, eqOp.CompareToNil(false), "null == value should be false")
	assert.False(t, gtOp.CompareToNil(false), "null > value should be false")
	assert.False(t, gteOp.CompareToNil(false), "null >= value should be false")
	assert.False(t, ltOp.CompareToNil(false), "null < value should be false")
	assert.False(t, lteOp.CompareToNil(false), "null <= value should be false")
}

func TestApplyCmp(t *testing.T) {
	eqOp := EqualsOp{}
	gtOp := GreaterOp{}
	gteOp := GreaterEqualOp{}
	ltOp := LessOp{}
	lteOp := LessEqualOp{}

	// n < 0 means v1 < v2
	assert.False(t, eqOp.ApplyCmp(-1))
	assert.False(t, gtOp.ApplyCmp(-1))
	assert.False(t, gteOp.ApplyCmp(-1))
	assert.True(t, ltOp.ApplyCmp(-1))
	assert.True(t, lteOp.ApplyCmp(-1))

	// n == 0 means v1 == v2
	assert.True(t, eqOp.ApplyCmp(0))
	assert.False(t, gtOp.ApplyCmp(0))
	assert.True(t, gteOp.ApplyCmp(0))
	assert.False(t, ltOp.ApplyCmp(0))
	assert.True(t, lteOp.ApplyCmp(0))

	// n > 0 means v1 > v2
	assert.False(t, eqOp.ApplyCmp(1))
	assert.True(t, gtOp.ApplyCmp(1))
	assert.True(t, gteOp.ApplyCmp(1))
	assert.False(t, ltOp.ApplyCmp(1))
	assert.False(t, lteOp.ApplyCmp(1))
}
