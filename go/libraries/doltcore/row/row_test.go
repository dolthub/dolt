// Copyright 2019 Liquidata, Inc.
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

package row

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func TestGetFieldByName(t *testing.T) {
	r, err := newTestRow()
	assert.NoError(t, err)

	val, ok := GetFieldByName(lnColName, r, sch)

	if !ok {
		t.Error("Expected to find value")
	} else if !val.Equals(lnVal) {
		t.Error("Unexpected value")
	}

	val, ok = GetFieldByName(reservedColName, r, sch)

	if ok {
		t.Error("should not find missing key")
	} else if val != nil {
		t.Error("missing key should return null value")
	}
}

func TestGetFieldByNameWithDefault(t *testing.T) {
	r, err := newTestRow()
	assert.NoError(t, err)
	defVal := types.String("default")

	val := GetFieldByNameWithDefault(lnColName, defVal, r, sch)

	if !val.Equals(lnVal) {
		t.Error("expected:", lnVal, "actual", val)
	}

	val = GetFieldByNameWithDefault(reservedColName, defVal, r, sch)

	if !val.Equals(defVal) {
		t.Error("expected:", defVal, "actual", val)
	}
}

func TestIsValid(t *testing.T) {
	r, err := newTestRow()
	assert.NoError(t, err)

	isv, err := IsValid(r, sch)
	assert.NoError(t, err)
	assert.True(t, isv)
	invCol, err := GetInvalidCol(r, sch)
	assert.NoError(t, err)
	assert.Nil(t, invCol)
	column, colConstraint, err := GetInvalidConstraint(r, sch)
	assert.NoError(t, err)
	assert.Nil(t, column)
	assert.Nil(t, colConstraint)

	updatedRow, err := r.SetColVal(lnColTag, nil, sch)
	assert.NoError(t, err)

	isv, err = IsValid(updatedRow, sch)
	assert.NoError(t, err)
	assert.False(t, isv)

	col, err := GetInvalidCol(updatedRow, sch)
	assert.NoError(t, err)
	assert.NotNil(t, col)
	assert.Equal(t, col.Tag, uint64(lnColTag))

	col, cnst, err := GetInvalidConstraint(updatedRow, sch)
	assert.NoError(t, err)
	assert.NotNil(t, col)
	assert.Equal(t, col.Tag, uint64(lnColTag))
	assert.Equal(t, cnst, schema.NotNullConstraint{})

	// Test getting a bad column without the constraint failure
	t.Run("invalid type", func(t *testing.T) {
		nonPkCols := []schema.Column{
			{Name: addrColName, Tag: addrColTag, Kind: types.BoolKind, IsPartOfPK: false, Constraints: nil},
		}
		nonKeyColColl, _ := schema.NewColCollection(nonPkCols...)
		newSch, err := schema.SchemaFromPKAndNonPKCols(testKeyColColl, nonKeyColColl)
		require.NoError(t, err)

		isv, err := IsValid(r, newSch)
		assert.NoError(t, err)
		assert.False(t, isv)

		col, err = GetInvalidCol(r, newSch)
		assert.NoError(t, err)
		require.NotNil(t, col)
		assert.Equal(t, col.Tag, uint64(addrColTag))

		col, cnst, err = GetInvalidConstraint(r, newSch)
		assert.NoError(t, err)
		assert.Nil(t, cnst)
		assert.Equal(t, col.Tag, uint64(addrColTag))
	})
}

func TestAreEqual(t *testing.T) {
	r, err := newTestRow()
	assert.NoError(t, err)

	updatedRow, err := r.SetColVal(lnColTag, types.String("new"), sch)
	assert.NoError(t, err)

	assert.True(t, AreEqual(r, r, sch))
	assert.False(t, AreEqual(r, updatedRow, sch))
}
