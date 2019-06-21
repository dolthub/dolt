package row

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGetFieldByName(t *testing.T) {
	r := newTestRow()

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
	r := newTestRow()
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
	r := newTestRow()

	assert.True(t, IsValid(r, sch))
	assert.Nil(t, GetInvalidCol(r, sch))
	column, colConstraint := GetInvalidConstraint(r, sch)
	assert.Nil(t, column)
	assert.Nil(t, colConstraint)

	updatedRow, err := r.SetColVal(lnColTag, nil, sch)
	assert.NoError(t, err)

	assert.False(t, IsValid(updatedRow, sch))

	col := GetInvalidCol(updatedRow, sch)
	assert.NotNil(t, col)
	assert.Equal(t, col.Tag, uint64(lnColTag))

	col, cnst := GetInvalidConstraint(updatedRow, sch)
	assert.NotNil(t, col)
	assert.Equal(t, col.Tag, uint64(lnColTag))
	assert.Equal(t, cnst, schema.NotNullConstraint{})

	// Test getting a bad column without the constraint failure
	t.Run("invalid type", func(t *testing.T) {
		nonPkCols := []schema.Column{
			{addrColName, addrColTag, types.BoolKind, false, nil},
		}
		nonKeyColColl, _ := schema.NewColCollection(nonPkCols...)
		newSch, err := schema.SchemaFromPKAndNonPKCols(testKeyColColl, nonKeyColColl)
		require.NoError(t, err)

		assert.False(t, IsValid(r, newSch))

		col = GetInvalidCol(r, newSch)
		require.NotNil(t, col)
		assert.Equal(t, col.Tag, uint64(addrColTag))

		col, cnst = GetInvalidConstraint(r, newSch)
		assert.Nil(t, cnst)
		assert.Equal(t, col.Tag, uint64(addrColTag))
	})
}

func TestAreEqual(t *testing.T) {
	r := newTestRow()

	updatedRow, err := r.SetColVal(lnColTag, types.String("new"), sch)
	assert.NoError(t, err)

	assert.True(t, AreEqual(r, r, sch))
	assert.False(t, AreEqual(r, updatedRow, sch))
}
