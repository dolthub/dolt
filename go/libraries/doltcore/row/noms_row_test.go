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

package row

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	lnColName       = "last"
	fnColName       = "first"
	addrColName     = "address"
	ageColName      = "age"
	titleColName    = "title"
	reservedColName = "reserved"
	indexName       = "idx_age"
	lnColTag        = 1
	fnColTag        = 0
	addrColTag      = 6
	ageColTag       = 4
	titleColTag     = 40
	reservedColTag  = 50
	unusedTag       = 100
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

var testKeyCols = []schema.Column{
	{Name: lnColName, Tag: lnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: []schema.ColConstraint{schema.NotNullConstraint{}}},
	{Name: fnColName, Tag: fnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType, Constraints: []schema.ColConstraint{schema.NotNullConstraint{}}},
}
var testCols = []schema.Column{
	{Name: addrColName, Tag: addrColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	{Name: ageColName, Tag: ageColTag, Kind: types.UintKind, IsPartOfPK: false, TypeInfo: typeinfo.Uint64Type, Constraints: nil},
	{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
	{Name: reservedColName, Tag: reservedColTag, Kind: types.StringKind, IsPartOfPK: false, TypeInfo: typeinfo.StringDefaultType, Constraints: nil},
}
var testKeyColColl = schema.NewColCollection(testKeyCols...)
var testNonKeyColColl = schema.NewColCollection(testCols...)
var sch, _ = schema.SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColColl)
var index schema.Index

func init() {
	index, _ = sch.Indexes().AddIndexByColTags(indexName, []uint64{ageColTag}, schema.IndexProperties{IsUnique: false, Comment: ""})
}

func newTestRow() (Row, error) {
	vals := TaggedValues{
		fnColTag:    fnVal,
		lnColTag:    lnVal,
		addrColTag:  addrVal,
		ageColTag:   ageVal,
		titleColTag: titleVal,
	}

	return New(types.Format_Default, sch, vals)
}

func TestItrRowCols(t *testing.T) {
	r, err := newTestRow()
	require.NoError(t, err)

	itrVals := make(TaggedValues)
	_, err = r.IterCols(func(tag uint64, val types.Value) (stop bool, err error) {
		itrVals[tag] = val
		return false, nil
	})
	require.NoError(t, err)

	assert.Equal(t, TaggedValues{
		lnColTag:    lnVal,
		fnColTag:    fnVal,
		ageColTag:   ageVal,
		addrColTag:  addrVal,
		titleColTag: titleVal,
	}, itrVals)
}

func TestFromNoms(t *testing.T) {
	// New() will faithfully return null values in the row, but such columns won't ever be set when loaded from Noms.
	// So we use a row here with no null values set to avoid this inconsistency.
	expectedRow, err := New(types.Format_Default, sch, TaggedValues{
		fnColTag:   fnVal,
		lnColTag:   lnVal,
		addrColTag: addrVal,
		ageColTag:  ageVal,
	})
	require.NoError(t, err)

	t.Run("all values specified", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
		)

		require.NoError(t, err)
		r, err := FromNoms(sch, keys, vals)

		require.NoError(t, err)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("only key", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default)
		require.NoError(t, err)

		expectedRow, err := New(types.Format_Default, sch, TaggedValues{
			fnColTag: fnVal,
			lnColTag: lnVal,
		})
		require.NoError(t, err)
		r, err := FromNoms(sch, keys, vals)
		require.NoError(t, err)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("additional tag not in schema is silently dropped", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)

		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
			types.Uint(unusedTag), fnVal,
		)

		require.NoError(t, err)

		r, err := FromNoms(sch, keys, vals)
		require.NoError(t, err)
		assert.Equal(t, expectedRow, r)
	})

	t.Run("bad type", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		require.NoError(t, err)
		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), fnVal,
		)
		require.NoError(t, err)

		_, err = FromNoms(sch, keys, vals)
		assert.Error(t, err)
	})

	t.Run("key col set in vals", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
		)
		require.NoError(t, err)
		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(fnColTag), fnVal,
		)
		require.NoError(t, err)

		_, err = FromNoms(sch, keys, vals)
		assert.Error(t, err)
	})

	t.Run("unknown tag in key", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
			types.Uint(unusedTag), fnVal,
		)

		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(ageColTag), ageVal,
			types.Uint(titleColTag), titleVal,
		)

		require.NoError(t, err)

		_, err = FromNoms(sch, keys, vals)
		assert.Error(t, err)
	})

	t.Run("value tag in key", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(fnColTag), fnVal,
			types.Uint(lnColTag), lnVal,
			types.Uint(ageColTag), ageVal,
		)

		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(addrColTag), addrVal,
			types.Uint(titleColTag), titleVal,
		)

		require.NoError(t, err)

		_, err = FromNoms(sch, keys, vals)
		assert.Error(t, err)
	})
}

func TestSetColVal(t *testing.T) {
	t.Run("valid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		updatedVal := types.String("sanchez")

		r, err := newTestRow()
		require.NoError(t, err)
		r2, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r2)

		updated, err := r.SetColVal(lnColTag, updatedVal, sch)
		require.NoError(t, err)

		// validate calling set does not mutate the original row
		r3, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r3)
		expected[lnColTag] = updatedVal
		r4, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, updated, r4)

		// set to a nil value
		updated, err = updated.SetColVal(titleColTag, nil, sch)
		require.NoError(t, err)
		delete(expected, titleColTag)
		r5, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, updated, r5)
	})

	t.Run("invalid update", func(t *testing.T) {
		expected := map[uint64]types.Value{
			lnColTag:    lnVal,
			fnColTag:    fnVal,
			ageColTag:   ageVal,
			addrColTag:  addrVal,
			titleColTag: titleVal}

		r, err := newTestRow()
		require.NoError(t, err)

		r2, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r2)

		// SetColVal allows an incorrect type to be set for a column
		updatedRow, err := r.SetColVal(lnColTag, types.Bool(true), sch)
		require.NoError(t, err)
		// IsValid fails for the type problem
		isv, err := IsValid(updatedRow, sch)
		require.NoError(t, err)
		assert.False(t, isv)
		invalidCol, err := GetInvalidCol(updatedRow, sch)
		require.NoError(t, err)
		assert.NotNil(t, invalidCol)
		assert.Equal(t, uint64(lnColTag), invalidCol.Tag)

		// validate calling set does not mutate the original row
		r3, err := New(types.Format_Default, sch, expected)
		require.NoError(t, err)
		assert.Equal(t, r, r3)
	})
}

func TestConvToAndFromTuple(t *testing.T) {
	ctx := context.Background()

	r, err := newTestRow()
	require.NoError(t, err)

	keyTpl := r.NomsMapKey(sch).(TupleVals)
	valTpl := r.NomsMapValue(sch).(TupleVals)
	keyVal, err := keyTpl.Value(ctx)
	require.NoError(t, err)
	valVal, err := valTpl.Value(ctx)
	require.NoError(t, err)
	r2, err := FromNoms(sch, keyVal.(types.Tuple), valVal.(types.Tuple))
	require.NoError(t, err)

	fmt.Println(Fmt(context.Background(), r, sch))
	fmt.Println(Fmt(context.Background(), r2, sch))

	if !AreEqual(r, r2, sch) {
		t.Error("Failed to convert to a noms tuple, and then convert back to the same row")
	}
}

func TestReduceToIndex(t *testing.T) {
	taggedValues := []struct {
		row           TaggedValues
		expectedIndex TaggedValues
	}{
		{
			TaggedValues{
				lnColTag:       types.String("yes"),
				fnColTag:       types.String("no"),
				addrColTag:     types.String("nonsense"),
				ageColTag:      types.Uint(55),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{
				lnColTag:  types.String("yes"),
				fnColTag:  types.String("no"),
				ageColTag: types.Uint(55),
			},
		},
		{
			TaggedValues{
				lnColTag:       types.String("yes"),
				addrColTag:     types.String("nonsense"),
				ageColTag:      types.Uint(55),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{
				lnColTag:  types.String("yes"),
				ageColTag: types.Uint(55),
			},
		},
		{
			TaggedValues{
				lnColTag: types.String("yes"),
				fnColTag: types.String("no"),
			},
			TaggedValues{
				lnColTag: types.String("yes"),
				fnColTag: types.String("no"),
			},
		},
		{
			TaggedValues{
				addrColTag:     types.String("nonsense"),
				titleColTag:    types.String("lol"),
				reservedColTag: types.String("what"),
			},
			TaggedValues{},
		},
	}

	for _, tvCombo := range taggedValues {
		row, err := New(types.Format_Default, sch, tvCombo.row)
		require.NoError(t, err)
		expectedIndex, err := New(types.Format_Default, index.Schema(), tvCombo.expectedIndex)
		require.NoError(t, err)
		indexRow, err := reduceToIndex(index, row)
		require.NoError(t, err)
		assert.True(t, AreEqual(expectedIndex, indexRow, index.Schema()))
	}
}

func TestFromNomsGeom(t *testing.T) {
	// Declare values
	pkColName := "pk"
	geomColName := "g"
	pkTag := uint64(0)
	geomTag := uint64(1)
	pkVal := types.Uint(0)
	pointVal := types.Point{SRID: 0, X: 1, Y: 2}

	// Create schema
	testGeomKeyCols := []schema.Column{
		{Name: pkColName, Tag: pkTag, Kind: types.UintKind, IsPartOfPK: true, TypeInfo: typeinfo.Uint64Type, Constraints: []schema.ColConstraint{schema.NotNullConstraint{}}},
	}
	testGeomCols := []schema.Column{
		{Name: geomColName, Tag: geomTag, Kind: types.GeometryKind, IsPartOfPK: false, TypeInfo: typeinfo.GeometryType, Constraints: nil},
	}
	testGeomKeyColColl := schema.NewColCollection(testGeomKeyCols...)
	testGeomNonKeyColColl := schema.NewColCollection(testGeomCols...)
	schGeom, _ := schema.SchemaFromPKAndNonPKCols(testGeomKeyColColl, testGeomNonKeyColColl)

	// New() will faithfully return null values in the row, but such columns won't ever be set when loaded from Noms.
	// So we use a row here with no null values set to avoid this inconsistency.
	expectedRow, err := New(types.Format_Default, schGeom, TaggedValues{
		pkTag:   pkVal,
		geomTag: pointVal,
	})
	require.NoError(t, err)

	t.Run("all values specified geometry", func(t *testing.T) {
		keys, err := types.NewTuple(types.Format_Default,
			types.Uint(0), pkVal,
		)
		require.NoError(t, err)

		vals, err := types.NewTuple(types.Format_Default,
			types.Uint(1), pointVal,
		)

		require.NoError(t, err)
		r, err := FromNoms(schGeom, keys, vals)

		require.NoError(t, err)
		assert.Equal(t, expectedRow, r)
	})
}

func reduceToIndex(idx schema.Index, r Row) (Row, error) {
	newRow := nomsRow{
		key:   make(TaggedValues),
		value: make(TaggedValues),
		nbf:   r.Format(),
	}
	for _, tag := range idx.AllTags() {
		if val, ok := r.GetColVal(tag); ok {
			newRow.key[tag] = val
		}
	}

	return newRow, nil
}
