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

package schema

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
	lnColTag        = 1
	fnColTag        = 0
	addrColTag      = 6
	ageColTag       = 4
	titleColTag     = 40
	reservedColTag  = 50
)

var lnVal = types.String("astley")
var fnVal = types.String("rick")
var addrVal = types.String("123 Fake St")
var ageVal = types.Uint(53)
var titleVal = types.NullValue

var pkCols = []Column{
	{lnColName, lnColTag, types.StringKind, true, typeinfo.StringDefaultType, "", false, "", nil},
	{fnColName, fnColTag, types.StringKind, true, typeinfo.StringDefaultType, "", false, "", nil},
}
var nonPkCols = []Column{
	{addrColName, addrColTag, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
	{ageColName, ageColTag, types.UintKind, false, typeinfo.FromKind(types.UintKind), "", false, "", nil},
	{titleColName, titleColTag, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
	{reservedColName, reservedColTag, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil},
}

var allCols = append(append([]Column(nil), pkCols...), nonPkCols...)

func TestSchema(t *testing.T) {
	colColl, err := NewColCollection(allCols...)
	require.NoError(t, err)
	schFromCols := SchemaFromCols(colColl)

	testSchema("SchemaFromCols", schFromCols, t)

	testKeyColColl, _ := NewColCollection(pkCols...)
	testNonKeyColsColl, _ := NewColCollection(nonPkCols...)
	schFromPKAndNonPKCols, _ := SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColsColl)

	testSchema("SchemaFromPKAndNonPKCols", schFromPKAndNonPKCols, t)

	eq, err := SchemasAreEqual(schFromCols, schFromPKAndNonPKCols)
	assert.NoError(t, err)
	assert.True(t, eq, "schemas should be equal")
}

func TestSchemaWithNoPKs(t *testing.T) {
	colColl, err := NewColCollection(nonPkCols...)
	require.NoError(t, err)

	assert.Panics(t, func() {
		SchemaFromCols(colColl)
	})

	assert.NotPanics(t, func() {
		UnkeyedSchemaFromCols(colColl)
	})
}

func TestValidateForInsert(t *testing.T) {
	t.Run("Validate good", func(t *testing.T) {
		colColl, err := NewColCollection(allCols...)
		require.NoError(t, err)
		assert.NoError(t, ValidateForInsert(colColl))
	})

	t.Run("Name collision", func(t *testing.T) {
		cols := append(allCols, Column{titleColName, 100, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil})
		colColl, err := NewColCollection(cols...)
		require.NoError(t, err)

		err = ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColNameCollision)
	})

	t.Run("No primary keys", func(t *testing.T) {
		colColl, err := NewColCollection(nonPkCols...)
		require.NoError(t, err)

		err = ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrNoPrimaryKeyColumns)
	})
}

func testSchema(method string, sch Schema, t *testing.T) {
	validateCols(t, allCols, sch.GetAllCols(), method+"GetAllCols")
	validateCols(t, pkCols, sch.GetPKCols(), method+"GetPKCols")
	validateCols(t, nonPkCols, sch.GetNonPKCols(), method+"GetNonPKCols")

	extracted, err := ExtractAllColNames(sch)
	assert.NoError(t, err)

	expExt := map[uint64]string{
		lnColTag:       lnColName,
		fnColTag:       fnColName,
		ageColTag:      ageColName,
		addrColTag:     addrColName,
		titleColTag:    titleColName,
		reservedColTag: reservedColName,
	}

	if !reflect.DeepEqual(extracted, expExt) {
		t.Error("extracted columns did not match expectation")
	}

	if col, ok := ColFromName(sch, titleColName); !ok {
		t.Error("Failed to get by name")
	} else if col.Tag != titleColTag {
		t.Error("Unexpected tag")
	}

	if col, ok := ColFromTag(sch, titleColTag); !ok {
		t.Error("Failed to get by name")
	} else if col.Name != titleColName {
		t.Error("Unexpected tag")
	}
}

func validateCols(t *testing.T, cols []Column, colColl *ColCollection, msg string) {
	if !reflect.DeepEqual(cols, colColl.cols) {
		t.Error()
	}
}

/*
func TestAutoGenerateTag(t *testing.T) {
	colColl, _ := NewColCollection()
	sch := SchemaFromCols(colColl)

	var max uint64 = 128
	for i := uint64(0); i < 128*128; i++ {
		if i > 8192 {
			max = 2097152
		} else if i > 64 {
			max = 16384
		}

		tag := AutoGenerateTag(sch)

		if tag >= max {
			t.Fatal("auto generated tag out of range")
		} else {
			var err error
			colColl, err = colColl.Append(NewColumn(strconv.FormatUint(i, 10), tag, types.StringKind, false))

			if err != nil {
				t.Fatal(err)
			}

			sch = SchemaFromCols(colColl)
		}
	}
}*/
