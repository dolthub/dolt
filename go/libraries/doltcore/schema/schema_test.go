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

package schema

import (
	"fmt"
	"reflect"
	"strings"
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
	colColl := NewColCollection(allCols...)
	schFromCols, err := SchemaFromCols(colColl)
	require.NoError(t, err)

	testSchema("SchemaFromCols", schFromCols, t)

	testKeyColColl := NewColCollection(pkCols...)
	testNonKeyColsColl := NewColCollection(nonPkCols...)
	schFromPKAndNonPKCols, _ := SchemaFromPKAndNonPKCols(testKeyColColl, testNonKeyColsColl)

	testSchema("SchemaFromPKAndNonPKCols", schFromPKAndNonPKCols, t)

	eq := SchemasAreEqual(schFromCols, schFromPKAndNonPKCols)
	assert.True(t, eq, "schemas should be equal")
}

func TestSchemaWithNoPKs(t *testing.T) {
	colColl := NewColCollection(nonPkCols...)
	_, _ = SchemaFromCols(colColl)

	assert.NotPanics(t, func() {
		UnkeyedSchemaFromCols(colColl)
	})
}

func TestGetSharedCols(t *testing.T) {
	colColl := NewColCollection(nonPkCols...)
	sch, _ := SchemaFromCols(colColl)

	names := []string{addrColName, ageColName}
	kinds := []types.NomsKind{types.StringKind, types.UintKind}
	res := GetSharedCols(sch, names, kinds)

	expected := []Column{
		mustGetCol(colColl, addrColName),
		mustGetCol(colColl, ageColName),
	}

	assert.Equal(t, expected, res)
}

func mustGetCol(collection *ColCollection, name string) Column {
	col, ok := collection.GetByName(name)
	if !ok {
		panic(fmt.Sprintf("%s not found", name))
	}
	return col
}

func TestIsKeyless(t *testing.T) {
	cc := NewColCollection(allCols...)
	pkSch, err := SchemaFromCols(cc)
	require.NoError(t, err)

	ok := IsKeyless(pkSch)
	assert.False(t, ok)

	cc = NewColCollection(nonPkCols...)

	keylessSch, err := SchemaFromCols(cc)
	assert.NoError(t, err)

	ok = IsKeyless(keylessSch)
	assert.True(t, ok)
}

func TestValidateForInsert(t *testing.T) {
	t.Run("Validate good", func(t *testing.T) {
		colColl := NewColCollection(allCols...)
		assert.NoError(t, ValidateForInsert(colColl))
	})

	t.Run("Name collision", func(t *testing.T) {
		cols := append(allCols, Column{titleColName, 100, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColNameCollision)
	})

	t.Run("Case insensitive collision", func(t *testing.T) {
		cols := append(allCols, Column{strings.ToUpper(titleColName), 100, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColNameCollision)
	})

	t.Run("Tag collision", func(t *testing.T) {
		cols := append(allCols, Column{"newCol", lnColTag, types.StringKind, false, typeinfo.StringDefaultType, "", false, "", nil})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColTagCollision)
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
