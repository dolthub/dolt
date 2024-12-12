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
	"github.com/dolthub/dolt/go/store/val"
"reflect"
	strings "strings"
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

var pkCols = []Column{
	{Name: lnColName, Tag: lnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType},
	{Name: fnColName, Tag: fnColTag, Kind: types.StringKind, IsPartOfPK: true, TypeInfo: typeinfo.StringDefaultType},
}
var nonPkCols = []Column{
	{Name: addrColName, Tag: addrColTag, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
	{Name: ageColName, Tag: ageColTag, Kind: types.UintKind, TypeInfo: typeinfo.FromKind(types.UintKind)},
	{Name: titleColName, Tag: titleColTag, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
	{Name: reservedColName, Tag: reservedColTag, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType},
}

var allCols = append(append([]Column(nil), pkCols...), nonPkCols...)

func TestNewSchema(t *testing.T) {
	allColColl := NewColCollection(allCols...)
	pkColColl := NewColCollection()

	indexCol := NewIndexCollection(allColColl, pkColColl)

	checkCol := NewCheckCollection()
	_, err := checkCol.AddCheck("chk_age", "age > 0", true)
	require.NoError(t, err)

	// Nil ordinals
	sch, err := NewSchema(allColColl, nil, Collation_Default, indexCol, checkCol)
	require.NoError(t, err)
	require.Equal(t, []int{0, 1}, sch.GetPkOrdinals())
	require.Equal(t, Collation_Default, sch.GetCollation())
	require.True(t, sch.Indexes().Equals(indexCol))
	require.True(t, sch.Checks().Equals(checkCol))

	// Set ordinals explicitly
	indexCol.(*indexCollectionImpl).pks = []uint64{fnColTag, lnColTag}
	sch, err = NewSchema(allColColl, []int{1, 0}, Collation_Default, indexCol, checkCol)
	require.NoError(t, err)
	require.Equal(t, []int{1, 0}, sch.GetPkOrdinals())
	require.Equal(t, Collation_Default, sch.GetCollation())
	require.True(t, sch.Indexes().Equals(indexCol))
	require.True(t, sch.Checks().Equals(checkCol))
}

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

func TestSetPkOrder(t *testing.T) {

	// GetPkCols() should always return columns in ordinal order
	// GetAllCols() should always return columns in the defined schema's order
	t.Run("returns the correct GetPkCols() order", func(t *testing.T) {
		allColColl := NewColCollection(allCols...)
		pkColColl := NewColCollection(pkCols...)
		sch, err := SchemaFromCols(allColColl)
		require.NoError(t, err)

		require.Equal(t, allColColl, sch.GetAllCols())
		require.Equal(t, pkColColl, sch.GetPKCols())

		err = sch.SetPkOrdinals([]int{1, 0})
		require.NoError(t, err)

		expectedPkColColl := NewColCollection(pkCols[1], pkCols[0])
		require.Equal(t, expectedPkColColl, sch.GetPKCols())
		require.Equal(t, allColColl, sch.GetAllCols())
	})

	t.Run("Can round-trip", func(t *testing.T) {
		allColColl := NewColCollection(allCols...)
		pkColColl := NewColCollection(pkCols...)
		sch, err := SchemaFromCols(allColColl)
		require.NoError(t, err)

		require.Equal(t, allColColl, sch.GetAllCols())
		require.Equal(t, pkColColl, sch.GetPKCols())

		err = sch.SetPkOrdinals([]int{1, 0})
		require.NoError(t, err)

		err = sch.SetPkOrdinals([]int{0, 1})
		require.NoError(t, err)

		require.Equal(t, allColColl, sch.GetAllCols())
		require.Equal(t, pkColColl, sch.GetPKCols())
	})
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
		cols := append(allCols, Column{Name: titleColName, Tag: 100, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColNameCollision)
	})

	t.Run("Case insensitive collision", func(t *testing.T) {
		cols := append(allCols, Column{Name: strings.ToUpper(titleColName), Tag: 100, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColNameCollision)
	})

	t.Run("Tag collision", func(t *testing.T) {
		cols := append(allCols, Column{Name: "newCol", Tag: lnColTag, Kind: types.StringKind, TypeInfo: typeinfo.StringDefaultType})
		colColl := NewColCollection(cols...)

		err := ValidateForInsert(colColl)
		assert.Error(t, err)
		assert.Equal(t, err, ErrColTagCollision)
	})
}

func TestArePrimaryKeySetsDiffable(t *testing.T) {
	tests := []struct {
		Name     string
		From     Schema
		To       Schema
		Diffable bool
		KeyMap   val.OrdinalMapping
	}{
		{
			Name: "Basic",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "PK-Column renames",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 1, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk2", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("col1", 0, types.IntKind, false),
				NewColumn("pk", 1, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability - inverse",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 1, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("col1", 2, types.IntKind, false),
				NewColumn("pk", 1, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0},
		},
		{
			Name: "Only pk ordering should matter for diffability - compound",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk1", 0, types.IntKind, true),
				NewColumn("col1", 1, types.IntKind, false),
				NewColumn("pk2", 2, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk1", 0, types.IntKind, true),
				NewColumn("pk2", 2, types.IntKind, true))),
			Diffable: true,
			KeyMap:   val.OrdinalMapping{0, 1},
		},
		{
			Name: "Tag mismatches",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 1, types.IntKind, true))),
			Diffable: false,
		},
		{
			Name: "PK Ordinal mismatches",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk1", 0, types.IntKind, true),
				NewColumn("pk2", 1, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk2", 1, types.IntKind, true),
				NewColumn("pk1", 0, types.IntKind, true))),
			Diffable: false,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			d := ArePrimaryKeySetsDiffable(types.Format_Default, test.From, test.To)
			require.Equal(t, test.Diffable, d)

			// If they are diffable then we should be able to map their schemas from one to another.
			if d {
				keyMap, _, err := MapSchemaBasedOnTagAndName(test.From, test.To)
				require.NoError(t, err)
				require.Equal(t, test.KeyMap, keyMap)
			}
		})
	}
}

func TestArePrimaryKeySetsDiffableTypeChanges(t *testing.T) {
	// New format compares underlying SQL types
	tests := []struct {
		Name     string
		From     Schema
		To       Schema
		Diffable bool
		Format   *types.NomsBinFormat
	}{
		{
			Name: "Int -> String (New Format)",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.StringKind, true))),
			Diffable: false,
			Format:   types.Format_DOLT,
		},
		{
			Name: "Int -> String (Old Format)",
			From: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.IntKind, true))),
			To: MustSchemaFromCols(NewColCollection(
				NewColumn("pk", 0, types.StringKind, true))),
			Diffable: true,
			Format:   types.Format_LD_1,
		},
	}

	for _, test := range tests {
		t.Run(test.Name, func(t *testing.T) {
			d := ArePrimaryKeySetsDiffable(test.Format, test.From, test.To)
			require.Equal(t, test.Diffable, d)
		})
	}
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
