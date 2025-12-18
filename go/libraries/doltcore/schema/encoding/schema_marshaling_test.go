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

package encoding

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"strconv"
	"testing"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/constants"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
)

func createTestSchema() schema.Schema {
	columns := []schema.Column{
		schema.NewColumn("id", 4, types.InlineBlobKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", 1, types.StringKind, false),
		schema.NewColumn("last", 2, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", 3, types.UintKind, false),
	}

	colColl := schema.NewColCollection(columns...)
	sch := schema.MustSchemaFromCols(colColl)
	_, _ = sch.Indexes().AddIndexByColTags("idx_age", []uint64{3}, nil, schema.IndexProperties{IsUnique: false, Comment: ""})
	return sch
}

func TestSchemaUnmarshal(t *testing.T) {
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)
	val, err := UnmarshalSchemaAtAddr(context.Background(), vrw, hash.Parse("badaddrvvvvvvvvvvvvvvvvvvvvvvvvv"))
	require.Error(t, err)
	require.Nil(t, val)
}

func TestNomsMarshalling(t *testing.T) {
	tSchema := createTestSchema()
	_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_Default, nil, nil)

	if err != nil {
		t.Fatal("Could not create in mem noms db.")
	}

	val, err := MarshalSchema(context.Background(), vrw, tSchema)

	if err != nil {
		t.Fatal("Failed to marshal Schema as a types.Value.")
	}

	unMarshalled, err := UnmarshalSchema(context.Background(), types.Format_Default, val)

	if err != nil {
		t.Fatal("Failed to unmarshal types.Value as Schema")
	}

	if !assert.Equal(t, tSchema, unMarshalled) {
		t.Error("Value different after marshalling and unmarshalling.")
	}

	// this validation step only makes sense for Noms encoded schemas
	if !types.Format_Default.UsesFlatbuffers() {
		validated, err := validateUnmarshaledNomsValue(context.Background(), types.Format_Default, val)
		assert.NoError(t, err,
			"Failed compatibility test. Schema could not be unmarshalled with mirror type, error: %v", err)
		if !assert.Equal(t, tSchema, validated) {
			t.Error("Value different after marshalling and unmarshalling.")
		}
	}
}

func getSqlTypes() []sql.Type {
	//TODO: determine the storage format for BINARY
	//TODO: determine the storage format for BLOB
	//TODO: determine the storage format for LONGBLOB
	//TODO: determine the storage format for MEDIUMBLOB
	//TODO: determine the storage format for TINYBLOB
	//TODO: determine the storage format for VARBINARY
	return []sql.Type{
		gmstypes.Int64,  //BIGINT
		gmstypes.Uint64, //BIGINT UNSIGNED
		//sql.MustCreateBinary(sqltypes.Binary, 10), //BINARY(10)
		gmstypes.MustCreateBitType(10), //BIT(10)
		//sql.Blob, //BLOB
		gmstypes.Boolean, //BOOLEAN
		gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 10), //CHAR(10)
		gmstypes.Date,     //DATE
		gmstypes.Datetime, //DATETIME
		gmstypes.MustCreateColumnDecimalType(9, 5), //DECIMAL(9, 5)
		gmstypes.Float64, //DOUBLE
		gmstypes.MustCreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default), //ENUM('a','b','c')
		gmstypes.Float32, //FLOAT
		gmstypes.Int32,   //INT
		gmstypes.Uint32,  //INT UNSIGNED
		//sql.LongBlob, //LONGBLOB
		gmstypes.LongText, //LONGTEXT
		//sql.MediumBlob, //MEDIUMBLOB
		gmstypes.Int24,      //MEDIUMINT
		gmstypes.Uint24,     //MEDIUMINT UNSIGNED
		gmstypes.MediumText, //MEDIUMTEXT
		gmstypes.MustCreateSetType([]string{"a", "b", "c"}, sql.Collation_Default), //SET('a','b','c')
		gmstypes.Int16,     //SMALLINT
		gmstypes.Uint16,    //SMALLINT UNSIGNED
		gmstypes.Text,      //TEXT
		gmstypes.Time,      //TIME
		gmstypes.Timestamp, //TIMESTAMP
		//sql.TinyBlob, //TINYBLOB
		gmstypes.Int8,     //TINYINT
		gmstypes.Uint8,    //TINYINT UNSIGNED
		gmstypes.TinyText, //TINYTEXT
		//sql.MustCreateBinary(sqltypes.VarBinary, 10), //VARBINARY(10)
		gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 10),                //VARCHAR(10)
		gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf8mb3_bin), //VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin
		gmstypes.Year, //YEAR
	}
}

func TestTypeInfoMarshalling(t *testing.T) {
	for _, sqlType := range getSqlTypes() {
		t.Run(sqlType.String(), func(t *testing.T) {
			ti, err := typeinfo.FromSqlType(sqlType)
			require.NoError(t, err)
			col, err := schema.NewColumnWithTypeInfo("pk", 1, ti, true, "", false, "", schema.NotNullConstraint{})
			require.NoError(t, err)
			colColl := schema.NewColCollection(col)
			originalSch, err := schema.SchemaFromCols(colColl)
			require.NoError(t, err)

			nbf, err := types.GetFormatForVersionString(constants.FormatDefaultString)
			require.NoError(t, err)
			_, vrw, _, err := dbfactory.MemFactory{}.CreateDB(context.Background(), nbf, nil, nil)
			require.NoError(t, err)
			val, err := MarshalSchema(context.Background(), vrw, originalSch)
			require.NoError(t, err)
			unmarshalledSch, err := UnmarshalSchema(context.Background(), nbf, val)
			require.NoError(t, err)
			ok := schema.SchemasAreEqual(originalSch, unmarshalledSch)
			assert.True(t, ok)
		})
	}
}

func validateUnmarshaledNomsValue(ctx context.Context, nbf *types.NomsBinFormat, schemaVal types.Value) (schema.Schema, error) {
	var sd testSchemaData
	err := marshal.Unmarshal(ctx, nbf, schemaVal, &sd)

	if err != nil {
		return nil, err
	}

	return sd.decodeSchema()
}

func TestMirroredTypes(t *testing.T) {
	realType := reflect.ValueOf(&encodedColumn{}).Elem()
	mirrorType := reflect.ValueOf(&testEncodedColumn{}).Elem()
	require.Equal(t, mirrorType.NumField(), realType.NumField())

	// TODO: create reflection tests to ensure that:
	// - no fields in testEncodeColumn have the 'omitempty' annotation
	// - no legacy fields in encodeColumn have the 'omitempty' annotation (with whitelist)
	// - all new fields in encodeColumn have the 'omitempty' annotation
}

// testEncodedColumn is a mirror type of encodedColumn that helps ensure compatibility between Dolt versions
//
// If a field in encodedColumn is not optional, it should be added WITHOUT the "omitempty" annotation here
// in order to guarantee that all fields in encodeColumn are always being written when encodedColumn is serialized.
// See the comment above type encodeColumn.
type testEncodedColumn struct {
	Tag uint64 `noms:"tag" json:"tag"`

	Name string `noms:"name" json:"name"`

	Kind string `noms:"kind" json:"kind"`

	IsPartOfPK bool `noms:"is_part_of_pk" json:"is_part_of_pk"`

	TypeInfo encodedTypeInfo `noms:"typeinfo" json:"typeinfo"`

	Default string `noms:"default,omitempty" json:"default,omitempty"`

	AutoIncrement bool `noms:"auto_increment,omitempty" json:"auto_increment,omitempty"`

	Comment string `noms:"comment,omitempty" json:"comment,omitempty"`

	Constraints []encodedConstraint `noms:"col_constraints" json:"col_constraints"`
}

type testEncodedIndex struct {
	Name    string   `noms:"name" json:"name"`
	Tags    []uint64 `noms:"tags" json:"tags"`
	Comment string   `noms:"comment" json:"comment"`
	Unique  bool     `noms:"unique" json:"unique"`
	Hidden  bool     `noms:"hidden,omitempty" json:"hidden,omitempty"`
}

type testSchemaData struct {
	Columns         []testEncodedColumn `noms:"columns" json:"columns"`
	IndexCollection []testEncodedIndex  `noms:"idxColl,omitempty" json:"idxColl,omitempty"`
	Collation       schema.Collation    `noms:"collation,omitempty" json:"collation,omitempty"`
}

func (tec testEncodedColumn) decodeColumn() (schema.Column, error) {
	var typeInfo typeinfo.TypeInfo
	var err error
	if tec.TypeInfo.Type != "" {
		typeInfo, err = tec.TypeInfo.decodeTypeInfo()
		if err != nil {
			return schema.Column{}, err
		}
	} else if tec.Kind != "" {
		typeInfo = typeinfo.FromKind(schema.LwrStrToKind[tec.Kind])
	} else {
		return schema.Column{}, errors.New("cannot decode column due to unknown schema format")
	}
	colConstraints := decodeAllColConstraint(tec.Constraints)
	return schema.NewColumnWithTypeInfo(tec.Name, tec.Tag, typeInfo, tec.IsPartOfPK, tec.Default, tec.AutoIncrement, tec.Comment, colConstraints...)
}

func (tsd testSchemaData) decodeSchema() (schema.Schema, error) {
	numCols := len(tsd.Columns)
	cols := make([]schema.Column, numCols)

	var err error
	for i, col := range tsd.Columns {
		cols[i], err = col.decodeColumn()
		if err != nil {
			return nil, err
		}
	}

	colColl := schema.NewColCollection(cols...)

	sch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	sch.SetCollation(tsd.Collation)

	for _, encodedIndex := range tsd.IndexCollection {
		_, err = sch.Indexes().AddIndexByColTags(encodedIndex.Name, encodedIndex.Tags, nil, schema.IndexProperties{IsUnique: encodedIndex.Unique, Comment: encodedIndex.Comment})
		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}

func TestSchemaMarshalling(t *testing.T) {
	ctx := context.Background()
	nbf := types.Format_Default
	vrw := getTestVRW(nbf)
	schemas := getSchemas(t, 1000)
	for _, sch := range schemas {
		v, err := MarshalSchema(ctx, vrw, sch)
		require.NoError(t, err)
		s, err := UnmarshalSchema(ctx, nbf, v)
		require.NoError(t, err)
		assert.Equal(t, sch, s)
	}
}

func getTypeinfo(t *testing.T) (ti []typeinfo.TypeInfo) {
	st := getSqlTypes()
	ti = make([]typeinfo.TypeInfo, len(st))
	for i := range st {
		var err error
		ti[i], err = typeinfo.FromSqlType(st[i])
		require.NoError(t, err)
	}
	return
}

func getColumns(t *testing.T) (cols []schema.Column) {
	ti := getTypeinfo(t)
	cols = make([]schema.Column, len(ti))
	var err error
	for i := range cols {
		name := "col" + strconv.Itoa(i)
		tag := uint64(i)
		cols[i], err = schema.NewColumnWithTypeInfo(name, tag, ti[i], false, "", false, "")
		require.NoError(t, err)
	}
	return
}

func getSchemas(t *testing.T, n int) (schemas []schema.Schema) {
	cols := getColumns(t)
	schemas = make([]schema.Schema, n)
	var err error
	for i := range schemas {
		rand.Shuffle(len(cols), func(i, j int) {
			cols[i], cols[j] = cols[j], cols[i]
		})
		k := rand.Intn(len(cols)-1) + 1
		cc := make([]schema.Column, k)
		copy(cc, cols)
		cc[0].IsPartOfPK = true
		cc[0].Constraints = []schema.ColConstraint{schema.NotNullConstraint{}}
		schemas[i], err = schema.SchemaFromCols(
			schema.NewColCollection(cc...))
		require.NoError(t, err)
	}
	return
}

func getTestVRW(nbf *types.NomsBinFormat) types.ValueReadWriter {
	ts := &chunks.TestStorage{}
	cs := ts.NewViewWithFormat(nbf.VersionString())
	return types.NewValueStore(cs)
}
