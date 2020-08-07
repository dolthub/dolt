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

package encoding

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/liquidata-inc/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/dbfactory"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/constants"
	"github.com/liquidata-inc/dolt/go/store/marshal"
	"github.com/liquidata-inc/dolt/go/store/types"
)

func createTestSchema() schema.Schema {
	columns := []schema.Column{
		schema.NewColumn("id", 4, types.UUIDKind, true, schema.NotNullConstraint{}),
		schema.NewColumn("first", 1, types.StringKind, false),
		schema.NewColumn("last", 2, types.StringKind, false, schema.NotNullConstraint{}),
		schema.NewColumn("age", 3, types.UintKind, false),
	}

	colColl, _ := schema.NewColCollection(columns...)
	sch := schema.SchemaFromCols(colColl)
	_, _ = sch.Indexes().AddIndexByColTags("idx_age", []uint64{3}, schema.IndexProperties{IsUnique: false, Comment: ""})
	return sch
}

func TestNomsMarshalling(t *testing.T) {
	tSchema := createTestSchema()
	db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), types.Format_7_18, nil, nil)

	if err != nil {
		t.Fatal("Could not create in mem noms db.")
	}

	val, err := MarshalSchemaAsNomsValue(context.Background(), db, tSchema)

	if err != nil {
		t.Fatal("Failed to marshal Schema as a types.Value.")
	}

	unMarshalled, err := UnmarshalSchemaNomsValue(context.Background(), types.Format_7_18, val)

	if err != nil {
		t.Fatal("Failed to unmarshal types.Value as Schema")
	}

	if !reflect.DeepEqual(tSchema, unMarshalled) {
		t.Error("Value different after marshalling and unmarshalling.")
	}

	validated, err := validateUnmarshaledNomsValue(context.Background(), types.Format_7_18, val)

	if err != nil {
		t.Fatal("Failed compatibility test. Schema could not be unmarshalled with mirror type")
	}

	if !reflect.DeepEqual(tSchema, validated) {
		t.Error("Value different after marshalling and unmarshalling.")
	}

	tSuperSchema, err := schema.NewSuperSchema(tSchema)
	require.NoError(t, err)

	ssVal, err := MarshalSuperSchemaAsNomsValue(context.Background(), db, tSuperSchema)
	require.NoError(t, err)

	unMarshalledSS, err := UnmarshalSuperSchemaNomsValue(context.Background(), types.Format_7_18, ssVal)
	require.NoError(t, err)

	if !reflect.DeepEqual(tSuperSchema, unMarshalledSS) {
		t.Error("Value different after marshalling and unmarshalling.")
	}

}

func TestTypeInfoMarshalling(t *testing.T) {
	//TODO: determine the storage format for BINARY
	//TODO: determine the storage format for BLOB
	//TODO: determine the storage format for LONGBLOB
	//TODO: determine the storage format for MEDIUMBLOB
	//TODO: determine the storage format for TINYBLOB
	//TODO: determine the storage format for VARBINARY
	sqlTypes := []sql.Type{
		sql.Int64,  //BIGINT
		sql.Uint64, //BIGINT UNSIGNED
		//sql.MustCreateBinary(sqltypes.Binary, 10), //BINARY(10)
		sql.MustCreateBitType(10), //BIT(10)
		//sql.Blob, //BLOB
		sql.Boolean, //BOOLEAN
		sql.MustCreateStringWithDefaults(sqltypes.Char, 10), //CHAR(10)
		sql.Date,                        //DATE
		sql.Datetime,                    //DATETIME
		sql.MustCreateDecimalType(9, 5), //DECIMAL(9, 5)
		sql.Float64,                     //DOUBLE
		sql.MustCreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default), //ENUM('a','b','c')
		sql.Float32, //FLOAT
		sql.Int32,   //INT
		sql.Uint32,  //INT UNSIGNED
		//sql.LongBlob, //LONGBLOB
		sql.LongText, //LONGTEXT
		//sql.MediumBlob, //MEDIUMBLOB
		sql.Int24,      //MEDIUMINT
		sql.Uint24,     //MEDIUMINT UNSIGNED
		sql.MediumText, //MEDIUMTEXT
		sql.MustCreateSetType([]string{"a", "b", "c"}, sql.Collation_Default), //SET('a','b','c')
		sql.Int16,     //SMALLINT
		sql.Uint16,    //SMALLINT UNSIGNED
		sql.Text,      //TEXT
		sql.Time,      //TIME
		sql.Timestamp, //TIMESTAMP
		//sql.TinyBlob, //TINYBLOB
		sql.Int8,     //TINYINT
		sql.Uint8,    //TINYINT UNSIGNED
		sql.TinyText, //TINYTEXT
		//sql.MustCreateBinary(sqltypes.VarBinary, 10), //VARBINARY(10)
		sql.MustCreateStringWithDefaults(sqltypes.VarChar, 10),                //VARCHAR(10)
		sql.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf8mb3_bin), //VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin
		sql.Year, //YEAR
	}

	for _, sqlType := range sqlTypes {
		t.Run(sqlType.String(), func(t *testing.T) {
			ti, err := typeinfo.FromSqlType(sqlType)
			require.NoError(t, err)
			col, err := schema.NewColumnWithTypeInfo("pk", 1, ti, true)
			require.NoError(t, err)
			colColl, err := schema.NewColCollection(col)
			require.NoError(t, err)
			originalSch := schema.SchemaFromCols(colColl)

			nbf, err := types.GetFormatForVersionString(constants.FormatDefaultString)
			require.NoError(t, err)
			db, err := dbfactory.MemFactory{}.CreateDB(context.Background(), nbf, nil, nil)
			require.NoError(t, err)
			val, err := MarshalSchemaAsNomsValue(context.Background(), db, originalSch)
			require.NoError(t, err)
			unmarshalledSch, err := UnmarshalSchemaNomsValue(context.Background(), nbf, val)
			require.NoError(t, err)
			ok, err := schema.SchemasAreEqual(originalSch, unmarshalledSch)
			assert.NoError(t, err)
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
// Fields in this test struct should be added WITHOUT the "omitempty" annotation in order to guarantee that
// all fields in encodeColumn are always being written when encodedColumn is serialized.
// See the comment above type encodeColumn.
type testEncodedColumn struct {
	Tag uint64 `noms:"tag" json:"tag"`

	Name string `noms:"name" json:"name"`

	Kind string `noms:"kind" json:"kind"`

	IsPartOfPK bool `noms:"is_part_of_pk" json:"is_part_of_pk"`

	TypeInfo encodedTypeInfo `noms:"typeinfo" json:"typeinfo"`

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
	return schema.NewColumnWithTypeInfo(tec.Name, tec.Tag, typeInfo, tec.IsPartOfPK, colConstraints...)
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

	colColl, err := schema.NewColCollection(cols...)

	if err != nil {
		return nil, err
	}

	sch := schema.SchemaFromCols(colColl)

	for _, encodedIndex := range tsd.IndexCollection {
		_, err = sch.Indexes().AddIndexByColTags(encodedIndex.Name, encodedIndex.Tags, schema.IndexProperties{IsUnique: encodedIndex.Unique, Comment: encodedIndex.Comment})
		if err != nil {
			return nil, err
		}
	}

	return sch, nil
}
