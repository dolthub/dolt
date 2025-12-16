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
	"math/rand"
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
	"github.com/dolthub/dolt/go/store/types"
)

func getSqlTypes() []sql.Type {
	// TODO: determine the storage format for BINARY
	// TODO: determine the storage format for BLOB
	// TODO: determine the storage format for LONGBLOB
	// TODO: determine the storage format for MEDIUMBLOB
	// TODO: determine the storage format for TINYBLOB
	// TODO: determine the storage format for VARBINARY
	return []sql.Type{
		gmstypes.Int64,  // BIGINT
		gmstypes.Uint64, // BIGINT UNSIGNED
		// sql.MustCreateBinary(sqltypes.Binary, 10), //BINARY(10)
		gmstypes.MustCreateBitType(10), // BIT(10)
		// sql.Blob, //BLOB
		gmstypes.Boolean, // BOOLEAN
		gmstypes.MustCreateStringWithDefaults(sqltypes.Char, 10), // CHAR(10)
		gmstypes.Date,     // DATE
		gmstypes.Datetime, // DATETIME
		gmstypes.MustCreateColumnDecimalType(9, 5), // DECIMAL(9, 5)
		gmstypes.Float64, // DOUBLE
		gmstypes.MustCreateEnumType([]string{"a", "b", "c"}, sql.Collation_Default), // ENUM('a','b','c')
		gmstypes.Float32, // FLOAT
		gmstypes.Int32,   // INT
		gmstypes.Uint32,  // INT UNSIGNED
		// sql.LongBlob, //LONGBLOB
		gmstypes.LongText, // LONGTEXT
		// sql.MediumBlob, //MEDIUMBLOB
		gmstypes.Int24,      // MEDIUMINT
		gmstypes.Uint24,     // MEDIUMINT UNSIGNED
		gmstypes.MediumText, // MEDIUMTEXT
		gmstypes.MustCreateSetType([]string{"a", "b", "c"}, sql.Collation_Default), // SET('a','b','c')
		gmstypes.Int16,     // SMALLINT
		gmstypes.Uint16,    // SMALLINT UNSIGNED
		gmstypes.Text,      // TEXT
		gmstypes.Time,      // TIME
		gmstypes.Timestamp, // TIMESTAMP
		// sql.TinyBlob, //TINYBLOB
		gmstypes.Int8,     // TINYINT
		gmstypes.Uint8,    // TINYINT UNSIGNED
		gmstypes.TinyText, // TINYTEXT
		// sql.MustCreateBinary(sqltypes.VarBinary, 10), //VARBINARY(10)
		gmstypes.MustCreateStringWithDefaults(sqltypes.VarChar, 10),                // VARCHAR(10)
		gmstypes.MustCreateString(sqltypes.VarChar, 10, sql.Collation_utf8mb3_bin), // VARCHAR(10) CHARACTER SET utf8mb3 COLLATE utf8mb3_bin
		gmstypes.Year, // YEAR
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
