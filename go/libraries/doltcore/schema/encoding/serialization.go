// Copyright 2022 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	fb "github.com/google/flatbuffers/go"

	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	builderBufferSize = 4096
)

func SerializeSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.Value, error) {
	buf, err := serializeSchemaAsFlatbuffer(ctx, sch)
	if err != nil {
		return nil, err
	}

	v := types.SerialMessage(buf)
	if _, err = vrw.WriteValue(ctx, v); err != nil {
		return nil, err
	}
	return v, nil
}

func DeserializeSchema(ctx context.Context, nbf *types.NomsBinFormat, v types.Value) (schema.Schema, error) {
	assertTrue(types.IsFormat_DOLT_1(nbf))
	sm, ok := v.(types.SerialMessage)
	assertTrue(ok)
	return deserializeSchemaFromFlatbuffer(ctx, []byte(sm))
}

func serializeSchemaAsFlatbuffer(ctx context.Context, sch schema.Schema) ([]byte, error) {
	if sch.Indexes().Count() > 0 {
		panic("indexes not supported")
	}
	if sch.Checks().Count() > 0 {
		panic("check constraints not supported")
	}
	if schema.IsKeyless(sch) {
		// todo(andy): keyless id column
		panic("keyless indexes not supported")
	}

	b := fb.NewBuilder(builderBufferSize)
	columns := serializeColumns(b, sch.GetAllCols().GetColumns())

	serial.TableSchemaStart(b)
	serial.TableSchemaAddColumns(b, columns)
	b.Finish(serial.TableSchemaEnd(b))
	return b.FinishedBytes(), nil
}

func serializeColumns(b *fb.Builder, cols []schema.Column) fb.UOffsetT {
	// first serialize Column objects
	offs := make([]fb.UOffsetT, len(cols))
	for i, col := range cols {
		no := b.CreateString(col.Name)
		do := b.CreateString(sqlTypeString(col.TypeInfo))
		vo := b.CreateString(col.Default)
		co := b.CreateString(col.Comment)
		serial.ColumnStart(b)
		serial.ColumnAddName(b, no)
		serial.ColumnAddSqlType(b, do)
		serial.ColumnAddDefaultValue(b, vo)
		serial.ColumnAddComment(b, co)
		serial.ColumnAddDisplayOrder(b, int16(i)) // todo(andy)
		serial.ColumnAddEncoding(b, getEncoding(col.TypeInfo))
		serial.ColumnAddPrimaryKey(b, col.IsPartOfPK)
		serial.ColumnAddNullable(b, col.IsNullable())
		serial.ColumnAddAutoIncrement(b, col.AutoIncrement)
		serial.ColumnAddAutoIncrement(b, col.AutoIncrement)
		offs[i] = serial.ColumnEnd(b)
	}
	// then serialize a vector of offsets to the Columns
	serial.TableSchemaStartColumnsVector(b, len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PlaceUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func sqlTypeString(t typeinfo.TypeInfo) string {
	return t.ToSqlType().String()
}

func getEncoding(t typeinfo.TypeInfo) serial.Encoding {
	// todo(andy)
	return serial.EncodingNull
}

func deserializeSchemaFromFlatbuffer(ctx context.Context, buf []byte) (schema.Schema, error) {
	s := serial.GetRootAsTableSchema(buf, 0)
	cols := make([]schema.Column, s.ColumnsLength())

	for i := range cols {
		var c *serial.Column
		assertTrue(s.Columns(c, i))

		typeInfo, err := typeinfoFromSqlType(ctx, string(c.SqlType()))
		if err != nil {
			return nil, err
		}

		cols[i], err = schema.NewColumnWithTypeInfo(
			string(c.Name()), uint64(i),
			typeInfo,
			c.PrimaryKey(),
			string(c.DefaultValue()),
			c.AutoIncrement(),
			string(c.Comment()),
		)
		if err != nil {
			return nil, err
		}
	}

	cc := schema.NewColCollection(cols...)
	return schema.SchemaFromCols(cc)
}

func typeinfoFromSqlType(ctx context.Context, s string) (typeinfo.TypeInfo, error) {
	t, err := parse.ParseColumnTypeString(sql.NewContext(ctx), s)
	if err != nil {
		return nil, err
	}
	return typeinfo.FromSqlType(t)
}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
