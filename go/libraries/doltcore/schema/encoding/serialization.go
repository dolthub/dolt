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
	builderBufferSize = 1500
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

	b := fb.NewBuilder(1024)
	cols := serializeSchemaColumns(b, sch.GetAllCols().GetColumns())

	serial.TableSchemaStart(b)
	serial.TableSchemaAddColumns(b, cols)
	root := serial.TableSchemaEnd(b)
	b.FinishWithFileIdentifier(root, []byte(serial.TableSchemaFileID))
	return b.FinishedBytes(), nil
}

func serializeSchemaColumns(b *fb.Builder, cols []schema.Column) fb.UOffsetT {
	offs := make([]fb.UOffsetT, len(cols))
	for i := len(cols) - 1; i >= 0; i-- {
		col := cols[i]
		no := b.CreateString(col.Name)
		to := b.CreateString(sqlTypeString(col.TypeInfo))
		serial.ColumnStart(b)
		serial.ColumnAddName(b, no)
		serial.ColumnAddSqlType(b, to)
		serial.ColumnAddDisplayOrder(b, int16(i))
		serial.ColumnAddTag(b, col.Tag)
		serial.ColumnAddEncoding(b, getEncoding(col.TypeInfo))
		serial.ColumnAddPrimaryKey(b, col.IsPartOfPK)
		serial.ColumnAddAutoIncrement(b, col.AutoIncrement)
		serial.ColumnAddNullable(b, col.IsNullable())
		serial.ColumnAddGenerated(b, false)
		serial.ColumnAddVirtual(b, false)
		serial.ColumnAddHidden(b, false)
		offs[i] = serial.ColumnEnd(b)
	}
	serial.TableSchemaStartColumnsVector(b, len(offs))
	for i := len(cols) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
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
	assertTrue(serial.GetFileID(buf) == serial.TableSchemaFileID)
	s := serial.GetRootAsTableSchema(buf, 0)

	cols := make([]schema.Column, s.ColumnsLength())
	c := new(serial.Column)
	for i := range cols {
		ok := s.Columns(c, i)
		assertTrue(ok)

		sqlType, err := typeinfoFromSqlType(ctx, string(c.SqlType()))
		if err != nil {
			return nil, err
		}

		cols[i], err = schema.NewColumnWithTypeInfo(
			string(c.Name()),
			c.Tag(),
			sqlType,
			c.PrimaryKey(),
			string(c.DefaultValue()),
			c.AutoIncrement(),
			string(c.Comment()))
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
