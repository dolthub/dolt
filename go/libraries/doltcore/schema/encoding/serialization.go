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
	"fmt"

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
	buf, err := serializeSchemaAsFlatbuffer(sch)
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
	assertTrue(nbf.UsesFlatbuffers())
	sm, ok := v.(types.SerialMessage)
	assertTrue(ok)
	return deserializeSchemaFromFlatbuffer(ctx, sm)
}

func serializeSchemaAsFlatbuffer(sch schema.Schema) ([]byte, error) {
	if schema.IsKeyless(sch) {
		// todo(andy): keyless id column
		return nil, fmt.Errorf("keyless schemas not supported")
	}

	b := fb.NewBuilder(1024)
	rows := serializeClusteredIndex(b, sch)
	columns := serializeSchemaColumns(b, sch.GetAllCols().GetColumns())
	indexes := serializeSecondaryIndexes(b, sch, sch.Indexes().AllIndexes())
	checks := serializeChecks(b, sch.Checks().AllChecks())

	serial.TableSchemaStart(b)
	serial.TableSchemaAddRows(b, rows)
	serial.TableSchemaAddColumns(b, columns)
	serial.TableSchemaAddIndexes(b, indexes)
	serial.TableSchemaAddChecks(b, checks)
	root := serial.TableSchemaEnd(b)
	b.FinishWithFileIdentifier(root, []byte(serial.TableSchemaFileID))
	return b.FinishedBytes(), nil
}

func deserializeSchemaFromFlatbuffer(ctx context.Context, buf []byte) (schema.Schema, error) {
	assertTrue(serial.GetFileID(buf) == serial.TableSchemaFileID)
	s := serial.GetRootAsTableSchema(buf, 0)

	cols, err := deserializeColumns(ctx, s)
	if err != nil {
		return nil, err
	}
	sch, err := schema.SchemaFromCols(schema.NewColCollection(cols...))
	if err != nil {
		return nil, err
	}

	err = sch.SetPkOrdinals(deserializeClusteredIndex(s))
	if err != nil {
		return nil, err
	}

	err = deserializeSecondaryIndexes(sch, s)
	if err != nil {
		return nil, err
	}

	err = deserializeChecks(sch, s)
	if err != nil {
		return nil, err
	}

	return sch, nil
}

func serializeClusteredIndex(b *fb.Builder, sch schema.Schema) fb.UOffsetT {
	// serialize key columns
	pkMap := sch.GetPkOrdinals()
	serial.IndexStartIndexColumnsVector(b, len(pkMap))
	for i := len(pkMap) - 1; i >= 0; i-- {
		b.PrependUint16(uint16(pkMap[i]))
	}
	// key_columns == index_columns
	ko := b.EndVector(len(pkMap))

	// serialize value columns
	nonPk := sch.GetNonPKCols().GetColumns()
	serial.IndexStartValueColumnsVector(b, len(nonPk))
	for i := len(nonPk) - 1; i >= 0; i-- {
		col := nonPk[i]
		idx := sch.GetAllCols().TagToIdx[col.Tag]
		b.PrependUint16(uint16(idx))
	}
	vo := b.EndVector(len(nonPk))

	serial.IndexStart(b)
	serial.IndexAddIndexColumns(b, ko)
	serial.IndexAddKeyColumns(b, ko)
	serial.IndexAddValueColumns(b, vo)
	serial.IndexAddPrimaryKey(b, true)
	serial.IndexAddUniqueKey(b, true)
	serial.IndexAddSystemDefined(b, false)
	return serial.IndexEnd(b)
}

func deserializeClusteredIndex(s *serial.TableSchema) []int {
	idx := s.Rows(nil)
	pkOrdinals := make([]int, idx.KeyColumnsLength())
	for i := range pkOrdinals {
		pkOrdinals[i] = int(idx.KeyColumns(i))
	}
	return pkOrdinals
}

func serializeSchemaColumns(b *fb.Builder, cols []schema.Column) fb.UOffsetT {
	offs := make([]fb.UOffsetT, len(cols))
	for i := len(cols) - 1; i >= 0; i-- {
		col := cols[i]
		co := b.CreateString(col.Comment)
		do := b.CreateString(col.Default)
		to := b.CreateString(sqlTypeString(col.TypeInfo))
		no := b.CreateString(col.Name)

		serial.ColumnStart(b)
		serial.ColumnAddName(b, no)
		serial.ColumnAddSqlType(b, to)
		serial.ColumnAddDefaultValue(b, do)
		serial.ColumnAddComment(b, co)
		serial.ColumnAddDisplayOrder(b, int16(i))
		serial.ColumnAddTag(b, col.Tag)
		serial.ColumnAddEncoding(b, encodingFromType(col.TypeInfo))
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

func deserializeColumns(ctx context.Context, s *serial.TableSchema) ([]schema.Column, error) {
	cols := make([]schema.Column, s.ColumnsLength())
	c := new(serial.Column)
	for i := range cols {
		ok := s.Columns(c, i)
		assertTrue(ok)

		sqlType, err := typeinfoFromSqlType(ctx, string(c.SqlType()))
		if err != nil {
			return nil, err
		}

		var constraints []schema.ColConstraint
		if !c.Nullable() || c.PrimaryKey() {
			constraints = append(constraints, schema.NotNullConstraint{})
		}

		cols[i], err = schema.NewColumnWithTypeInfo(
			string(c.Name()),
			c.Tag(),
			sqlType,
			c.PrimaryKey(),
			string(c.DefaultValue()),
			c.AutoIncrement(),
			string(c.Comment()),
			constraints...)
		if err != nil {
			return nil, err
		}
	}
	return cols, nil
}

func serializeSecondaryIndexes(b *fb.Builder, sch schema.Schema, indexes []schema.Index) fb.UOffsetT {
	ordinalMap := sch.GetAllCols().TagToIdx
	offs := make([]fb.UOffsetT, len(indexes))
	for i := len(offs) - 1; i >= 0; i-- {
		idx := indexes[i]
		no := b.CreateString(idx.Name())
		co := b.CreateString(idx.Comment())

		// serialize indexed columns
		tags := idx.IndexedColumnTags()
		serial.IndexStartIndexColumnsVector(b, len(tags))
		for j := len(tags) - 1; j >= 0; j-- {
			ord := ordinalMap[tags[j]]
			b.PrependUint16(uint16(ord))
		}
		ico := b.EndVector(len(tags))

		// serialize key columns
		tags = idx.AllTags()
		serial.IndexStartKeyColumnsVector(b, len(tags))
		for j := len(tags) - 1; j >= 0; j-- {
			ord := ordinalMap[tags[j]]
			b.PrependUint16(uint16(ord))
		}
		ko := b.EndVector(len(tags))

		serial.IndexStart(b)
		serial.IndexAddName(b, no)
		serial.IndexAddComment(b, co)
		serial.IndexAddIndexColumns(b, ico)
		serial.IndexAddKeyColumns(b, ko)
		serial.IndexAddPrimaryKey(b, false)
		serial.IndexAddUniqueKey(b, idx.IsUnique())
		serial.IndexAddSystemDefined(b, !idx.IsUserDefined())
		offs[i] = serial.IndexEnd(b)
	}

	serial.TableSchemaStartIndexesVector(b, len(indexes))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(indexes))
}

func deserializeSecondaryIndexes(sch schema.Schema, s *serial.TableSchema) error {
	idx := new(serial.Index)
	col := new(serial.Column)
	for i := 0; i < s.IndexesLength(); i++ {
		s.Indexes(idx, i)
		assertTrue(!idx.PrimaryKey())

		name := string(idx.Name())
		props := schema.IndexProperties{
			IsUnique:      idx.UniqueKey(),
			IsUserDefined: !idx.SystemDefined(),
			Comment:       string(idx.Comment()),
		}

		tags := make([]uint64, idx.IndexColumnsLength())
		for j := range tags {
			ord := idx.IndexColumns(j)
			s.Columns(col, int(ord))
			tags[j] = col.Tag()
		}

		_, err := sch.Indexes().AddIndexByColTags(name, tags, props)
		if err != nil {
			return err
		}
	}
	return nil
}

func serializeChecks(b *fb.Builder, checks []schema.Check) fb.UOffsetT {
	offs := make([]fb.UOffsetT, len(checks))
	for i := len(offs) - 1; i >= 0; i-- {
		eo := b.CreateString(checks[i].Expression())
		no := b.CreateString(checks[i].Name())
		serial.CheckConstraintStart(b)
		serial.CheckConstraintAddEnforced(b, checks[i].Enforced())
		serial.CheckConstraintAddExpression(b, eo)
		serial.CheckConstraintAddName(b, no)
		offs[i] = serial.CheckConstraintEnd(b)
	}

	serial.TableSchemaStartChecksVector(b, len(checks))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(checks))
}

func deserializeChecks(sch schema.Schema, s *serial.TableSchema) error {
	coll := sch.Checks()
	c := new(serial.CheckConstraint)
	for i := 0; i < s.ChecksLength(); i++ {
		s.Checks(c, i)
		n, e := string(c.Name()), string(c.Expression())
		if _, err := coll.AddCheck(n, e, c.Enforced()); err != nil {
			return err
		}
	}
	return nil
}

func sqlTypeString(t typeinfo.TypeInfo) string {
	return t.ToSqlType().String()
}

func typeinfoFromSqlType(ctx context.Context, s string) (typeinfo.TypeInfo, error) {
	t, err := parse.ParseColumnTypeString(sql.NewContext(ctx), s)
	if err != nil {
		return nil, err
	}
	return typeinfo.FromSqlType(t)
}

func encodingFromType(t typeinfo.TypeInfo) serial.Encoding {
	// todo(andy)
	return serial.EncodingNull
}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
