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

	keylessIdCol   = "keyless_hash_id"
	keylessCardCol = "keyless_cardinality"
)

// SerializeSchema serializes a schema.Schema as a Flatbuffer message wrapped in a serial.Message.
func SerializeSchema(ctx context.Context, vrw types.ValueReadWriter, sch schema.Schema) (types.SerialMessage, error) {
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

func serializeSchemaAsFlatbuffer(sch schema.Schema) ([]byte, error) {
	b := fb.NewBuilder(1024)
	columns := serializeSchemaColumns(b, sch)
	rows := serializeClusteredIndex(b, sch)
	indexes := serializeSecondaryIndexes(b, sch, sch.Indexes().AllIndexes())
	checks := serializeChecks(b, sch.Checks().AllChecks())

	serial.TableSchemaStart(b)
	serial.TableSchemaAddClusteredIndex(b, rows)
	serial.TableSchemaAddColumns(b, columns)
	serial.TableSchemaAddSecondaryIndexes(b, indexes)
	serial.TableSchemaAddChecks(b, checks)
	root := serial.TableSchemaEnd(b)
	b.FinishWithFileIdentifier(root, []byte(serial.TableSchemaFileID))
	return b.FinishedBytes(), nil
}

// DeserializeSchema deserializes a schema.Schema from a serial.Message.
func DeserializeSchema(ctx context.Context, nbf *types.NomsBinFormat, v types.Value) (schema.Schema, error) {
	assertTrue(nbf.UsesFlatbuffers())
	sm, ok := v.(types.SerialMessage)
	assertTrue(ok)
	return deserializeSchemaFromFlatbuffer(ctx, sm)
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

// clustered indexes

func serializeClusteredIndex(b *fb.Builder, sch schema.Schema) fb.UOffsetT {
	keyless := schema.IsKeyless(sch)

	// serialize key columns
	var ko fb.UOffsetT
	if keyless {
		// keyless id is the 2nd to last column
		// in the columns vector (by convention)
		// and the only field in key tuples of
		// the clustered index.
		idPos := sch.GetAllCols().Size()
		serial.IndexStartIndexColumnsVector(b, 1)
		b.PrependUint16(uint16(idPos))
		ko = b.EndVector(1)
	} else {
		pkMap := sch.GetPkOrdinals()
		serial.IndexStartIndexColumnsVector(b, len(pkMap))
		for i := len(pkMap) - 1; i >= 0; i-- {
			b.PrependUint16(uint16(pkMap[i]))
		}
		ko = b.EndVector(len(pkMap))
	}

	// serialize value columns
	nonPk := sch.GetNonPKCols().GetColumns()
	length := len(nonPk)
	if keyless {
		length++
	}
	serial.IndexStartValueColumnsVector(b, length)
	for i := len(nonPk) - 1; i >= 0; i-- {
		col := nonPk[i]
		pos := sch.GetAllCols().TagToIdx[col.Tag]
		b.PrependUint16(uint16(pos))
	}
	if keyless {
		// keyless cardinality is the last column
		// in the columns vector (by convention)
		// and the first field in value tuples of
		// the clustered index.
		cardPos := sch.GetAllCols().Size() + 1
		b.PrependUint16(uint16(cardPos))
	}
	vo := b.EndVector(length)

	serial.IndexStart(b)
	// key_columns == index_columns for clustered index
	serial.IndexAddIndexColumns(b, ko)
	serial.IndexAddKeyColumns(b, ko)
	serial.IndexAddValueColumns(b, vo)
	serial.IndexAddPrimaryKey(b, true)
	serial.IndexAddUniqueKey(b, true)
	serial.IndexAddSystemDefined(b, false)
	return serial.IndexEnd(b)
}

func deserializeClusteredIndex(s *serial.TableSchema) []int {
	// check for keyless schema
	if keylessSerialSchema(s) {
		return nil
	}

	ci := s.ClusteredIndex(nil)
	pkOrdinals := make([]int, ci.KeyColumnsLength())
	for i := range pkOrdinals {
		pkOrdinals[i] = int(ci.KeyColumns(i))
	}
	return pkOrdinals
}

func serializeSchemaColumns(b *fb.Builder, sch schema.Schema) fb.UOffsetT {
	cols := sch.GetAllCols().GetColumns()
	offs := make([]fb.UOffsetT, len(cols))

	if schema.IsKeyless(sch) {
		// (6/15/22)
		// currently, keyless id and cardinality columns
		// do not exist in schema.Schema
		// we do serialize them in the flatbuffer
		// message, in order to describe index storage.
		// by convention, they are stored as the last
		// two columns in the columns vector.
		id, card := serializeHiddenKeylessColumns(b)
		offs = append(offs, id, card)
	}

	// serialize columns in |cols|
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
		// schema.Schema determines display order
		serial.ColumnAddDisplayOrder(b, int16(i))
		serial.ColumnAddTag(b, col.Tag)
		serial.ColumnAddEncoding(b, encodingFromTypeinfo(col.TypeInfo))
		serial.ColumnAddPrimaryKey(b, col.IsPartOfPK)
		serial.ColumnAddAutoIncrement(b, col.AutoIncrement)
		serial.ColumnAddNullable(b, col.IsNullable())
		serial.ColumnAddGenerated(b, false)
		serial.ColumnAddVirtual(b, false)
		serial.ColumnAddHidden(b, false)
		offs[i] = serial.ColumnEnd(b)
	}

	// create the columns array with all columns
	serial.TableSchemaStartColumnsVector(b, len(offs))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(offs))
}

func serializeHiddenKeylessColumns(b *fb.Builder) (id, card fb.UOffsetT) {
	// cardinality column
	no := b.CreateString(keylessCardCol)
	serial.ColumnStart(b)
	serial.ColumnAddName(b, no)
	serial.ColumnAddDisplayOrder(b, int16(-1))
	serial.ColumnAddTag(b, schema.KeylessRowCardinalityTag)
	serial.ColumnAddEncoding(b, serial.EncodingUint64)
	// set hidden and generated to true
	serial.ColumnAddGenerated(b, true)
	serial.ColumnAddHidden(b, true)
	serial.ColumnAddPrimaryKey(b, false)
	serial.ColumnAddAutoIncrement(b, false)
	serial.ColumnAddNullable(b, false)
	serial.ColumnAddVirtual(b, false)
	card = serial.ColumnEnd(b)

	// hash id column
	no = b.CreateString(keylessIdCol)
	serial.ColumnStart(b)
	serial.ColumnAddName(b, no)
	serial.ColumnAddDisplayOrder(b, int16(-1))
	serial.ColumnAddTag(b, schema.KeylessRowIdTag)
	serial.ColumnAddEncoding(b, serial.EncodingHash128)
	// set hidden and generated to true
	serial.ColumnAddGenerated(b, true)
	serial.ColumnAddHidden(b, true)
	serial.ColumnAddPrimaryKey(b, false)
	serial.ColumnAddAutoIncrement(b, false)
	serial.ColumnAddNullable(b, false)
	serial.ColumnAddVirtual(b, false)
	id = serial.ColumnEnd(b)

	return
}

func deserializeColumns(ctx context.Context, s *serial.TableSchema) ([]schema.Column, error) {
	length := s.ColumnsLength()
	if keylessSerialSchema(s) {
		// (6/15/22)
		// currently, keyless id and cardinality columns
		// do not exist in schema.Schema
		// we do serialize them in the flatbuffer
		// message, in order to describe index storage.
		// by convention, they are stored as the last
		// two columns in the columns vector.
		length -= 2
	}

	cols := make([]schema.Column, length)
	c := new(serial.Column)
	for i := range cols {
		s.Columns(c, i)
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
			string(c.Comment()),
			constraintsFromSerialColumn(c)...)
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
			pos := ordinalMap[tags[j]]
			b.PrependUint16(uint16(pos))
		}
		ico := b.EndVector(len(tags))

		// serialize key columns
		tags = idx.AllTags()
		serial.IndexStartKeyColumnsVector(b, len(tags))
		for j := len(tags) - 1; j >= 0; j-- {
			pos := ordinalMap[tags[j]]
			b.PrependUint16(uint16(pos))
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

	serial.TableSchemaStartSecondaryIndexesVector(b, len(indexes))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(indexes))
}

func deserializeSecondaryIndexes(sch schema.Schema, s *serial.TableSchema) error {
	idx := new(serial.Index)
	col := new(serial.Column)
	for i := 0; i < s.SecondaryIndexesLength(); i++ {
		s.SecondaryIndexes(idx, i)
		assertTrue(!idx.PrimaryKey())

		name := string(idx.Name())
		props := schema.IndexProperties{
			IsUnique:      idx.UniqueKey(),
			IsUserDefined: !idx.SystemDefined(),
			Comment:       string(idx.Comment()),
		}

		tags := make([]uint64, idx.IndexColumnsLength())
		for j := range tags {
			pos := idx.IndexColumns(j)
			s.Columns(col, int(pos))
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

func keylessSerialSchema(s *serial.TableSchema) bool {
	n := s.ColumnsLength()
	if n < 2 {
		return false
	}
	// keyless id is the 2nd to last column
	// in the columns vector (by convention)
	// and the only field in key tuples of
	// the clustered index.
	id := serial.Column{}
	s.Columns(&id, n-2)
	ok := id.Generated() && id.Hidden() &&
		string(id.Name()) == keylessIdCol
	if !ok {
		return false
	}

	// keyless cardinality is the last column
	// in the columns vector (by convention)
	// and the first field in value tuples of
	// the clustered index.
	card := serial.Column{}
	s.Columns(&card, n-1)
	return card.Generated() && card.Hidden() &&
		string(card.Name()) == keylessCardCol
}

func sqlTypeString(t typeinfo.TypeInfo) string {
	typ := t.ToSqlType()
	if st, ok := typ.(sql.SpatialColumnType); ok {
		// for spatial types, we must append the SRID
		if srid, ok := st.GetSpatialTypeSRID(); ok {
			return fmt.Sprintf("%s SRID %d", typ.String(), srid)
		}
	}
	return typ.String()
}

func typeinfoFromSqlType(ctx context.Context, s string) (typeinfo.TypeInfo, error) {
	t, err := parse.ParseColumnTypeString(sql.NewContext(ctx), s)
	if err != nil {
		return nil, err
	}
	return typeinfo.FromSqlType(t)
}

func encodingFromTypeinfo(t typeinfo.TypeInfo) serial.Encoding {
	return schema.EncodingFromSqlType(t.ToSqlType().Type())
}

func constraintsFromSerialColumn(col *serial.Column) (cc []schema.ColConstraint) {
	if !col.Nullable() || col.PrimaryKey() {
		cc = append(cc, schema.NotNullConstraint{})
	}
	return
}

func assertTrue(b bool) {
	if !b {
		panic("assertion failed")
	}
}
