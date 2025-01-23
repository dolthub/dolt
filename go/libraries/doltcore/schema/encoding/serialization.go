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

	fb "github.com/dolthub/flatbuffers/v23/go"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/vector"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	sqltypes "github.com/dolthub/go-mysql-server/sql/types"

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
	comment := b.CreateString(sch.GetComment())

	var hasFeaturesAfterTryAccessors bool
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.OnUpdate != "" {
			hasFeaturesAfterTryAccessors = true
			break
		}
	}

	serial.TableSchemaStart(b)
	serial.TableSchemaAddClusteredIndex(b, rows)
	serial.TableSchemaAddColumns(b, columns)
	serial.TableSchemaAddSecondaryIndexes(b, indexes)
	serial.TableSchemaAddChecks(b, checks)
	serial.TableSchemaAddCollation(b, serial.Collation(sch.GetCollation()))
	if sch.GetComment() != "" {
		serial.TableSchemaAddComment(b, comment)
		hasFeaturesAfterTryAccessors = true
	}
	if hasFeaturesAfterTryAccessors {
		serial.TableSchemaAddHasFeaturesAfterTryAccessors(b, hasFeaturesAfterTryAccessors)
	}
	root := serial.TableSchemaEnd(b)
	bs := serial.FinishMessage(b, root, []byte(serial.TableSchemaFileID))
	return bs, nil
}

// DeserializeSchema deserializes a schema.Schema from a serial.Message.
func DeserializeSchema(ctx context.Context, nbf *types.NomsBinFormat, v types.Value) (schema.Schema, error) {
	assertTrue(nbf.UsesFlatbuffers(), "cannot call DeserializeSchema with non-Flatbuffers NomsBinFormat")
	sm, ok := v.(types.SerialMessage)
	assertTrue(ok, "must pass types.SerialMessage value to DeserializeSchema")
	return deserializeSchemaFromFlatbuffer(ctx, sm)
}

func deserializeSchemaFromFlatbuffer(ctx context.Context, buf []byte) (schema.Schema, error) {
	assertTrue(serial.GetFileID(buf) == serial.TableSchemaFileID, "serialized schema must have FileID == TableSchemaFileID")
	s, err := serial.TryGetRootAsTableSchema(buf, serial.MessagePrefixSz)
	if err != nil {
		return nil, err
	}

	cols, err := deserializeColumns(ctx, s)
	if err != nil {
		return nil, err
	}
	sch, err := schema.SchemaFromCols(schema.NewColCollection(cols...))
	if err != nil {
		return nil, err
	}

	dci, err := deserializeClusteredIndex(s)
	if err != nil {
		return nil, err
	}
	err = sch.SetPkOrdinals(dci)
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

	sch.SetCollation(schema.Collation(s.Collation()))
	sch.SetComment(string(s.Comment()))

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
	serial.IndexAddSpatialKey(b, false)
	serial.IndexAddSystemDefined(b, false)
	return serial.IndexEnd(b)
}

func deserializeClusteredIndex(s *serial.TableSchema) ([]int, error) {
	// check for keyless schema
	kss, err := keylessSerialSchema(s)
	if err != nil {
		return nil, err
	}
	if kss {
		return nil, nil
	}

	ci, err := s.TryClusteredIndex(nil)
	if err != nil {
		return nil, err
	}
	pkOrdinals := make([]int, ci.KeyColumnsLength())
	for i := range pkOrdinals {
		pkOrdinals[i] = int(ci.KeyColumns(i))
	}
	return pkOrdinals, nil
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
		var defVal, onUpdateVal string
		if col.Default != "" {
			defVal = col.Default
		} else {
			defVal = col.Generated
		}

		if col.OnUpdate != "" {
			onUpdateVal = col.OnUpdate
		}

		co := b.CreateString(col.Comment)
		do := b.CreateString(defVal)
		ou := b.CreateString(onUpdateVal)

		typeString := sqlTypeString(col.TypeInfo)
		to := b.CreateString(typeString)
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
		serial.ColumnAddGenerated(b, col.Generated != "")
		serial.ColumnAddVirtual(b, col.Virtual)
		if onUpdateVal != "" {
			serial.ColumnAddOnUpdateValue(b, ou)
		}
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
	isKeyless, err := keylessSerialSchema(s)
	if err != nil {
		return nil, err
	}
	if isKeyless {
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
	c := serial.Column{}
	for i := range cols {
		_, err := s.TryColumns(&c, i)
		if err != nil {
			return nil, err
		}
		sqlType, err := typeinfoFromSqlType(string(c.SqlType()))
		if err != nil {
			return nil, err
		}

		var defVal, generatedVal, onUpdateVal string
		if c.DefaultValue() != nil {
			if c.Generated() {
				generatedVal = string(c.DefaultValue())
			} else {
				defVal = string(c.DefaultValue())
			}
		}

		if c.OnUpdateValue() != nil {
			onUpdateVal = string(c.OnUpdateValue())
		}

		cols[i] = schema.Column{
			Name:          string(c.Name()),
			Tag:           c.Tag(),
			Kind:          sqlType.NomsKind(),
			IsPartOfPK:    c.PrimaryKey(),
			TypeInfo:      sqlType,
			Default:       defVal,
			Generated:     generatedVal,
			OnUpdate:      onUpdateVal,
			Virtual:       c.Virtual(),
			AutoIncrement: c.AutoIncrement(),
			Comment:       string(c.Comment()),
			Constraints:   constraintsFromSerialColumn(&c),
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

		// serialize prefix lengths
		prefixLengths := idx.PrefixLengths()
		serial.IndexStartPrefixLengthsVector(b, len(prefixLengths))
		for j := len(prefixLengths) - 1; j >= 0; j-- {
			b.PrependUint16(prefixLengths[j])
		}
		po := b.EndVector(len(prefixLengths))

		var ftInfo fb.UOffsetT
		if idx.IsFullText() {
			ftInfo = serializeFullTextInfo(b, idx)
		}

		var vectorInfo fb.UOffsetT
		if idx.IsVector() {
			vectorInfo = serializeVectorInfo(b, idx)
		}

		serial.IndexStart(b)
		serial.IndexAddName(b, no)
		serial.IndexAddComment(b, co)
		serial.IndexAddIndexColumns(b, ico)
		serial.IndexAddKeyColumns(b, ko)
		serial.IndexAddPrimaryKey(b, false)
		serial.IndexAddUniqueKey(b, idx.IsUnique())
		serial.IndexAddSystemDefined(b, !idx.IsUserDefined())
		serial.IndexAddPrefixLengths(b, po)
		serial.IndexAddSpatialKey(b, idx.IsSpatial())
		serial.IndexAddFulltextKey(b, idx.IsFullText())
		if idx.IsFullText() {
			serial.IndexAddFulltextInfo(b, ftInfo)
		}
		if idx.IsVector() {
			serial.IndexAddVectorKey(b, true)
			serial.IndexAddVectorInfo(b, vectorInfo)
		}
		offs[i] = serial.IndexEnd(b)
	}

	serial.TableSchemaStartSecondaryIndexesVector(b, len(indexes))
	for i := len(offs) - 1; i >= 0; i-- {
		b.PrependUOffsetT(offs[i])
	}
	return b.EndVector(len(indexes))
}

func deserializeSecondaryIndexes(sch schema.Schema, s *serial.TableSchema) error {
	idx := serial.Index{}
	col := serial.Column{}
	for i := 0; i < s.SecondaryIndexesLength(); i++ {
		_, err := s.TrySecondaryIndexes(&idx, i)
		if err != nil {
			return err
		}
		assertTrue(!idx.PrimaryKey(), "cannot deserialize secondary index with PrimaryKey() == true")

		fti, err := deserializeFullTextInfo(&idx)
		if err != nil {
			return err
		}

		vi, err := deserializeVectorInfo(&idx)
		if err != nil {
			return err
		}

		name := string(idx.Name())
		props := schema.IndexProperties{
			IsUnique:           idx.UniqueKey(),
			IsSpatial:          idx.SpatialKey(),
			IsFullText:         idx.FulltextKey(),
			IsVector:           idx.VectorKey(),
			IsUserDefined:      !idx.SystemDefined(),
			Comment:            string(idx.Comment()),
			FullTextProperties: fti,
			VectorProperties:   vi,
		}

		tags := make([]uint64, idx.IndexColumnsLength())
		for j := range tags {
			pos := idx.IndexColumns(j)
			_, err := s.TryColumns(&col, int(pos))
			if err != nil {
				return err
			}
			tags[j] = col.Tag()
		}

		var prefixLengths []uint16
		prefixLengthsLength := idx.PrefixLengthsLength()
		if prefixLengthsLength > 0 {
			prefixLengths = make([]uint16, prefixLengthsLength)
			for j := range prefixLengths {
				prefixLengths[j] = idx.PrefixLengths(j)
			}
		}

		_, err = sch.Indexes().AddIndexByColTags(name, tags, prefixLengths, props)
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
	c := serial.CheckConstraint{}
	for i := 0; i < s.ChecksLength(); i++ {
		_, err := s.TryChecks(&c, i)
		if err != nil {
			return err
		}
		n, e := string(c.Name()), string(c.Expression())
		if _, err := coll.AddCheck(n, e, c.Enforced()); err != nil {
			return err
		}
	}
	return nil
}

func serializeFullTextInfo(b *fb.Builder, idx schema.Index) fb.UOffsetT {
	props := idx.FullTextProperties()

	configTable := b.CreateString(props.ConfigTable)
	posTable := b.CreateString(props.PositionTable)
	docCountTable := b.CreateString(props.DocCountTable)
	globalCountTable := b.CreateString(props.GlobalCountTable)
	rowCountTable := b.CreateString(props.RowCountTable)
	keyName := b.CreateString(props.KeyName)

	keyPositions := idx.FullTextProperties().KeyPositions
	serial.FulltextInfoStartKeyPositionsVector(b, len(keyPositions))
	for j := len(keyPositions) - 1; j >= 0; j-- {
		b.PrependUint16(keyPositions[j])
	}
	keyPos := b.EndVector(len(keyPositions))

	serial.FulltextInfoStart(b)
	serial.FulltextInfoAddConfigTable(b, configTable)
	serial.FulltextInfoAddPositionTable(b, posTable)
	serial.FulltextInfoAddDocCountTable(b, docCountTable)
	serial.FulltextInfoAddGlobalCountTable(b, globalCountTable)
	serial.FulltextInfoAddRowCountTable(b, rowCountTable)
	serial.FulltextInfoAddKeyType(b, props.KeyType)
	serial.FulltextInfoAddKeyName(b, keyName)
	serial.FulltextInfoAddKeyPositions(b, keyPos)
	return serial.FulltextInfoEnd(b)
}

func serializeVectorInfo(b *fb.Builder, idx schema.Index) fb.UOffsetT {
	props := idx.VectorProperties()

	serial.VectorInfoStart(b)

	switch props.DistanceType {
	case vector.DistanceL2Squared{}:
		serial.VectorInfoAddDistanceType(b, serial.DistanceTypeL2_Squared)
	}

	return serial.VectorInfoEnd(b)
}

func deserializeFullTextInfo(idx *serial.Index) (schema.FullTextProperties, error) {
	fulltext := serial.FulltextInfo{}
	has, err := idx.TryFulltextInfo(&fulltext)
	if err != nil {
		return schema.FullTextProperties{}, err
	}
	if has == nil {
		return schema.FullTextProperties{}, nil
	}

	var keyPositions []uint16
	keyPositionsLength := fulltext.KeyPositionsLength()
	if keyPositionsLength > 0 {
		keyPositions = make([]uint16, keyPositionsLength)
		for j := range keyPositions {
			keyPositions[j] = fulltext.KeyPositions(j)
		}
	}

	return schema.FullTextProperties{
		ConfigTable:      string(fulltext.ConfigTable()),
		PositionTable:    string(fulltext.PositionTable()),
		DocCountTable:    string(fulltext.DocCountTable()),
		GlobalCountTable: string(fulltext.GlobalCountTable()),
		RowCountTable:    string(fulltext.RowCountTable()),
		KeyType:          fulltext.KeyType(),
		KeyName:          string(fulltext.KeyName()),
		KeyPositions:     keyPositions,
	}, nil
}

func deserializeVectorInfo(idx *serial.Index) (schema.VectorProperties, error) {
	vectorInfo := serial.VectorInfo{}
	has, err := idx.TryVectorInfo(&vectorInfo)
	if err != nil {
		return schema.VectorProperties{}, err
	}
	if has == nil {
		return schema.VectorProperties{}, nil
	}

	switch vectorInfo.DistanceType() {
	case serial.DistanceTypeL2_Squared:
		return schema.VectorProperties{
			DistanceType: vector.DistanceL2Squared{},
		}, nil
	}
	return schema.VectorProperties{}, fmt.Errorf("unknown distance type in vector index info: %s", vectorInfo.DistanceType())
}

func keylessSerialSchema(s *serial.TableSchema) (bool, error) {
	n := s.ColumnsLength()
	if n < 2 {
		return false, nil
	}
	// keyless id is the 2nd to last column
	// in the columns vector (by convention)
	// and the only field in key tuples of
	// the clustered index.
	id := serial.Column{}
	_, err := s.TryColumns(&id, n-2)
	if err != nil {
		return false, err
	}
	ok := id.Generated() && id.Hidden() &&
		string(id.Name()) == keylessIdCol
	if !ok {
		return false, nil
	}

	// keyless cardinality is the last column
	// in the columns vector (by convention)
	// and the first field in value tuples of
	// the clustered index.
	card := serial.Column{}
	_, err = s.TryColumns(&card, n-1)
	if err != nil {
		return false, err
	}
	return card.Generated() && card.Hidden() &&
		string(card.Name()) == keylessCardCol, nil
}

func sqlTypeString(t typeinfo.TypeInfo) string {
	typ := t.ToSqlType()
	if st, ok := typ.(sql.SpatialColumnType); ok {
		// for spatial types, we must append the SRID
		if srid, ok := st.GetSpatialTypeSRID(); ok {
			return fmt.Sprintf("%s SRID %d", typ.String(), srid)
		}
	}

	// For datetime types, always store the precision explicitly so that it can be read back precisely, although MySQL
	// omits the precision when it's 0 (the default).
	if sqltypes.IsDatetimeType(typ) || sqltypes.IsTimestampType(typ) {
		dt := typ.(sql.DatetimeType)
		if dt.Precision() == 0 {
			return fmt.Sprintf("%s(0)", typ.String())
		}
		return typ.String()
	}

	// Extended types are string serializable, so we'll just prepend a tag
	if extendedType, ok := typ.(sqltypes.ExtendedType); ok {
		serializedType, err := sqltypes.SerializeTypeToString(extendedType)
		if err != nil {
			panic(err)
		}
		return planbuilder.ExtendedTypeTag + serializedType
	}

	return typ.String()
}

func typeinfoFromSqlType(s string) (typeinfo.TypeInfo, error) {
	sqlType, err := planbuilder.ParseColumnTypeString(s)
	if err != nil {
		return nil, err
	}
	return typeinfo.FromSqlType(sqlType)
}

func encodingFromTypeinfo(t typeinfo.TypeInfo) serial.Encoding {
	return schema.EncodingFromSqlType(t.ToSqlType())
}

func constraintsFromSerialColumn(col *serial.Column) (cc []schema.ColConstraint) {
	if !col.Nullable() || col.PrimaryKey() {
		cc = append(cc, schema.NotNullConstraint{})
	}
	return
}

func assertTrue(b bool, msg string) {
	if !b {
		panic("assertion failed: " + msg)
	}
}
