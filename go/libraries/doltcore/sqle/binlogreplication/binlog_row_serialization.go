// Copyright 2024 Dolthub, Inc.
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

package binlogreplication

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/expranalysis"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// binlogRowSerializer resolves virtual expressions once for all row images in a table delta.
type binlogRowSerializer struct {
	fromSch, toSch     schema.Schema
	virtualExpressions []sql.Expression
	columns            []schema.Column
	fromOrdinals       []int
	conversionTypes    []sql.Type
}

// newBinlogRowSerializer prepares source-to-target mappings and reuses the target's analyzed expressions.
func newBinlogRowSerializer(fromSch, toSch schema.Schema, resolved sql.Schema) *binlogRowSerializer {
	s := &binlogRowSerializer{fromSch: fromSch, toSch: toSch}
	if resolved == nil {
		return s
	}
	s.virtualExpressions = make([]sql.Expression, toSch.GetAllCols().Size())
	for i := range s.virtualExpressions {
		col := toSch.GetAllCols().GetByIndex(i)
		if col.Virtual && !col.SystemHidden {
			s.virtualExpressions[i] = resolved[i].Generated
		}
	}
	s.columns = toSch.GetAllCols().GetNonSystemHiddenCols()
	if schema.SchemasAreEqual(fromSch, toSch) {
		return s
	}
	s.fromOrdinals = make([]int, toSch.GetAllCols().Size())
	s.conversionTypes = make([]sql.Type, len(s.fromOrdinals))
	for i := range s.fromOrdinals {
		col := toSch.GetAllCols().GetByIndex(i)
		fromCol, ok := fromSch.GetAllCols().GetByTag(col.Tag)
		if !ok {
			fromCol, ok = fromSch.GetAllCols().GetByName(col.Name)
		}
		s.fromOrdinals[i] = -1
		if ok && !col.Virtual {
			s.fromOrdinals[i] = fromSch.GetAllCols().TagToIdx[fromCol.Tag]
			targetType := col.TypeInfo.ToSqlType()
			if !fromCol.TypeInfo.ToSqlType().Equals(targetType) {
				s.conversionTypes[i] = targetType
			}
		}
	}
	return s
}

// serialize converts stored values to the target schema before evaluating and encoding its virtual columns.
func (s *binlogRowSerializer) serialize(ctx *sql.Context, key, value tree.Item, ns tree.NodeStore) ([]byte, mysql.Bitmap, error) {
	if s.columns == nil {
		return serializeRowToBinlogBytes(ctx, s.fromSch, s.toSch, key, value, ns)
	}
	row, err := index.BuildRow(ctx, val.Tuple(key), val.Tuple(value), s.fromSch, ns)
	if err != nil {
		return nil, mysql.Bitmap{}, err
	}
	if s.fromOrdinals != nil {
		targetRow := make(sql.Row, len(s.fromOrdinals))
		for i, ordinal := range s.fromOrdinals {
			if ordinal < 0 || row[ordinal] == nil {
				continue
			}
			targetRow[i] = row[ordinal]
			if typ := s.conversionTypes[i]; typ != nil {
				var inRange sql.ConvertInRange
				if roundingType, ok := typ.(sql.RoundingNumberType); ok {
					targetRow[i], inRange, err = roundingType.ConvertRound(ctx, targetRow[i])
				} else {
					targetRow[i], inRange, err = typ.Convert(ctx, targetRow[i])
				}
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				if inRange != sql.InRange {
					return nil, mysql.Bitmap{}, sql.ErrValueOutOfRange.New(row[ordinal], typ)
				}
			}
		}
		row = targetRow
	}
	if err = sql.EvalProjections(ctx, s.virtualExpressions, row); err != nil {
		return nil, mysql.Bitmap{}, err
	}

	nulls := mysql.NewServerBitmap(len(s.columns))
	var data []byte
	for i, col := range s.columns {
		ordinal := s.toSch.GetAllCols().TagToIdx[col.Tag]
		if row[ordinal] == nil {
			nulls.Set(i, true)
			continue
		}
		typ := col.TypeInfo.ToSqlType()
		serializer, ok := typeSerializersMap[typ.Type()]
		if !ok {
			return nil, nulls, fmt.Errorf("unsupported type: %v (%d)", typ.String(), typ.Type())
		}
		encoded, err := serializer.serialize(ctx, typ, row[ordinal], ns)
		if err != nil {
			return nil, nulls, err
		}
		data = append(data, encoded...)
	}
	return data, nulls, nil
}

// rowSerializationIter iterates over the columns in a schema and abstracts access to the key and value tuples storing
// the data for a row, so that callers can ask for the next column information and get the right descriptor, tuple,
// and tuple index to use to load that column's data.
type rowSerializationIter struct {
	fromSch schema.Schema // The schema representing the start of the diff being serialized
	toSch   schema.Schema // The schema representing the end of the diff row being serialized

	keyDesc   *val.TupleDesc // The descriptor for the key tuple (from fromSch)
	valueDesc *val.TupleDesc // The descriptor for the value tuple (from fromSch)
	key       val.Tuple      // The key tuple for the row being serialized
	value     val.Tuple      // The value tuple for the row being serialized

	colIdx int // The stored (non-virtual) column index in toSch for the current column
}

// newRowSerializationIter creates a new rowSerializationIter using the |fromSch|, which describes the format of the
// |key| and |value| tuple data, and using the |toSch| which describes the target format for the rows being serialized
// by the serializeRowToBinlogBytes() function. Both schemas are required, because in the case of a schema change, the
// diff data will use the "from" schema for the start of the diff, and the "to" schema for the end of the diff.
func newRowSerializationIter(ctx *sql.Context, fromSch, toSch schema.Schema, key, value tree.Item, ns tree.NodeStore) *rowSerializationIter {
	return &rowSerializationIter{
		fromSch:   fromSch,
		toSch:     toSch,
		key:       val.Tuple(key),
		keyDesc:   fromSch.GetKeyDescriptor(ns),
		value:     val.Tuple(value),
		valueDesc: fromSch.GetValueDescriptor(ns),
		colIdx:    0,
	}
}

// hasNext returns true if this iterator has more columns to provide and the |nextColumn| method can be called.
func (rsi *rowSerializationIter) hasNext() bool {
	return rsi.colIdx < rsi.toSch.GetAllCols().StoredSize()
}

// nextColumn provides the data needed to process the next column in a row, including the column from the "from" schema,
// and the column from the "to" schema, the tuple holding the data, the tuple descriptor, and the position index into
// that tuple where the column is stored. Note that the first column returned (the column from the "from" schema), may
// be nil when a column has been added to the schema as part of this diff (i.e. the new column exists in the "to" schema
// but not in the "from" schema). Callers should always call hasNext() before calling nextColumn() to ensure that it is
// safe to call.
func (rsi *rowSerializationIter) nextColumn() (*schema.Column, *schema.Column, *val.TupleDesc, val.Tuple, int) {
	// Ultimately, we are serializing to the "to" schema so that we can send a binlog encoded row to a replica, so
	// we iterate over the "to" schema's non-virtual columns to assemble the serialized row in the format the
	// replica is expecting.
	toCol := rsi.toSch.GetAllCols().GetByStoredIndex(rsi.colIdx)
	rsi.colIdx++

	// Look up the matching, non-virtual column in the "from" schema.
	var pFromCol *schema.Column
	if fromCol, ok := rsi.fromSch.GetAllCols().GetByTag(toCol.Tag); ok && !fromCol.Virtual {
		pFromCol = &fromCol
	} else {
		// Try to look up by name in case we don't find by tag?
		// The tag can change in some cases, such as type changes... need to test this
		if fromCol, ok = rsi.fromSch.GetAllCols().GetByName(toCol.Name); ok && !fromCol.Virtual {
			pFromCol = &fromCol
		}
	}

	// For keyless schemas, the key is a single hash column representing the row's unique identity, so we
	// always use the value descriptor for all columns. Additionally, the first field in the value is a
	// count of how many times that row appears in the table, so we increment |idx| by one extra field to
	// skip over that row count field and get to the real data fields.
	if schema.IsKeyless(rsi.fromSch) {
		return pFromCol, &toCol, rsi.valueDesc, rsi.value, rsi.findFromTupleIndex(pFromCol) + 1
	}

	// Otherwise, for primary key tables, we need to check if the next column is stored in the key or value.
	if toCol.IsPartOfPK {
		return pFromCol, &toCol, rsi.keyDesc, rsi.key, rsi.findFromTupleIndex(pFromCol)
	} else {
		return pFromCol, &toCol, rsi.valueDesc, rsi.value, rsi.findFromTupleIndex(pFromCol)
	}
}

// findFromTupleIndex returns the index of |pFromCol| into the tuple it is stored in (i.e. either the key tuple or
// the value tuple). If |pFromCol| is nil, then -1 is returned. This uses the storage index (via StoredIndexByTag),
// rather than the column's position.
func (rsi *rowSerializationIter) findFromTupleIndex(pFromCol *schema.Column) int {
	if pFromCol == nil {
		return -1
	}

	var idx int
	var ok bool
	if pFromCol.IsPartOfPK {
		idx, ok = rsi.fromSch.GetPKCols().StoredIndexByTag(pFromCol.Tag)
	} else {
		idx, ok = rsi.fromSch.GetNonPKCols().StoredIndexByTag(pFromCol.Tag)
	}
	if !ok {
		return -1
	}
	return idx
}

// serializeRowToBinlogBytes serializes the row formed by |key| and |value| into a binlog encoded row. The |fromSch| is
// the schema at the start of the diff, and is used to extract the values from |key| and |value|. The |toSch| is the
// schema at the end of the diff and is used to serialize the data into the binlog encoding. In many cases, these two
// schemas may be the same, but when there is a schema change, they will be two different schemas and both are needed
// to correctly deserialize the data and then serialize it into the expected format.
// For data stored out of band (e.g. BLOB, TEXT, GEOMETRY, JSON), |ns| is used to load the
// out-of-band data. This function returns the binary representation of the row, as well as a bitmap that indicates
// which fields of the row are null (and therefore don't contribute any bytes to the returned binary data).
func serializeRowToBinlogBytes(ctx *sql.Context, fromSch, toSch schema.Schema, key, value tree.Item, ns tree.NodeStore) (data []byte, nullBitmap mysql.Bitmap, err error) {
	nullBitmap = mysql.NewServerBitmap(toSch.GetAllCols().StoredSize())

	iter := newRowSerializationIter(ctx, fromSch, toSch, key, value, ns)
	rowIdx := -1
	for iter.hasNext() {
		rowIdx++
		fromCol, toCol, fromDescriptor, tuple, tupleIdx := iter.nextColumn()

		// If there is no matching "from" column, this then column must have been added during a schema change,
		// so mark it as nil and continue to the next column.
		if fromCol == nil {
			nullBitmap.Set(rowIdx, true)
			continue
		}

		typ := fromCol.TypeInfo.ToSqlType()
		deserializer, ok := typeSerializersMap[typ.Type()]
		if !ok {
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}

		// Use the "from" column to deserialize the value from the old schema
		value, err := deserializer.deserialize(ctx, typ, fromDescriptor, tuple, tupleIdx, ns)
		if err != nil {
			return nil, nullBitmap, err
		}
		if value == nil {
			nullBitmap.Set(rowIdx, true)
			continue
		}

		// Use the "to" column to serialize the value to the new/current/target schema
		serializer, ok := typeSerializersMap[toCol.TypeInfo.ToSqlType().Type()]
		if !ok {
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}

		newData, err := serializer.serialize(ctx, toCol.TypeInfo.ToSqlType(), value, ns)
		if err != nil {
			return nil, mysql.Bitmap{}, err
		}
		data = append(data, newData...)
	}

	return data, nullBitmap, nil
}

// resolveBinlogSchema resolves generated expressions once per table delta; stored-only tables skip analysis.
func resolveBinlogSchema(ctx *sql.Context, tableName string, sch schema.Schema) (sql.Schema, error) {
	if !schema.IsVirtual(sch) {
		return nil, nil
	}
	cols := sch.GetAllCols()
	for i := 0; i < cols.Size(); i++ {
		col := cols.GetByIndex(i)
		if col.Virtual && !col.SystemHidden {
			return expranalysis.ResolveSchema(ctx, tableName, sch)
		}
	}
	return nil, nil
}
