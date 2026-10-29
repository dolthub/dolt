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
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

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

// resolveBinlogSchema resolves generated expressions once per table delta, rather than
// once per row. Tables without virtual columns do not need expression analysis.
func resolveBinlogSchema(ctx *sql.Context, tableName string, sch schema.Schema) (sql.Schema, error) {
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Virtual && !col.SystemHidden {
			return expranalysis.ResolveSchema(ctx, tableName, sch)
		}
	}
	return nil, nil
}

// serializeRowToBinlogBytes reads stored values using fromSch and emits a full row
// image in toSch order. resolved contains toSch's analyzed generated expressions.
// Virtual values must be evaluated separately for the before and after images;
// they are not present in either storage tuple. System-hidden columns used by
// functional indexes are excluded, just as they are from the table map.
func serializeRowToBinlogBytes(ctx *sql.Context, fromSch, toSch schema.Schema, resolved sql.Schema, key, value tree.Item, ns tree.NodeStore) (data []byte, nullBitmap mysql.Bitmap, err error) {
	cols := toSch.GetAllCols()
	row := make(sql.Row, cols.Size())
	iter := newRowSerializationIter(ctx, fromSch, toSch, key, value, ns)
	for iter.hasNext() {
		fromCol, toCol, descriptor, tuple, tupleIdx := iter.nextColumn()
		// A column added during a schema change has no value in the old image.
		if fromCol == nil {
			continue
		}
		typ := fromCol.TypeInfo.ToSqlType()
		deserializer, ok := typeSerializersMap[typ.Type()]
		if !ok {
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)", typ.String(), typ.Type())
		}
		v, err := deserializer.deserialize(ctx, typ, descriptor, tuple, tupleIdx, ns)
		if err != nil {
			return nil, nullBitmap, err
		}
		row[cols.TagToIdx[toCol.Tag]] = v
	}

	// All stored fields are loaded first: a generated column can refer to a
	// later stored column. Evaluate virtual columns in schema order so that a
	// generated expression can refer to an earlier generated column as well.
	for i, col := range cols.GetColumns() {
		if !col.Virtual || col.SystemHidden {
			continue
		}
		v, err := resolved[i].Generated.Eval(ctx, row)
		if err != nil {
			return nil, nullBitmap, err
		}
		// Generated.Eval also converts to the declared column type, so dependent
		// expressions see the same value that will be sent to the consumer.
		row[i] = v
	}

	nullBitmap = mysql.NewServerBitmap(len(cols.GetNonSystemHiddenCols()))
	rowIdx := 0
	for i, col := range cols.GetColumns() {
		if col.SystemHidden {
			continue
		}
		v := row[i]
		if v == nil {
			nullBitmap.Set(rowIdx, true)
		} else {
			typ := col.TypeInfo.ToSqlType()
			serializer, ok := typeSerializersMap[typ.Type()]
			if !ok {
				return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)", typ.String(), typ.Type())
			}
			newData, err := serializer.serialize(ctx, typ, v, ns)
			if err != nil {
				return nil, nullBitmap, err
			}
			data = append(data, newData...)
		}
		rowIdx++
	}
	return data, nullBitmap, nil
}
