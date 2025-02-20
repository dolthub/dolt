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
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// rowSerializationIter iterates over the columns in a schema and abstracts access to the key and value tuples storing
// the data for a row, so that callers can ask for the next column information and get the right descriptor, tuple,
// and tuple index to use to load that column's data.
type rowSerializationIter struct {
	sch    schema.Schema // The schema representing the row being serialized
	colIdx int           // The position in the schema for the current column

	key     val.Tuple     // The key tuple for the row being serialized
	keyDesc val.TupleDesc // The descriptor for the key tuple
	keyIdx  int           // The last index in the key tuple used for a column

	value     val.Tuple     // The value tuple for the row being serialized
	valueDesc val.TupleDesc // The descriptor for the value tuple
	valueIdx  int           // The last index in the value tuple used for a column
}

// newRowSerializationIter creates a new rowSerializationIter for the specified |schema| and row data from the
// |key| and |value| tuples.
func newRowSerializationIter(sch schema.Schema, key, value tree.Item, ns tree.NodeStore) *rowSerializationIter {
	return &rowSerializationIter{
		sch:       sch,
		key:       val.Tuple(key),
		keyDesc:   sch.GetKeyDescriptor(ns),
		value:     val.Tuple(value),
		valueDesc: sch.GetValueDescriptor(ns),
		keyIdx:    -1,
		valueIdx:  -1,
		colIdx:    0,
	}
}

// hasNext returns true if this iterator has more columns to provide and the |nextColumn| method can be called.
func (rsi *rowSerializationIter) hasNext() bool {
	return rsi.colIdx < rsi.sch.GetAllCols().Size()
}

// nextColumn provides the data needed to process the next column in a row, including the column itself, the tuple
// holding the data, the tuple descriptor for that tuple, and the position index into that tuple where the column
// is stored. Callers should always call hasNext() before calling nextColumn() to ensure that it is safe to call.
func (rsi *rowSerializationIter) nextColumn() (schema.Column, val.TupleDesc, val.Tuple, int) {
	col := rsi.sch.GetAllCols().GetColumns()[rsi.colIdx]
	rsi.colIdx++

	// For keyless schemas, the key is a single hash column representing the row's unique identity, so we
	// always use the value descriptor for all columns. Additionally, the first field in the value is a
	// count of how many times that row appears in the table, so we increment |idx| by one extra field to
	// skip over that row count field and get to the real data fields.
	if schema.IsKeyless(rsi.sch) {
		rsi.valueIdx++
		return col, rsi.valueDesc, rsi.value, rsi.valueIdx + 1
	}

	// Otherwise, for primary key tables, we need to check if the next column is stored in the key or value.
	if col.IsPartOfPK {
		rsi.keyIdx++
		return col, rsi.keyDesc, rsi.key, rsi.keyIdx
	} else {
		rsi.valueIdx++
		return col, rsi.valueDesc, rsi.value, rsi.valueIdx
	}
}

// serializeRowToBinlogBytes serializes the row formed by |key| and |value| and defined by the |schema| structure, into
// MySQL binlog binary format. For data stored out of band (e.g. BLOB, TEXT, GEOMETRY, JSON), |ns| is used to load the
// out-of-band data. This function returns the binary representation of the row, as well as a bitmap that indicates
// which fields of the row are null (and therefore don't contribute any bytes to the returned binary data).
func serializeRowToBinlogBytes(ctx *sql.Context, sch schema.Schema, key, value tree.Item, ns tree.NodeStore) (data []byte, nullBitmap mysql.Bitmap, err error) {
	columns := sch.GetAllCols().GetColumns()
	nullBitmap = mysql.NewServerBitmap(len(columns))

	iter := newRowSerializationIter(sch, key, value, ns)
	rowIdx := -1
	for iter.hasNext() {
		rowIdx++
		col, descriptor, tuple, tupleIdx := iter.nextColumn()

		typ := col.TypeInfo.ToSqlType()
		serializer, ok := typeSerializersMap[typ.Type()]
		if !ok {
			return nil, nullBitmap, fmt.Errorf(
				"unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}
		newData, err := serializer.serialize(ctx, typ, descriptor, tuple, tupleIdx, ns)
		if err != nil {
			return nil, mysql.Bitmap{}, err
		}
		if newData == nil {
			nullBitmap.Set(rowIdx, true)
		} else {
			data = append(data, newData...)
		}
	}

	return data, nullBitmap, nil
}
