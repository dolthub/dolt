// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"context"
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// KVToSqlRowConverter takes noms types.Value key value pairs and converts them directly to a sql.Row directly.  It
// can be configured to only process a portion of the columns and map columens to desired output columns.
type KVToSqlRowConverter struct {
	tagToSqlColIdx map[uint64]int
	cols           []schema.Column
	// rowSize is the number of columns in the output row.  This may be bigger than the number of columns being converted,
	// but not less.  When rowSize is bigger than the number of columns being processed that means that some of the columns
	// in the output row will be filled with nils
	rowSize int
}

// NewKVToSqlRowConverterForCols returns a KVToSqlConverter instance based on the list of rows passed in
func NewKVToSqlRowConverterForCols(cols []schema.Column) *KVToSqlRowConverter {
	tagToSqlColIdx := make(map[uint64]int)
	for i, col := range cols {
		tagToSqlColIdx[col.Tag] = i
	}

	return &KVToSqlRowConverter{
		tagToSqlColIdx: tagToSqlColIdx,
		cols:           cols,
		rowSize:        len(cols),
	}
}

// ConvertKVToSqlRow returns a sql.Row generated from the key and value provided.
func (conv *KVToSqlRowConverter) ConvertKVToSqlRow(k, v types.Value) (sql.Row, error) {
	keyTup, ok := k.(types.Tuple)

	if !ok {
		return nil, errors.New("invalid key is not a tuple")
	}

	var valTup types.Tuple
	if !types.IsNull(v) {
		valTup, ok = v.(types.Tuple)

		if !ok {
			return nil, errors.New("invalid value is not a tuple")
		}
	}

	cols := make([]interface{}, conv.rowSize)
	filled, err := conv.processTuple(cols, 0, keyTup)
	if err != nil {
		return nil, err
	}

	if !valTup.Empty() {
		filled, err = conv.processTuple(cols, filled, valTup)
		if err != nil {
			return nil, err
		}
	}

	return cols, err
}

func (conv *KVToSqlRowConverter) processTuple(cols []interface{}, filled int, tup types.Tuple) (int, error) {
	tupItr, err := tup.Iterator()

	if err != nil {
		return 0, err
	}

	for filled < len(conv.tagToSqlColIdx) {
		_, tag, err := tupItr.Next()

		if err != nil {
			return 0, err
		}

		if tag == nil {
			break
		}

		if sqlColIdx, ok := conv.tagToSqlColIdx[uint64(tag.(types.Uint))]; !ok {
			err = tupItr.Skip()

			if err != nil {
				return 0, err
			}
		} else {
			_, val, err := tupItr.Next()

			if err != nil {
				return 0, err
			}

			cols[sqlColIdx], err = conv.cols[sqlColIdx].TypeInfo.ConvertNomsValueToValue(val)

			if err != nil {
				return 0, err
			}

			filled++
		}
	}

	return filled, nil
}

// KVGetFunc defines a function that returns a Key Value pair
type KVGetFunc func(ctx context.Context) (types.Value, types.Value, error)

// DoltMapIter uses a types.MapIterator to iterate over a types.Map and returns sql.Row instances that it reads and
// converts
type DoltMapIter struct {
	kvGet KVGetFunc
	conv  *KVToSqlRowConverter
}

// NewDoltMapIterFromNomsMapItr returns an iterator which returns sql.Row instances read from a types.Map.  The cols
// passed in are used to limit the values that are processed
func NewDoltMapIterFromNomsMapItr(mapItr types.MapIterator, cols []schema.Column) *DoltMapIter {
	getFunc := func(ctx context.Context) (types.Value, types.Value, error) {
		k, v, err := mapItr.Next(ctx)

		if err != nil {
			return nil, nil, err
		} else if k == nil {
			return nil, nil, io.EOF
		}

		return k, v, nil
	}

	return NewDoltMapIter(getFunc, cols)
}

// NewDoltMapIter returns a new DoltMapIter
func NewDoltMapIter(keyValGet KVGetFunc, cols []schema.Column) *DoltMapIter {
	return &DoltMapIter{
		kvGet: keyValGet,
		conv:  NewKVToSqlRowConverterForCols(cols),
	}
}

// Next returns the next sql.Row until all rows are returned at which point (nil, io.EOF) is returned.
func (dmi *DoltMapIter) Next(ctx context.Context) (sql.Row, error) {
	k, v, err := dmi.kvGet(ctx)

	if err != nil {
		return nil, err
	}

	return dmi.conv.ConvertKVToSqlRow(k, v)
}
