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

func maxU64(x, y uint64) uint64 {
	if x > y {
		return x
	}

	return y
}

// KVToSqlRowConverter takes noms types.Value key value pairs and converts them directly to a sql.Row.  It
// can be configured to only process a portion of the columns and map columns to desired output columns.
type KVToSqlRowConverter struct {
	nbf            *types.NomsBinFormat
	cols           []schema.Column
	tagToSqlColIdx map[uint64]int
	// rowSize is the number of columns in the output row.  This may be bigger than the number of columns being converted,
	// but not less.  When rowSize is bigger than the number of columns being processed that means that some of the columns
	// in the output row will be filled with nils
	rowSize     int
	valsFromKey int
	valsFromVal int
	maxValTag   uint64
}

func NewKVToSqlRowConverter(nbf *types.NomsBinFormat, tagToSqlColIdx map[uint64]int, cols []schema.Column, rowSize int) *KVToSqlRowConverter {
	valsFromKey, valsFromVal, maxValTag := getValLocations(tagToSqlColIdx, cols)

	return &KVToSqlRowConverter{
		nbf:            nbf,
		cols:           cols,
		tagToSqlColIdx: tagToSqlColIdx,
		rowSize:        rowSize,
		valsFromKey:    valsFromKey,
		valsFromVal:    valsFromVal,
		maxValTag:      maxValTag,
	}
}

// get counts of where the values we want converted come from so we can skip entire tuples at times.
func getValLocations(tagToSqlColIdx map[uint64]int, cols []schema.Column) (int, int, uint64) {
	var fromKey int
	var fromVal int
	var maxValTag uint64
	for _, col := range cols {
		if _, ok := tagToSqlColIdx[col.Tag]; ok {
			if col.IsPartOfPK {
				fromKey++
			} else {
				fromVal++
				maxValTag = maxU64(maxValTag, col.Tag)
			}
		}
	}

	return fromKey, fromVal, maxValTag
}

// NewKVToSqlRowConverterForCols returns a KVToSqlConverter instance based on the list of columns passed in
func NewKVToSqlRowConverterForCols(nbf *types.NomsBinFormat, cols []schema.Column) *KVToSqlRowConverter {
	tagToSqlColIdx := make(map[uint64]int)
	for i, col := range cols {
		tagToSqlColIdx[col.Tag] = i
	}

	return NewKVToSqlRowConverter(nbf, tagToSqlColIdx, cols, len(cols))
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
	} else {
		valTup = types.EmptyTuple(conv.nbf)
	}

	return conv.ConvertKVTuplesToSqlRow(keyTup, valTup)
}

// ConvertKVToSqlRow returns a sql.Row generated from the key and value provided.
func (conv *KVToSqlRowConverter) ConvertKVTuplesToSqlRow(k, v types.Tuple) (sql.Row, error) {
	tupItr := types.TupleItrPool.Get().(*types.TupleIterator)
	defer types.TupleItrPool.Put(tupItr)

	cols := make([]interface{}, conv.rowSize)
	if conv.valsFromKey > 0 {
		// keys are not in sorted order so cannot use max tag to early exit
		err := conv.processTuple(cols, conv.valsFromKey, 0xFFFFFFFFFFFFFFFF, k, tupItr)

		if err != nil {
			return nil, err
		}
	}

	if conv.valsFromVal > 0 {
		err := conv.processTuple(cols, conv.valsFromVal, conv.maxValTag, v, tupItr)

		if err != nil {
			return nil, err
		}
	}

	return cols, nil
}

func (conv *KVToSqlRowConverter) processTuple(cols []interface{}, valsToFill int, maxTag uint64, tup types.Tuple, tupItr *types.TupleIterator) error {
	err := tupItr.InitForTuple(tup)

	if err != nil {
		return err
	}

	nbf := tup.Format()
	primReader, numPrimitives := tupItr.CodecReader()

	filled := 0
	for pos := uint64(0); pos+1 < numPrimitives; pos += 2 {
		if filled >= valsToFill {
			break
		}

		tagKind := primReader.ReadKind()

		if tagKind != types.UintKind {
			return errors.New("Encountered unexpected kind while attempting to read tag")
		}

		tag64 := primReader.ReadUint()
		if tag64 > maxTag {
			break
		}

		if sqlColIdx, ok := conv.tagToSqlColIdx[tag64]; !ok {
			err = primReader.SkipValue(nbf)

			if err != nil {
				return err
			}
		} else {
			cols[sqlColIdx], err = conv.cols[sqlColIdx].TypeInfo.ReadFrom(nbf, primReader)

			if err != nil {
				return err
			}

			filled++
		}
	}

	return nil
}

// KVGetFunc defines a function that returns a Key Value pair
type KVGetFunc func(ctx context.Context) (types.Tuple, types.Tuple, error)

func GetGetFuncForMapIter(nbf *types.NomsBinFormat, mapItr types.MapIterator) func(ctx context.Context) (types.Tuple, types.Tuple, error) {
	return func(ctx context.Context) (types.Tuple, types.Tuple, error) {
		k, v, err := mapItr.Next(ctx)

		if err != nil {
			return types.Tuple{}, types.Tuple{}, err
		} else if k == nil {
			return types.Tuple{}, types.Tuple{}, io.EOF
		}

		valTup, ok := v.(types.Tuple)
		if !ok {
			valTup = types.EmptyTuple(nbf)
		}

		return k.(types.Tuple), valTup, nil
	}
}

// DoltMapIter uses a types.MapIterator to iterate over a types.Map and returns sql.Row instances that it reads and
// converts
type DoltMapIter struct {
	ctx           context.Context
	kvGet         KVGetFunc
	closeKVGetter func() error
	conv          *KVToSqlRowConverter
}

// NewDoltMapIter returns a new DoltMapIter
func NewDoltMapIter(ctx context.Context, keyValGet KVGetFunc, closeKVGetter func() error, conv *KVToSqlRowConverter) *DoltMapIter {
	return &DoltMapIter{
		ctx:           ctx,
		kvGet:         keyValGet,
		closeKVGetter: closeKVGetter,
		conv:          conv,
	}
}

// Next returns the next sql.Row until all rows are returned at which point (nil, io.EOF) is returned.
func (dmi *DoltMapIter) Next() (sql.Row, error) {
	k, v, err := dmi.kvGet(dmi.ctx)

	if err != nil {
		return nil, err
	}

	return dmi.conv.ConvertKVTuplesToSqlRow(k, v)
}

func (dmi *DoltMapIter) Close() error {
	if dmi.closeKVGetter != nil {
		return dmi.closeKVGetter()
	}

	return nil
}
