package sqle

import (
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

type KVToSqlRowConverter struct {
	tagToSqlColIdx map[uint64]int
	cols           []schema.Column
	rowSize        int
}

func (conv *KVToSqlRowConverter) ConvertKVToSqlRow(k, v types.Value) (sql.Row, error) {
	keyTup := k.(types.Tuple)
	var valTup types.Tuple
	if !types.IsNull(v) {
		valTup = v.(types.Tuple)
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

type KVGetFunc func(ctx context.Context) (types.Value, types.Value, error)

type DoltMapIter struct {
	kvGet KVGetFunc
	conv  KVToSqlRowConverter
}

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

func NewDoltMapIter(keyValGet KVGetFunc, cols []schema.Column) *DoltMapIter {
	tagToSqlColIdx := make(map[uint64]int)
	for i, col := range cols {
		tagToSqlColIdx[col.Tag] = i
	}

	return &DoltMapIter{
		kvGet: keyValGet,
		conv: KVToSqlRowConverter{
			tagToSqlColIdx: tagToSqlColIdx,
			cols:           cols,
		},
	}
}

func (dmi *DoltMapIter) Next(ctx context.Context) (sql.Row, error) {
	k, v, err := dmi.kvGet(ctx)

	if err != nil {
		return nil, err
	}

	return dmi.conv.ConvertKVToSqlRow(k, v)
}
