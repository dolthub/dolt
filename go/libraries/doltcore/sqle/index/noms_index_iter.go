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

package index

import (
	"context"
	"github.com/dolthub/go-mysql-server/sql"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/types"
)

type indexLookupRowIterAdapter struct {
	idx       DoltIndex
	keyIter   nomsKeyIter
	tableRows types.Map

	lookupTags map[uint64]int
	conv       *KVToSqlRowConverter
	cancelF    func()

	read  uint64
	count uint64

	resultBuf *async.RingBuffer
}

// NewIndexLookupRowIterAdapter returns a new indexLookupRowIterAdapter.
func NewIndexLookupRowIterAdapter(ctx *sql.Context, idx DoltIndex, durableState *durableIndexState, keyIter nomsKeyIter, columns []uint64) (*indexLookupRowIterAdapter, error) {
	rows := durable.NomsMapFromIndex(durableState.Primary)
	return &indexLookupRowIterAdapter{
		idx:        idx,
		keyIter:    keyIter,
		tableRows:  rows,
		conv:       idx.sqlRowConverter(durableState, columns),
		lookupTags: idx.lookupTags(durableState),
	}, nil
}

// Next returns the next row from the iterator.
func (i *indexLookupRowIterAdapter) Next(ctx *sql.Context) (sql.Row, error) {
	for i.count == 0 || i.read < i.count {
		indexKey, err := i.keyIter.ReadKey(ctx)
		if err != nil {
			return nil, err
		}
		i.count++
		return i.processKey(ctx, indexKey)
	}

	return nil, io.EOF
}

func (i *indexLookupRowIterAdapter) Close(*sql.Context) error {
	return nil
}

func (i *indexLookupRowIterAdapter) indexKeyToTableKey(nbf *types.NomsBinFormat, indexKey types.Tuple) (types.Tuple, error) {
	tplItr, err := indexKey.Iterator()

	if err != nil {
		return types.Tuple{}, err
	}

	resVals := make([]types.Value, len(i.lookupTags)*2)
	for {
		_, tag, err := tplItr.NextUint64()

		if err != nil {
			if err == io.EOF {
				break
			}

			return types.Tuple{}, err
		}

		idx, inKey := i.lookupTags[tag]

		if inKey {
			_, valVal, err := tplItr.Next()

			if err != nil {
				return types.Tuple{}, err
			}

			resVals[idx*2] = types.Uint(tag)
			resVals[idx*2+1] = valVal
		} else {
			err := tplItr.Skip()

			if err != nil {
				return types.Tuple{}, err
			}
		}
	}

	return types.NewTuple(nbf, resVals...)
}

// processKey is called within queueRows and processes each key, sending the resulting row to the row channel.
func (i *indexLookupRowIterAdapter) processKey(ctx context.Context, indexKey types.Tuple) (sql.Row, error) {
	pkTupleVal, err := i.indexKeyToTableKey(i.idx.Format(), indexKey)
	if err != nil {
		return nil, err
	}

	fieldsVal, ok, err := i.tableRows.MaybeGetTuple(ctx, pkTupleVal)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	sqlRow, err := i.conv.ConvertKVTuplesToSqlRow(pkTupleVal, fieldsVal)
	if err != nil {
		return nil, err
	}

	return sqlRow, nil
}

type CoveringIndexRowIterAdapter struct {
	idx       DoltIndex
	rr        *noms.NomsRangeReader
	conv      *KVToSqlRowConverter
	ctx       *sql.Context
	pkCols    *schema.ColCollection
	nonPKCols *schema.ColCollection
	nbf       *types.NomsBinFormat
}

func NewCoveringIndexRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter *noms.NomsRangeReader, resultCols []uint64) *CoveringIndexRowIterAdapter {
	idxCols := idx.IndexSchema().GetPKCols()
	tblPKCols := idx.Schema().GetPKCols()
	sch := idx.Schema()
	allCols := sch.GetAllCols().GetColumns()
	cols := make([]schema.Column, 0)
	for _, col := range allCols {
		for _, tag := range resultCols {
			if col.Tag == tag {
				cols = append(cols, col)
			}
		}

	}
	tagToSqlColIdx := make(map[uint64]int)
	isPrimaryKeyIdx := idx.ID() == "PRIMARY"

	resultColSet := make(map[uint64]bool)
	for _, k := range resultCols {
		resultColSet[k] = true
	}
	for i, col := range cols {
		_, partOfIdxKey := idxCols.GetByNameCaseInsensitive(col.Name)
		// Either this is a primary key index or the key is a part of the index and this part of the result column set.
		if (partOfIdxKey || isPrimaryKeyIdx) && (len(resultCols) == 0 || resultColSet[col.Tag]) {
			tagToSqlColIdx[col.Tag] = i
		}
	}

	for i, col := range cols {
		_, partOfIndexKey := idxCols.GetByTag(col.Tag)
		_, partOfTableKeys := tblPKCols.GetByTag(col.Tag)
		if partOfIndexKey != partOfTableKeys {
			cols[i], _ = schema.NewColumnWithTypeInfo(col.Name, col.Tag, col.TypeInfo, partOfIndexKey, col.Default, col.AutoIncrement, col.Comment, col.Constraints...)
		}
	}

	return &CoveringIndexRowIterAdapter{
		idx:       idx,
		rr:        keyIter,
		conv:      NewKVToSqlRowConverter(idx.Format(), tagToSqlColIdx, cols, len(cols)),
		ctx:       ctx,
		pkCols:    sch.GetPKCols(),
		nonPKCols: sch.GetNonPKCols(),
		nbf:       idx.Format(),
	}
}

// Next returns the next row from the iterator.
func (ci *CoveringIndexRowIterAdapter) Next(ctx *sql.Context) (sql.Row, error) {
	key, value, err := ci.rr.ReadKV(ctx)

	if err != nil {
		return nil, err
	}

	return ci.conv.ConvertKVTuplesToSqlRow(key, value)
}

func (ci *CoveringIndexRowIterAdapter) Close(*sql.Context) error {
	return nil
}
