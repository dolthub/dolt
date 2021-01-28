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
	"errors"
	"io"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type indexLookupRowIterAdapter struct {
	ctx     *sql.Context
	nbf     *types.NomsBinFormat
	idx     DoltIndex
	keyIter nomsKeyIter
	pkTags  map[uint64]int
	conv    *KVToSqlRowConverter
}

type keyPos struct {
	key      types.Tuple
	position int
}

// NewIndexLookupRowIterAdapter returns a new indexLookupRowIterAdapter.
func NewIndexLookupRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter nomsKeyIter) *indexLookupRowIterAdapter {
	pkTags := make(map[uint64]int)
	for i, tag := range idx.Schema().GetPKCols().Tags {
		pkTags[tag] = i
	}

	cols := idx.Schema().GetAllCols().GetColumns()
	conv := NewKVToSqlRowConverterForCols(idx.IndexRowData().Format(), cols)

	iter := &indexLookupRowIterAdapter{
		ctx:     ctx,
		nbf:     idx.IndexRowData().Format(),
		idx:     idx,
		keyIter: keyIter,
		conv:    conv,
		pkTags:  pkTags,
	}

	return iter
}

// Next returns the next row from the iterator.
func (i *indexLookupRowIterAdapter) Next() (sql.Row, error) {
	indexKey, err := i.keyIter.ReadKey(i.ctx)

	if err != nil {
		return nil, err
	}

	tableKey, err := i.indexKeyToTableKey(i.nbf, indexKey)

	if err != nil {
		return nil ,err
	}

	tableData := i.idx.TableData()
	pkTupleVal, err := i.indexKeyToTableKey(tableData.Format(), tableKey)

	if err != nil {
		return nil, err
	}

	fieldsVal, ok, err := tableData.MaybeGetTuple(i.ctx, pkTupleVal)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, errors.New("missing indexed row in table row data")
	}

	sqlRow, err := i.conv.ConvertKVTuplesToSqlRow(pkTupleVal, fieldsVal)

	if err != nil {
		return nil, err
	}

	return sqlRow, nil
}

func (*indexLookupRowIterAdapter) Close() error {
	return nil
}

func (i *indexLookupRowIterAdapter) indexKeyToTableKey(nbf *types.NomsBinFormat, indexKey types.Tuple) (types.Tuple, error) {
	tplItr, err := indexKey.Iterator()

	if err != nil {
		return types.Tuple{}, err
	}

	resVals := make([]types.Value, len(i.pkTags)*2)
	for {
		_, tag, err := tplItr.NextUint64()

		if err != nil {
			if err == io.EOF {
				break
			}

			return types.Tuple{}, err
		}

		idx, inPK := i.pkTags[tag]

		if inPK {
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

type coveringIndexRowIterAdapter struct {
	idx       DoltIndex
	keyIter   nomsKeyIter
	conv      *KVToSqlRowConverter
	ctx       *sql.Context
	pkCols    *schema.ColCollection
	nonPKCols *schema.ColCollection
	nbf       *types.NomsBinFormat
}

func NewCoveringIndexRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter nomsKeyIter, resultCols []string) *coveringIndexRowIterAdapter {
	idxCols := idx.IndexSchema().GetPKCols()
	tblPKCols := idx.Schema().GetPKCols()
	sch := idx.Schema()
	cols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)

	resultColSet := set.NewCaseInsensitiveStrSet(resultCols)
	for i, col := range cols {
		_, partOfIdxKey := idxCols.GetByNameCaseInsensitive(col.Name)
		if partOfIdxKey && (len(resultCols) == 0 || resultColSet.Contains(col.Name)) {
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

	return &coveringIndexRowIterAdapter{
		idx:       idx,
		keyIter:   keyIter,
		conv:      NewKVToSqlRowConverter(idx.IndexRowData().Format(), tagToSqlColIdx, cols, len(cols)),
		ctx:       ctx,
		pkCols:    sch.GetPKCols(),
		nonPKCols: sch.GetNonPKCols(),
		nbf:       idx.TableData().Format(),
	}
}

// Next returns the next row from the iterator.
func (ci *coveringIndexRowIterAdapter) Next() (sql.Row, error) {
	key, err := ci.keyIter.ReadKey(ci.ctx)

	if err != nil {
		return nil, err
	}

	return ci.conv.ConvertKVTuplesToSqlRow(key, types.Tuple{})
}

func (ci *coveringIndexRowIterAdapter) Close() error {
	return nil
}
