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
	"io"
	"runtime"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

type indexLookupRowIterAdapter struct {
	idx     DoltIndex
	keyIter nomsKeyIter
	pkTags  map[uint64]int
	conv    *KVToSqlRowConverter
	ctx     *sql.Context
	rowChan chan sql.Row
	err     error
	buffer  []sql.Row
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
		idx:     idx,
		keyIter: keyIter,
		conv:    conv,
		pkTags:  pkTags,
		ctx:     ctx,
		rowChan: make(chan sql.Row, runtime.NumCPU()*10),
		buffer:  make([]sql.Row, runtime.NumCPU()*5),
	}
	go iter.queueRows()
	return iter
}

// Next returns the next row from the iterator.
func (i *indexLookupRowIterAdapter) Next() (sql.Row, error) {
	r, ok := <-i.rowChan
	if !ok { // Only closes when we are finished iterating over the keys or an error has occurred.
		if i.err != nil {
			return nil, i.err
		}
		return nil, io.EOF
	}
	return r, nil
}

func (*indexLookupRowIterAdapter) Close() error {
	return nil
}

// queueRows reads each key from the key iterator and runs a goroutine for each logical processor to process the keys.
func (i *indexLookupRowIterAdapter) queueRows() {
	defer close(i.rowChan)
	exec := async.NewActionExecutor(i.ctx, i.processKey, uint32(runtime.NumCPU()), 0)

	var err error
	for {
		shouldBreak := false
		pos := 0
		for ; pos < len(i.buffer); pos++ {
			var indexKey types.Value
			indexKey, err = i.keyIter.ReadKey(i.ctx)
			if err != nil {
				break
			}
			exec.Execute(keyPos{
				key:      indexKey.(types.Tuple),
				position: pos,
			})
		}
		if err != nil {
			if err == io.EOF {
				shouldBreak = true
			} else {
				i.err = err
				return
			}
		}
		i.err = exec.WaitForEmpty()
		if err != nil {
			if err == io.EOF {
				shouldBreak = true
			} else {
				i.err = err
				return
			}
		}
		for idx, r := range i.buffer {
			if idx == pos {
				break
			}
			i.rowChan <- r
		}
		if shouldBreak {
			break
		}
	}
}

func (i *indexLookupRowIterAdapter) indexKeyToTableKey(nbf *types.NomsBinFormat, indexKey types.Tuple) (types.Value, error) {
	tplItr, err := indexKey.Iterator()

	if err != nil {
		return nil, err
	}

	resVals := make([]types.Value, len(i.pkTags)*2)
	for {
		_, tagVal, err := tplItr.Next()

		if err != nil {
			return nil, err
		}

		if tagVal == nil {
			break
		}

		tag := uint64(tagVal.(types.Uint))
		idx, inPK := i.pkTags[tag]

		if inPK {
			_, valVal, err := tplItr.Next()

			if err != nil {
				return nil, err
			}

			resVals[idx*2] = tagVal
			resVals[idx*2+1] = valVal
		} else {
			err := tplItr.Skip()

			if err != nil {
				return nil, err
			}
		}
	}

	return types.NewTuple(nbf, resVals...)
}

// processKey is called within queueRows and processes each key, sending the resulting row to the row channel.
func (i *indexLookupRowIterAdapter) processKey(_ context.Context, valInt interface{}) error {
	val := valInt.(keyPos)

	tableData := i.idx.TableData()
	pkTupleVal, err := i.indexKeyToTableKey(tableData.Format(), val.key)

	if err != nil {
		return err
	}

	fieldsVal, _, err := tableData.MaybeGet(i.ctx, pkTupleVal)
	if err != nil {
		return err
	}

	if fieldsVal == nil {
		return nil
	}

	sqlRow, err := i.conv.ConvertKVToSqlRow(pkTupleVal, fieldsVal)
	if err != nil {
		return err
	}

	i.buffer[val.position] = sqlRow
	return nil
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

	return ci.conv.ConvertKVToSqlRow(key, nil)
}

func (ci *coveringIndexRowIterAdapter) Close() error {
	return nil
}
