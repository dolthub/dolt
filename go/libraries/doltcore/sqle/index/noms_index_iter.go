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
	"io"
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	ringBufferAllocSize = 1024
)

var resultBufferPool = &sync.Pool{
	New: func() interface{} {
		return async.NewRingBuffer(ringBufferAllocSize)
	},
}

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
func NewIndexLookupRowIterAdapter(ctx *sql.Context, idx DoltIndex, tableData durable.Index, keyIter nomsKeyIter) (*indexLookupRowIterAdapter, error) {
	lookupTags := make(map[uint64]int)
	for i, tag := range idx.Schema().GetPKCols().Tags {
		lookupTags[tag] = i
	}

	// handle keyless case, where no columns are pk's and rowIdTag is the only lookup tag
	if len(lookupTags) == 0 {
		lookupTags[schema.KeylessRowIdTag] = 0
	}

	rows := durable.NomsMapFromIndex(tableData)

	conv := NewKVToSqlRowConverterForCols(idx.Format(), idx.Schema())
	resBuf := resultBufferPool.Get().(*async.RingBuffer)
	epoch := resBuf.Reset()

	queueCtx, cancelF := context.WithCancel(ctx)

	iter := &indexLookupRowIterAdapter{
		idx:        idx,
		keyIter:    keyIter,
		tableRows:  rows,
		conv:       conv,
		lookupTags: lookupTags,
		cancelF:    cancelF,
		resultBuf:  resBuf,
	}

	go iter.queueRows(queueCtx, epoch)
	return iter, nil
}

// Next returns the next row from the iterator.
func (i *indexLookupRowIterAdapter) Next(ctx *sql.Context) (sql.Row, error) {
	for i.count == 0 || i.read < i.count {
		item, err := i.resultBuf.Pop()

		if err != nil {
			return nil, err
		}

		res := item.(lookupResult)

		i.read++
		if res.err != nil {
			if res.err == io.EOF {
				i.count = res.idx
				continue
			}

			return nil, res.err
		}

		return res.r, res.err
	}

	return nil, io.EOF
}

func (i *indexLookupRowIterAdapter) Close(*sql.Context) error {
	i.cancelF()
	resultBufferPool.Put(i.resultBuf)
	return nil
}

// queueRows reads each key from the key iterator and writes it to lookups.toLookupCh which manages a pool of worker
// routines which will process the requests in parallel.
func (i *indexLookupRowIterAdapter) queueRows(ctx context.Context, epoch int) {
	for idx := uint64(1); ; idx++ {
		indexKey, err := i.keyIter.ReadKey(ctx)

		if err != nil {
			i.resultBuf.Push(lookupResult{
				idx: idx,
				r:   nil,
				err: err,
			}, epoch)

			return
		}

		lookup := toLookup{
			idx:        idx,
			t:          indexKey,
			tupleToRow: i.processKey,
			resBuf:     i.resultBuf,
			epoch:      epoch,
			ctx:        ctx,
		}

		select {
		case lookups.toLookupCh <- lookup:
		case <-ctx.Done():
			err := ctx.Err()
			if err == nil {
				err = io.EOF
			}
			i.resultBuf.Push(lookupResult{
				idx: idx,
				r:   nil,
				err: err,
			}, epoch)

			return
		}
	}
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

func NewCoveringIndexRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter *noms.NomsRangeReader, resultCols []string) *CoveringIndexRowIterAdapter {
	idxCols := idx.IndexSchema().GetPKCols()
	tblPKCols := idx.Schema().GetPKCols()
	sch := idx.Schema()
	cols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)
	isPrimaryKeyIdx := idx.ID() == "PRIMARY"

	resultColSet := set.NewCaseInsensitiveStrSet(resultCols)
	for i, col := range cols {
		_, partOfIdxKey := idxCols.GetByNameCaseInsensitive(col.Name)
		// Either this is a primary key index or the key is a part of the index and this part of the result column set.
		if (partOfIdxKey || isPrimaryKeyIdx) && (len(resultCols) == 0 || resultColSet.Contains(col.Name)) {
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
