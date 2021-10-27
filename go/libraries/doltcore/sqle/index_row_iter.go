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
	"sync"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/val"
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
	idx        DoltIndex
	keyIter    keyIter
	lookupTags map[uint64]int
	ctx        *sql.Context
	cancelF    func()

	read  uint64
	count uint64

	resultBuf *async.RingBuffer
}

// NewIndexLookupRowIterAdapter returns a new indexLookupRowIterAdapter.
func NewIndexLookupRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter keyIter) *indexLookupRowIterAdapter {
	lookupTags := make(map[uint64]int)
	for i, tag := range idx.Schema().GetPKCols().Tags {
		lookupTags[tag] = i
	}

	// handle keyless case, where no columns are pk's and rowIdTag is the only lookup tag
	if len(lookupTags) == 0 {
		lookupTags[schema.KeylessRowIdTag] = 0
	}

	resBuf := resultBufferPool.Get().(*async.RingBuffer)
	epoch := resBuf.Reset()

	queueCtx, cancelF := context.WithCancel(ctx)

	iter := &indexLookupRowIterAdapter{
		idx:        idx,
		keyIter:    keyIter,
		lookupTags: lookupTags,
		ctx:        ctx,
		cancelF:    cancelF,
		resultBuf:  resBuf,
	}

	go iter.queueRows(queueCtx, epoch)
	return iter
}

// Next returns the next row from the iterator.
func (i *indexLookupRowIterAdapter) Next() (sql.Row, error) {
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
		indexKey, err := i.keyIter.ReadKey(i.ctx)

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
		}

		select {
		case lookups.toLookupCh <- lookup:
		case <-ctx.Done():
			i.resultBuf.Push(lookupResult{
				idx: idx,
				r:   nil,
				err: ctx.Err(),
			}, epoch)

			return
		}
	}
}

func (i *indexLookupRowIterAdapter) indexKeyToTableKey(indexKey val.Tuple) (val.Tuple, error) {
	panic("unimplement")
	//tplItr, err := indexKey.Iterator()
	//
	//if err != nil {
	//	return types.Tuple{}, err
	//}
	//
	//resVals := make([]types.Value, len(i.lookupTags)*2)
	//for {
	//	_, tag, err := tplItr.NextUint64()
	//
	//	if err != nil {
	//		if err == io.EOF {
	//			break
	//		}
	//
	//		return types.Tuple{}, err
	//	}
	//
	//	idx, inKey := i.lookupTags[tag]
	//
	//	if inKey {
	//		_, valVal, err := tplItr.Next()
	//
	//		if err != nil {
	//			return types.Tuple{}, err
	//		}
	//
	//		resVals[idx*2] = types.Uint(tag)
	//		resVals[idx*2+1] = valVal
	//	} else {
	//		err := tplItr.Skip()
	//
	//		if err != nil {
	//			return types.Tuple{}, err
	//		}
	//	}
	//}
	//
	//return types.NewTuple(nbf, resVals...)
}

// processKey is called within queueRows and processes each key, sending the resulting row to the row channel.
func (i *indexLookupRowIterAdapter) processKey(indexKey val.Tuple) (sql.Row, error) {
	panic("unimplement")
	//tableData := i.idx.TableData()
	//
	//pkTupleVal, err := i.indexKeyToTableKey(tableData.Format(), indexKey)
	//if err != nil {
	//	return nil, err
	//}
	//
	//fieldsVal, ok, err := tableData.MaybeGetTuple(i.ctx, pkTupleVal)
	//if err != nil {
	//	return nil, err
	//}
	//
	//if !ok {
	//	return nil, nil
	//}
	//
	//sqlRow, err := i.conv.ConvertKVTuplesToSqlRow(pkTupleVal, fieldsVal)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return sqlRow, nil
}

type coveringIndexRowIterAdapter struct {
	ctx       *sql.Context
	idx       DoltIndex
	keyIter   keyIter
	pkCols    *schema.ColCollection
	nonPKCols *schema.ColCollection
}

func NewCoveringIndexRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter keyIter, resultCols []string) *coveringIndexRowIterAdapter {
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
		ctx:       ctx,
		idx:       idx,
		keyIter:   keyIter,
		pkCols:    sch.GetPKCols(),
		nonPKCols: sch.GetNonPKCols(),
	}
}

// Next returns the next row from the iterator.
func (ci *coveringIndexRowIterAdapter) Next() (sql.Row, error) {
	panic("unimplement")
	//key, err := ci.keyIter.ReadKey(ci.ctx)
	//if err != nil {
	//	return nil, err
	//}
	//
	//return nil, nil
}

func (ci *coveringIndexRowIterAdapter) Close(*sql.Context) error {
	return nil
}
