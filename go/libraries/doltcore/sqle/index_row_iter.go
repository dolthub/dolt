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

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/utils/async"
	"github.com/dolthub/dolt/go/store/types"

	"github.com/dolthub/go-mysql-server/sql"
)

type indexLookupRowIterAdapter struct {
	idx     DoltIndex
	keyIter IndexLookupKeyIterator
	ctx     *sql.Context
	rowChan chan sql.Row
	err     error
	buffer  []sql.Row
}

type keyPos struct {
	key      row.TaggedValues
	position int
}

// NewIndexLookupRowIterAdapter returns a new indexLookupRowIterAdapter.
func NewIndexLookupRowIterAdapter(ctx *sql.Context, idx DoltIndex, keyIter IndexLookupKeyIterator) *indexLookupRowIterAdapter {
	iter := &indexLookupRowIterAdapter{
		idx:     idx,
		keyIter: keyIter,
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
			var indexKey row.TaggedValues
			indexKey, err = i.keyIter.NextKey(i.ctx)
			if err != nil {
				break
			}
			exec.Execute(keyPos{
				key:      indexKey,
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

// processKey is called within queueRows and processes each key, sending the resulting row to the row channel.
func (i *indexLookupRowIterAdapter) processKey(_ context.Context, valInt interface{}) error {
	val := valInt.(keyPos)

	tableData := i.idx.TableData()
	pkTuple := val.key.NomsTupleForPKCols(tableData.Format(), i.idx.Schema().GetPKCols())
	pkTupleVal, err := pkTuple.Value(i.ctx)
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

	r, err := row.FromNoms(i.idx.Schema(), pkTupleVal.(types.Tuple), fieldsVal.(types.Tuple))
	if err != nil {
		return err
	}

	sqlRow, err := row.DoltRowToSqlRow(r, i.idx.Schema())
	if err != nil {
		return err
	}
	i.buffer[val.position] = sqlRow
	return nil
}
