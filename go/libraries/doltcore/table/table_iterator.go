// Copyright 2022 Dolthub, Inc.
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

package table

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/types"
)

// RowIter wraps a sql.RowIter and abstracts away sql.Context for a
// context.Context.
type RowIter interface {
	Next(ctx context.Context) (sql.Row, error)
	Close(ctx context.Context) error
}

type rowIterImpl struct {
	inner  sql.RowIter
	sqlCtx *sql.Context
}

// NewRowIter returns a RowIter that wraps |inner|. Ctx passed to Next is
// converted to *sql.Context.
func NewRowIter(inner sql.RowIter) RowIter {
	return rowIterImpl{inner: inner}
}

// Next implements RowIter.
func (i rowIterImpl) Next(ctx context.Context) (sql.Row, error) {
	r, err := i.inner.Next(&sql.Context{Context: ctx})
	if err != nil {
		return nil, err
	}
	return r, nil
}

// Close implements RowIter.
func (i rowIterImpl) Close(ctx context.Context) error {
	return i.inner.Close(&sql.Context{Context: ctx})
}

// NewTableIterator creates a RowIter that iterates sql.Row's from |idx|.
// |offset| can be supplied to read at some start point in |idx|.
func NewTableIterator(ctx context.Context, sch schema.Schema, idx durable.Index) (RowIter, error) {
	var rowItr sql.RowIter
	if types.IsFormat_DOLT(idx.Format()) {
		m := durable.MapFromIndex(idx)
		itr, err := m.IterAll(ctx)
		if err != nil {
			return nil, err
		}
		rowItr = index.NewProllyRowIterForMap(sch, m, itr, nil)
		if err != nil {
			return nil, err
		}
	} else {

		noms := durable.NomsMapFromIndex(idx)
		itr, err := noms.IteratorAt(ctx, 0)
		if err != nil {
			return nil, err
		}
		conv := makeNomsConverter(idx.Format(), sch)
		rowItr = index.NewDoltMapIter(itr.NextTuple, nil, conv)
	}
	return NewRowIter(rowItr), nil
}

// makeNomsConverter creates a *index.KVToSqlRowConverter.
func makeNomsConverter(nbf *types.NomsBinFormat, sch schema.Schema) *index.KVToSqlRowConverter {
	cols := sch.GetAllCols().GetColumns()
	tagToSqlColIdx := make(map[uint64]int)
	for i, col := range cols {
		tagToSqlColIdx[col.Tag] = i
	}
	return index.NewKVToSqlRowConverter(nbf, tagToSqlColIdx, cols, len(cols))
}
