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

package table

import (
	"context"
	"io"

	"github.com/dolthub/go-mysql-server/sql"
	"golang.org/x/sync/errgroup"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type pkTableReader struct {
	iter types.MapIterator
	sch  schema.Schema
}

var _ SqlTableReader = pkTableReader{}
var _ TableReadCloser = pkTableReader{}

// GetSchema implements the TableReader interface.
func (rdr pkTableReader) GetSchema() schema.Schema {
	return rdr.sch
}

// ReadRow implements the TableReader interface.
func (rdr pkTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	key, val, err := rdr.iter.Next(ctx)

	if err != nil {
		return nil, err
	} else if key == nil {
		return nil, io.EOF
	}

	return row.FromNoms(rdr.sch, key.(types.Tuple), val.(types.Tuple))
}

// ReadSqlRow implements the SqlTableReader interface.
func (rdr pkTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	key, val, err := rdr.iter.Next(ctx)

	if err != nil {
		return nil, err
	} else if key == nil {
		return nil, io.EOF
	}

	return noms.SqlRowFromTuples(rdr.sch, key.(types.Tuple), val.(types.Tuple))
}

// Close implements the TableReadCloser interface.
func (rdr pkTableReader) Close(_ context.Context) error {
	return nil
}

func newPkTableReader(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, buffered bool) (pkTableReader, error) {
	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return pkTableReader{}, err
	}

	var iter types.MapIterator
	if buffered {
		iter, err = rows.Iterator(ctx)
	} else {
		iter, err = rows.BufferedIterator(ctx)
	}
	if err != nil {
		return pkTableReader{}, err
	}

	return pkTableReader{
		iter: iter,
		sch:  sch,
	}, nil
}

func newPkTableReaderFrom(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, val types.Value) (SqlTableReader, error) {
	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := rows.IteratorFrom(ctx, val)
	if err != nil {
		return nil, err
	}

	return pkTableReader{
		iter: iter,
		sch:  sch,
	}, nil
}

type partitionTableReader struct {
	eg  *errgroup.Group
	kvs <-chan types.Value

	sch schema.Schema
}

var _ SqlTableReader = partitionTableReader{}

func newPkTableReaderForPartition(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, start, end uint64) (SqlTableReader, error) {
	rows, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	eg, ctx := errgroup.WithContext(ctx)
	kvs := make(chan types.Value, 2)

	eg.Go(func() error {
		defer close(kvs)
		return rows.IterRange(ctx, start, end, func(key, value types.Value) error {
			select {
			case <-ctx.Done():
				return nil
			default:
				kvs <- key
				kvs <- value
			}
			return nil
		})
	})

	return partitionTableReader{
		eg:  eg,
		kvs: kvs,
		sch: sch,
	}, nil
}

// GetSchema implements the TableReader interface.
func (rdr partitionTableReader) GetSchema() schema.Schema {
	return rdr.sch
}

// ReadSqlRow implements the SqlTableReader interface.
func (rdr partitionTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	key := <-rdr.kvs
	val := <-rdr.kvs

	if key == nil || val == nil {
		return nil, io.EOF
	}

	return noms.SqlRowFromTuples(rdr.sch, key.(types.Tuple), val.(types.Tuple))
}

// Close implements the TableReadCloser interface.
func (rdr partitionTableReader) Close(_ context.Context) error {
	return nil
}
