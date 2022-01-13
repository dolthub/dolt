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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

type keylessTableReader struct {
	iter types.MapIterator
	sch  schema.Schema

	row              row.Row
	remainingCopies  uint64
	bounded          bool
	remainingEntries uint64
}

var _ SqlTableReader = &keylessTableReader{}
var _ TableReadCloser = &keylessTableReader{}

// GetSchema implements the TableReader interface.
func (rdr *keylessTableReader) GetSchema() schema.Schema {
	return rdr.sch
}

// ReadSqlRow implements the SqlTableReader interface.
func (rdr *keylessTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	if rdr.remainingCopies <= 0 {
		if rdr.bounded && rdr.remainingEntries == 0 {
			return nil, io.EOF
		}

		key, val, err := rdr.iter.Next(ctx)
		if err != nil {
			return nil, err
		} else if key == nil {
			return nil, io.EOF
		}

		rdr.row, rdr.remainingCopies, err = row.KeylessRowsFromTuples(key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return nil, err
		}

		if rdr.remainingEntries > 0 {
			rdr.remainingEntries -= 1
		}

		if rdr.remainingCopies == 0 {
			return nil, row.ErrZeroCardinality
		}
	}

	rdr.remainingCopies -= 1

	return rdr.row, nil
}

// ReadSqlRow implements the SqlTableReader interface.
func (rdr *keylessTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	r, err := rdr.ReadRow(ctx)
	if err != nil {
		return nil, err
	}

	return sqlutil.DoltRowToSqlRow(r, rdr.sch)
}

// Close implements the TableReadCloser interface.
func (rdr *keylessTableReader) Close(_ context.Context) error {
	return nil
}

func newKeylessTableReader(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, buffered bool) (*keylessTableReader, error) {
	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	return newKeylessTableReaderForRows(ctx, rows, sch, buffered)
}

func newKeylessTableReaderForRows(ctx context.Context, rows types.Map, sch schema.Schema, buffered bool) (*keylessTableReader, error) {
	var err error
	var iter types.MapIterator
	if buffered {
		iter, err = rows.Iterator(ctx)
	} else {
		iter, err = rows.BufferedIterator(ctx)
	}
	if err != nil {
		return nil, err
	}

	return &keylessTableReader{
		iter: iter,
		sch:  sch,
	}, nil
}

func newKeylessTableReaderForPartition(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, start, end uint64) (SqlTableReader, error) {
	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := rows.BufferedIteratorAt(ctx, start)
	if err != nil {
		return nil, err
	}

	return &keylessTableReader{
		iter:             iter,
		sch:              sch,
		remainingEntries: end - start,
		bounded:          true,
	}, nil
}

func newKeylessTableReaderFrom(ctx context.Context, tbl *doltdb.Table, sch schema.Schema, val types.Value) (SqlTableReader, error) {
	rows, err := tbl.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	iter, err := rows.IteratorFrom(ctx, val)
	if err != nil {
		return nil, err
	}

	return &keylessTableReader{
		iter: iter,
		sch:  sch,
	}, nil
}
