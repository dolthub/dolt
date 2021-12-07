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
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/store/types"
)

type pkTableReader struct {
	iter types.MapIterator
	sch  schema.Schema
}

var _ TableReadCloser = pkTableReader{}

// GetSchema implements the TableReader interface.
func (rdr pkTableReader) GetSchema() schema.Schema {
	return rdr.sch
}

func (rdr pkTableReader) GetSqlSchema() sql.PrimaryKeySchema {
	panic("todo")
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

	return newPkTableReaderForRows(ctx, rows, sch, buffered)
}

func newPkTableReaderForRows(ctx context.Context, rows types.Map, sch schema.Schema, buffered bool) (pkTableReader, error) {
	var err error
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
