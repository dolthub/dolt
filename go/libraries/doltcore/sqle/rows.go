// Copyright 2019 Dolthub, Inc.
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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
)

var _ sql.RowIter = (*keylessRowIter)(nil)

type keylessRowIter struct {
	keyedIter *DoltMapIter

	cardIdx     int
	nonCardCols int

	lastRead sql.Row
	lastCard uint64
}

func (k *keylessRowIter) Next() (sql.Row, error) {
	if k.lastCard == 0 {
		r, err := k.keyedIter.Next()

		if err != nil {
			return nil, err
		}

		k.lastCard = r[k.cardIdx].(uint64)
		k.lastRead = r[:k.nonCardCols]
	}

	k.lastCard--
	return k.lastRead, nil
}

func (k keylessRowIter) Close(ctx *sql.Context) error {
	return k.keyedIter.Close(ctx)
}

// An iterator over the rows of a table.
type doltTableRowIter struct {
	sql.RowIter
	ctx    context.Context
	reader table.SqlTableReader
}

// Returns a new row iterator for the table given
func newRowIterator(ctx *sql.Context, tbl *doltdb.Table, projCols []string, partition *doltTablePartition) (sql.RowIter, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		// would be more optimal to project columns into keyless tables also
		return newKeylessRowIterator(ctx, tbl, projCols, partition)
	} else {
		return newKeyedRowIter(ctx, tbl, projCols, partition)
	}
}

func newKeylessRowIterator(ctx *sql.Context, tbl *doltdb.Table, projectedCols []string, partition *doltTablePartition) (sql.RowIter, error) {
	panic("unimplement")
}

// Next returns the next row in this row iterator, or an io.EOF error if there aren't any more.
func (itr *doltTableRowIter) Next() (sql.Row, error) {
	return itr.reader.ReadSqlRow(itr.ctx)
}

// Close required by sql.RowIter interface
func (itr *doltTableRowIter) Close(*sql.Context) error {
	return nil
}
