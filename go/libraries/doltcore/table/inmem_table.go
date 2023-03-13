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

package table

import (
	"context"
	"io"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/types"
)

// InMemTable holds a simple list of rows that can be retrieved, or appended to.  It is meant primarily for testing.
type InMemTable struct {
	sch  schema.Schema
	rows []row.Row
}

// NewInMemTable creates an empty Table with the expectation that any rows added will have the given
// Schema
func NewInMemTable(sch schema.Schema) *InMemTable {
	return NewInMemTableWithData(sch, []row.Row{})
}

// NewInMemTableWithData creates a Table with the riven rows
func NewInMemTableWithData(sch schema.Schema, rows []row.Row) *InMemTable {
	return NewInMemTableWithDataAndValidationType(sch, rows)
}

func NewInMemTableWithDataAndValidationType(sch schema.Schema, rows []row.Row) *InMemTable {
	return &InMemTable{sch, rows}
}

// AppendRow appends a row.  Appended rows must be valid for the table's schema. Sorts rows as they are inserted.
func (imt *InMemTable) AppendRow(ctx context.Context, vr types.ValueReader, r row.Row) error {
	if isv, err := row.IsValid(r, imt.sch); err != nil {
		return err
	} else if !isv {
		col, err := row.GetInvalidCol(r, imt.sch)

		if err != nil {
			return err
		}

		val, ok := r.GetColVal(col.Tag)

		if !ok {
			return NewBadRow(r, col.Name+" is missing")
		} else {
			encValStr, err := types.EncodedValue(context.Background(), val)

			if err != nil {
				return err
			}

			return NewBadRow(r, col.Name+":"+encValStr+" is not valid.")
		}
	}

	imt.rows = append(imt.rows, r)

	var err error
	// If we are going to pipe these into noms, they need to be sorted.
	sort.Slice(imt.rows, func(i, j int) bool {
		if err != nil {
			return false
		}

		iRow := imt.rows[i]
		jRow := imt.rows[j]

		isLess := false
		isLess, err = iRow.NomsMapKey(imt.sch).Less(ctx, vr.Format(), jRow.NomsMapKey(imt.sch))

		return isLess
	})

	return nil
}

// GetRow gets a row by index
func (imt *InMemTable) GetRow(index int) (row.Row, error) {
	return imt.rows[index], nil
}

// GetSchema gets the table's schema
func (imt *InMemTable) GetSchema() schema.Schema {
	return imt.sch
}

// NumRows returns the number of rows in the table
func (imt *InMemTable) NumRows() int {
	return len(imt.rows)
}

// InMemTableReader is an implementation of a TableReader for an InMemTable
type InMemTableReader struct {
	tt      *InMemTable
	current int
}

var _ SqlTableReader = &InMemTableReader{}

// NewInMemTableReader creates an instance of a TableReader from an InMemTable
func NewInMemTableReader(imt *InMemTable) *InMemTableReader {
	return &InMemTableReader{imt, 0}
}

// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and callin IsBadRow(err)
// will be return true. This is a potentially non-fatal error and callers can decide if they want to continue on a bad row, or fail.
func (rd *InMemTableReader) ReadRow(ctx context.Context) (row.Row, error) {
	numRows := rd.tt.NumRows()

	if rd.current < numRows {
		r := rd.tt.rows[rd.current]
		rd.current++

		return r, nil
	}

	return nil, io.EOF
}

func (rd *InMemTableReader) ReadSqlRow(ctx context.Context) (sql.Row, error) {
	r, err := rd.ReadRow(ctx)
	if err != nil {
		return nil, err
	}

	return sqlutil.DoltRowToSqlRow(r, rd.GetSchema())
}

// Close should release resources being held
func (rd *InMemTableReader) Close(ctx context.Context) error {
	rd.current = -1
	return nil
}

// GetSchema gets the schema of the rows that this reader will return
func (rd *InMemTableReader) GetSchema() schema.Schema {
	return rd.tt.sch
}

// VerifySchema checks that the incoming schema matches the schema from the existing table
func (rd *InMemTableReader) VerifySchema(outSch schema.Schema) (bool, error) {
	return schema.VerifyInSchema(rd.tt.sch, outSch)
}
