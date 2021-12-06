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

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// TableReader is an interface for reading rows from a table
type TableReader interface {
	// GetSchema gets the schema of the rows that this reader will return // TODO: Depecrate
	GetSchema() schema.Schema

	// ReadRow reads a row from a table. If there is a bad row the returned error will be non nil, and calling
	// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
	// continue on a bad row, or fail. // TODO: Deprecate
	ReadRow(ctx context.Context) (row.Row, error)

	// GetSqlSchema gets the schema of the rows that this reader will return in sql.Schema form.
	GetSqlSchema() sql.Schema

	// ReadSqlRow read a sql.Row from the table. If there is a bad row the returned error will be non nil, and calling
	// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
	// continue on a bad row, or fail
	ReadSqlRow(ctx context.Context) (sql.Row, error)
}

// TableCloser is an interface for a table stream that can be closed to release resources
type TableCloser interface {
	// Close should release resources being held
	Close(ctx context.Context) error
}

// TableReadCloser is an interface for reading rows from a table, that can be closed.
type TableReadCloser interface {
	TableReader
	TableCloser
}

// NewDoltTableReader creates a SqlTableReader from |tbl| starting from the first record.
func NewDoltTableReader(ctx context.Context, tbl *doltdb.Table) (TableReadCloser, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return newKeylessTableReader(ctx, tbl, sch, false)
	}
	return newPkTableReader(ctx, tbl, sch, false)
}

// NewBufferedTableReader creates a buffered SqlTableReader from |tbl| starting from the first record.
func NewBufferedTableReader(ctx context.Context, tbl *doltdb.Table) (TableReadCloser, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return newKeylessTableReader(ctx, tbl, sch, true)
	}
	return newPkTableReader(ctx, tbl, sch, true)
}
