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

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// TableReader is an interface for reading rows from a table
type TableReader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() schema.Schema

	// ReadRow reads a row from a table.  If there is a bad row the returned error will be non nil, and calling
	// IsBadRow(err) will be return true. This is a potentially non-fatal error and callers can decide if they want to
	// continue on a bad row, or fail.
	ReadRow(ctx context.Context) (row.Row, error)
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

// SqlTableReader is a  TableReader that can read rows as sql.Row.
type SqlTableReader interface {
	// GetSchema gets the schema of the rows that this reader will return
	GetSchema() schema.Schema

	// ReadRow reads a row from a table as go-mysql-server sql.Row.
	ReadSqlRow(ctx context.Context) (sql.Row, error)
}
