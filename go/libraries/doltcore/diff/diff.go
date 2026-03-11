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

package diff

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"
)

// ChangeType is an enum that represents the type of change in a diff
type ChangeType int

const (
	// None is no change
	None ChangeType = iota
	// Added is the ChangeType value for a row that was newly added (In new, but not in old)
	Added
	// Removed is the ChangeTypeProp value for a row that was newly deleted (In old, but not in new)
	Removed
	// ModifiedOld is the ChangeType value for the row which represents the old value of the row before it was changed.
	ModifiedOld
	// ModifiedNew is the ChangeType value for the row which represents the new value of the row after it was changed.
	ModifiedNew
)

// Mode is an enum that represents the presentation of a diff
type Mode int

const (
	ModeRow     Mode = 0
	ModeLine    Mode = 1
	ModeInPlace Mode = 2
	ModeContext Mode = 3
)

// SqlRowDiffWriter knows how to write diff rows for a table to an arbitrary format and destination.
type SqlRowDiffWriter interface {
	// WriteRow writes the diff row given, of the diff type provided. colDiffTypes is guaranteed to be the same length as
	// the input row.
	WriteRow(ctx *sql.Context, row sql.Row, diffType ChangeType, colDiffTypes []ChangeType) error

	// WriteCombinedRow writes the diff of the rows given as a single, combined row.
	WriteCombinedRow(ctx *sql.Context, oldRow, newRow sql.Row, mode Mode) error

	// Close finalizes the work of this writer.
	Close(ctx context.Context) error
}

// SchemaDiffWriter knows how to write SQL DDL statements for a schema diff for a table to an arbitrary format and
// destination.
type SchemaDiffWriter interface {
	// WriteSchemaDiff writes the schema diff given (a SQL statement) and returns any error. A single table may have
	// many SQL statements for a single diff. WriteSchemaDiff will be called before any row diffs via |WriteRow|
	WriteSchemaDiff(schemaDiffStatement string) error
	// Close finalizes the work of this writer.
	Close() error
}
