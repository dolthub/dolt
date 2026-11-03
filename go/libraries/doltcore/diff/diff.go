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
	"github.com/dolthub/go-mysql-server/sql/types"
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

// contentAddressComparer is implemented by stored values that can match
// another value by content address. Callers can check every storage type
// through this interface instead of a type switch. A false result means the
// values must be compared by content, since equal values can have different
// addresses.
type contentAddressComparer interface {
	SameContentAddress(other interface{}) bool
}

// ValuesEqual reports whether |fromValue| and |toValue| match in a diff. Text
// that unwraps to strings compares exactly, so case-only changes are reported.
// Stored values with equal content addresses match without being loaded. Other
// values compare with |typ|.
func ValuesEqual(ctx *sql.Context, typ sql.Type, fromValue, toValue interface{}) (bool, error) {
	if stored, ok := fromValue.(contentAddressComparer); ok && stored.SameContentAddress(toValue) {
		return true, nil
	}
	if types.IsTextOnly(typ) && fromValue != nil && toValue != nil {
		fromText, fromOK, err := sql.Unwrap[string](ctx, fromValue)
		if err != nil {
			return false, err
		}
		toText, toOK, err := sql.Unwrap[string](ctx, toValue)
		if err != nil {
			return false, err
		}
		if fromOK && toOK {
			return fromText == toText, nil
		}
	}
	cmp, err := typ.Compare(ctx, fromValue, toValue)
	return cmp == 0, err
}
