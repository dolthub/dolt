// Copyright 2025 Dolthub, Inc.
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

package dtables

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
)

// systemTableForeignKeyEditor is an implementation of sql.ForeignKeyEditor for system tables.
// User tables may declare foreign key references to system tables that support that, but do not
// support foreign keys on system tables referencing other tables and do not support FK referential
// actions such as CASCADE. Thus, this is essentially a no-op implementation of sql.ForeignKeyEditor.
type systemTableForeignKeyEditor struct {
	// indexedTable is the underlying system table associated with this ForeignKeyEditor.
	indexedTable sql.IndexedTable
}

var _ sql.ForeignKeyEditor = (*systemTableForeignKeyEditor)(nil)

// StatementBegin implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) StatementBegin(ctx *sql.Context) {}

// StatementComplete implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) StatementComplete(ctx *sql.Context) error {
	return nil
}

// DiscardChanges implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return nil
}

// Close implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) Close(context *sql.Context) error {
	return nil
}

// Insert implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) Insert(context *sql.Context, row sql.Row) error {
	return fmt.Errorf("dolt system tables do not support inserting rows referenced by foreign keys")
}

// Delete implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) Delete(context *sql.Context, row sql.Row) error {
	return fmt.Errorf("dolt system tables do not support deleting rows referenced by foreign keys")
}

// Update implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	return fmt.Errorf("dolt system tables do not support updating rows referenced by foreign keys")
}

// IndexedAccess implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) IndexedAccess(ctx *sql.Context, _ sql.IndexLookup) sql.IndexedTable {
	return m.indexedTable
}

// GetIndexes implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

// PreciseMatch implements sql.ForeignKeyEditor
func (m systemTableForeignKeyEditor) PreciseMatch() bool {
	return false
}
