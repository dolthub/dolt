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
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ sql.Table = (*TestsTable)(nil)
var _ sql.UpdatableTable = (*TestsTable)(nil)
var _ sql.DeletableTable = (*TestsTable)(nil)
var _ sql.InsertableTable = (*TestsTable)(nil)
var _ sql.ReplaceableTable = (*TestsTable)(nil)
var _ sql.IndexAddressableTable = (*TestsTable)(nil)

// TestsTable is the system table that stores test definitions.
type TestsTable struct {
	backingTable VersionableTable
}

func (tt *TestsTable) Name() string {
	return doltdb.TestsTableName
}

func (tt *TestsTable) String() string {
	return doltdb.TestsTableName
}

func doltTestsSchema() sql.Schema {
	return []*sql.Column{
		{Name: "test_name", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: true},
		{Name: "test_group", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: true},
		{Name: "test_query", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_type", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_comparator", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
		{Name: "assertion_value", Type: sqlTypes.Text, Source: doltdb.TestsTableName, PrimaryKey: false, Nullable: false},
	}
}

// GetDoltTestsSchema returns the schema of the dolt_tests system table. This is used
// by Doltgres to update the dolt_tests schema using Doltgres types.
var GetDoltTestsSchema = doltTestsSchema

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_tests system table.
func (tt *TestsTable) Schema() sql.Schema {
	return GetDoltTestsSchema()
}

func (tt *TestsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.
func (tt *TestsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	if tt.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return tt.backingTable.Partitions(context)
}

func (tt *TestsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if tt.backingTable == nil {
		return sql.RowsToRowIter(), nil
	}

	return tt.backingTable.PartitionRows(context, partition)
}

// NewTestsTable creates a TestsTable
func NewTestsTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &TestsTable{backingTable: backingTable}
}

// NewEmptyTestsTable creates an empty TestsTable
func NewEmptyTestsTable(_ *sql.Context) sql.Table {
	return &TestsTable{}
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete
// called once for each row, followed by a call to Close() when all rows have been processed.
func (tt *TestsTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newTestsWriter(tt)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row
// to be updated, followed by a call to Close() when all rows have been processed.
func (tt *TestsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return newTestsWriter(tt)
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to
// be inserted, and will end with a call to Close() to finalize the insert operation.
func (tt *TestsTable) Inserter(*sql.Context) sql.RowInserter {
	return newTestsWriter(tt)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to
// be deleted, and will end with a call to Close() to finalize the delete operation.
func (tt *TestsTable) Deleter(*sql.Context) sql.RowDeleter {
	return newTestsWriter(tt)
}

func (tt *TestsTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if tt.backingTable == nil {
		return tt, nil
	}
	return tt.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but TestsTable has no indexes.
// Thus, this should never be called.
func (tt *TestsTable) IndexedAccess(_ *sql.Context, _ sql.IndexLookup) sql.IndexedTable {
	panic("Unreachable")
}

// GetIndexes implements IndexAddressableTable, but TestsTable has no indexes.
func (tt *TestsTable) GetIndexes(_ *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (tt *TestsTable) PreciseMatch() bool {
	return true
}

var _ sql.RowReplacer = (*testsWriter)(nil)
var _ sql.RowUpdater = (*testsWriter)(nil)
var _ sql.RowInserter = (*testsWriter)(nil)
var _ sql.RowDeleter = (*testsWriter)(nil)

type testsWriter struct {
	tt                      *TestsTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             dsess.TableWriter
}

func newTestsWriter(tt *TestsTable) *testsWriter {
	return &testsWriter{tt, nil, nil, nil}
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (tw *testsWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := tw.errDuringStatementBegin; err != nil {
		return err
	}
	return tw.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (tw *testsWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := tw.errDuringStatementBegin; err != nil {
		return err
	}
	return tw.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for each
// row to process for the delete operation, which may involve many rows. After all rows have been processed, Close is called.
func (tw *testsWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := tw.errDuringStatementBegin; err != nil {
		return err
	}
	return tw.tableWriter.Delete(ctx, r)
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (tw *testsWriter) StatementBegin(ctx *sql.Context) {
	name := getDoltTestsTableName()
	prevHash, tableWriter, err := createWriteableSystemTable(ctx, name, tw.tt.Schema())
	if err != nil {
		tw.errDuringStatementBegin = err
		return
	}
	tw.prevHash = prevHash
	tw.tableWriter = tableWriter
}

func getDoltTestsTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.TestsTableName}
	}
	return doltdb.TableName{Name: doltdb.GetTestsTableName()}
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the
// statement beginning should be discarded.
func (tw *testsWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if tw.tableWriter != nil {
		return tw.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (tw *testsWriter) StatementComplete(ctx *sql.Context) error {
	if tw.tableWriter != nil {
		return tw.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (tw *testsWriter) Close(ctx *sql.Context) error {
	if tw.tableWriter != nil {
		return tw.tableWriter.Close(ctx)
	}
	return nil
}
