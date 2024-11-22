// Copyright 2023 Dolthub, Inc.
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
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ sql.Table = (*IgnoreTable)(nil)
var _ sql.UpdatableTable = (*IgnoreTable)(nil)
var _ sql.DeletableTable = (*IgnoreTable)(nil)
var _ sql.InsertableTable = (*IgnoreTable)(nil)
var _ sql.ReplaceableTable = (*IgnoreTable)(nil)
var _ sql.IndexAddressableTable = (*IgnoreTable)(nil)

// IgnoreTable is the system table that stores patterns for table names that should not be committed.
type IgnoreTable struct {
	backingTable VersionableTable
}

func (i *IgnoreTable) Name() string {
	return doltdb.IgnoreTableName
}

func (i *IgnoreTable) String() string {
	return doltdb.IgnoreTableName
}

func doltIgnoreSchema() sql.Schema {
	return []*sql.Column{
		{Name: "pattern", Type: sqlTypes.Text, Source: doltdb.IgnoreTableName, PrimaryKey: true},
		{Name: "ignored", Type: sqlTypes.Boolean, Source: doltdb.IgnoreTableName, PrimaryKey: false, Nullable: false},
	}
}

// GetDoltIgnoreSchema returns the schema of the dolt_ignore system table. This is used
// by Doltgres to update the dolt_ignore schema using Doltgres types.
var GetDoltIgnoreSchema = doltIgnoreSchema

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_ignore system table.
func (i *IgnoreTable) Schema() sql.Schema {
	return GetDoltIgnoreSchema()
}

func (i *IgnoreTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.
func (i *IgnoreTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	if i.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return i.backingTable.Partitions(context)
}

func (i *IgnoreTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if i.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}

	return i.backingTable.PartitionRows(context, partition)
}

// NewIgnoreTable creates an IgnoreTable
func NewIgnoreTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &IgnoreTable{backingTable: backingTable}
}

// NewEmptyIgnoreTable creates an IgnoreTable
func NewEmptyIgnoreTable(_ *sql.Context) sql.Table {
	return &IgnoreTable{}
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (it *IgnoreTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newIgnoreWriter(it)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (it *IgnoreTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return newIgnoreWriter(it)
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (it *IgnoreTable) Inserter(*sql.Context) sql.RowInserter {
	return newIgnoreWriter(it)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (it *IgnoreTable) Deleter(*sql.Context) sql.RowDeleter {
	return newIgnoreWriter(it)
}

func (it *IgnoreTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if it.backingTable == nil {
		return it, nil
	}
	return it.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but IgnoreTables has no indexes.
// Thus, this should never be called.
func (it *IgnoreTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	panic("Unreachable")
}

// GetIndexes implements IndexAddressableTable, but IgnoreTables has no indexes.
func (it *IgnoreTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (i *IgnoreTable) PreciseMatch() bool {
	return true
}

var _ sql.RowReplacer = (*ignoreWriter)(nil)
var _ sql.RowUpdater = (*ignoreWriter)(nil)
var _ sql.RowInserter = (*ignoreWriter)(nil)
var _ sql.RowDeleter = (*ignoreWriter)(nil)

type ignoreWriter struct {
	it                      *IgnoreTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             dsess.TableWriter
}

func newIgnoreWriter(it *IgnoreTable) *ignoreWriter {
	return &ignoreWriter{it, nil, nil, nil}
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (iw *ignoreWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (iw *ignoreWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (iw *ignoreWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Delete(ctx, r)
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (iw *ignoreWriter) StatementBegin(ctx *sql.Context) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	// TODO: this needs to use a revision qualified name
	roots, _ := dSess.GetRoots(ctx, dbName)
	dbState, ok, err := dSess.LookupDbState(ctx, dbName)
	if err != nil {
		iw.errDuringStatementBegin = err
		return
	}
	if !ok {
		iw.errDuringStatementBegin = fmt.Errorf("no root value found in session")
		return
	}

	prevHash, err := roots.Working.HashOf()
	if err != nil {
		iw.errDuringStatementBegin = err
		return
	}

	iw.prevHash = &prevHash

	tname := doltdb.TableName{Name: doltdb.IgnoreTableName}
	found, err := roots.Working.HasTable(ctx, tname)

	if err != nil {
		iw.errDuringStatementBegin = err
		return
	}

	if !found {
		sch := sql.NewPrimaryKeySchema(iw.it.Schema())
		doltSch, err := sqlutil.ToDoltSchema(ctx, roots.Working, tname, sch, roots.Head, sql.Collation_Default)
		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}

		// underlying table doesn't exist. Record this, then create the table.
		newRootValue, err := doltdb.CreateEmptyTable(ctx, roots.Working, tname, doltSch)

		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}

		if dbState.WorkingSet() == nil {
			iw.errDuringStatementBegin = doltdb.ErrOperationNotSupportedInDetachedHead
			return
		}

		// We use WriteSession.SetWorkingSet instead of DoltSession.SetWorkingRoot because we want to avoid modifying the root
		// until the end of the transaction, but we still want the WriteSession to be able to find the newly
		// created table.
		if ws := dbState.WriteSession(); ws != nil {
			err = ws.SetWorkingSet(ctx, dbState.WorkingSet().WithWorkingRoot(newRootValue))
			if err != nil {
				iw.errDuringStatementBegin = err
				return
			}
		}

		dSess.SetWorkingRoot(ctx, dbName, newRootValue)
	}

	if ws := dbState.WriteSession(); ws != nil {
		tableWriter, err := ws.GetTableWriter(ctx, tname, dbName, dSess.SetWorkingRoot, false)
		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}
		iw.tableWriter = tableWriter
		tableWriter.StatementBegin(ctx)
	}
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
// should be discarded.
func (iw *ignoreWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (iw *ignoreWriter) StatementComplete(ctx *sql.Context) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (iw ignoreWriter) Close(ctx *sql.Context) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.Close(ctx)
	}
	return nil
}
