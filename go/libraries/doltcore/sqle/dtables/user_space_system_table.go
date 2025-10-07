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

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/store/hash"
)

var _ sql.Table = (*UserSpaceSystemTable)(nil)
var _ sql.UpdatableTable = (*UserSpaceSystemTable)(nil)
var _ sql.DeletableTable = (*UserSpaceSystemTable)(nil)
var _ sql.InsertableTable = (*UserSpaceSystemTable)(nil)
var _ sql.ReplaceableTable = (*UserSpaceSystemTable)(nil)
var _ sql.IndexAddressableTable = (*UserSpaceSystemTable)(nil)

// A UserSpaceSystemTable is a system table backed by a normal table in storage.
// Like other system tables, it always exists. If the backing table doesn't exist, then reads return an empty table,
// and writes will create the table.
type UserSpaceSystemTable struct {
	backingTable VersionableTable
	tableName    doltdb.TableName
	schema       sql.Schema
}

func (bst *UserSpaceSystemTable) Name() string {
	return bst.tableName.Name
}

func (bst *UserSpaceSystemTable) String() string {
	return bst.tableName.Name
}

func (bst *UserSpaceSystemTable) Schema() sql.Schema {
	return bst.schema
}

func (bst *UserSpaceSystemTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.
func (bst *UserSpaceSystemTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	if bst.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return bst.backingTable.Partitions(context)
}

func (bst *UserSpaceSystemTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if bst.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}

	return bst.backingTable.PartitionRows(context, partition)
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (bst *UserSpaceSystemTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newBackedSystemTableWriter(bst)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (bst *UserSpaceSystemTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return newBackedSystemTableWriter(bst)
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (bst *UserSpaceSystemTable) Inserter(*sql.Context) sql.RowInserter {
	return newBackedSystemTableWriter(bst)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (bst *UserSpaceSystemTable) Deleter(*sql.Context) sql.RowDeleter {
	return newBackedSystemTableWriter(bst)
}

func (bst *UserSpaceSystemTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if bst.backingTable == nil {
		return bst, nil
	}
	return bst.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but UserSpaceSystemTable has no indexes.
// Thus, this should never be called.
func (bst *UserSpaceSystemTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	panic("Unreachable")
}

// GetIndexes implements IndexAddressableTable, but IgnoreTables has no indexes.
func (bst *UserSpaceSystemTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (bst *UserSpaceSystemTable) PreciseMatch() bool {
	return true
}

var _ sql.RowReplacer = (*backedSystemTableWriter)(nil)
var _ sql.RowUpdater = (*backedSystemTableWriter)(nil)
var _ sql.RowInserter = (*backedSystemTableWriter)(nil)
var _ sql.RowDeleter = (*backedSystemTableWriter)(nil)

type backedSystemTableWriter struct {
	bst                     *UserSpaceSystemTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             dsess.TableWriter
}

func newBackedSystemTableWriter(bst *UserSpaceSystemTable) *backedSystemTableWriter {
	return &backedSystemTableWriter{bst, nil, nil, nil}
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (bstw *backedSystemTableWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := bstw.errDuringStatementBegin; err != nil {
		return err
	}
	return bstw.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (bstw *backedSystemTableWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := bstw.errDuringStatementBegin; err != nil {
		return err
	}
	return bstw.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (bstw *backedSystemTableWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := bstw.errDuringStatementBegin; err != nil {
		return err
	}
	return bstw.tableWriter.Delete(ctx, r)
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (bstw *backedSystemTableWriter) StatementBegin(ctx *sql.Context) {
	prevHash, tableWriter, err := createWriteableSystemTable(ctx, bstw.bst.tableName, bstw.bst.Schema())
	if err != nil {
		bstw.errDuringStatementBegin = err
		return
	}
	bstw.prevHash = prevHash
	bstw.tableWriter = tableWriter
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
// should be discarded.
func (bstw *backedSystemTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if bstw.tableWriter != nil {
		return bstw.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (bstw *backedSystemTableWriter) StatementComplete(ctx *sql.Context) error {
	if bstw.tableWriter != nil {
		return bstw.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (bstw backedSystemTableWriter) Close(ctx *sql.Context) error {
	if bstw.tableWriter != nil {
		return bstw.tableWriter.Close(ctx)
	}
	return nil
}

// CreateWriteableSystemTable is a helper function that creates a writeable system table (dolt_ignore, dolt_docs...) if it does not exist
// Then returns the hash of the previous working root, and a TableWriter.
func createWriteableSystemTable(ctx *sql.Context, tblName doltdb.TableName, tblSchema sql.Schema) (*hash.Hash, dsess.TableWriter, error) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	roots, _ := dSess.GetRoots(ctx, dbName)
	dbState, ok, err := dSess.LookupDbState(ctx, dbName)
	if err != nil {
		return nil, nil, err

	}
	if !ok {
		return nil, nil, fmt.Errorf("no root value found in session")
	}

	prevHash, err := roots.Working.HashOf()
	if err != nil {
		return nil, nil, err
	}

	found, err := roots.Working.HasTable(ctx, tblName)
	if err != nil {
		return nil, nil, err
	}

	if !found {
		sch := sql.NewPrimaryKeySchema(tblSchema)
		doltSch, err := sqlutil.ToDoltSchema(ctx, roots.Working, tblName, sch, roots.Head, sql.Collation_Default)
		if err != nil {
			return nil, nil, err
		}

		// underlying table doesn't exist. Record this, then create the table.
		newRootValue, err := doltdb.CreateEmptyTable(ctx, roots.Working, tblName, doltSch)

		if err != nil {
			return nil, nil, err
		}

		if dbState.WorkingSet() == nil {
			return nil, nil, doltdb.ErrOperationNotSupportedInDetachedHead
		}

		// We use WriteSession.SetWorkingSet instead of DoltSession.SetWorkingRoot because we want to avoid modifying the root
		// until the end of the transaction, but we still want the WriteSession to be able to find the newly
		// created table.
		if ws := dbState.WriteSession(); ws != nil {
			err = ws.SetWorkingSet(ctx, dbState.WorkingSet().WithWorkingRoot(newRootValue))
			if err != nil {
				return nil, nil, err
			}
		} else {
			return nil, nil, fmt.Errorf("could not create %s table, database does not allow writing", tblName)
		}
	}

	var tableWriter dsess.TableWriter
	if ws := dbState.WriteSession(); ws != nil {
		tableWriter, err = ws.GetTableWriter(ctx, tblName, dbName, dSess.SetWorkingRoot, false)
		if err != nil {
			return nil, nil, err
		}
		tableWriter.StatementBegin(ctx)
	} else {
		return nil, nil, fmt.Errorf("could not create %s table, database does not allow writing", tblName)
	}

	return &prevHash, tableWriter, nil
}
