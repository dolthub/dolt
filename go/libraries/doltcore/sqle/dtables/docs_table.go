// Copyright 2021 Dolthub, Inc.
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
	"github.com/dolthub/vitess/go/sqltypes"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/store/hash"
)

var DoltDocsSqlSchema sql.PrimaryKeySchema
var OldDoltDocsSqlSchema sql.PrimaryKeySchema

func init() {
	DoltDocsSqlSchema, _ = sqlutil.FromDoltSchema("", doltdb.DocTableName, doltdb.DocsSchema)
	OldDoltDocsSqlSchema, _ = sqlutil.FromDoltSchema("", doltdb.DocTableName, doltdb.OldDocsSchema)
}

var _ sql.Table = (*DocsTable)(nil)
var _ sql.UpdatableTable = (*DocsTable)(nil)
var _ sql.DeletableTable = (*DocsTable)(nil)
var _ sql.InsertableTable = (*DocsTable)(nil)
var _ sql.ReplaceableTable = (*DocsTable)(nil)
var _ sql.IndexAddressableTable = (*DocsTable)(nil)

// DocsTable is the system table that stores Dolt docs, such as LICENSE and README.
type DocsTable struct {
	backingTable VersionableTable
}

func (dt *DocsTable) Name() string {
	return doltdb.DocTableName
}

func (dt *DocsTable) String() string {
	return doltdb.DocTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_docs system table.
func (dt *DocsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.DocPkColumnName, Type: sqlTypes.MustCreateString(sqltypes.VarChar, 16383, sql.Collation_Default), Source: doltdb.DocTableName, PrimaryKey: true, Nullable: false},
		{Name: doltdb.DocTextColumnName, Type: sqlTypes.LongText, Source: doltdb.DocTableName, PrimaryKey: false},
	}
}

func (dt *DocsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

// Partitions is a sql.Table interface function that returns a partition of the data.
func (dt *DocsTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	if dt.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return dt.backingTable.Partitions(context)
}

func (dt *DocsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if dt.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}

	return dt.backingTable.PartitionRows(context, partition)
}

// NewDocsTable creates a DocsTable
func NewDocsTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &DocsTable{backingTable: backingTable}
}

// NewEmptyDocsTable creates a DocsTable
func NewEmptyDocsTable(_ *sql.Context) sql.Table {
	return &DocsTable{}
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (dt *DocsTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newDocsWriter(dt)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (dt *DocsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return newDocsWriter(dt)
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (dt *DocsTable) Inserter(*sql.Context) sql.RowInserter {
	return newDocsWriter(dt)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (dt *DocsTable) Deleter(*sql.Context) sql.RowDeleter {
	return newDocsWriter(dt)
}

func (dt *DocsTable) LockedToRoot(ctx *sql.Context, root *doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if dt.backingTable == nil {
		return dt, nil
	}
	return dt.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but DocsTables has no indexes.
// Thus, this should never be called.
func (dt *DocsTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	panic("Unreachable")
}

// GetIndexes implements IndexAddressableTable, but DocsTables has no indexes.
func (dt *DocsTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (dt *DocsTable) PreciseMatch() bool {
	return true
}

var _ sql.RowReplacer = (*docsWriter)(nil)
var _ sql.RowUpdater = (*docsWriter)(nil)
var _ sql.RowInserter = (*docsWriter)(nil)
var _ sql.RowDeleter = (*docsWriter)(nil)

type docsWriter struct {
	it                      *DocsTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             writer.TableWriter
}

func newDocsWriter(it *DocsTable) *docsWriter {
	return &docsWriter{it, nil, nil, nil}
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (iw *docsWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (iw *docsWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (iw *docsWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := iw.errDuringStatementBegin; err != nil {
		return err
	}
	return iw.tableWriter.Delete(ctx, r)
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (iw *docsWriter) StatementBegin(ctx *sql.Context) {
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

	found, err := roots.Working.HasTable(ctx, doltdb.DocTableName)

	if err != nil {
		iw.errDuringStatementBegin = err
		return
	}

	if !found {
		// TODO: This is effectively a duplicate of the schema declaration above in a different format.
		// We should find a way to not repeat ourselves.
		newSchema := doltdb.DocsSchema

		// underlying table doesn't exist. Record this, then create the table.
		newRootValue, err := roots.Working.CreateEmptyTable(ctx, doltdb.DocTableName, newSchema)
		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}

		if dbState.WorkingSet() == nil {
			iw.errDuringStatementBegin = doltdb.ErrOperationNotSupportedInDetachedHead
			return
		}

		// We use WriteSession.SetWorkingSet instead of DoltSession.SetRoot because we want to avoid modifying the root
		// until the end of the transaction, but we still want the WriteSession to be able to find the newly
		// created table.
		err = dbState.WriteSession().SetWorkingSet(ctx, dbState.WorkingSet().WithWorkingRoot(newRootValue))
		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}

		err = dSess.SetRoot(ctx, dbName, newRootValue)
		if err != nil {
			iw.errDuringStatementBegin = err
			return
		}
	}

	tableWriter, err := dbState.WriteSession().GetTableWriter(ctx, doltdb.DocTableName, dbName, dSess.SetRoot)
	if err != nil {
		iw.errDuringStatementBegin = err
		return
	}

	iw.tableWriter = tableWriter

	tableWriter.StatementBegin(ctx)
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
// should be discarded.
func (iw *docsWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (iw *docsWriter) StatementComplete(ctx *sql.Context) error {
	return iw.tableWriter.StatementComplete(ctx)
}

// Close finalizes the delete operation, persisting the result.
func (iw docsWriter) Close(ctx *sql.Context) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.Close(ctx)
	}
	return nil
}
