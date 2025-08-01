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
	"github.com/dolthub/go-mysql-server/sql"
	sqlTypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
	"io"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/store/hash"
)

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

// NewDocsTable creates a DocsTable
func NewDocsTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &DocsTable{backingTable: backingTable}
}

// NewEmptyDocsTable creates a DocsTable
func NewEmptyDocsTable(_ *sql.Context) sql.Table {
	return &DocsTable{}
}

func (dt *DocsTable) Name() string {
	return doltdb.GetDocTableName()
}

func (dt *DocsTable) String() string {
	return doltdb.GetDocTableName()
}

const defaultStringsLen = 16383 / 16

// GetDocsSchema returns the schema of the dolt_docs system table. This is used
// by Doltgres to update the dolt_docs schema using Doltgres types.
var GetDocsSchema = getDoltDocsSchema

func getDoltDocsSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.DocPkColumnName, Type: sqlTypes.MustCreateString(sqltypes.VarChar, defaultStringsLen, sql.Collation_Default), Source: doltdb.GetDocTableName(), PrimaryKey: true, Nullable: false},
		{Name: doltdb.DocTextColumnName, Type: sqlTypes.LongText, Source: doltdb.GetDocTableName(), PrimaryKey: false},
	}
}

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_docs system table.
func (dt *DocsTable) Schema() sql.Schema {
	return GetDocsSchema()
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

func (dt *DocsTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	var rowIter sql.RowIter
	if dt.backingTable == nil {
		// no backing table; empty iter.
		rowIter = sql.RowsToRowIter()
	} else {
		var err error
		rowIter, err = dt.backingTable.PartitionRows(ctx, partition)

		if err != nil && err != io.EOF {
			return nil, err
		}
	}

	rows, err := sql.RowIterToRows(ctx, rowIter)

	if err != nil {
		return nil, err
	}

	found := false
	for i := range rows {
		name, ok := rows[i][0].(string)
		if !ok {
			continue
		}

		if name == doltdb.AgentDoc {
			found = true
			break
		}
	}

	if !found {
		rows = append(rows, []interface{}{
			doltdb.AgentDoc,
			doltdb.DefaultAgentDocValue,
		})
	}

	rowIter = sql.RowsToRowIter(rows...)

	return rowIter, nil
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

func (dt *DocsTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if dt.backingTable == nil {
		return dt, nil
	}
	return dt.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but DocsTables has no indexes.
// Thus, this should never be called.
func (dt *DocsTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
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
	tableWriter             dsess.TableWriter
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

func getDoltDocsTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetDocTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetDocTableName()}
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (iw *docsWriter) StatementBegin(ctx *sql.Context) {
	name := getDoltDocsTableName()
	prevHash, tableWriter, err := createWriteableSystemTable(ctx, name, iw.it.Schema())
	if err != nil {
		iw.errDuringStatementBegin = err
	}
	iw.prevHash = prevHash
	iw.tableWriter = tableWriter
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
	if iw.tableWriter != nil {
		return iw.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (iw docsWriter) Close(ctx *sql.Context) error {
	if iw.tableWriter != nil {
		return iw.tableWriter.Close(ctx)
	}
	return nil
}
