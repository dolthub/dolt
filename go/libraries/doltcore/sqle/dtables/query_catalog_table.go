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

var _ sql.Table = (*QueryCatalogTable)(nil)
var _ sql.UpdatableTable = (*QueryCatalogTable)(nil)
var _ sql.DeletableTable = (*QueryCatalogTable)(nil)
var _ sql.InsertableTable = (*QueryCatalogTable)(nil)
var _ sql.ReplaceableTable = (*QueryCatalogTable)(nil)
var _ VersionableTable = (*QueryCatalogTable)(nil)
var _ sql.IndexAddressableTable = (*QueryCatalogTable)(nil)

// QueryCatalogTable is the system table that stores saved queries.
type QueryCatalogTable struct {
	backingTable VersionableTable
}

func (i *QueryCatalogTable) Name() string {
	return doltdb.DoltQueryCatalogTableName
}

func (i *QueryCatalogTable) String() string {
	return doltdb.DoltQueryCatalogTableName
}

func doltQueryCatalogSchema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.QueryCatalogIdCol, Type: sqlTypes.LongText, Source: doltdb.GetQueryCatalogTableName(), PrimaryKey: true},
		{Name: doltdb.QueryCatalogOrderCol, Type: sqlTypes.Int32, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogNameCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogQueryCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName(), Nullable: false},
		{Name: doltdb.QueryCatalogDescriptionCol, Type: sqlTypes.Text, Source: doltdb.GetQueryCatalogTableName()},
	}
}

var GetDoltQueryCatalogSchema = doltQueryCatalogSchema

func (i *QueryCatalogTable) Schema() sql.Schema {
	return GetDoltQueryCatalogSchema()
}

func (i *QueryCatalogTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (i *QueryCatalogTable) Partitions(context *sql.Context) (sql.PartitionIter, error) {
	if i.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return i.backingTable.Partitions(context)
}

func (i *QueryCatalogTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if i.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}
	return i.backingTable.PartitionRows(context, partition)
}

// NewQueryCatalogTable creates a QueryCatalogTable
func NewQueryCatalogTable(_ *sql.Context, backingTable VersionableTable) sql.Table {
	return &QueryCatalogTable{backingTable: backingTable}
}

// NewEmptyQueryCatalogTable creates an QueryCatalogTable
func NewEmptyQueryCatalogTable(_ *sql.Context) sql.Table {
	return &QueryCatalogTable{}
}

func (qt *QueryCatalogTable) Replacer(_ *sql.Context) sql.RowReplacer {
	return newQueryCatalogWriter(qt)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (qt *QueryCatalogTable) Updater(_ *sql.Context) sql.RowUpdater {
	return newQueryCatalogWriter(qt)
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (qt *QueryCatalogTable) Inserter(*sql.Context) sql.RowInserter {
	return newQueryCatalogWriter(qt)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (qt *QueryCatalogTable) Deleter(*sql.Context) sql.RowDeleter {
	return newQueryCatalogWriter(qt)
}

func (qt *QueryCatalogTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if qt.backingTable == nil {
		return qt, nil
	}
	return qt.backingTable.LockedToRoot(ctx, root)
}

// IndexedAccess implements IndexAddressableTable, but IgnoreTables has no indexes.
// Thus, this should never be called.
func (qt *QueryCatalogTable) IndexedAccess(ctx *sql.Context, lookup sql.IndexLookup) sql.IndexedTable {
	panic("Unreachable")
}

// GetIndexes implements IndexAddressableTable, but IgnoreTables has no indexes.
func (qt *QueryCatalogTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (qt *QueryCatalogTable) PreciseMatch() bool {
	return true
}

var _ sql.RowReplacer = (*queryCatalogWriter)(nil)
var _ sql.RowUpdater = (*queryCatalogWriter)(nil)
var _ sql.RowInserter = (*queryCatalogWriter)(nil)
var _ sql.RowDeleter = (*queryCatalogWriter)(nil)

type queryCatalogWriter struct {
	qt                      *QueryCatalogTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             dsess.TableWriter
}

func newQueryCatalogWriter(qt *QueryCatalogTable) *queryCatalogWriter {
	return &queryCatalogWriter{qt, nil, nil, nil}
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (qw *queryCatalogWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := qw.errDuringStatementBegin; err != nil {
		return err
	}
	return qw.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (qw *queryCatalogWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := qw.errDuringStatementBegin; err != nil {
		return err
	}
	return qw.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (qw *queryCatalogWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := qw.errDuringStatementBegin; err != nil {
		return err
	}
	return qw.tableWriter.Delete(ctx, r)
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (qw *queryCatalogWriter) StatementBegin(ctx *sql.Context) {
	name := getDoltQueryCatalogTableName()
	prevHash, tableWriter, err := createWriteableSystemTable(ctx, name, qw.qt.Schema())
	if err != nil {
		qw.errDuringStatementBegin = err
		return
	}
	qw.prevHash = prevHash
	qw.tableWriter = tableWriter
}

func getDoltQueryCatalogTableName() doltdb.TableName {
	if resolve.UseSearchPath {
		return doltdb.TableName{Schema: doltdb.DoltNamespace, Name: doltdb.GetQueryCatalogTableName()}
	}
	return doltdb.TableName{Name: doltdb.GetQueryCatalogTableName()}
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
// should be discarded.
func (qw *queryCatalogWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if qw.tableWriter != nil {
		return qw.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (qw *queryCatalogWriter) StatementComplete(ctx *sql.Context) error {
	if qw.tableWriter != nil {
		return qw.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Close finalizes the delete operation, persisting the result.
func (qw *queryCatalogWriter) Close(ctx *sql.Context) error {
	if qw.tableWriter != nil {
		return qw.tableWriter.Close(ctx)
	}
	return nil
}
