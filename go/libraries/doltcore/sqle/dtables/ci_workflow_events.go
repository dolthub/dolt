// Copyright 2024 Dolthub, Inc.
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
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/store/hash"
	stypes "github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/sqltypes"
)

const workflowEventsDefaultRowCount = 10

var _ sql.StatisticsTable = (*WorkflowEventsTable)(nil)
var _ sql.Table = (*WorkflowEventsTable)(nil)

var _ sql.UpdatableTable = (*WorkflowEventsTable)(nil)

var _ sql.DeletableTable = (*WorkflowEventsTable)(nil)
var _ sql.InsertableTable = (*WorkflowEventsTable)(nil)

var _ sql.ReplaceableTable = (*WorkflowEventsTable)(nil)

// WorkflowEventsTable is a sql.Table implementation that implements a system table which stores dolt CI workflow events
type WorkflowEventsTable struct {
	ddb          *doltdb.DoltDB
	backingTable VersionableTable
}

// NewWorkflowEventsTable creates a WorkflowEventsTable
func NewWorkflowEventsTable(_ *sql.Context, ddb *doltdb.DoltDB, backingTable VersionableTable) sql.Table {
	return &WorkflowEventsTable{ddb: ddb, backingTable: backingTable}
}

// NewEmptyWorkflowEventsTable creates a WorkflowEventsTable
func NewEmptyWorkflowEventsTable(_ *sql.Context) sql.Table {
	return &WorkflowEventsTable{}
}

func (w *WorkflowEventsTable) Name() string {
	return doltdb.WorkflowEventsTableName
}

func (w *WorkflowEventsTable) String() string {
	return doltdb.WorkflowEventsTableName
}

// Schema is a sql.Table interface function that gets the sql.Schema of the dolt_ignore system table.
func (w *WorkflowEventsTable) Schema() sql.Schema {
	return []*sql.Column{
		{Name: doltdb.WorkflowEventsIdPkColName, Type: types.MustCreateString(sqltypes.VarChar, 36, sql.Collation_utf8mb4_0900_ai_ci), Source: doltdb.WorkflowEventsIdPkColName, PrimaryKey: true, Nullable: false},
		{Name: doltdb.WorkflowEventsWorkflowNameFkColName, Type: types.MustCreateString(sqltypes.VarChar, 2048, sql.Collation_utf8mb4_0900_ai_ci), Source: doltdb.WorkflowEventsWorkflowNameFkColName, PrimaryKey: false, Nullable: false},
		{Name: doltdb.WorkflowEventsEventTypeColName, Type: types.Int32, Source: doltdb.WorkflowEventsEventTypeColName, PrimaryKey: false, Nullable: false},
	}
}

func (w *WorkflowEventsTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (w *WorkflowEventsTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if w.backingTable == nil {
		// no backing table; return an empty iter.
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return w.backingTable.Partitions(ctx)
}

func (w *WorkflowEventsTable) PartitionRows(context *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if w.backingTable == nil {
		// no backing table; return an empty iter.
		return sql.RowsToRowIter(), nil
	}
	return w.backingTable.PartitionRows(context, partition)
}

func (w *WorkflowEventsTable) DataLength(ctx *sql.Context) (uint64, error) {
	numBytesPerRow := schema.SchemaAvgLength(w.Schema())
	numRows, _, err := w.RowCount(ctx)
	if err != nil {
		return 0, err
	}
	return numBytesPerRow * numRows, nil
}

func (w *WorkflowEventsTable) RowCount(_ *sql.Context) (uint64, bool, error) {
	return workflowEventsDefaultRowCount, false, nil
}

// Inserter returns an Inserter for this table. The Inserter will get one call to Insert() for each row to be
// inserted, and will end with a call to Close() to finalize the insert operation.
func (w *WorkflowEventsTable) Inserter(context *sql.Context) sql.RowInserter {
	return newWorkflowEventsWriter(w)
}

// Updater returns a RowUpdater for this table. The RowUpdater will have Update called once for each row to be
// updated, followed by a call to Close() when all rows have been processed.
func (w *WorkflowEventsTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return newWorkflowEventsWriter(w)
}

// Deleter returns a RowDeleter for this table. The RowDeleter will get one call to Delete for each row to be deleted,
// and will end with a call to Close() to finalize the delete operation.
func (w *WorkflowEventsTable) Deleter(context *sql.Context) sql.RowDeleter {
	return newWorkflowEventsWriter(w)
}

// Replacer returns a RowReplacer for this table. The RowReplacer will have Insert and optionally Delete called once
// for each row, followed by a call to Close() when all rows have been processed.
func (w *WorkflowEventsTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newWorkflowEventsWriter(w)
}

var _ sql.RowReplacer = (*workflowEventsWriter)(nil)
var _ sql.RowUpdater = (*workflowEventsWriter)(nil)
var _ sql.RowInserter = (*workflowEventsWriter)(nil)
var _ sql.RowDeleter = (*workflowEventsWriter)(nil)

type workflowEventsWriter struct {
	it                      *WorkflowEventsTable
	errDuringStatementBegin error
	prevHash                *hash.Hash
	tableWriter             dsess.TableWriter
}

func newWorkflowEventsWriter(it *WorkflowEventsTable) *workflowEventsWriter {
	return &workflowEventsWriter{it, nil, nil, nil}
}

// StatementBegin is called before the first operation of a statement. Integrators should mark the state of the data
// in some way that it may be returned to in the case of an error.
func (w *workflowEventsWriter) StatementBegin(ctx *sql.Context) {
	dbName := ctx.GetCurrentDatabase()
	dSess := dsess.DSessFromSess(ctx.Session)

	// TODO: this needs to use a revision qualified name
	roots, _ := dSess.GetRoots(ctx, dbName)
	dbState, ok, err := dSess.LookupDbState(ctx, dbName)
	if err != nil {
		w.errDuringStatementBegin = err
		return
	}
	if !ok {
		w.errDuringStatementBegin = fmt.Errorf("no root value found in session")
		return
	}

	prevHash, err := roots.Working.HashOf()
	if err != nil {
		w.errDuringStatementBegin = err
		return
	}

	w.prevHash = &prevHash

	found, err := roots.Working.HasTable(ctx, doltdb.TableName{Name: doltdb.WorkflowEventsTableName})

	if err != nil {
		w.errDuringStatementBegin = err
		return
	}

	if !found {
		// TODO: This is effectively a duplicate of the schema declaration above in a different format.
		// We should find a way to not repeat ourselves.
		colCollection := schema.NewColCollection(
			schema.Column{
				Name:          doltdb.WorkflowEventsIdPkColName,
				Tag:           schema.WorkflowEventsIdTag,
				Kind:          stypes.StringKind,
				IsPartOfPK:    true,
				TypeInfo:      typeinfo.FromKind(stypes.StringKind),
				Default:       "",
				AutoIncrement: false,
				Comment:       "",
				Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
			},
			schema.Column{
				Name:          doltdb.WorkflowEventsWorkflowNameFkColName,
				Tag:           schema.WorkflowEventsWorkflowNameFkTag,
				Kind:          stypes.StringKind,
				IsPartOfPK:    false,
				TypeInfo:      typeinfo.FromKind(stypes.StringKind),
				Default:       "",
				AutoIncrement: false,
				Comment:       "",
				Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
			},
			schema.Column{
				Name:          doltdb.WorkflowEventsEventTypeColName,
				Tag:           schema.WorkflowEventsEventTypeTag,
				Kind:          stypes.IntKind,
				IsPartOfPK:    false,
				TypeInfo:      typeinfo.FromKind(stypes.IntKind),
				Default:       "",
				AutoIncrement: false,
				Comment:       "",
				Constraints:   []schema.ColConstraint{schema.NotNullConstraint{}},
			},
		)

		newSchema, err := schema.NewSchema(colCollection, nil, schema.Collation_Default, nil, nil)
		if err != nil {
			w.errDuringStatementBegin = err
			return
		}

		// underlying table doesn't exist. Record this, then create the table.
		newRootValue, err := doltdb.CreateEmptyTable(ctx, roots.Working, doltdb.TableName{Name: doltdb.WorkflowEventsTableName}, newSchema)
		if err != nil {
			w.errDuringStatementBegin = err
			return
		}

		if dbState.WorkingSet() == nil {
			w.errDuringStatementBegin = doltdb.ErrOperationNotSupportedInDetachedHead
			return
		}

		// We use WriteSession.SetWorkingSet instead of DoltSession.SetWorkingRoot because we want to avoid modifying the root
		// until the end of the transaction, but we still want the WriteSession to be able to find the newly
		// created table.

		if ws := dbState.WriteSession(); ws != nil {
			err = ws.SetWorkingSet(ctx, dbState.WorkingSet().WithWorkingRoot(newRootValue))
			if err != nil {
				w.errDuringStatementBegin = err
				return
			}
		}

		err = dSess.SetWorkingRoot(ctx, dbName, newRootValue)
		if err != nil {
			w.errDuringStatementBegin = err
			return
		}
	}

	if ws := dbState.WriteSession(); ws != nil {
		tableWriter, err := ws.GetTableWriter(ctx, doltdb.TableName{Name: doltdb.WorkflowEventsTableName}, dbName, dSess.SetWorkingRoot)
		if err != nil {
			w.errDuringStatementBegin = err
			return
		}
		w.tableWriter = tableWriter
		tableWriter.StatementBegin(ctx)
	}
}

// DiscardChanges is called if a statement encounters an error, and all current changes since the statement beginning
// should be discarded.
func (w *workflowEventsWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if w.tableWriter != nil {
		return w.tableWriter.DiscardChanges(ctx, errorEncountered)
	}
	return nil
}

// StatementComplete is called after the last operation of the statement, indicating that it has successfully completed.
// The mark set in StatementBegin may be removed, and a new one should be created on the next StatementBegin.
func (w *workflowEventsWriter) StatementComplete(ctx *sql.Context) error {
	if w.tableWriter != nil {
		return w.tableWriter.StatementComplete(ctx)
	}
	return nil
}

// Insert inserts the row given, returning an error if it cannot. Insert will be called once for each row to process
// for the insert operation, which may involve many rows. After all rows in an operation have been processed, Close
// is called.
func (w *workflowEventsWriter) Insert(ctx *sql.Context, r sql.Row) error {
	if err := w.errDuringStatementBegin; err != nil {
		return err
	}
	return w.tableWriter.Insert(ctx, r)
}

// Update the given row. Provides both the old and new rows.
func (w *workflowEventsWriter) Update(ctx *sql.Context, old sql.Row, new sql.Row) error {
	if err := w.errDuringStatementBegin; err != nil {
		return err
	}
	return w.tableWriter.Update(ctx, old, new)
}

// Delete deletes the given row. Returns ErrDeleteRowNotFound if the row was not found. Delete will be called once for
// each row to process for the delete operation, which may involve many rows. After all rows have been processed,
// Close is called.
func (w workflowEventsWriter) Delete(ctx *sql.Context, r sql.Row) error {
	if err := w.errDuringStatementBegin; err != nil {
		return err
	}
	return w.tableWriter.Delete(ctx, r)
}

// Close finalizes the delete operation, persisting the result.
func (w *workflowEventsWriter) Close(ctx *sql.Context) error {
	if w.tableWriter != nil {
		return w.tableWriter.Close(ctx)
	}
	return nil
}
