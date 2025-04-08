// Copyright 2019 Dolthub, Inc.
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

package writer

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// AutoIncrementGetter is implemented by editors that support AUTO_INCREMENT to return the next value that will be
// inserted.
type AutoIncrementGetter interface {
	GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error)
}

// nomsTableWriter is a wrapper for *doltdb.SessionedTableEditor that complies with the SQL interface.
//
// The nomsTableWriter has two levels of batching: one supported at the SQL engine layer where a single UPDATE, DELETE or
// INSERT statement will touch many rows, and we want to avoid unnecessary intermediate writes; and one at the dolt
// layer as a "batch mode" in DoltDatabase. In the latter mode, it's possible to mix inserts, updates and deletes in any
// order. In general, this is unsafe and will produce incorrect results in many cases. The editor makes reasonable
// attempts to produce correct results when interleaving insert and delete statements, but this is almost entirely to
// support REPLACE statements, which are implemented as a DELETE followed by an INSERT. In general, not flushing the
// editor after every SQL statement is incorrect and will return incorrect results. The single reliable exception is an
// unbroken chain of INSERT statements, where we have taken pains to batch writes to speed things up.
type nomsTableWriter struct {
	tableName   string
	dbName      string
	sch         schema.Schema
	sqlSch      sql.Schema
	vrw         types.ValueReadWriter
	kvToSQLRow  *index.KVToSqlRowConverter
	tableEditor editor.TableEditor
	flusher     dsess.WriteSessionFlusher

	autoInc                globalstate.AutoIncrementTracker
	nextAutoIncrementValue map[string]uint64

	setter         dsess.SessionRootSetter
	errEncountered error
}

func (te *nomsTableWriter) PreciseMatch() bool {
	return true
}

var _ dsess.TableWriter = &nomsTableWriter{}
var _ AutoIncrementGetter = &nomsTableWriter{}

func (te *nomsTableWriter) duplicateKeyErrFunc(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	oldRow, err := te.kvToSQLRow.ConvertKVTuplesToSqlRow(k, v)
	if err != nil {
		return err
	}

	return sql.NewUniqueKeyErr(keyString, isPk, oldRow)
}

func (te *nomsTableWriter) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if schema.IsKeyless(te.sch) {
		return te.keylessInsert(ctx, sqlRow)
	}
	return te.keyedInsert(ctx, sqlRow)
}

func (te *nomsTableWriter) keylessInsert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
	if err != nil {
		return err
	}
	return te.tableEditor.InsertRow(ctx, dRow, te.duplicateKeyErrFunc)
}

func (te *nomsTableWriter) keyedInsert(ctx *sql.Context, sqlRow sql.Row) error {
	k, v, tagToVal, err := sqlutil.DoltKeyValueAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
	if err != nil {
		return err
	}
	return te.tableEditor.InsertKeyVal(ctx, k, v, tagToVal, te.duplicateKeyErrFunc)
}

func (te *nomsTableWriter) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(te.sch) {
		k, tagToVal, err := sqlutil.DoltKeyAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		return te.tableEditor.DeleteByKey(ctx, k, tagToVal)
	} else {
		dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		return te.tableEditor.DeleteRow(ctx, dRow)
	}
}

func (te *nomsTableWriter) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, oldRow, te.sch)
	if err != nil {
		return err
	}
	dNewRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, newRow, te.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, te.duplicateKeyErrFunc)
}

func (te *nomsTableWriter) GetNextAutoIncrementValue(ctx *sql.Context, insertVal interface{}) (uint64, error) {
	return te.autoInc.Next(ctx, te.tableName, insertVal)
}

func (te *nomsTableWriter) SetAutoIncrementValue(ctx *sql.Context, val uint64) error {
	seq, err := te.autoInc.CoerceAutoIncrementValue(ctx, val)
	if err != nil {
		return err
	}

	te.nextAutoIncrementValue = make(map[string]uint64)
	te.nextAutoIncrementValue[te.tableName] = seq

	return te.flush(ctx)
}

func (te *nomsTableWriter) AcquireAutoIncrementLock(ctx *sql.Context) (func(), error) {
	return te.autoInc.AcquireTableLock(ctx, te.tableName)
}

func (te *nomsTableWriter) IndexedAccess(_ *sql.Context, i sql.IndexLookup) sql.IndexedTable {
	idx := index.DoltIndexFromSqlIndex(i.Index)
	return &nomsFkIndexer{
		writer:  te,
		idxName: idx.ID(),
		idxSch:  idx.IndexSchema(),
	}
}

func (te *nomsTableWriter) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	indexes := ctx.GetIndexRegistry().IndexesByTable(te.dbName, te.tableName)
	ret := make([]sql.Index, len(indexes))
	for i := range indexes {
		ret[i] = indexes[i]
	}
	return ret, nil
}

// Close implements Closer
func (te *nomsTableWriter) Close(ctx *sql.Context) error {
	if te.errEncountered == nil {
		return te.flush(ctx)
	}
	return nil
}

// StatementBegin implements the interface sql.TableEditor.
func (te *nomsTableWriter) StatementBegin(ctx *sql.Context) {
	// Table writers are reused in a session, which means we need to reset the error state resulting from previous
	// errors on every new statement.
	te.errEncountered = nil
	te.tableEditor.StatementStarted(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (te *nomsTableWriter) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	if _, ignored := errorEncountered.(sql.IgnorableError); !ignored {
		te.errEncountered = errorEncountered
	}
	return te.tableEditor.StatementFinished(ctx, true)
}

// StatementComplete implements the interface sql.TableEditor.
func (te *nomsTableWriter) StatementComplete(ctx *sql.Context) error {
	return te.tableEditor.StatementFinished(ctx, false)
}

func (te *nomsTableWriter) flush(ctx *sql.Context) error {
	ws, err := te.flusher.Flush(ctx)
	if err != nil {
		return err
	}
	return te.setter(ctx, te.dbName, ws.WorkingRoot())
}
