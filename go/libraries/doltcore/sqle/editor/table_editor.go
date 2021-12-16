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

package editor

import (
	"context"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor/creation"
	"github.com/dolthub/dolt/go/store/types"
)

type TableEditor interface {
	sql.RowReplacer
	sql.RowUpdater
	sql.RowInserter
	sql.RowDeleter
	sql.AutoIncrementSetter

	NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error)
}

// sqlTableEditor is a wrapper for *doltdb.SessionedTableEditor that complies with the SQL interface.
//
// The sqlTableEditor has two levels of batching: one supported at the SQL engine layer where a single UPDATE, DELETE or
// INSERT statement will touch many rows, and we want to avoid unnecessary intermediate writes; and one at the dolt
// layer as a "batch mode" in DoltDatabase. In the latter mode, it's possible to mix inserts, updates and deletes in any
// order. In general, this is unsafe and will produce incorrect results in many cases. The editor makes reasonable
// attempts to produce correct results when interleaving insert and delete statements, but this is almost entirely to
// support REPLACE statements, which are implemented as a DELETE followed by an INSERT. In general, not flushing the
// editor after every SQL statement is incorrect and will return incorrect results. The single reliable exception is an
// unbroken chain of INSERT statements, where we have taken pains to batch writes to speed things up.
type sqlTableEditor struct {
	tableName         string
	dbName            string
	sch               schema.Schema
	autoIncCol        schema.Column
	vrw               types.ValueReadWriter
	autoIncrementType typeinfo.TypeInfo
	kvToSQLRow        *index.KVToSqlRowConverter
	tableEditor       editor.TableEditor
	sess              *editor.TableEditSession
	temporary         bool
	aiTracker         globalstate.AutoIncrementTracker
}

var _ TableEditor = &sqlTableEditor{}

func NewSqlTableEditor(
	ctx *sql.Context,
	sess *dsess.DoltSession,
	t sql.Table,
	db sql.Database,
	gs globalstate.GlobalState,
	sch schema.Schema,
	vrw types.ValueReadWriter,
) (TableEditor, error) {
	state, ok, err := sess.LookupDbState(ctx, db.Name())
	if !ok {
		err = sql.ErrDatabaseNotFound.New(db.Name())
	}
	if err != nil {
		return nil, err
	}

	var temp bool
	if tt, ok := t.(sql.TemporaryTable); ok {
		temp = tt.IsTemporary()
	}

	editSess := state.EditSession
	if temp {
		editSess = state.TempTableEditSession
	}

	tableEditor, err := editSess.GetTableEditor(ctx, t.Name(), sch)
	if err != nil {
		return nil, err
	}

	ws := state.WorkingSet
	ait := gs.GetAutoIncrementTracker(ws.Ref())
	conv := index.NewKVToSqlRowConverterForCols(vrw.Format(), sch)
	ac := autoIncrementColFromSchema(sch)

	return &sqlTableEditor{
		tableName:   t.Name(),
		dbName:      db.Name(),
		sch:         sch,
		autoIncCol:  ac,
		vrw:         vrw,
		kvToSQLRow:  conv,
		tableEditor: tableEditor,
		sess:        editSess,
		temporary:   temp,
		aiTracker:   ait,
	}, nil
}

func (te *sqlTableEditor) duplicateKeyErrFunc(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	oldRow, err := te.kvToSQLRow.ConvertKVTuplesToSqlRow(k, v)
	if err != nil {
		return err
	}

	return sql.NewUniqueKeyErr(keyString, isPk, oldRow)
}

func (te *sqlTableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(te.sch) {
		k, v, tagToVal, err := sqlutil.DoltKeyValueAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		err = te.tableEditor.InsertKeyVal(ctx, k, v, tagToVal, te.duplicateKeyErrFunc)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.InsertKeyVal(ctx, k, v, tagToVal, te.duplicateKeyErrFunc)
		}
		return err
	}
	dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
	if err != nil {
		return err
	}
	err = te.tableEditor.InsertRow(ctx, dRow, te.duplicateKeyErrFunc)
	if sql.ErrForeignKeyNotResolved.Is(err) {
		if err = te.resolveFks(ctx); err != nil {
			return err
		}
		return te.tableEditor.InsertRow(ctx, dRow, te.duplicateKeyErrFunc)
	}
	return err
}

func (te *sqlTableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	if !schema.IsKeyless(te.sch) {
		k, tagToVal, err := sqlutil.DoltKeyAndMappingFromSqlRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}

		err = te.tableEditor.DeleteByKey(ctx, k, tagToVal)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.DeleteByKey(ctx, k, tagToVal)
		}
		return err
	} else {
		dRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, sqlRow, te.sch)
		if err != nil {
			return err
		}
		err = te.tableEditor.DeleteRow(ctx, dRow)
		if sql.ErrForeignKeyNotResolved.Is(err) {
			if err = te.resolveFks(ctx); err != nil {
				return err
			}
			return te.tableEditor.DeleteRow(ctx, dRow)
		}
		return err
	}
}

func (te *sqlTableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, oldRow, te.sch)
	if err != nil {
		return err
	}
	dNewRow, err := sqlutil.SqlRowToDoltRow(ctx, te.vrw, newRow, te.sch)
	if err != nil {
		return err
	}

	err = te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, te.duplicateKeyErrFunc)
	if sql.ErrForeignKeyNotResolved.Is(err) {
		if err = te.resolveFks(ctx); err != nil {
			return err
		}
		return te.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, te.duplicateKeyErrFunc)
	}
	return err
}

func (te *sqlTableEditor) NextAutoIncrementValue(potentialVal, tableVal interface{}) (interface{}, error) {
	return te.aiTracker.Next(te.tableName, potentialVal, tableVal)
}

func (te *sqlTableEditor) GetAutoIncrementValue() (interface{}, error) {
	val := te.tableEditor.GetAutoIncrementValue()
	return te.autoIncCol.TypeInfo.ConvertNomsValueToValue(val)
}

func (te *sqlTableEditor) SetAutoIncrementValue(ctx *sql.Context, val interface{}) error {
	nomsVal, err := te.autoIncCol.TypeInfo.ConvertValueToNomsValue(ctx, te.vrw, val)
	if err != nil {
		return err
	}
	if err = te.tableEditor.SetAutoIncrementValue(nomsVal); err != nil {
		return err
	}

	te.aiTracker.Reset(te.tableName, val)

	return te.flush(ctx)
}

// Close implements Closer
func (te *sqlTableEditor) Close(ctx *sql.Context) error {
	sess := dsess.DSessFromSess(ctx.Session)

	// If we're running in batched mode, don't flush the edits until explicitly told to do so
	if sess.BatchMode() == dsess.Batched {
		return nil
	}
	return te.flush(ctx)
}

// StatementBegin implements the interface sql.TableEditor.
func (te *sqlTableEditor) StatementBegin(ctx *sql.Context) {
	te.tableEditor.StatementStarted(ctx)
}

// DiscardChanges implements the interface sql.TableEditor.
func (te *sqlTableEditor) DiscardChanges(ctx *sql.Context, errorEncountered error) error {
	return te.tableEditor.StatementFinished(ctx, true)
}

// StatementComplete implements the interface sql.TableEditor.
func (te *sqlTableEditor) StatementComplete(ctx *sql.Context) error {
	return te.tableEditor.StatementFinished(ctx, false)
}

func (te *sqlTableEditor) flush(ctx *sql.Context) error {
	newRoot, err := te.sess.Flush(ctx)
	if err != nil {
		return err
	}

	return te.setRoot(ctx, newRoot)
}

func (te *sqlTableEditor) setRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	sess := dsess.DSessFromSess(ctx.Session)

	if te.temporary {
		return sess.SetTempTableRoot(ctx, te.dbName, newRoot)
	}

	return sess.SetRoot(ctx, te.dbName, newRoot)
}

func (te *sqlTableEditor) resolveFks(ctx *sql.Context) error {
	tbl, err := te.tableEditor.Table(ctx)
	if err != nil {
		return err
	}
	return te.sess.UpdateRoot(ctx, func(ctx context.Context, root *doltdb.RootValue) (*doltdb.RootValue, error) {
		fkc, err := root.GetForeignKeyCollection(ctx)
		if err != nil {
			return nil, err
		}
		for _, foreignKey := range fkc.UnresolvedForeignKeys() {
			newRoot, _, err := creation.ResolveForeignKey(ctx, root, tbl, foreignKey, te.sess.Opts)
			if err == nil {
				root = newRoot
			}
		}
		return root, nil
	})
}

func autoIncrementColFromSchema(sch schema.Schema) (autoCol schema.Column) {
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if col.AutoIncrement {
			autoCol = col
			stop = true
		}
		return
	})
	return
}
