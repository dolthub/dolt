// Copyright 2019 Liquidata, Inc.
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

package sqle

import (
	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

// sqlTableEditor is a wrapper for *doltdb.TableEditor that complies with the SQL interface.
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
	t           *WritableDoltTable
	tableEditor *doltdb.TableEditor
}

var _ sql.RowReplacer = (*sqlTableEditor)(nil)
var _ sql.RowUpdater = (*sqlTableEditor)(nil)
var _ sql.RowInserter = (*sqlTableEditor)(nil)
var _ sql.RowDeleter = (*sqlTableEditor)(nil)

func newSqlTableEditor(ctx *sql.Context, t *WritableDoltTable) (*sqlTableEditor, error) {
	tableEditor, err := doltdb.NewTableEditor(ctx, t.table, t.sch)
	if err != nil {
		return nil, err
	}
	return &sqlTableEditor{
		t:           t,
		tableEditor: tableEditor,
	}, nil
}

func (te *sqlTableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.Insert(ctx, dRow)
}

func (te *sqlTableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.Delete(ctx, dRow)
}

func (te *sqlTableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := SqlRowToDoltRow(te.t.table.Format(), oldRow, te.t.sch)
	if err != nil {
		return err
	}
	dNewRow, err := SqlRowToDoltRow(te.t.table.Format(), newRow, te.t.sch)
	if err != nil {
		return err
	}

	return te.tableEditor.Update(ctx, dOldRow, dNewRow)
}

// Close implements Closer
func (te *sqlTableEditor) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don't flush the edits until explicitly told to do so by the parent table.
	if te.t.db.batchMode == batched {
		return nil
	}
	return te.flush(ctx)
}

func (te *sqlTableEditor) flush(ctx *sql.Context) error {
	newTable, err := te.tableEditor.Flush(ctx)
	if err != nil {
		return err
	}

	root, err := te.t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, te.t.name, newTable)
	if err != nil {
		return errhand.BuildDError("failed to write table back to database").AddCause(err).Build()
	}

	te.t.table = newTable
	return te.t.db.SetRoot(ctx, newRoot)
}
