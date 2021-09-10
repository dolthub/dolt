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

package mvdata

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	// tableWriterStatUpdateRate is the number of writes that will process before the updated stats are displayed.
	tableWriterStatUpdateRate = 64 * 1024
)

// ErrNoPK is an error returned if a schema is missing a required primary key
var ErrNoPK = errors.New("schema does not contain a primary key")

var _ DataLocation = TableDataLocation{}

// TableDataLocation is a dolt table that that can be imported from or exported to.
type TableDataLocation struct {
	// Name the name of a table
	Name string
}

// String returns a string representation of the data location.
func (dl TableDataLocation) String() string {
	return DoltDB.ReadableStr() + ":" + dl.Name
}

// Exists returns true if the DataLocation already exists
func (dl TableDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, _ filesys.ReadableFS) (bool, error) {
	return root.HasTable(ctx, dl.Name)
}

// NewReader creates a TableReadCloser for the DataLocation
func (dl TableDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, _ filesys.ReadableFS, _ interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
	tbl, ok, err := root.GetTable(ctx, dl.Name)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, doltdb.ErrTableNotFound
	}

	rd, err := table.NewDoltTableReader(ctx, tbl)
	if err != nil {
		return nil, false, err
	}

	return rd, true, nil
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (dl TableDataLocation) NewCreatingWriter(ctx context.Context, _ DataMoverOptions, dEnv *env.DoltEnv, root *doltdb.RootValue, _ bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	updatedRoot, err := root.CreateEmptyTable(ctx, dl.Name, outSch)
	if err != nil {
		return nil, err
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	sess := editor.CreateTableEditSession(updatedRoot, opts)
	tableEditor, err := sess.GetTableEditor(ctx, dl.Name, outSch)
	if err != nil {
		return nil, err
	}

	return &tableEditorWriteCloser{
		dEnv:        dEnv,
		insertOnly:  true,
		initialData: types.EmptyMap,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		sess:        sess,
		tableSch:    outSch,
	}, nil
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (dl TableDataLocation) NewUpdatingWriter(ctx context.Context, _ DataMoverOptions, dEnv *env.DoltEnv, root *doltdb.RootValue, _ bool, _ schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	tbl, ok, err := root.GetTable(ctx, dl.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Could not find table " + dl.Name)
	}

	m, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	bulkTeaf := editor.NewBulkImportTEAFactory(tbl.Format(), dEnv.DoltDB.ValueReadWriter(), dEnv.TempTableFilesDir())
	sess := editor.CreateTableEditSession(root, editor.Options{Deaf: bulkTeaf})
	tableEditor, err := sess.GetTableEditor(ctx, dl.Name, tblSch)
	if err != nil {
		return nil, err
	}

	// keyless tables are updated as append only
	insertOnly := schema.IsKeyless(tblSch)

	return &tableEditorWriteCloser{
		dEnv:        dEnv,
		insertOnly:  insertOnly,
		initialData: m,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		sess:        sess,
		tableSch:    tblSch,
	}, nil
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table while
// preserving schema
func (dl TableDataLocation) NewReplacingWriter(ctx context.Context, _ DataMoverOptions, dEnv *env.DoltEnv, root *doltdb.RootValue, _ bool, _ schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	tbl, ok, err := root.GetTable(ctx, dl.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Could not find table " + dl.Name)
	}

	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// overwrites existing table
	updatedRoot, err := root.CreateEmptyTable(ctx, dl.Name, tblSch)
	if err != nil {
		return nil, err
	}

	opts := editor.Options{Deaf: dEnv.DbEaFactory()}
	sess := editor.CreateTableEditSession(updatedRoot, opts)
	tableEditor, err := sess.GetTableEditor(ctx, dl.Name, tblSch)
	if err != nil {
		return nil, err
	}

	return &tableEditorWriteCloser{
		dEnv:        dEnv,
		insertOnly:  true,
		initialData: types.EmptyMap,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		sess:        sess,
		tableSch:    tblSch,
	}, nil
}

type tableEditorWriteCloser struct {
	dEnv        *env.DoltEnv
	tableEditor editor.TableEditor
	sess        *editor.TableEditSession
	initialData types.Map
	tableSch    schema.Schema
	insertOnly  bool

	statsCB noms.StatsCB
	stats   types.AppliedEditStats
	statOps int64
}

var _ DataMoverCloser = (*tableEditorWriteCloser)(nil)

func (te *tableEditorWriteCloser) Flush(ctx context.Context) (*doltdb.RootValue, error) {
	return te.sess.Flush(ctx)
}

// GetSchema implements TableWriteCloser
func (te *tableEditorWriteCloser) GetSchema() schema.Schema {
	return te.tableSch
}

// WriteRow implements TableWriteCloser
func (te *tableEditorWriteCloser) WriteRow(ctx context.Context, r row.Row) error {
	if te.statsCB != nil && atomic.LoadInt64(&te.statOps) >= tableWriterStatUpdateRate {
		atomic.StoreInt64(&te.statOps, 0)
		te.statsCB(te.stats)
	}

	if te.insertOnly {
		err := te.tableEditor.InsertRow(ctx, r, nil)

		if err != nil {
			return err
		}

		_ = atomic.AddInt64(&te.statOps, 1)
		te.stats.Additions++
		return nil

	} else {
		pkTuple, err := r.NomsMapKey(te.tableSch).Value(ctx)
		if err != nil {
			return err
		}
		val, ok, err := te.initialData.MaybeGet(ctx, pkTuple)
		if err != nil {
			return err
		}
		if !ok {
			err := te.tableEditor.InsertRow(ctx, r, nil)

			if err != nil {
				return err
			}

			_ = atomic.AddInt64(&te.statOps, 1)
			te.stats.Additions++
			return nil
		}
		oldRow, err := row.FromNoms(te.tableSch, pkTuple.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		if row.AreEqual(r, oldRow, te.tableSch) {
			te.stats.SameVal++
			return nil
		}
		err = te.tableEditor.UpdateRow(ctx, oldRow, r, nil)

		if err != nil {
			return err
		}

		_ = atomic.AddInt64(&te.statOps, 1)
		te.stats.Modifications++
		return nil
	}
}

// Close implements TableWriteCloser
func (te *tableEditorWriteCloser) Close(ctx context.Context) error {
	if te.statsCB != nil {
		te.statsCB(te.stats)
	}
	return nil
}
