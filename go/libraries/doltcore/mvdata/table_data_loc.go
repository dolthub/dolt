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

package mvdata

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// TableDataLocationUpdateRate is the number of writes that will process before the updated stats are displayed.
const TableDataLocationUpdateRate = 32768

// ErrNoPK is an error returned if a schema is missing a required primary key
var ErrNoPK = errors.New("schema does not contain a primary key")

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
func (dl TableDataLocation) Exists(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	return root.HasTable(ctx, dl.Name)
}

// NewReader creates a TableReadCloser for the DataLocation
func (dl TableDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
	tbl, ok, err := root.GetTable(ctx, dl.Name)

	if err != nil {
		return nil, false, err
	}

	if !ok {
		return nil, false, doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, false, err
	}

	rowData, err := tbl.GetRowData(ctx)

	if err != nil {
		return nil, false, err
	}

	rd, err := noms.NewNomsMapReader(ctx, rowData, sch)

	if err != nil {
		return nil, false, err
	}

	return rd, true, nil
}

// NewCreatingWriter will create a TableWriteCloser for a DataLocation that will create a new table, or overwrite
// an existing table.
func (dl TableDataLocation) NewCreatingWriter(ctx context.Context, mvOpts DataMoverOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	if outSch.GetPKCols().Size() == 0 {
		return nil, ErrNoPK
	}

	m, err := types.NewMap(ctx, root.VRW())
	if err != nil {
		return nil, err
	}
	tblSchVal, err := encoding.MarshalSchemaAsNomsValue(ctx, root.VRW(), outSch)
	if err != nil {
		return nil, err
	}

	tbl, err := doltdb.NewTable(ctx, root.VRW(), tblSchVal, m, nil)
	if err != nil {
		return nil, err
	}
	updatedRoot, err := root.PutTable(ctx, dl.Name, tbl)
	if err != nil {
		return nil, err
	}

	tableEditor, err := doltdb.CreateTableEditSession(updatedRoot, doltdb.TableEditSessionProps{}).GetTableEditor(ctx, dl.Name, outSch)
	if err != nil {
		return nil, err
	}

	return &tableEditorWriteCloser{
		insertOnly:  true,
		initialData: m,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		tableSch:    outSch,
	}, nil
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (dl TableDataLocation) NewUpdatingWriter(ctx context.Context, mvOpts DataMoverOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
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

	tableEditor, err := doltdb.CreateTableEditSession(root, doltdb.TableEditSessionProps{}).GetTableEditor(ctx, dl.Name, tblSch)
	if err != nil {
		return nil, err
	}

	return &tableEditorWriteCloser{
		insertOnly:  false,
		initialData: m,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		tableSch:    tblSch,
	}, nil
}

// NewReplacingWriter will create a TableWriteCloser for a DataLocation that will overwrite an existing table while
// preserving schema
func (dl TableDataLocation) NewReplacingWriter(ctx context.Context, mvOpts DataMoverOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	tbl, ok, err := root.GetTable(ctx, dl.Name)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("Could not find table " + dl.Name)
	}

	m, err := types.NewMap(ctx, root.VRW())
	if err != nil {
		return nil, err
	}
	tblSch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}
	tblSchVal, err := encoding.MarshalSchemaAsNomsValue(ctx, root.VRW(), tblSch)
	if err != nil {
		return nil, err
	}

	tbl, err = doltdb.NewTable(ctx, root.VRW(), tblSchVal, m, nil)
	if err != nil {
		return nil, err
	}

	updatedRoot, err := root.PutTable(ctx, dl.Name, tbl)
	if err != nil {
		return nil, err
	}

	tableEditor, err := doltdb.CreateTableEditSession(updatedRoot, doltdb.TableEditSessionProps{}).GetTableEditor(ctx, dl.Name, tblSch)
	if err != nil {
		return nil, err
	}

	return &tableEditorWriteCloser{
		insertOnly:  true,
		initialData: m,
		statsCB:     statsCB,
		tableEditor: tableEditor,
		tableSch:    tblSch,
	}, nil
}

type tableEditorWriteCloser struct {
	stats       types.AppliedEditStats
	insertOnly  bool
	initialData types.Map
	opsSoFar    int64
	statsCB     noms.StatsCB
	tableEditor *doltdb.SessionedTableEditor
	tableSch    schema.Schema
}

var _ DataMoverCloser = (*tableEditorWriteCloser)(nil)

func (te *tableEditorWriteCloser) GetRoot(ctx context.Context) (*doltdb.RootValue, error) {
	return te.tableEditor.GetRoot(ctx)
}

// GetSchema implements TableWriteCloser
func (te *tableEditorWriteCloser) GetSchema() schema.Schema {
	return te.tableSch
}

// WriteRow implements TableWriteCloser
func (te *tableEditorWriteCloser) WriteRow(ctx context.Context, r row.Row) error {
	if te.statsCB != nil && atomic.LoadInt64(&te.opsSoFar) >= TableDataLocationUpdateRate {
		atomic.StoreInt64(&te.opsSoFar, 0)
		te.statsCB(te.stats)
	}
	if te.insertOnly {
		_ = atomic.AddInt64(&te.opsSoFar, 1)
		te.stats.Additions++
		return te.tableEditor.InsertRow(ctx, r)
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
			_ = atomic.AddInt64(&te.opsSoFar, 1)
			te.stats.Additions++
			return te.tableEditor.InsertRow(ctx, r)
		}
		oldRow, err := row.FromNoms(te.tableSch, pkTuple.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return err
		}
		if row.AreEqual(r, oldRow, te.tableSch) {
			te.stats.SameVal++
			return nil
		}
		_ = atomic.AddInt64(&te.opsSoFar, 1)
		te.stats.Modifications++
		return te.tableEditor.UpdateRow(ctx, oldRow, r)
	}
}

// Close implements TableWriteCloser
func (te *tableEditorWriteCloser) Close(ctx context.Context) error {
	_, err := te.tableEditor.GetRoot(ctx)
	if te.statsCB != nil {
		te.statsCB(te.stats)
	}
	return err
}
