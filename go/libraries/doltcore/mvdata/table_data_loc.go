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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/types"
)

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
func (dl TableDataLocation) NewReader(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, schPath string, opts interface{}) (rdCl table.TableReadCloser, sorted bool, err error) {
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
func (dl TableDataLocation) NewCreatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, sortedInput bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
	if outSch.GetPKCols().Size() == 0 {
		return nil, ErrNoPK
	}

	if sortedInput {
		return noms.NewNomsMapCreator(ctx, root.VRW(), outSch), nil
	} else {
		m, err := types.NewMap(ctx, root.VRW())

		if err != nil {
			return nil, err
		}

		return noms.NewNomsMapUpdater(ctx, root.VRW(), m, outSch, statsCB), nil
	}
}

// NewUpdatingWriter will create a TableWriteCloser for a DataLocation that will update and append rows based on
// their primary key.
func (dl TableDataLocation) NewUpdatingWriter(ctx context.Context, mvOpts *MoveOptions, root *doltdb.RootValue, fs filesys.WritableFS, srcIsSorted bool, outSch schema.Schema, statsCB noms.StatsCB) (table.TableWriteCloser, error) {
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

	return noms.NewNomsMapUpdater(ctx, root.VRW(), m, outSch, statsCB), nil
}
