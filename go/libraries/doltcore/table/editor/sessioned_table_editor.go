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

package editor

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/types"
)

// sessionedTableEditor represents a table editor obtained from a TableEditSession. This table editor may be shared
// by multiple callers. It is thread safe.
type sessionedTableEditor struct {
	tableEditor TableEditor
	sess        *TableEditSession

	deps []editDependency
}

var _ TableEditor = &sessionedTableEditor{}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (ste *sessionedTableEditor) InsertRow(ctx context.Context, dRow row.Row) error {
	ste.sess.lock.RLock()
	defer ste.sess.lock.RUnlock()

	// broadcast to dependencies
	for _, dep := range ste.deps {
		if err := dep.insertRow(ctx, dRow); err != nil {
			return err
		}
	}

	return ste.tableEditor.InsertRow(ctx, dRow)
}

// DeleteKey removes the given key from the table.
func (ste *sessionedTableEditor) DeleteRow(ctx context.Context, dRow row.Row) error {
	ste.sess.lock.RLock()
	defer ste.sess.lock.RUnlock()

	// broadcast to dependencies
	for _, dep := range ste.deps {
		if err := dep.deleteRow(ctx, dRow); err != nil {
			return err
		}
	}

	return ste.tableEditor.DeleteRow(ctx, dRow)
}

// UpdateRow takes the current row and new row, and updates it accordingly. Any applicable rows from tables that have a
// foreign key referencing this table will also be updated.
func (ste *sessionedTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row) error {
	ste.sess.lock.RLock()
	defer ste.sess.lock.RUnlock()

	// broadcast to dependencies
	for _, dep := range ste.deps {
		if err := dep.updateRow(ctx, dOldRow, dNewRow); err != nil {
			return err
		}
	}

	return ste.tableEditor.UpdateRow(ctx, dOldRow, dNewRow)
}

func (ste *sessionedTableEditor) GetAutoIncrementValue() types.Value {
	return ste.tableEditor.GetAutoIncrementValue()
}

func (ste *sessionedTableEditor) SetAutoIncrementValue(v types.Value) error {
	ste.sess.lock.RLock()
	defer ste.sess.lock.RUnlock()
	
	return ste.tableEditor.SetAutoIncrementValue(v)
}

func (ste *sessionedTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	root, err := ste.sess.Flush(ctx)
	if err != nil {
		return nil, err
	}

	name := ste.tableEditor.Name()
	tbl, ok, err := root.GetTable(ctx, name)
	if !ok {
		return nil, fmt.Errorf("edit session failed to flush table %s", name)
	}
	if err != nil {
		return nil, err
	}
	return tbl, nil
}

func (ste *sessionedTableEditor) Schema() schema.Schema {
	return ste.tableEditor.Schema()
}

func (ste *sessionedTableEditor) Name() string {
	return ste.tableEditor.Name()
}

func (ste *sessionedTableEditor) Format() *types.NomsBinFormat {
	return ste.tableEditor.Format()
}

func (ste *sessionedTableEditor) Close() error {
	return ste.tableEditor.Close()
}

func (ste *sessionedTableEditor) updateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, checkParent bool) error {
	for _, dep := range ste.deps {
		_, isParent := dep.(fkParentDependency)

		if !isParent || checkParent {
			if err := dep.updateRow(ctx, dOldRow, dNewRow); err != nil {
				return err
			}
		}
	}

	return ste.tableEditor.UpdateRow(ctx, dOldRow, dNewRow)
}
