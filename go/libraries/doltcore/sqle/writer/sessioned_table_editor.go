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

package writer

import (
	"context"
	"fmt"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// sessionedTableEditor represents a table editor obtained from a nomsWriteSession. This table editor may be shared
// by multiple callers. It is thread safe.
type sessionedTableEditor struct {
	tableEditSession *nomsWriteSession
	tableEditor      editor.TableEditor
	indexSchemaCache map[string]schema.Schema
	dirty            bool
}

var _ editor.TableEditor = &sessionedTableEditor{}

func (ste *sessionedTableEditor) GetIndexedRows(ctx context.Context, key types.Tuple, indexName string, idxSch schema.Schema) ([]row.Row, error) {
	return ste.tableEditor.GetIndexedRows(ctx, key, indexName, idxSch)
}

func (ste *sessionedTableEditor) InsertKeyVal(ctx context.Context, key, val types.Tuple, tagToVal map[uint64]types.Value, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.mut.RLock()
	defer ste.tableEditSession.mut.RUnlock()

	ste.dirty = true
	return ste.tableEditor.InsertKeyVal(ctx, key, val, tagToVal, errFunc)
}

func (ste *sessionedTableEditor) DeleteByKey(ctx context.Context, key types.Tuple, tagToVal map[uint64]types.Value) error {
	ste.tableEditSession.mut.RLock()
	defer ste.tableEditSession.mut.RUnlock()

	ste.dirty = true
	return ste.tableEditor.DeleteByKey(ctx, key, tagToVal)
}

// InsertRow adds the given row to the table. If the row already exists, use UpdateRow.
func (ste *sessionedTableEditor) InsertRow(ctx context.Context, dRow row.Row, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.mut.RLock()
	defer ste.tableEditSession.mut.RUnlock()

	ste.dirty = true
	return ste.tableEditor.InsertRow(ctx, dRow, errFunc)
}

// DeleteRow removes the given key from the table.
func (ste *sessionedTableEditor) DeleteRow(ctx context.Context, r row.Row) error {
	ste.tableEditSession.mut.RLock()
	defer ste.tableEditSession.mut.RUnlock()

	ste.dirty = true
	return ste.tableEditor.DeleteRow(ctx, r)
}

// UpdateRow takes the current row and new row, and updates it accordingly. Any applicable rows from tables that have a
// foreign key referencing this table will also be updated.
func (ste *sessionedTableEditor) UpdateRow(ctx context.Context, dOldRow row.Row, dNewRow row.Row, errFunc editor.PKDuplicateErrFunc) error {
	ste.tableEditSession.mut.RLock()
	defer ste.tableEditSession.mut.RUnlock()

	ste.dirty = true
	return ste.tableEditor.UpdateRow(ctx, dOldRow, dNewRow, errFunc)
}

// HasEdits returns whether the table editor has had any write operations, whether they were successful or unsuccessful
// (on the underlying table editor). This makes it possible for this to return true when the table editor does not
// actually contain any new edits, which is preferable to potentially returning false when there are edits.
func (ste *sessionedTableEditor) HasEdits() bool {
	if ste.dirty {
		return true
	}
	return ste.tableEditor.HasEdits()
}

// MarkDirty implements TableEditor.
func (ste *sessionedTableEditor) MarkDirty() {
	ste.tableEditor.MarkDirty()
}

// Table implements TableEditor.
func (ste *sessionedTableEditor) Table(ctx context.Context) (*doltdb.Table, error) {
	ws, err := ste.tableEditSession.Flush(ctx)
	if err != nil {
		return nil, err
	}
	root := ws.WorkingRoot()

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

// Schema implements TableEditor.
func (ste *sessionedTableEditor) Schema() schema.Schema {
	return ste.tableEditor.Schema()
}

// Name implements TableEditor.
func (ste *sessionedTableEditor) Name() string {
	return ste.tableEditor.Name()
}

// Format implements TableEditor.
func (ste *sessionedTableEditor) Format() *types.NomsBinFormat {
	return ste.tableEditor.Format()
}

// ValueReadWriter implements TableEditor.
func (ste *sessionedTableEditor) ValueReadWriter() types.ValueReadWriter {
	return ste.tableEditor.ValueReadWriter()
}

// StatementStarted implements TableEditor.
func (ste *sessionedTableEditor) StatementStarted(ctx context.Context) {
	ste.tableEditor.StatementStarted(ctx)
}

// StatementFinished implements TableEditor.
func (ste *sessionedTableEditor) StatementFinished(ctx context.Context, errored bool) error {
	return ste.tableEditor.StatementFinished(ctx, errored)
}

// SetConstraintViolation implements TableEditor.
func (ste *sessionedTableEditor) SetConstraintViolation(ctx context.Context, k types.LesserValuable, v types.Valuable) error {
	ste.dirty = true
	return ste.tableEditor.SetConstraintViolation(ctx, k, v)
}

// Close implements TableEditor.
func (ste *sessionedTableEditor) Close(ctx context.Context) error {
	//TODO: I don't think this gets called anymore...is this leaking?
	return ste.tableEditor.Close(ctx)
}
