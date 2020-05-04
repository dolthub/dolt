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
	"context"
	"fmt"

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrDuplicatePrimaryKeyFmt = "duplicate primary key given: (%v)"

// tableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
//
// The tableEditor has two levels of batching: one supported at the SQL engine layer where a single UPDATE, DELETE or
// INSERT statement will touch many rows, and we want to avoid unnecessary intermediate writes; and one at the dolt
// layer as a "batch mode" in DoltDatabase. In the latter mode, it's possible to mix inserts, updates and deletes in any
// order. In general, this is unsafe and will produce incorrect results in many cases. The editor makes reasonable
// attempts to produce correct results when interleaving insert and delete statements, but this is almost entirely to
// support REPLACE statements, which are implemented as a DELETE followed by an INSERT. In general, not flushing the
// editor after every SQL statement is incorrect and will return incorrect results. The single reliable exception is an
// unbroken chain of INSERT statements, where we have taken pains to batch writes to speed things up.
type tableEditor struct {
	t        *WritableDoltTable
	tableEd  *subTableEditor
	indexEds []*subTableEditor
}

type subTableEditor struct {
	parent       *tableEditor
	sch          schema.Schema
	ed           *types.MapEditor
	idx          schema.Index
	insertedKeys map[hash.Hash]types.Value
	addedKeys    map[hash.Hash]types.Value
	removedKeys  map[hash.Hash]types.Value
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

func newTableEditor(ctx *sql.Context, t *WritableDoltTable) *tableEditor {
	te := &tableEditor{
		t:        t,
		indexEds: make([]*subTableEditor, t.sch.Indexes().Count()),
	}
	te.tableEd = &subTableEditor{
		parent:       te,
		sch:          t.sch,
		insertedKeys: make(map[hash.Hash]types.Value),
		addedKeys:    make(map[hash.Hash]types.Value),
		removedKeys:  make(map[hash.Hash]types.Value),
	}
	for i, index := range t.sch.Indexes().AllIndexes() {
		te.indexEds[i] = &subTableEditor{
			parent:       te,
			sch:          index.Schema(),
			idx:          index,
			insertedKeys: make(map[hash.Hash]types.Value),
			addedKeys:    make(map[hash.Hash]types.Value),
			removedKeys:  make(map[hash.Hash]types.Value),
		}
	}
	return te
}

func (te *tableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	err = te.tableEd.Insert(ctx, dRow)
	if err != nil {
		return err
	}

	for _, indexEd := range te.indexEds {
		dIndexRow, err := dRow.ReduceToIndex(indexEd.idx)
		if err != nil {
			return err
		}
		err = indexEd.Insert(ctx, dIndexRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (te *subTableEditor) Insert(ctx *sql.Context, dRow row.Row) error {
	key, err := dRow.NomsMapKey(te.sch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}

	hash, err := key.Hash(dRow.Format())
	if err != nil {
		return err
	}

	// If we've already inserted this key as part of this insert operation, that's an error. Inserting a row that already
	// exists in the table will be handled in Close().
	if _, ok := te.addedKeys[hash]; ok {
		value, err := types.EncodedValue(ctx, key)
		if err != nil {
			return err
		}
		return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, value)
	}
	te.insertedKeys[hash] = key
	te.addedKeys[hash] = key

	if te.ed == nil {
		te.ed, err = te.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Set(key, dRow.NomsMapValue(te.sch))
	return nil
}

func (te *tableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	err = te.tableEd.Delete(ctx, dRow)
	if err != nil {
		return err
	}

	for _, indexEd := range te.indexEds {
		dIndexRow, err := dRow.ReduceToIndex(indexEd.idx)
		if err != nil {
			return err
		}
		err = indexEd.Delete(ctx, dIndexRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (te *subTableEditor) Delete(ctx *sql.Context, dRow row.Row) error {
	key, err := dRow.NomsMapKey(te.sch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}
	hash, err := key.Hash(dRow.Format())
	if err != nil {
		return err
	}

	delete(te.addedKeys, hash)
	te.removedKeys[hash] = key

	if te.ed == nil {
		te.ed, err = te.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Remove(key)
	return nil
}

func (te *subTableEditor) newMapEditor(ctx context.Context) (*types.MapEditor, error) {
	if te.idx == nil {
		typesMap, err := te.parent.t.table.GetRowData(ctx)
		if err != nil {
			return nil, errhand.BuildDError("failed to get row data.").AddCause(err).Build()
		}
		return typesMap.Edit(), nil
	} else {
		typesMap, err := te.parent.t.table.GetIndexRowData(ctx, te.idx.Name())
		if err != nil {
			return nil, errhand.BuildDError("failed to get row data.").AddCause(err).Build()
		}
		return typesMap.Edit(), nil
	}
}

func (te *tableEditor) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := SqlRowToDoltRow(te.t.table.Format(), oldRow, te.t.sch)
	if err != nil {
		return err
	}
	dNewRow, err := SqlRowToDoltRow(te.t.table.Format(), newRow, te.t.sch)
	if err != nil {
		return err
	}

	err = te.tableEd.Update(ctx, dOldRow, dNewRow)
	if err != nil {
		return err
	}

	for _, indexEd := range te.indexEds {
		dOldIndexRow, err := dOldRow.ReduceToIndex(indexEd.idx)
		if err != nil {
			return err
		}
		dNewIndexRow, err := dNewRow.ReduceToIndex(indexEd.idx)
		if err != nil {
			return err
		}
		err = indexEd.Update(ctx, dOldIndexRow, dNewIndexRow)
		if err != nil {
			return err
		}
	}

	return nil
}

func (te *subTableEditor) Update(ctx *sql.Context, dOldRow row.Row, dNewRow row.Row) error {
	// If the PK is changed then we need to delete the old value and insert the new one
	dOldKey := dOldRow.NomsMapKey(te.sch)
	dOldKeyVal, err := dOldKey.Value(ctx)
	if err != nil {
		return err
	}
	dNewKey := dNewRow.NomsMapKey(te.sch)
	dNewKeyVal, err := dNewKey.Value(ctx)
	if err != nil {
		return err
	}

	if !dOldKeyVal.Equals(dNewKeyVal) {
		oldHash, err := dOldKeyVal.Hash(dOldRow.Format())
		if err != nil {
			return err
		}
		newHash, err := dNewKeyVal.Hash(dNewRow.Format())
		if err != nil {
			return err
		}

		// If the old value of the primary key we just updated was previously inserted, then we need to remove it now.
		if _, ok := te.insertedKeys[oldHash]; ok {
			delete(te.insertedKeys, oldHash)
			te.ed.Remove(dOldKeyVal)
		}

		te.addedKeys[newHash] = dNewKeyVal
		te.removedKeys[oldHash] = dOldKeyVal
	}

	if te.ed == nil {
		te.ed, err = te.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed.Set(dNewKeyVal, dNewRow.NomsMapValue(te.sch))
	return nil
}

// Close implements Closer
func (te *tableEditor) Close(ctx *sql.Context) error {
	// If we're running in batched mode, don't flush the edits until explicitly told to do so by the parent table.
	if te.t.db.batchMode == batched {
		return nil
	}
	return te.flush(ctx)
}

func (te *tableEditor) flush(ctx *sql.Context) error {
	// For all added keys, check for and report a collision
	for keyHash, addedKey := range te.tableEd.addedKeys {
		if _, ok := te.tableEd.removedKeys[keyHash]; !ok {
			_, rowExists, err := te.t.table.GetRow(ctx, addedKey.(types.Tuple), te.t.sch)
			if err != nil {
				return errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				value, err := types.EncodedValue(ctx, addedKey)
				if err != nil {
					return err
				}
				return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, value)
			}
		}
	}
	// For all removed keys, remove the map entries that weren't added elsewhere by other updates
	for keyHash, removedKey := range te.tableEd.removedKeys {
		if _, ok := te.tableEd.addedKeys[keyHash]; !ok {
			te.tableEd.ed.Remove(removedKey)
		} else {
			// Due to how REPLACE works, what equates to an UPDATE on the parent may actually be a DELETE on one PK and INSERT on another
			dRow, rowExists, err := te.t.table.GetRow(ctx, removedKey.(types.Tuple), te.t.sch)
			if err != nil {
				return errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				for _, indexEd := range te.indexEds {
					dIndexRow, err := dRow.ReduceToIndex(indexEd.idx)
					if err != nil {
						return errhand.BuildDError("failed to reduce row to index").AddCause(err).Build()
					}
					err = indexEd.Delete(ctx, dIndexRow)
					if err != nil {
						return errhand.BuildDError("failed to remove old row from index").AddCause(err).Build()
					}
				}
			}
		}
	}

	if te.tableEd.ed == nil {
		return nil
	}

	updated, err := te.tableEd.ed.Map(ctx)
	if err != nil {
		return errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}
	newTable, err := te.t.table.UpdateRows(ctx, updated)
	if err != nil {
		return errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}

	for _, indexEd := range te.indexEds {
		for keyHash, removedKey := range indexEd.removedKeys {
			if _, ok := indexEd.addedKeys[keyHash]; !ok {
				indexEd.ed.Remove(removedKey)
			}
		}

		indexMap, err := indexEd.ed.Map(ctx)
		if err != nil {
			return errhand.BuildDError("failed to modify index `%v`", indexEd.idx.Name()).AddCause(err).Build()
		}
		newTable, err = newTable.SetIndexRowData(ctx, indexEd.idx.Name(), indexMap)
		if err != nil {
			return errhand.BuildDError("failed to update index `%v`", indexEd.idx.Name()).AddCause(err).Build()
		}
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
