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

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var ErrDuplicatePrimaryKeyFmt = "duplicate primary key given: (%v)"

// tableEditor supports making multiple row edits (inserts, updates, deletes) to a table. It does error checking for key
// collision etc. in the Close() method, as well as during Insert / Update.
// Right now a table editor allows you to combine inserts, updates, and deletes in any order, and makes reasonable
// attempts to produce correct results when doing so. But this probably (definitely) doesn't work in every case, and
// higher-level clients should carefully flush the editor when necessary (i.e. before an update after many inserts).
type tableEditor struct {
	t            *DoltTable
	ed           *types.MapEditor
	insertedKeys map[hash.Hash]types.Value
	addedKeys    map[hash.Hash]types.Value
	removedKeys  map[hash.Hash]types.Value
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableEditor)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

func newTableEditor(t *DoltTable) *tableEditor {
	return &tableEditor{
		t:            t,
		insertedKeys: make(map[hash.Hash]types.Value),
		addedKeys:    make(map[hash.Hash]types.Value),
		removedKeys:  make(map[hash.Hash]types.Value),
	}
}

func (te *tableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	key, err := dRow.NomsMapKey(te.t.sch).Value(ctx)
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
		return fmt.Errorf(ErrDuplicatePrimaryKeyFmt, types.EncodedValue(ctx, key))
	}
	te.insertedKeys[hash] = key
	te.addedKeys[hash] = key

	if te.ed == nil {
		te.ed, err = te.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Set(key, dRow.NomsMapValue(te.t.sch))
	return nil
}

func (te *tableEditor) Delete(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	key, err := dRow.NomsMapKey(te.t.sch).Value(ctx)
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
		te.ed, err = te.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Remove(key)
	return nil
}

func (t *DoltTable) newMapEditor(ctx context.Context) (*types.MapEditor, error) {
	typesMap, err := t.table.GetRowData(ctx)
	if err != nil {
		return nil, errhand.BuildDError("failed to get row data.").AddCause(err).Build()
	}

	return typesMap.Edit(), nil
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

	// If the PK is changed then we need to delete the old value and insert the new one
	dOldKey := dOldRow.NomsMapKey(te.t.sch)
	dOldKeyVal, err := dOldKey.Value(ctx)
	if err != nil {
		return err
	}
	dNewKey := dNewRow.NomsMapKey(te.t.sch)
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
		te.ed, err = te.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed.Set(dNewKeyVal, dNewRow.NomsMapValue(te.t.sch))
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

func (te *tableEditor) flush(ctx context.Context) error {
	// For all added keys, check for and report a collision
	for hash, addedKey := range te.addedKeys {
		if _, ok := te.removedKeys[hash]; !ok {
			_, rowExists, err := te.t.table.GetRow(ctx, addedKey.(types.Tuple), te.t.sch)
			if err != nil {
				return errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				return fmt.Errorf("duplicate primary key given: (%v)", types.EncodedValue(ctx, addedKey))
			}
		}
	}
	// For all removed keys, remove the map entries that weren't added elsewhere by other updates
	for hash, removedKey := range te.removedKeys {
		if _, ok := te.addedKeys[hash]; !ok {
			te.ed.Remove(removedKey)
		}
	}

	if te.ed != nil {
		return te.t.updateTable(ctx, te.ed)
	}
	return nil
}
