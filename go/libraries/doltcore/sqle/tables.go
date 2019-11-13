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
	"errors"
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/hash"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var _ sql.Table = (*DoltTable)(nil)
var _ sql.UpdatableTable = (*DoltTable)(nil)
var _ sql.DeletableTable = (*DoltTable)(nil)
var _ sql.InsertableTable = (*DoltTable)(nil)
var _ sql.ReplaceableTable = (*DoltTable)(nil)

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	name  string
	table *doltdb.Table
	sch   schema.Schema
	db    *Database
}

// Implements sql.IndexableTable
func (t *DoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	dil, ok := lookup.(*doltIndexLookup)
	if !ok {
		panic(fmt.Sprintf("Unrecognized indexLookup %T", lookup))
	}

	return &IndexedDoltTable{
		table:       t,
		indexLookup: dil,
	}
}

// Implements sql.IndexableTable
func (t *DoltTable) IndexKeyValues(*sql.Context, []string) (sql.PartitionIndexKeyValueIter, error) {
	return nil, errors.New("creating new indexes not supported")
}

// Implements sql.IndexableTable
func (t *DoltTable) IndexLookup() sql.IndexLookup {
	panic("IndexLookup called on DoltTable, should be on IndexedDoltTable")
}

// Name returns the name of the table.
func (t *DoltTable) Name() string {
	return t.name
}

// Not sure what the purpose of this method is, so returning the name for now.
func (t *DoltTable) String() string {
	return t.name
}

// Schema returns the schema for this table.
func (t *DoltTable) Schema() sql.Schema {
	// TODO: fix panics
	sch, err := t.table.GetSchema(context.TODO())

	if err != nil {
		panic(err)
	}

	// TODO: fix panics
	sqlSch, err := doltSchemaToSqlSchema(t.name, sch)

	if err != nil {
		panic(err)
	}

	return sqlSch
}

// Returns the partitions for this table. We return a single partition, but could potentially get more performance by
// returning multiple.
func (t *DoltTable) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &doltTablePartitionIter{}, nil
}

// Returns the table rows for the partition given (all rows of the table).
func (t *DoltTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return newRowIterator(t, ctx)
}

type tableEditor struct {
	t           *DoltTable
	ed          *types.MapEditor
	addedKeys   map[hash.Hash]bool
	deletedKeys map[hash.Hash]bool
}

func newTableEditor(t *DoltTable) *tableEditor {
	return &tableEditor{
		t:           t,
		addedKeys:   make(map[hash.Hash]bool),
		deletedKeys: make(map[hash.Hash]bool),
	}
}

var _ sql.RowReplacer = (*tableEditor)(nil)
var _ sql.RowUpdater = (*tableUpdater)(nil)
var _ sql.RowInserter = (*tableEditor)(nil)
var _ sql.RowDeleter = (*tableEditor)(nil)

func (te *tableEditor) Insert(ctx *sql.Context, sqlRow sql.Row) error {
	dRow, err := SqlRowToDoltRow(te.t.table.Format(), sqlRow, te.t.sch)
	if err != nil {
		return err
	}

	key, err := dRow.NomsMapKey(te.t.sch).Value(ctx)
	if err != nil {
		return errhand.BuildDError("failed to get row key").AddCause(err).Build()
	}
	_, rowExists, err := te.t.table.GetRow(ctx, key.(types.Tuple), te.t.sch)
	if err != nil {
		return errhand.BuildDError("failed to read table").AddCause(err).Build()
	}

	hash, err := key.Hash(dRow.Format())
	if err != nil {
		return err
	}

	if (rowExists && !te.deletedKeys[hash]) || te.addedKeys[hash] {
		return errors.New("duplicate primary key given")
	}
	te.addedKeys[hash] = true

	if te.ed == nil {
		te.ed, err = te.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Set(key, dRow.NomsMapValue(te.t.sch))
	return nil
}

func (t *DoltTable) newMapEditor(ctx context.Context) (*types.MapEditor, error) {
	typesMap, err := t.table.GetRowData(ctx)
	if err != nil {
		return nil, errhand.BuildDError("failed to get row data.").AddCause(err).Build()
	}

	return typesMap.Edit(), nil
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
	te.deletedKeys[hash] = true

	if te.ed == nil {
		te.ed, err = te.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
	}

	te.ed = te.ed.Remove(key)
	return nil
}

// tableUpdater wraps tableEditor to override the close method, necessary to enforce primary key constraints when
// updates to primary key columns are applied in an arbitrary order
type tableUpdater struct {
	t           *DoltTable
	ed          *types.MapEditor
	addedKeys   map[hash.Hash]types.LesserValuable
	removedKeys map[hash.Hash]types.LesserValuable
}

func (tu *tableUpdater) Close(ctx *sql.Context) error {
	// For all added keys, check for and report a collision
	for hash, addedKey := range tu.addedKeys {
		if _, ok := tu.removedKeys[hash]; !ok {
			_, rowExists, err := tu.t.table.GetRow(ctx, addedKey.(types.Tuple), tu.t.sch)
			if err != nil {
				return errhand.BuildDError("failed to read table").AddCause(err).Build()
			}
			if rowExists {
				return fmt.Errorf("primary key collision: (%v)", addedKey)
			}
		}
	}
	// For all removed keys, remove the map entries that weren't added elsewhere by other updates
	for hash, removedKey := range tu.removedKeys {
		if _, ok := tu.addedKeys[hash]; !ok {
			tu.ed.Remove(removedKey)
		}
	}

	if tu.ed != nil {
		return tu.t.updateTable(ctx, tu.ed)
	}
	return nil
}

func (tu *tableUpdater) Update(ctx *sql.Context, oldRow sql.Row, newRow sql.Row) error {
	dOldRow, err := SqlRowToDoltRow(tu.t.table.Format(), oldRow, tu.t.sch)
	if err != nil {
		return err
	}
	dNewRow, err := SqlRowToDoltRow(tu.t.table.Format(), newRow, tu.t.sch)
	if err != nil {
		return err
	}

	// If the PK is changed then we have to delete the old row first
	dOldKey := dOldRow.NomsMapKey(tu.t.sch)
	dOldKeyVal, err := dOldKey.Value(ctx)
	if err != nil {
		return err
	}
	dNewKey := dNewRow.NomsMapKey(tu.t.sch)
	dNewKeyVal, err := dNewKey.Value(ctx)
	if err != nil {
		return err
	}

	if tu.ed == nil {
		tu.ed, err = tu.t.newMapEditor(ctx)
		if err != nil {
			return err
		}
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

		tu.addedKeys[newHash] = dNewKeyVal
		tu.removedKeys[oldHash] = dOldKey
	}

	tu.ed.Set(dNewKey, dNewRow.NomsMapValue(tu.t.sch))
	return nil
}

func (te *tableEditor) Close(ctx *sql.Context) error {
	if te.ed != nil {
		return te.t.updateTable(ctx, te.ed)
	}
	return nil
}

func (t *DoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return newTableEditor(t)
}

func (t *DoltTable) Deleter(*sql.Context) sql.RowDeleter {
	return newTableEditor(t)
}

func (t *DoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return newTableEditor(t)
}

func (t *DoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return &tableUpdater{
		t:           t,
		addedKeys:   make(map[hash.Hash]types.LesserValuable),
		removedKeys: make(map[hash.Hash]types.LesserValuable),
	}
}

// doltTablePartitionIter, an object that knows how to return the single partition exactly once.
type doltTablePartitionIter struct {
	sql.PartitionIter
	i int
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *doltTablePartitionIter) Close() error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *doltTablePartitionIter) Next() (sql.Partition, error) {
	if itr.i > 0 {
		return nil, io.EOF
	}
	itr.i++

	return &doltTablePartition{}, nil
}

// A table partition, currently an unused layer of abstraction but required for the framework.
type doltTablePartition struct {
	sql.Partition
}

const partitionName = "single"

// Key returns the key for this partition, which must uniquely identity the partition. We have only a single partition
// per table, so we use a constant.
func (p doltTablePartition) Key() []byte {
	return []byte(partitionName)
}

func (t *DoltTable) updateTable(ctx *sql.Context, mapEditor *types.MapEditor) error {
	updated, err := mapEditor.Map(ctx)
	if err != nil {
		return errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}

	newTable, err := t.table.UpdateRows(ctx, updated)
	if err != nil {
		return errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}

	newRoot, err := doltdb.PutTable(ctx, t.db.root, t.db.root.VRW(), t.name, newTable)
	if err != nil {
		return errhand.BuildDError("failed to write table back to database").AddCause(err).Build()
	}

	t.table = newTable
	t.db.root = newRoot
	return nil
}
