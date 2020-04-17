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
	"errors"
	"fmt"
	"io"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	name   string
	table  *doltdb.Table
	sch    schema.Schema
	sqlSch sql.Schema
	db     Database
}

var _ sql.Table = (*DoltTable)(nil)

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
	return t.sqlSchema()
}

func (t *DoltTable) sqlSchema() sql.Schema {
	if t.sqlSch != nil {
		return t.sqlSch
	}

	// TODO: fix panics
	sqlSch, err := doltSchemaToSqlSchema(t.name, t.sch)
	if err != nil {
		panic(err)
	}

	t.sqlSch = sqlSch
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

// WritableDoltTable allows updating, deleting, and inserting new rows. It implements sql.UpdatableTable and friends.
type WritableDoltTable struct {
	DoltTable
	ed *tableEditor
}

var _ sql.UpdatableTable = (*WritableDoltTable)(nil)
var _ sql.DeletableTable = (*WritableDoltTable)(nil)
var _ sql.InsertableTable = (*WritableDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableDoltTable)(nil)

// Inserter implements sql.InsertableTable
func (t *WritableDoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	return t.getTableEditor()
}

func (t *WritableDoltTable) getTableEditor() *tableEditor {
	if t.db.batchMode == batched {
		if t.ed != nil {
			return t.ed
		}
		t.ed = newTableEditor(t)
		return t.ed
	}
	return newTableEditor(t)
}

func (t *WritableDoltTable) flushBatchedEdits(ctx *sql.Context) error {
	if t.ed != nil {
		err := t.ed.flush(ctx)
		t.ed = nil
		return err
	}
	return nil
}

// Deleter implements sql.DeletableTable
func (t *WritableDoltTable) Deleter(*sql.Context) sql.RowDeleter {
	return t.getTableEditor()
}

// Replacer implements sql.ReplaceableTable
func (t *WritableDoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	return t.getTableEditor()
}

// Updater implements sql.UpdatableTable
func (t *WritableDoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	return t.getTableEditor()
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
	root, err := t.db.GetRoot(ctx)

	if err != nil {
		return err
	}

	updated, err := mapEditor.Map(ctx)
	if err != nil {
		return errhand.BuildDError("failed to modify table").AddCause(err).Build()
	}

	newTable, err := t.table.UpdateRows(ctx, updated)
	if err != nil {
		return errhand.BuildDError("failed to update rows").AddCause(err).Build()
	}

	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return errhand.BuildDError("failed to write table back to database").AddCause(err).Build()
	}

	t.table = newTable
	return t.db.SetRoot(ctx, newRoot)
}

// AlterableDoltTable allows altering the schema of the table. It implements sql.AlterableTable.
type AlterableDoltTable struct {
	WritableDoltTable
}

var _ sql.AlterableTable = (*AlterableDoltTable)(nil)

// AddColumn implements sql.AlterableTable
func (t *AlterableDoltTable) AddColumn(ctx *sql.Context, column *sql.Column, order *sql.ColumnOrder) error {
	root, err := t.db.GetRoot(ctx)

	if err != nil {
		return err
	}

	table, _, err := root.GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	tag := extractTag(column)
	if tag == schema.InvalidTag {
		// generate a tag if we don't have a user-defined tag
		ti, err := typeinfo.FromSqlType(column.Type)
		if err != nil {
			return err
		}

		tt, err := root.GenerateTagsForNewColumns(ctx, t.name, []string{column.Name}, []types.NomsKind{ti.NomsKind()})
		if err != nil {
			return err
		}
		tag = tt[0]
	}

	col, err := SqlColToDoltCol(tag, column)
	if err != nil {
		return err
	}

	if col.IsPartOfPK {
		return errors.New("adding primary keys is not supported")
	}

	nullable := alterschema.NotNull
	if col.IsNullable() {
		nullable = alterschema.Null
	}

	var defaultVal types.Value
	if column.Default != nil {
		defaultVal, err = col.TypeInfo.ConvertValueToNomsValue(column.Default)
		if err != nil {
			return err
		}
	}

	updatedTable, err := alterschema.AddColumnToTable(ctx, root, table, t.name, col.Tag, col.Name, col.TypeInfo, nullable, defaultVal, orderToOrder(order))
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	return t.db.SetRoot(ctx, newRoot)
}

func orderToOrder(order *sql.ColumnOrder) *alterschema.ColumnOrder {
	if order == nil {
		return nil
	}
	return &alterschema.ColumnOrder{
		First: order.First,
		After: order.AfterColumn,
	}
}

// DropColumn implements sql.AlterableTable
func (t *AlterableDoltTable) DropColumn(ctx *sql.Context, columnName string) error {
	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}

	table, _, err := root.GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	updatedTable, err := alterschema.DropColumn(ctx, table, columnName)
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	return t.db.SetRoot(ctx, newRoot)
}

// ModifyColumn implements sql.AlterableTable
func (t *AlterableDoltTable) ModifyColumn(ctx *sql.Context, columnName string, column *sql.Column, order *sql.ColumnOrder) error {
	root, err := t.db.GetRoot(ctx)

	if err != nil {
		return err
	}

	table, _, err := root.GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return err
	}

	existingCol, ok := sch.GetAllCols().GetByName(columnName)
	if !ok {
		panic(fmt.Sprintf("Column %s not found. This is a bug.", columnName))
	}

	tag := extractTag(column)
	if tag == schema.InvalidTag {
		tag = existingCol.Tag
	}

	col, err := SqlColToDoltCol(tag, column)
	if err != nil {
		return err
	}

	var defVal types.Value
	if column.Default != nil {
		defVal, err = col.TypeInfo.ConvertValueToNomsValue(column.Default)
		if err != nil {
			return err
		}
	}

	updatedTable, err := alterschema.ModifyColumn(ctx, table, existingCol, col, defVal, orderToOrder(order))
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	return t.db.SetRoot(ctx, newRoot)
}
