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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// IndexDriver implementation. Not ready for prime time.

type DoltIndexDriver struct {
	db *Database
}

func NewDoltIndexDriver(database *Database) *DoltIndexDriver {
	return &DoltIndexDriver{database}
}

func (*DoltIndexDriver) ID() string {
	return "doltDbIndexDriver"
}

func (*DoltIndexDriver) Create(db, table, id string, expressions []sql.Expression, config map[string]string) (sql.Index, error) {
	panic("creating indexes not supported")
}

func (i *DoltIndexDriver) Save(*sql.Context, sql.Index, sql.PartitionIndexKeyValueIter) error {
	panic("saving indexes not supported")
}

func (i *DoltIndexDriver) Delete(sql.Index, sql.PartitionIter) error {
	panic("deleting indexes not supported")
}

func (i *DoltIndexDriver) LoadAll(db, table string) ([]sql.Index, error) {
	if db != i.db.name {
		panic("Unexpected db: " + db)
	}

	tbl, ok, err := i.db.root.GetTable(context.TODO(), table)

	if err != nil {
		return nil, err
	}

	if !ok {
		panic(fmt.Sprintf("No table found with name %s", table))
	}

	sch, err := tbl.GetSchema(context.TODO())

	if err != nil {
		return nil, err
	}

	return []sql.Index{&doltIndex{sch, table, i.db, i}}, nil
}

type doltIndex struct {
	sch       schema.Schema
	tableName string
	db        *Database
	driver    *DoltIndexDriver
}

func (di *doltIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	taggedVals, err := keyColsToTuple(di.sch, key)
	if err != nil {
		return nil, err
	}

	return &doltIndexLookup{di, taggedVals}, nil
}

func keyColsToTuple(sch schema.Schema, key []interface{}) (row.TaggedValues, error) {
	if sch.GetPKCols().Size() != len(key) {
		return nil, errors.New("key must specify all columns")
	}

	var i int
	taggedVals := make(row.TaggedValues)
	err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		taggedVals[tag] = keyColToValue(key[i], col)
		i++
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return taggedVals, nil
}

func keyColToValue(v interface{}, column schema.Column) types.Value {
	// TODO: type conversion
	switch column.Kind {
	case types.BoolKind:
		return types.Bool(v.(bool))
	case types.IntKind:
		return types.Int(v.(int64))
	case types.FloatKind:
		return types.Float(v.(float64))
	case types.UintKind:
		return types.Uint(v.(uint64))
	case types.UUIDKind:
		panic("Implement me")
	case types.StringKind:
		return types.String(v.(string))
	default:
		panic("unhandled type")
	}
}

func (*doltIndex) Has(partition sql.Partition, key ...interface{}) (bool, error) {
	// appears to be unused for the moment
	panic("implement me")
}

func (di *doltIndex) ID() string {
	return fmt.Sprintf("%s:primaryKey", di.tableName)
}

func (di *doltIndex) Database() string {
	return di.db.name
}

func (di *doltIndex) Table() string {
	return di.tableName
}

func (di *doltIndex) Expressions() []string {
	strs, err := primaryKeytoIndexStrings(di.tableName, di.sch)

	// TODO: fix panics
	if err != nil {
		panic(err)
	}

	return strs
}

// Returns the expression strings needed for this index to work. This needs to match the implementation in the sql
// engine, which requires $table.$column
func primaryKeytoIndexStrings(tableName string, sch schema.Schema)([]string, error) {
	colNames := make([]string, sch.GetPKCols().Size())
	var i int
	err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colNames[i] = tableName + "." + col.Name
		i++
		return true, nil
	})

	if err != nil {
		return nil, err
	}

	return colNames, nil
}

func (di *doltIndex) Driver() string {
	return di.driver.ID()
}

// IndexedDoltTable is a wrapper for a DoltTable and a doltIndexLookup. It implements the sql.Table interface like
// DoltTable, but its RowIter function returns values that match the indexLookup, instead of all rows. It's returned by
// the DoltTable WithIndexLookup function.
type IndexedDoltTable struct {
	table       *DoltTable
	indexLookup *doltIndexLookup
}

func (idt *IndexedDoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	return idt.table.WithIndexLookup(lookup)
}

func (idt *IndexedDoltTable) IndexKeyValues(*sql.Context, []string) (sql.PartitionIndexKeyValueIter, error) {
	return idt.table.IndexKeyValues(nil, nil)
}

func (idt *IndexedDoltTable) Name() string {
	return idt.table.Name()
}

func (idt *IndexedDoltTable) String() string {
	return idt.table.String()
}

func (idt *IndexedDoltTable) Schema() sql.Schema {
	return idt.table.Schema()
}

func (idt *IndexedDoltTable) IndexLookup() sql.IndexLookup {
	return idt.indexLookup
}

func (idt *IndexedDoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	return idt.table.Partitions(ctx)
}

func (idt *IndexedDoltTable) PartitionRows(ctx *sql.Context, _ sql.Partition) (sql.RowIter, error) {
	return idt.indexLookup.RowIter(ctx)
}

type doltIndexLookup struct {
	idx *doltIndex
	key row.TaggedValues
}

func (il *doltIndexLookup) Indexes() []string {
	return []string{il.idx.ID()}
}

// No idea what this is used for, examples aren't useful. From stepping through the code I know that we get index values
// by wrapping tables via the WithIndexLookup method. The iterator that this method returns yields []byte instead of
// sql.Row and its purpose is yet unclear.
func (il *doltIndexLookup) Values(p sql.Partition) (sql.IndexValueIter, error) {
	panic("implement me")
}

// RowIter returns a row iterator for this index lookup. The iterator will return the single matching row for the index.
func (il *doltIndexLookup) RowIter(ctx *sql.Context) (sql.RowIter, error) {
	return &indexLookupRowIterAdapter{indexLookup: il, ctx: ctx}, nil
}

type indexLookupRowIterAdapter struct {
	indexLookup *doltIndexLookup
	ctx         *sql.Context
	i           int
}

func (i *indexLookupRowIterAdapter) Next() (sql.Row, error) {
	if i.i > 0 {
		return nil, io.EOF
	}

	i.i++
	table, _, err := i.indexLookup.idx.db.root.GetTable(i.ctx.Context, i.indexLookup.idx.tableName)

	if err != nil {
		return nil, err
	}

	r, ok, err := table.GetRowByPKVals(i.ctx.Context, i.indexLookup.key, i.indexLookup.idx.sch)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, io.EOF
	}

	return doltRowToSqlRow(r, i.indexLookup.idx.sch)
}

func (*indexLookupRowIterAdapter) Close() error {
	return nil
}
