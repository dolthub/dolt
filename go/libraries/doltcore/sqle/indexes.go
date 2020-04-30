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

	"github.com/liquidata-inc/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

// IndexDriver implementation. Not ready for prime time.

type DoltIndexDriver struct {
	dbs map[string]Database
}

func NewDoltIndexDriver(dbs ...Database) *DoltIndexDriver {
	nameToDB := make(map[string]Database)
	for _, db := range dbs {
		nameToDB[db.Name()] = db
	}

	return &DoltIndexDriver{nameToDB}
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

func (i *DoltIndexDriver) LoadAll(ctx *sql.Context, db, table string) ([]sql.Index, error) {
	database, ok := i.dbs[db]
	if !ok {
		panic("Unexpected db: " + db)
	}

	root, err := database.GetRoot(ctx)

	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, table)

	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, nil
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	return []sql.Index{&doltIndex{sch, table, database, i}}, nil
}

type doltIndex struct {
	sch       schema.Schema
	tableName string
	db        Database
	driver    *DoltIndexDriver
}

func (di *doltIndex) Get(key ...interface{}) (sql.IndexLookup, error) {
	if di.sch.GetPKCols().Size() != len(key) {
		return nil, errors.New("key must specify all columns")
	}

	var i int
	taggedVals := make(row.TaggedValues)
	err := di.sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		val, err := col.TypeInfo.ConvertValueToNomsValue(key[i])
		if err != nil {
			return true, err
		}
		taggedVals[tag] = val
		i++
		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return &doltIndexLookup{di, taggedVals}, nil
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
func primaryKeytoIndexStrings(tableName string, sch schema.Schema) ([]string, error) {
	colNames := make([]string, sch.GetPKCols().Size())
	var i int
	err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		colNames[i] = tableName + "." + col.Name
		i++
		return false, nil
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

	root, err := i.indexLookup.idx.db.GetRoot(i.ctx)

	if err != nil {
		return nil, err
	}

	table, _, err := root.GetTable(i.ctx.Context, i.indexLookup.idx.tableName)

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

	return DoltRowToSqlRow(r, i.indexLookup.idx.sch)
}

func (*indexLookupRowIterAdapter) Close() error {
	return nil
}
