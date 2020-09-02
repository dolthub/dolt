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
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"github.com/liquidata-inc/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/typeinfo"
	sqleSchema "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/schema"
	"github.com/liquidata-inc/dolt/go/libraries/utils/set"
	"github.com/liquidata-inc/dolt/go/store/types"
)

const (
	partitionMultiplier = 2.0
)

var MinRowsPerPartition uint64 = 1024

func init() {
	isTest := false
	for _, arg := range os.Args {
		lwr := strings.ToLower(arg)
		if lwr == "-test.v" ||
			lwr == "-test.run" ||
			strings.HasPrefix(lwr, "-test.testlogfile") ||
			strings.HasPrefix(lwr, "-test.timeout") ||
			strings.HasPrefix(lwr, "-test.count") {
			isTest = true
			break
		}
	}

	if isTest {
		MinRowsPerPartition = 2
	}
}

// DoltTable implements the sql.Table interface and gives access to dolt table rows and schema.
type DoltTable struct {
	name   string
	table  *doltdb.Table
	sch    schema.Schema
	sqlSch sql.Schema
	db     Database
}

var _ sql.Table = (*DoltTable)(nil)
var _ sql.IndexedTable = (*DoltTable)(nil)

// WithIndexLookup implements sql.IndexedTable
func (t *DoltTable) WithIndexLookup(lookup sql.IndexLookup) sql.Table {
	dil, ok := lookup.(*doltIndexLookup)
	if !ok {
		return newStaticErrorTable(t, fmt.Errorf("Unrecognized indexLookup %T", lookup))
	}
	return &IndexedDoltTable{
		table:       t,
		indexLookup: dil,
	}
}

// GetIndexes implements sql.IndexedTable
func (t *DoltTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	tbl := t.table

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	cols := sch.GetPKCols().GetColumns()
	sqlIndexes := []sql.Index{
		&doltIndex{
			cols:         cols,
			db:           t.db,
			id:           "PRIMARY",
			indexRowData: rowData,
			indexSch:     sch,
			table:        tbl,
			tableData:    rowData,
			tableName:    t.Name(),
			tableSch:     sch,
			unique:       true,
		},
	}

	for _, index := range sch.Indexes().AllIndexes() {
		indexRowData, err := tbl.GetIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
		cols := make([]schema.Column, index.Count())
		for i, tag := range index.IndexedColumnTags() {
			cols[i], _ = index.GetColumn(tag)
		}
		sqlIndexes = append(sqlIndexes, &doltIndex{
			cols:         cols,
			db:           t.db,
			id:           index.Name(),
			indexRowData: indexRowData,
			indexSch:     index.Schema(),
			table:        tbl,
			tableData:    rowData,
			tableName:    t.Name(),
			tableSch:     sch,
			unique:       index.IsUnique(),
			comment:      index.Comment(),
		})
	}

	return sqlIndexes, nil
}

// Name returns the name of the table.
func (t *DoltTable) Name() string {
	return t.name
}

// String returns a human-readable string to display the name of this SQL node.
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
	sqlSch, err := sqleSchema.FromDoltSchema(t.name, t.sch)
	if err != nil {
		panic(err)
	}

	t.sqlSch = sqlSch
	return sqlSch
}

// Returns the partitions for this table.
func (t *DoltTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	rowData, err := t.table.GetRowData(ctx)

	if err != nil {
		return nil, err
	}

	numElements := rowData.Len()

	if numElements == 0 {
		return newSinglePartitionIter(), nil
	}

	maxPartitions := uint64(partitionMultiplier * runtime.NumCPU())
	numPartitions := (numElements / MinRowsPerPartition) + 1

	if numPartitions > maxPartitions {
		numPartitions = maxPartitions
	}

	partitions := make([]doltTablePartition, numPartitions)
	itemsPerPartition := numElements / numPartitions
	for i := uint64(0); i < numPartitions-1; i++ {
		partitions[i] = doltTablePartition{i * itemsPerPartition, (i + 1) * itemsPerPartition}
	}
	partitions[numPartitions-1] = doltTablePartition{(numPartitions - 1) * itemsPerPartition, numElements}

	return newDoltTablePartitionIter(rowData, partitions), nil
}

// Returns the table rows for the partition given
func (t *DoltTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	switch typedPartition := partition.(type) {
	case doltTablePartition:
		return newRowIterator(t, ctx, &typedPartition)
	case singlePartition:
		return newRowIterator(t, ctx, nil)
	}

	return nil, errors.New("unsupported partition type")
}

// WritableDoltTable allows updating, deleting, and inserting new rows. It implements sql.UpdatableTable and friends.
type WritableDoltTable struct {
	DoltTable
	ed *sqlTableEditor
}

var _ sql.UpdatableTable = (*WritableDoltTable)(nil)
var _ sql.DeletableTable = (*WritableDoltTable)(nil)
var _ sql.InsertableTable = (*WritableDoltTable)(nil)
var _ sql.ReplaceableTable = (*WritableDoltTable)(nil)

// Inserter implements sql.InsertableTable
func (t *WritableDoltTable) Inserter(ctx *sql.Context) sql.RowInserter {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return newStaticErrorEditor(err)
	}
	return te
}

func (t *WritableDoltTable) getTableEditor(ctx *sql.Context) (*sqlTableEditor, error) {
	if t.db.batchMode == batched {
		if t.ed != nil {
			return t.ed, nil
		}
		var err error
		t.ed, err = newSqlTableEditor(ctx, t)
		return t.ed, err
	}
	return newSqlTableEditor(ctx, t)
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
func (t *WritableDoltTable) Deleter(ctx *sql.Context) sql.RowDeleter {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return newStaticErrorEditor(err)
	}
	return te
}

// Replacer implements sql.ReplaceableTable
func (t *WritableDoltTable) Replacer(ctx *sql.Context) sql.RowReplacer {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return newStaticErrorEditor(err)
	}
	return te
}

// Updater implements sql.UpdatableTable
func (t *WritableDoltTable) Updater(ctx *sql.Context) sql.RowUpdater {
	te, err := t.getTableEditor(ctx)
	if err != nil {
		return newStaticErrorEditor(err)
	}
	return te
}

var _ sql.PartitionIter = singlePartitionIter{}

type singlePartitionIter struct {
	once *sync.Once
}

func newSinglePartitionIter() singlePartitionIter {
	return singlePartitionIter{&sync.Once{}}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr singlePartitionIter) Close() error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr singlePartitionIter) Next() (sql.Partition, error) {
	first := false
	itr.once.Do(func() {
		first = true
	})

	if !first {
		return nil, io.EOF
	}

	return singlePartition{}, nil
}

var _ sql.Partition = singlePartition{}

type singlePartition struct{}

// Key returns the key for this partition, which must uniquely identity the partition. We have only a single partition
// per table, so we use a constant.
func (sp singlePartition) Key() []byte {
	return []byte("single")
}

var _ sql.PartitionIter = (*doltTablePartitionIter)(nil)

// doltTablePartitionIter, an object that knows how to return the single partition exactly once.
type doltTablePartitionIter struct {
	i          int
	mu         *sync.Mutex
	rowData    types.Map
	partitions []doltTablePartition
}

func newDoltTablePartitionIter(rowData types.Map, partitions []doltTablePartition) *doltTablePartitionIter {
	return &doltTablePartitionIter{0, &sync.Mutex{}, rowData, partitions}
}

// Close is required by the sql.PartitionIter interface. Does nothing.
func (itr *doltTablePartitionIter) Close() error {
	return nil
}

// Next returns the next partition if there is one, or io.EOF if there isn't.
func (itr *doltTablePartitionIter) Next() (sql.Partition, error) {
	itr.mu.Lock()
	defer itr.mu.Unlock()

	if itr.i >= len(itr.partitions) {
		return nil, io.EOF
	}

	partition := itr.partitions[itr.i]
	itr.i++

	return partition, nil
}

var _ sql.Partition = (*doltTablePartition)(nil)

type doltTablePartition struct {
	// start is the first index of this partition (inclusive)
	start uint64
	// all elements in the partition will be less than end (exclusive)
	end uint64
}

// Key returns the key for this partition, which must uniquely identity the partition.
func (p doltTablePartition) Key() []byte {
	return []byte(strconv.FormatUint(p.start, 10) + " >= i < " + strconv.FormatUint(p.end, 10))
}

// AlterableDoltTable allows altering the schema of the table. It implements sql.AlterableTable.
type AlterableDoltTable struct {
	WritableDoltTable
}

var _ sql.AlterableTable = (*AlterableDoltTable)(nil)
var _ sql.IndexAlterableTable = (*AlterableDoltTable)(nil)
var _ sql.ForeignKeyAlterableTable = (*AlterableDoltTable)(nil)
var _ sql.ForeignKeyTable = (*AlterableDoltTable)(nil)

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

	tag := sqleSchema.ExtractTag(column)
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

	col, err := sqleSchema.ToDoltCol(tag, column)
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

	updatedTable, err := alterschema.AddColumnToTable(ctx, root, table, t.name, col.Tag, col.Name, col.TypeInfo, nullable, col.Default, orderToOrder(order))
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

	updatedTable, _, err := root.GetTable(ctx, t.name)
	if err != nil {
		return err
	}

	sch, err := updatedTable.GetSchema(ctx)
	if err != nil {
		return err
	}

	for _, index := range sch.Indexes().IndexesWithColumn(columnName) {
		_, err = sch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return err
		}
		updatedTable, err = updatedTable.DeleteIndexRowData(ctx, index.Name())
		if err != nil {
			return err
		}
	}

	updatedTable, err = updatedTable.UpdateSchema(ctx, sch)
	if err != nil {
		return err
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	declaresFk, referencesFk := fkCollection.KeysForTable(t.name)

	updatedTable, err = alterschema.DropColumn(ctx, updatedTable, columnName, append(declaresFk, referencesFk...))
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

	tag := sqleSchema.ExtractTag(column)
	if tag != existingCol.Tag && tag != schema.InvalidTag {
		return errors.New("cannot change the tag of an existing column")
	}

	col, err := sqleSchema.ToDoltCol(existingCol.Tag, column)
	if err != nil {
		return err
	}

	fkCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	declaresFk, _ := fkCollection.KeysForTable(t.name)
	for _, foreignKey := range declaresFk {
		if (foreignKey.OnUpdate == doltdb.ForeignKeyReferenceOption_SetNull || foreignKey.OnDelete == doltdb.ForeignKeyReferenceOption_SetNull) &&
			col.IsNullable() {
			return fmt.Errorf("foreign key `%s` has SET NULL thus column `%s` cannot be altered to accept null values", foreignKey.Name, col.Name)
		}
	}

	updatedTable, err := alterschema.ModifyColumn(ctx, table, existingCol, col, orderToOrder(order))
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.name, updatedTable)
	if err != nil {
		return err
	}

	return t.db.SetRoot(ctx, newRoot)
}

// CreateIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) CreateIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	newTable, _, _, err := createIndexForTable(ctx, t.table, indexName, using, constraint, columns, comment)
	if err != nil {
		return err
	}
	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}
	err = t.db.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// DropIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) DropIndex(ctx *sql.Context, indexName string) error {
	// We disallow removing internal dolt_ tables from SQL directly
	if strings.HasPrefix(indexName, "dolt_") {
		return fmt.Errorf("dolt internal indexes may not be dropped")
	}
	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	ourKeys, referencingKeys := fkc.KeysForTable(t.name)
	for _, k := range ourKeys {
		if k.TableIndex == indexName {
			return fmt.Errorf("cannot drop index: %s is referenced by foreign key %s",
				k.TableIndex, k.Name)
		}
	}
	for _, k := range referencingKeys {
		if k.ReferencedTableIndex == indexName {
			return fmt.Errorf("cannot drop index: %s is referenced by foreign key %s",
				k.ReferencedTableIndex, k.Name)
		}
	}

	newTable, _, err := t.dropIndex(ctx, indexName)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}
	err = t.db.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// RenameIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) RenameIndex(ctx *sql.Context, fromIndexName string, toIndexName string) error {
	// RenameIndex will error if there is a name collision or an index does not exist
	_, err := t.sch.Indexes().RenameIndex(fromIndexName, toIndexName)
	if err != nil {
		return err
	}
	newTable, err := t.table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return err
	}

	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}

	err = t.db.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// CreateForeignKey implements sql.ForeignKeyAlterableTable
func (t *AlterableDoltTable) CreateForeignKey(
	ctx *sql.Context,
	fkName string,
	columns []string,
	refTblName string,
	refColumns []string,
	onUpdate, onDelete sql.ForeignKeyReferenceOption) error {
	if fkName != "" && !doltdb.IsValidTableName(fkName) {
		return fmt.Errorf("invalid foreign key name `%s` as it must match the regular expression %s", fkName, doltdb.TableNameRegexStr)
	}
	//TODO: move this into go-mysql-server
	if len(columns) != len(refColumns) {
		return fmt.Errorf("the foreign key must reference an equivalent number of columns")
	}

	tblCols := make([]schema.Column, len(columns))
	colTags := make([]uint64, len(columns))
	sqlColNames := make([]sql.IndexColumn, len(columns))
	for i, col := range columns {
		tableCol, ok := t.sch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			//TODO: fix go-mysql-server equivalent check, needs two vals
			return fmt.Errorf("table `%s` does not have column `%s`", t.name, col)
		}
		if (onUpdate == sql.ForeignKeyReferenceOption_SetNull || onDelete == sql.ForeignKeyReferenceOption_SetNull) &&
			!tableCol.IsNullable() {
			return fmt.Errorf("cannot use SET NULL as column `%s` is non-nullable", tableCol.Name)
		}
		tblCols[i] = tableCol
		colTags[i] = tableCol.Tag
		sqlColNames[i] = sql.IndexColumn{
			Name:   tableCol.Name,
			Length: 0,
		}
	}

	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	refTbl, _, ok, err := root.GetTableInsensitive(ctx, refTblName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("referenced table `%s` does not exist", refTblName)
	}
	refSch, err := refTbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	refColTags := make([]uint64, len(refColumns))
	for i, name := range refColumns {
		refCol, ok := refSch.GetAllCols().GetByNameCaseInsensitive(name)
		if !ok {
			return fmt.Errorf("table `%s` does not have column `%s`", refTblName, name)
		}
		if !tblCols[i].TypeInfo.Equals(refCol.TypeInfo) {
			return fmt.Errorf("column type mismatch on `%s` and `%s`", columns[i], refCol.Name)
		}
		sqlParserType := refCol.TypeInfo.ToSqlType().Type()
		if sqlParserType == sqltypes.Blob || sqlParserType == sqltypes.Text {
			return fmt.Errorf("TEXT/BLOB are not valid types for foreign keys")
		}
		refColTags[i] = refCol.Tag
	}

	onUpdateRefOp, err := parseFkReferenceOption(onUpdate)
	if err != nil {
		return err
	}
	onDeleteRefOp, err := parseFkReferenceOption(onDelete)
	if err != nil {
		return err
	}

	tableIndex, ok := t.sch.Indexes().GetIndexByTags(colTags...)
	if !ok {
		// if child index doesn't exist, create it
		t.table, _, tableIndex, err = createIndexForTable(ctx, t.table, "", sql.IndexUsing_Default, sql.IndexConstraint_None, sqlColNames, "")
		if err != nil {
			return err
		}
		root, err = root.PutTable(ctx, t.name, t.table)
		if err != nil {
			return err
		}
	}

	refTableIndex, ok := refSch.Indexes().GetIndexByTags(refColTags...)
	if !ok {
		parentPKs := set.NewUint64Set(refSch.GetPKCols().Tags)
		if parentPKs.ContainsAll(refColTags) {
			// special exception for parent table primary keys
			// todo: make clustered PK index usable as parent table FK index
			var colNames []sql.IndexColumn
			for _, t := range refColTags {
				c, _ := refSch.GetAllCols().GetByTag(t)
				colNames = append(colNames, sql.IndexColumn{Name: c.Name})
			}
			refTbl, _, refTableIndex, err = createIndexForTable(ctx, refTbl, "", sql.IndexUsing_Default, sql.IndexConstraint_None, colNames, "")
			if err != nil {
				return err
			}
			root, err = root.PutTable(ctx, refTblName, refTbl)
			if err != nil {
				return err
			}
		} else {
			// parent index must exist
			return fmt.Errorf("missing index for constraint '%s' in the referenced table '%s'", fkName, refTblName)
		}
	}

	foreignKeyCollection, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	foreignKey := doltdb.ForeignKey{
		Name:                   fkName,
		TableName:              t.name,
		TableIndex:             tableIndex.Name(),
		TableColumns:           colTags,
		ReferencedTableName:    refTblName,
		ReferencedTableIndex:   refTableIndex.Name(),
		ReferencedTableColumns: refColTags,
		OnUpdate:               onUpdateRefOp,
		OnDelete:               onDeleteRefOp,
	}
	err = foreignKeyCollection.AddKeys(foreignKey)
	if err != nil {
		return err
	}
	newRoot, err := root.PutForeignKeyCollection(ctx, foreignKeyCollection)
	if err != nil {
		return err
	}

	tableIndexData, err := t.table.GetIndexRowData(ctx, tableIndex.Name())
	if err != nil {
		return err
	}
	refTableIndexData, err := refTbl.GetIndexRowData(ctx, refTableIndex.Name())
	if err != nil {
		return err
	}
	err = foreignKey.ConstraintIsSatisfied(ctx, tableIndexData, refTableIndexData, tableIndex, refTableIndex)
	if err != nil {
		return err
	}

	err = t.db.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// DropForeignKey implements sql.ForeignKeyAlterableTable
func (t *AlterableDoltTable) DropForeignKey(ctx *sql.Context, fkName string) error {
	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	err = fkc.RemoveKeyByName(fkName)
	if err != nil {
		return err
	}
	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)

	err = t.db.SetRoot(ctx, newRoot)
	if err != nil {
		return err
	}
	return t.updateFromRoot(ctx, newRoot)
}

// GetForeignKeys implements sql.ForeignKeyTable
func (t *AlterableDoltTable) GetForeignKeys(ctx *sql.Context) ([]sql.ForeignKeyConstraint, error) {
	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	fkc, err := root.GetForeignKeyCollection(ctx)
	if err != nil {
		return nil, err
	}

	declaredFk, _ := fkc.KeysForTable(t.name)
	toReturn := make([]sql.ForeignKeyConstraint, len(declaredFk))

	for i, fk := range declaredFk {
		parent, ok, err := root.GetTable(ctx, fk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			return nil, fmt.Errorf("cannot find table %s "+
				"referenced in foreign key %s", fk.ReferencedTableName, fk.Name)
		}

		parentSch, err := parent.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		toReturn[i], err = toForeignKeyConstraint(fk, t.sch, parentSch)
		if err != nil {
			return nil, err
		}
	}

	return toReturn, nil
}

func toForeignKeyConstraint(fk doltdb.ForeignKey, childSch, parentSch schema.Schema) (cst sql.ForeignKeyConstraint, err error) {
	cst = sql.ForeignKeyConstraint{
		Name:              fk.Name,
		Columns:           make([]string, len(fk.TableColumns)),
		ReferencedTable:   fk.ReferencedTableName,
		ReferencedColumns: make([]string, len(fk.ReferencedTableColumns)),
		OnUpdate:          toReferenceOption(fk.OnUpdate),
		OnDelete:          toReferenceOption(fk.OnDelete),
	}

	for i, tag := range fk.TableColumns {
		c, ok := childSch.GetAllCols().GetByTag(tag)
		if !ok {
			return cst, fmt.Errorf("cannot find column for tag %d "+
				"in table %s used in foreign key %s", tag, fk.TableName, fk.Name)
		}
		cst.Columns[i] = c.Name
	}

	for i, tag := range fk.ReferencedTableColumns {
		c, ok := parentSch.GetAllCols().GetByTag(tag)
		if !ok {
			return cst, fmt.Errorf("cannot find column for tag %d "+
				"in table %s used in foreign key %s", tag, fk.ReferencedTableName, fk.Name)
		}
		cst.ReferencedColumns[i] = c.Name

	}

	return cst, nil
}

func toReferenceOption(opt doltdb.ForeignKeyReferenceOption) sql.ForeignKeyReferenceOption {
	switch opt {
	case doltdb.ForeignKeyReferenceOption_DefaultAction:
		return sql.ForeignKeyReferenceOption_DefaultAction
	case doltdb.ForeignKeyReferenceOption_Cascade:
		return sql.ForeignKeyReferenceOption_Cascade
	case doltdb.ForeignKeyReferenceOption_NoAction:
		return sql.ForeignKeyReferenceOption_NoAction
	case doltdb.ForeignKeyReferenceOption_Restrict:
		return sql.ForeignKeyReferenceOption_Restrict
	case doltdb.ForeignKeyReferenceOption_SetNull:
		return sql.ForeignKeyReferenceOption_SetNull
	default:
		panic(fmt.Sprintf("Unhandled foreign key reference option %v", opt))
	}
}

func parseFkReferenceOption(refOp sql.ForeignKeyReferenceOption) (doltdb.ForeignKeyReferenceOption, error) {
	switch refOp {
	case sql.ForeignKeyReferenceOption_DefaultAction:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, nil
	case sql.ForeignKeyReferenceOption_Restrict:
		return doltdb.ForeignKeyReferenceOption_Restrict, nil
	case sql.ForeignKeyReferenceOption_Cascade:
		return doltdb.ForeignKeyReferenceOption_Cascade, nil
	case sql.ForeignKeyReferenceOption_NoAction:
		return doltdb.ForeignKeyReferenceOption_NoAction, nil
	case sql.ForeignKeyReferenceOption_SetNull:
		return doltdb.ForeignKeyReferenceOption_SetNull, nil
	case sql.ForeignKeyReferenceOption_SetDefault:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, fmt.Errorf(`"SET DEFAULT" is not supported`)
	default:
		return doltdb.ForeignKeyReferenceOption_DefaultAction, fmt.Errorf("unknown foreign key reference option: %v", refOp)
	}
}

// createIndex creates the given index on the given table with the given schema. Returns the updated table, updated schema, and created index.
func createIndexForTable(ctx *sql.Context, table *doltdb.Table, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) (*doltdb.Table, schema.Schema, schema.Index, error) {
	if constraint != sql.IndexConstraint_None && constraint != sql.IndexConstraint_Unique {
		return nil, nil, nil, fmt.Errorf("not yet supported")
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// get the real column names as CREATE INDEX columns are case-insensitive
	var realColNames []string
	allTableCols := sch.GetAllCols()
	for _, indexCol := range columns {
		tableCol, ok := allTableCols.GetByNameCaseInsensitive(indexCol.Name)
		if !ok {
			return nil, nil, nil, fmt.Errorf("column `%s` does not exist for the table", indexCol.Name)
		}
		realColNames = append(realColNames, tableCol.Name)
	}

	if indexName == "" {
		indexName = strings.Join(realColNames, "")
		_, ok := sch.Indexes().GetByNameCaseInsensitive(indexName)
		var i int
		for ok {
			i++
			indexName = fmt.Sprintf("%s_%d", strings.Join(realColNames, ""), i)
			_, ok = sch.Indexes().GetByNameCaseInsensitive(indexName)
		}
	}
	if !doltdb.IsValidTableName(indexName) {
		return nil, nil, nil, fmt.Errorf("invalid index name `%s` as they must match the regular expression %s", indexName, doltdb.TableNameRegexStr)
	}

	// create the index metadata, will error if index names are taken or an index with the same columns in the same order exists
	index, err := sch.Indexes().AddIndexByColNames(indexName, realColNames, schema.IndexProperties{IsUnique: constraint == sql.IndexConstraint_Unique, Comment: comment})
	if err != nil {
		return nil, nil, nil, err
	}

	// update the table schema with the new index
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, table.ValueReadWriter(), sch)
	if err != nil {
		return nil, nil, nil, err
	}
	tableRowData, err := table.GetRowData(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	indexData, err := table.GetIndexData(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	newTable, err := doltdb.NewTable(ctx, table.ValueReadWriter(), newSchemaVal, tableRowData, &indexData)
	if err != nil {
		return nil, nil, nil, err
	}

	// set the index row data and get a new root with the updated table
	indexRowData, err := newTable.RebuildIndexRowData(ctx, index.Name())
	if err != nil {
		return nil, nil, nil, err
	}
	newTable, err = newTable.SetIndexRowData(ctx, index.Name(), indexRowData)
	if err != nil {
		return nil, nil, nil, err
	}
	return newTable, sch, index, nil
}

// dropIndex drops the given index on the given table with the given schema. Returns the updated table and updated schema.
func (t *AlterableDoltTable) dropIndex(ctx *sql.Context, indexName string) (*doltdb.Table, schema.Schema, error) {
	// RemoveIndex returns an error if the index does not exist, no need to do twice
	_, err := t.sch.Indexes().RemoveIndex(indexName)
	if err != nil {
		return nil, nil, err
	}
	newTable, err := t.table.UpdateSchema(ctx, t.sch)
	if err != nil {
		return nil, nil, err
	}
	newTable, err = newTable.DeleteIndexRowData(ctx, indexName)
	if err != nil {
		return nil, nil, err
	}
	tblSch, err := newTable.GetSchema(ctx)
	if err != nil {
		return nil, nil, err
	}
	return newTable, tblSch, nil
}

func (t *AlterableDoltTable) updateFromRoot(ctx *sql.Context, root *doltdb.RootValue) error {
	updatedTableSql, ok, err := t.db.getTable(ctx, root, t.name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table `%s` cannot find itself", t.name)
	}
	updatedTable := updatedTableSql.(*AlterableDoltTable)
	t.WritableDoltTable.DoltTable = updatedTable.WritableDoltTable.DoltTable
	return nil
}
