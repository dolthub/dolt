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
	"strings"

	"github.com/liquidata-inc/go-mysql-server/sql"
	"vitess.io/vitess/go/sqltypes"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
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

	tag := extractTag(column)
	if tag != existingCol.Tag && tag != schema.InvalidTag {
		return errors.New("cannot change the tag of an existing column")
	}

	col, err := SqlColToDoltCol(existingCol.Tag, column)
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

// CreateIndex implements sql.IndexAlterableTable
func (t *AlterableDoltTable) CreateIndex(ctx *sql.Context, indexName string, using sql.IndexUsing, constraint sql.IndexConstraint, columns []sql.IndexColumn, comment string) error {
	newTable, _, _, err := t.createIndex(ctx, t.table, t.sch, false, indexName, using, constraint, columns, comment)
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
	newTable, _, err := t.dropIndex(ctx, t.table, t.sch, indexName)
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
func (t *AlterableDoltTable) CreateForeignKey(ctx *sql.Context, fkName string, columns []string, referencedTable string, referencedColumns []string,
	onUpdate, onDelete sql.ForeignKeyReferenceOption) error {
	if fkName != "" && !doltdb.IsValidTableName(fkName) {
		return fmt.Errorf("invalid foreign key name `%s` as it must match the regular expression %s", fkName, doltdb.TableNameRegexStr)
	}
	//TODO: move this into go-mysql-server
	if len(columns) != len(referencedColumns) {
		return fmt.Errorf("the foreign key must reference an equivalent number of columns")
	}

	root, err := t.db.GetRoot(ctx)
	if err != nil {
		return err
	}
	refTable, refTableName, ok, err := root.GetTableInsensitive(ctx, referencedTable)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("referenced table `%s` does not exist", referencedTable)
	}
	refTableSch, err := refTable.GetSchema(ctx)
	if err != nil {
		return err
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

	refColTags := make([]uint64, len(referencedColumns))
	sqlRefColNames := make([]sql.IndexColumn, len(referencedColumns))
	for i, col := range referencedColumns {
		tableCol, ok := refTableSch.GetAllCols().GetByNameCaseInsensitive(col)
		if !ok {
			return fmt.Errorf("table `%s` does not have column `%s`", refTableName, col)
		}
		refColTags[i] = tableCol.Tag
		sqlRefColNames[i] = sql.IndexColumn{
			Name:   tableCol.Name,
			Length: 0,
		}
		if !tblCols[i].TypeInfo.Equals(tableCol.TypeInfo) {
			return fmt.Errorf("column type mismatch on `%s` and `%s`", columns[i], tableCol.Name)
		}
		sqlparserType := tableCol.TypeInfo.ToSqlType().Type()
		if sqlparserType == sqltypes.Blob || sqlparserType == sqltypes.Text {
			return fmt.Errorf("TEXT/BLOB are not valid types for foreign keys")
		}
	}

	onUpdateRefOp, err := t.parseFkReferenceOption(onUpdate)
	if err != nil {
		return err
	}
	onDeleteRefOp, err := t.parseFkReferenceOption(onDelete)
	if err != nil {
		return err
	}

	uniqueIndexName := func(tblSch schema.Schema) string {
		indexName := "dolt_fk_1"
		for i := 2; tblSch.Indexes().Contains(indexName); i++ {
			indexName = fmt.Sprintf("dolt_fk_%d", i)
		}
		return indexName
	}
	newTable, _, tableIndex, err := t.createIndex(ctx, t.table, t.sch, true, uniqueIndexName(t.sch), sql.IndexUsing_Default, sql.IndexConstraint_None, sqlColNames, "")
	if err != nil {
		return err
	}
	newRefTable, _, refTableIndex, err := t.createIndex(ctx, refTable, refTableSch, true, uniqueIndexName(refTableSch), sql.IndexUsing_Default, sql.IndexConstraint_None, sqlRefColNames, "")
	if err != nil {
		return err
	}

	newRoot, err := root.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.PutTable(ctx, refTableName, newRefTable)
	if err != nil {
		return err
	}

	foreignKeyCollection, err := newRoot.GetForeignKeyCollection(ctx)
	if err != nil {
		return err
	}
	foreignKey := &doltdb.ForeignKey{
		Name:                   fkName,
		TableName:              t.name,
		TableIndex:             tableIndex.Name(),
		TableColumns:           colTags,
		ReferencedTableName:    refTableName,
		ReferencedTableIndex:   refTableIndex.Name(),
		ReferencedTableColumns: refColTags,
		OnUpdate:               onUpdateRefOp,
		OnDelete:               onDeleteRefOp,
	}
	err = foreignKeyCollection.AddKey(foreignKey)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.PutForeignKeyCollection(ctx, foreignKeyCollection)
	if err != nil {
		return err
	}

	tableIndexData, err := newTable.GetIndexRowData(ctx, tableIndex.Name())
	if err != nil {
		return err
	}
	refTableIndexData, err := newRefTable.GetIndexRowData(ctx, refTableIndex.Name())
	if err != nil {
		return err
	}
	err = foreignKey.ValidateData(ctx, tableIndexData, refTableIndex, refTableIndexData)
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
	foreignKey, err := fkc.RemoveKey(fkName)
	if err != nil {
		return err
	}
	newRoot, err := root.PutForeignKeyCollection(ctx, fkc)

	newTable, _, err := t.dropIndex(ctx, t.table, t.sch, foreignKey.TableIndex)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.PutTable(ctx, t.name, newTable)
	if err != nil {
		return err
	}

	referencedTable, ok, err := newRoot.GetTable(ctx, foreignKey.ReferencedTableName)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unable to find referenced table `%s`", foreignKey.ReferencedTableName)
	}
	referencedSch, err := referencedTable.GetSchema(ctx)
	if err != nil {
		return err
	}
	newOtherTable, _, err := t.dropIndex(ctx, referencedTable, referencedSch, foreignKey.ReferencedTableIndex)
	if err != nil {
		return err
	}
	newRoot, err = newRoot.PutTable(ctx, foreignKey.ReferencedTableName, newOtherTable)
	if err != nil {
		return err
	}

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

	fks, err := fkc.KeysForDisplay(ctx, t.name, root)
	if err != nil {
		return nil, err
	}

	toReturn := make([]sql.ForeignKeyConstraint, len(fks))
	for i, fk := range fks {
		toReturn[i] = t.toForeignKeyConstraint(fk)
	}

	return toReturn, nil
}

func (t *AlterableDoltTable) toForeignKeyConstraint(key *doltdb.DisplayForeignKey) sql.ForeignKeyConstraint {
	return sql.ForeignKeyConstraint{
		Name:              key.Name,
		Columns:           key.TableColumns,
		ReferencedTable:   key.ReferencedTableName,
		ReferencedColumns: key.ReferencedTableColumns,
		OnUpdate:          toReferenceOption(key.OnUpdate),
		OnDelete:          toReferenceOption(key.OnDelete),
	}
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

// createIndex creates the given index on the given table with the given schema. Although this is called on an instance
// of AlterableDoltTable, it is only a method to free up the package namespace, and is not dependent (nor modifies) the
// instance. Returns the updated table, updated schema, and created index.
func (t *AlterableDoltTable) createIndex(
	ctx *sql.Context,
	tbl *doltdb.Table,
	tblSch schema.Schema,
	hidden bool,
	indexName string,
	using sql.IndexUsing,
	constraint sql.IndexConstraint,
	columns []sql.IndexColumn,
	comment string,
) (*doltdb.Table, schema.Schema, schema.Index, error) {
	if constraint != sql.IndexConstraint_None && constraint != sql.IndexConstraint_Unique {
		return nil, nil, nil, fmt.Errorf("not yet supported")
	}

	if indexName == "" {
		indexName = columns[0].Name
		for i := 2; tblSch.Indexes().Contains(indexName); i++ {
			indexName = fmt.Sprintf("%s_%d", columns[0].Name, i)
		}
	}
	if !hidden && !doltdb.IsValidTableName(indexName) {
		return nil, nil, nil, fmt.Errorf("invalid index name `%s` as they must match the regular expression %s", indexName, doltdb.TableNameRegexStr)
	}

	// get the real column names as CREATE INDEX columns are case-insensitive
	var realColNames []string
	allTableCols := tblSch.GetAllCols()
	for _, indexCol := range columns {
		tableCol, ok := allTableCols.GetByNameCaseInsensitive(indexCol.Name)
		if !ok {
			return nil, nil, nil, fmt.Errorf("column `%s` does not exist for the table", indexCol.Name)
		}
		realColNames = append(realColNames, tableCol.Name)
	}

	// create the index metadata, will error if index names are taken or an index with the same columns in the same order exists
	index, err := tblSch.Indexes().AddIndexByColNames(indexName, realColNames, schema.IndexProperties{IsUnique: constraint == sql.IndexConstraint_Unique, IsHidden: hidden, Comment: comment})
	if err != nil {
		return nil, nil, nil, err
	}

	// update the table schema with the new index
	newSchemaVal, err := encoding.MarshalSchemaAsNomsValue(ctx, tbl.ValueReadWriter(), tblSch)
	if err != nil {
		return nil, nil, nil, err
	}
	tableRowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	indexData, err := tbl.GetIndexData(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	newTable, err := doltdb.NewTable(ctx, tbl.ValueReadWriter(), newSchemaVal, tableRowData, &indexData)
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
	return newTable, tblSch, index, nil
}

// dropIndex drops the given index on the given table with the given schema. Although this is called on an instance
// of AlterableDoltTable, it is only a method to free up the package namespace, and is not dependent (nor modifies) the
// instance. Returns the updated table and updated schema.
func (t *AlterableDoltTable) dropIndex(ctx *sql.Context, tbl *doltdb.Table, tblSch schema.Schema, indexName string) (*doltdb.Table, schema.Schema, error) {
	// RemoveIndex returns an error if the index does not exist, no need to do twice
	_, err := tblSch.Indexes().RemoveIndex(indexName)
	if err != nil {
		return nil, nil, err
	}
	newTable, err := tbl.UpdateSchema(ctx, tblSch)
	if err != nil {
		return nil, nil, err
	}
	newTable, err = newTable.DeleteIndexRowData(ctx, indexName)
	if err != nil {
		return nil, nil, err
	}

	return newTable, tblSch, nil
}

func (t *AlterableDoltTable) parseFkReferenceOption(refOp sql.ForeignKeyReferenceOption) (doltdb.ForeignKeyReferenceOption, error) {
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
