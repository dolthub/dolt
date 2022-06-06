// Copyright 2022 Dolthub, Inc.
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
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

// renameTable renames a table with in a RootValue and returns the updated root.
func renameTable(ctx context.Context, root *doltdb.RootValue, oldName, newName string) (*doltdb.RootValue, error) {
	if newName == oldName {
		return root, nil
	} else if root == nil {
		panic("invalid parameters")
	}

	return root.RenameTable(ctx, oldName, newName)
}

// Nullable represents whether a column can have a null value.
type Nullable bool

const (
	NotNull Nullable = false
	Null    Nullable = true
)

// addColumnToTable adds a new column to the schema given and returns the new table value. Non-null column additions
// rewrite the entire table, since we must write a value for each row. If the column is not nullable, a default value
// must be provided.
//
// Returns an error if the column added conflicts with the existing schema in tag or name.
func addColumnToTable(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
	nullable Nullable,
	defaultVal *sql.ColumnDefaultValue,
	comment string,
	order *sql.ColumnOrder,
) (*doltdb.Table, error) {
	oldSchema, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(oldSchema) {
		return nil, ErrKeylessAltTbl
	}

	if err := validateNewColumn(ctx, root, tbl, tblName, tag, newColName, typeInfo); err != nil {
		return nil, err
	}

	newCol, err := createColumn(nullable, newColName, tag, typeInfo, defaultVal.String(), comment)
	if err != nil {
		return nil, err
	}

	newSchema, err := oldSchema.AddColumn(newCol, orderToOrder(order))
	if err != nil {
		return nil, err
	}

	newTable, err := tbl.UpdateSchema(ctx, newSchema)
	if err != nil {
		return nil, err
	}

	// TODO: we do a second pass in the engine to set a default if there is one. We should only do a single table scan.
	return newTable.AddColumnToRows(ctx, newColName, newSchema)
}

func orderToOrder(order *sql.ColumnOrder) *schema.ColumnOrder {
	if order == nil {
		return nil
	}
	return &schema.ColumnOrder{
		First:       order.First,
		AfterColumn: order.AfterColumn,
	}
}

func createColumn(nullable Nullable, newColName string, tag uint64, typeInfo typeinfo.TypeInfo, defaultVal, comment string) (schema.Column, error) {
	if nullable {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment)
	} else {
		return schema.NewColumnWithTypeInfo(newColName, tag, typeInfo, false, defaultVal, false, comment, schema.NotNullConstraint{})
	}
}

// ValidateNewColumn returns an error if the column as specified cannot be added to the schema given.
func validateNewColumn(
	ctx context.Context,
	root *doltdb.RootValue,
	tbl *doltdb.Table,
	tblName string,
	tag uint64,
	newColName string,
	typeInfo typeinfo.TypeInfo,
) error {
	if typeInfo == nil {
		return fmt.Errorf(`typeinfo may not be nil`)
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return err
	}

	cols := sch.GetAllCols()
	err = cols.Iter(func(currColTag uint64, currCol schema.Column) (stop bool, err error) {
		if currColTag == tag {
			return false, schema.ErrTagPrevUsed(tag, newColName, tblName)
		} else if strings.ToLower(currCol.Name) == strings.ToLower(newColName) {
			return true, fmt.Errorf("A column with the name %s already exists in table %s.", newColName, tblName)
		}

		return false, nil
	})

	if err != nil {
		return err
	}

	_, tblName, found, err := root.GetTableByColTag(ctx, tag)
	if err != nil {
		return err
	}
	if found {
		return schema.ErrTagPrevUsed(tag, newColName, tblName)
	}

	return nil
}

var ErrPrimaryKeySetsIncompatible = errors.New("primary key sets incompatible")

// modifyColumn modifies the column with the name given, replacing it with the new definition provided. A column with
// the name given must exist in the schema of the table.
func modifyColumn(
	ctx context.Context,
	tbl *doltdb.Table,
	existingCol schema.Column,
	newCol schema.Column,
	order *sql.ColumnOrder,
) (*doltdb.Table, error) {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: write test of changing column case

	// Modify statements won't include key info, so fill it in from the old column
	// TODO: fix this in GMS
	if existingCol.IsPartOfPK {
		newCol.IsPartOfPK = true
		if schema.IsColSpatialType(newCol) {
			return nil, fmt.Errorf("can't use Spatial Types as Primary Key for table")
		}
		foundNotNullConstraint := false
		for _, constraint := range newCol.Constraints {
			if _, ok := constraint.(schema.NotNullConstraint); ok {
				foundNotNullConstraint = true
				break
			}
		}
		if !foundNotNullConstraint {
			newCol.Constraints = append(newCol.Constraints, schema.NotNullConstraint{})
		}
	}

	newSchema, err := replaceColumnInSchema(sch, existingCol, newCol, order)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateSchema(ctx, newSchema)
}

// replaceColumnInSchema replaces the column with the name given with its new definition, optionally reordering it.
// TODO: make this a schema API?
func replaceColumnInSchema(sch schema.Schema, oldCol schema.Column, newCol schema.Column, order *sql.ColumnOrder) (schema.Schema, error) {
	// If no order is specified, insert in the same place as the existing column
	prevColumn := ""
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name == oldCol.Name {
			if prevColumn == "" {
				if order == nil {
					order = &sql.ColumnOrder{First: true}
				}
			}
			break
		} else {
			prevColumn = col.Name
		}
	}

	if order == nil {
		if prevColumn != "" {
			order = &sql.ColumnOrder{AfterColumn: prevColumn}
		} else {
			return nil, fmt.Errorf("Couldn't find column %s", oldCol.Name)
		}
	}

	var newCols []schema.Column
	if order.First {
		newCols = append(newCols, newCol)
	}

	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name != oldCol.Name {
			newCols = append(newCols, col)
		}

		if order.AfterColumn == col.Name {
			newCols = append(newCols, newCol)
		}
	}

	collection := schema.NewColCollection(newCols...)

	err := schema.ValidateForInsert(collection)
	if err != nil {
		return nil, err
	}

	newSch, err := schema.SchemaFromCols(collection)
	if err != nil {
		return nil, err
	}
	for _, index := range sch.Indexes().AllIndexes() {
		tags := index.IndexedColumnTags()
		for i := range tags {
			if tags[i] == oldCol.Tag {
				tags[i] = newCol.Tag
			}
		}
		_, err = newSch.Indexes().AddIndexByColTags(index.Name(), tags, schema.IndexProperties{
			IsUnique:      index.IsUnique(),
			IsUserDefined: index.IsUserDefined(),
			Comment:       index.Comment(),
		})
		if err != nil {
			return nil, err
		}
	}

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSch.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	pkOrds, err := modifyPkOrdinals(sch, newSch)
	if err != nil {
		return nil, err
	}
	err = newSch.SetPkOrdinals(pkOrds)
	if err != nil {
		return nil, err
	}
	return newSch, nil
}

// modifyPkOrdinals tries to create primary key ordinals for a newSch maintaining
// the relative positions of PKs from the oldSch. Return an ErrPrimaryKeySetsIncompatible
// error if the two schemas have a different number of primary keys, or a primary
// key column's tag changed between the two sets.
// TODO: move this to schema package
func modifyPkOrdinals(oldSch, newSch schema.Schema) ([]int, error) {
	if newSch.GetPKCols().Size() != oldSch.GetPKCols().Size() {
		return nil, ErrPrimaryKeySetsIncompatible
	}

	newPkOrdinals := make([]int, len(newSch.GetPkOrdinals()))
	for _, newCol := range newSch.GetPKCols().GetColumns() {
		// ordIdx is the relative primary key order (that stays the same)
		ordIdx, ok := oldSch.GetPKCols().TagToIdx[newCol.Tag]
		if !ok {
			// if pk tag changed, use name to find the new newCol tag
			oldCol, ok := oldSch.GetPKCols().NameToCol[newCol.Name]
			if !ok {
				return nil, ErrPrimaryKeySetsIncompatible
			}
			ordIdx = oldSch.GetPKCols().TagToIdx[oldCol.Tag]
		}

		// ord is the schema ordering index, which may have changed in newSch
		ord := newSch.GetAllCols().TagToIdx[newCol.Tag]
		newPkOrdinals[ordIdx] = ord
	}

	return newPkOrdinals, nil
}

func addPrimaryKeyToTable(ctx context.Context, table *doltdb.Table, tableName string, nbf *types.NomsBinFormat, columns []sql.IndexColumn, opts editor.Options) (*doltdb.Table, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.GetPKCols().Size() > 0 {
		return nil, sql.ErrMultiplePrimaryKeysDefined.New() // Also caught in GMS
	}

	if schema.IsUsingSpatialColAsKey(sch) {
		return nil, schema.ErrUsingSpatialKey.New(tableName)
	}

	pkColOrdering := make(map[string]int, len(columns))
	for i, newCol := range columns {
		pkColOrdering[newCol.Name] = i
	}

	newColl := make([]schema.Column, sch.GetAllCols().Size())
	pkOrdinals := make([]int, len(columns))
	for ord, col := range sch.GetAllCols().GetColumns() {
		if i, ok := pkColOrdering[col.Name]; ok {
			pkOrdinals[i] = ord
			// Only add NOT NULL constraint iff it doesn't exist
			if col.IsNullable() {
				col.Constraints = append(col.Constraints, schema.NotNullConstraint{})
			}
			col.IsPartOfPK = true
		}
		newColl[ord] = col
	}
	newCollection := schema.NewColCollection(newColl...)

	rows, err := table.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	// enforce primary key nullability
	err = rows.Iter(ctx, func(key, value types.Value) (stop bool, err error) {
		r, err := row.FromNoms(sch, key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return false, err
		}

		err = newCollection.Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if !col.IsPartOfPK {
				return false, nil
			}

			val, ok := r.GetColVal(tag)
			if !ok || val == nil || val == types.NullValue {
				return true, fmt.Errorf("primary key cannot have NULL values")
			}
			return false, nil
		})

		if err != nil {
			return true, err
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	newSchema, err := schema.SchemaFromCols(newCollection)
	if err != nil {
		return nil, err
	}

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSchema.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	err = newSchema.SetPkOrdinals(pkOrdinals)
	if err != nil {
		return nil, err
	}
	newSchema.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Rebuild all of the indexes now that the primary key has been changed
	return insertKeyedData(ctx, nbf, table, newSchema, tableName, opts)
}

func insertKeyedData(ctx context.Context, nbf *types.NomsBinFormat, oldTable *doltdb.Table, newSchema schema.Schema, name string, opts editor.Options) (*doltdb.Table, error) {
	empty, err := types.NewMap(ctx, oldTable.ValueReadWriter())
	if err != nil {
		return nil, err
	}

	// Create the new Table and rebuild all the indexes
	newTable, err := doltdb.NewNomsTable(ctx, oldTable.ValueReadWriter(), newSchema, empty, nil, nil)
	if err != nil {
		return nil, err
	}

	newTable, err = editor.RebuildAllIndexes(ctx, newTable, opts)
	if err != nil {
		return nil, err
	}

	// Create the table editor and insert all of the new data into it
	tableEditor, err := editor.NewTableEditor(ctx, newTable, newSchema, name, opts)
	if err != nil {
		return nil, err
	}

	oldRowData, err := oldTable.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	err = oldRowData.Iter(ctx, func(key types.Value, value types.Value) (stop bool, err error) {
		keyless, card, err := row.KeylessRowsFromTuples(key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}

		// A row that exists more than once must be a duplicate.
		if card > 1 {
			return true, fmtPrimaryKeyError(newSchema, keyless)
		}

		taggedVals, err := keyless.TaggedValues()
		if err != nil {
			return true, err
		}

		keyedRow, err := row.New(nbf, newSchema, taggedVals)
		if err != nil {
			return true, err
		}

		err = tableEditor.InsertRow(ctx, keyedRow, duplicatePkFunction)
		if err != nil {
			return true, err
		}

		return false, nil
	})

	if err != nil {
		return nil, err
	}

	return tableEditor.Table(ctx)
}

func fmtPrimaryKeyError(sch schema.Schema, keylessRow row.Row) error {
	pkTags := sch.GetPKCols().Tags

	vals := make([]string, len(pkTags))
	for i, tg := range sch.GetPKCols().Tags {
		val, ok := keylessRow.GetColVal(tg)
		if !ok {
			panic("tag for primary key wasn't found")
		}

		vals[i] = val.HumanReadableString()
	}

	return sql.NewUniqueKeyErr(fmt.Sprintf("[%s]", strings.Join(vals, ",")), true, sql.Row{vals})
}

func duplicatePkFunction(keyString, indexName string, k, v types.Tuple, isPk bool) error {
	return sql.NewUniqueKeyErr(fmt.Sprintf("%s", keyString), true, sql.Row{})
}

var ErrKeylessAltTbl = errors.New("schema alterations not supported for keyless tables")

// dropColumn drops a column from a table, and removes its associated cell values
func dropColumn(ctx context.Context, tbl *doltdb.Table, colName string) (*doltdb.Table, error) {
	if tbl == nil {
		panic("invalid parameters")
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if schema.IsKeyless(sch) {
		return nil, ErrKeylessAltTbl
	}

	if col, ok := sch.GetAllCols().GetByName(colName); !ok {
		return nil, schema.ErrColNotFound
	} else if col.IsPartOfPK {
		return nil, errors.New("Cannot drop column in primary key")
	}

	for _, index := range sch.Indexes().IndexesWithColumn(colName) {
		_, err = sch.Indexes().RemoveIndex(index.Name())
		if err != nil {
			return nil, err
		}
		tbl, err = tbl.DeleteIndexRowData(ctx, index.Name())
		if err != nil {
			return nil, err
		}
	}

	cols := make([]schema.Column, 0)
	for _, col := range sch.GetAllCols().GetColumns() {
		if col.Name == colName {
			continue
		}
		cols = append(cols, col)
	}

	colColl := schema.NewColCollection(cols...)
	newSch, err := schema.SchemaFromCols(colColl)
	if err != nil {
		return nil, err
	}
	newSch.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err = newSch.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	pkOrds, err := modifyPkOrdinals(sch, newSch)
	if err != nil {
		return nil, err
	}
	err = newSch.SetPkOrdinals(pkOrds)
	if err != nil {
		return nil, err
	}

	return tbl.UpdateSchema(ctx, newSch)
}

// backupFkcIndexesForKeyDrop finds backup indexes to cover foreign key references during a primary
// key drop. If multiple indexes are valid, we sort by unique and select the first.
// This will not work with a non-pk index drop without an additional index filter argument.
func backupFkcIndexesForPkDrop(ctx *sql.Context, sch schema.Schema, fkc *doltdb.ForeignKeyCollection) ([]doltdb.FkIndexUpdate, error) {
	indexes := sch.Indexes().AllIndexes()

	// pkBackups is a mapping from the table's PK tags to potentially compensating indexes
	pkBackups := make(map[uint64][]schema.Index, len(sch.GetPKCols().TagToIdx))
	for tag, _ := range sch.GetPKCols().TagToIdx {
		pkBackups[tag] = nil
	}

	// prefer unique key backups
	sort.Slice(indexes[:], func(i, j int) bool {
		return indexes[i].IsUnique() && !indexes[j].IsUnique()
	})

	for _, idx := range indexes {
		if !idx.IsUserDefined() {
			continue
		}

		for _, tag := range idx.AllTags() {
			if _, ok := pkBackups[tag]; ok {
				pkBackups[tag] = append(pkBackups[tag], idx)
			}
		}
	}

	fkUpdates := make([]doltdb.FkIndexUpdate, 0)
	for _, fk := range fkc.AllKeys() {
		// check if this FK references a parent PK tag we are trying to change
		if backups, ok := pkBackups[fk.ReferencedTableColumns[0]]; ok {
			covered := false
			for _, idx := range backups {
				idxTags := idx.AllTags()
				if len(fk.TableColumns) > len(idxTags) {
					continue
				}
				failed := false
				for i := 0; i < len(fk.ReferencedTableColumns); i++ {
					if idxTags[i] != fk.ReferencedTableColumns[i] {
						failed = true
						break
					}
				}
				if failed {
					continue
				}
				fkUpdates = append(fkUpdates, doltdb.FkIndexUpdate{FkName: fk.Name, FromIdx: fk.ReferencedTableIndex, ToIdx: idx.Name()})
				covered = true
				break
			}
			if !covered {
				return nil, sql.ErrCantDropIndex.New("PRIMARY")
			}
		}
	}
	return fkUpdates, nil
}

func dropPrimaryKeyFromTable(ctx context.Context, table *doltdb.Table, nbf *types.NomsBinFormat, opts editor.Options) (*doltdb.Table, error) {
	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	if sch.GetPKCols().Size() == 0 {
		return nil, sql.ErrCantDropFieldOrKey.New("PRIMARY")
	}

	// Modify the schema to convert the primary key cols into non primary key cols
	newCollection := schema.MapColCollection(sch.GetAllCols(), func(col schema.Column) schema.Column {
		// If it was part of primary key
		if col.IsPartOfPK {
			// No longer part of primary key
			col.IsPartOfPK = false
			// Removing PK does not remove NOT NULL constraint, so add it back if it's somehow gone
			if col.IsNullable() {
				col.Constraints = append(col.Constraints, schema.NotNullConstraint{})
			}
		}
		return col
	})

	newSchema, err := schema.SchemaFromCols(newCollection)
	if err != nil {
		return nil, err
	}

	newSchema.Indexes().AddIndex(sch.Indexes().AllIndexes()...)

	// Copy over all checks from the old schema
	for _, check := range sch.Checks().AllChecks() {
		_, err := newSchema.Checks().AddCheck(check.Name(), check.Expression(), check.Enforced())
		if err != nil {
			return nil, err
		}
	}

	table, err = table.UpdateSchema(ctx, newSchema)
	if err != nil {
		return nil, err
	}

	// Convert all of the keyed row data to keyless row data
	rowData, err := table.GetNomsRowData(ctx)
	if err != nil {
		return nil, err
	}

	newRowData, err := keyedRowDataToKeylessRowData(ctx, nbf, table.ValueReadWriter(), rowData, newSchema)
	if err != nil {
		return nil, err
	}

	table, err = table.UpdateNomsRows(ctx, newRowData)
	if err != nil {
		return nil, err
	}

	// Rebuild all of the indexes now that the primary key has been changed
	return editor.RebuildAllIndexes(ctx, table, opts)
}

func keyedRowDataToKeylessRowData(ctx context.Context, nbf *types.NomsBinFormat, vrw types.ValueReadWriter, rowData types.Map, newSch schema.Schema) (types.Map, error) {
	newMap, err := types.NewMap(ctx, vrw)
	if err != nil {
		return types.Map{}, err
	}

	mapEditor := newMap.Edit()

	err = rowData.Iter(ctx, func(key types.Value, value types.Value) (stop bool, err error) {
		taggedVals, err := row.TaggedValuesFromTupleKeyAndValue(key.(types.Tuple), value.(types.Tuple))
		if err != nil {
			return true, err
		}

		keyedRow, err := row.New(nbf, newSch, taggedVals)
		if err != nil {
			return true, nil
		}

		mapEditor = mapEditor.Set(keyedRow.NomsMapKey(newSch), keyedRow.NomsMapValue(newSch))

		return false, nil
	})

	if err != nil {
		return types.Map{}, err
	}

	return mapEditor.Map(ctx)
}
