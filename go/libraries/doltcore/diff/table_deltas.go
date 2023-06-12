// Copyright 2019 Dolthub, Inc.
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

package diff

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/set"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
)

type TableDiffType int

const (
	AddedTable TableDiffType = iota
	ModifiedTable
	RenamedTable
	RemovedTable
)

type TableDeltaBase struct {
	FromName         string
	ToName           string
	FromSch          schema.Schema
	ToSch            schema.Schema
	FromFks          []doltdb.ForeignKey
	ToFks            []doltdb.ForeignKey
	ToFksParentSch   map[string]schema.Schema
	FromFksParentSch map[string]schema.Schema
}

type TableDeltaEngine struct {
	TableDeltaBase
	FromTable     *doltdb.Table
	ToTable       *doltdb.Table
	FromNodeStore tree.NodeStore
	ToNodeStore   tree.NodeStore
	FromVRW       types.ValueReadWriter
	ToVRW         types.ValueReadWriter
}

var _ TableDelta = &TableDeltaEngine{}

type TableDeltaSummary struct {
	DiffType      string
	DataChange    bool
	SchemaChange  bool
	TableName     string
	FromTableName string
	ToTableName   string
}

// GetStagedUnstagedTableDeltas represents staged and unstaged changes as TableDelta slices.
func GetStagedUnstagedTableDeltas(ctx context.Context, roots doltdb.Roots) (staged, unstaged []TableDeltaEngine, err error) {
	staged, err = GetTableDeltas(ctx, roots.Head, roots.Staged)
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltas(ctx, roots.Staged, roots.Working)
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots by finding Schemas with Column tags in common.
func GetTableDeltas(ctx context.Context, fromRoot, toRoot *doltdb.RootValue) ([]TableDeltaEngine, error) {
	fromVRW := fromRoot.VRW()
	fromNS := fromRoot.NodeStore()
	toVRW := toRoot.VRW()
	toNS := toRoot.NodeStore()

	fromDeltas := make([]TableDeltaEngine, 0)
	err := fromRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := fromRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}
		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, fromRoot, fks...)
		if err != nil {
			return false, err
		}

		fromDeltas = append(fromDeltas, TableDeltaEngine{
			TableDeltaBase: TableDeltaBase{
				FromName:         name,
				FromSch:          sch,
				FromFks:          fks,
				FromFksParentSch: parentSchs,
			},
			FromTable: tbl,
			//FromRoot:      fromRoot,
			FromVRW:       fromVRW,
			FromNodeStore: fromNS,
			ToVRW:         toVRW,
			ToNodeStore:   toNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	toDeltas := make([]TableDeltaEngine, 0)

	err = toRoot.IterTables(ctx, func(name string, tbl *doltdb.Table, sch schema.Schema) (stop bool, err error) {
		c, err := toRoot.GetForeignKeyCollection(ctx)
		if err != nil {
			return true, err
		}

		fks, _ := c.KeysForTable(name)
		parentSchs, err := getFkParentSchs(ctx, toRoot, fks...)
		if err != nil {
			return false, err
		}

		toDeltas = append(toDeltas, TableDeltaEngine{
			TableDeltaBase: TableDeltaBase{
				ToName:         name,
				ToSch:          sch,
				ToFks:          fks,
				ToFksParentSch: parentSchs,
			},
			ToTable: tbl,
			//ToRoot:      toRoot,
			ToVRW:       toVRW,
			ToNodeStore: toNS,

			FromVRW:       fromVRW,
			FromNodeStore: fromNS,
		})
		return
	})
	if err != nil {
		return nil, err
	}

	deltas, err := matchTableDeltas(fromDeltas, toDeltas)
	if err != nil {
		return nil, err
	}
	deltas, err = filterUnmodifiedTableDeltas(deltas)
	if err != nil {
		return nil, err
	}

	// Make sure we always return the same order of deltas
	sort.Slice(deltas, func(i, j int) bool {
		if deltas[i].FromName == deltas[j].FromName {
			return deltas[i].ToName < deltas[j].ToName
		}
		return deltas[i].FromName < deltas[j].FromName
	})

	return deltas, nil
}

func getFkParentSchs(ctx context.Context, root *doltdb.RootValue, fks ...doltdb.ForeignKey) (map[string]schema.Schema, error) {
	schs := make(map[string]schema.Schema)
	for _, toFk := range fks {
		toRefTable, _, ok, err := root.GetTableInsensitive(ctx, toFk.ReferencedTableName)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue // as the schemas are for display-only, we can skip on any missing parents (they were deleted, etc.)
		}
		toRefSch, err := toRefTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}
		schs[toFk.ReferencedTableName] = toRefSch
	}
	return schs, nil
}

func filterUnmodifiedTableDeltas(deltas []TableDeltaEngine) ([]TableDeltaEngine, error) {
	var filtered []TableDeltaEngine
	for _, d := range deltas {
		if d.ToTable == nil || d.FromTable == nil {
			// Table was added or dropped
			filtered = append(filtered, d)
			continue
		}

		hasChanges, err := d.HasChanges()
		if err != nil {
			return nil, err
		}

		if hasChanges {
			// Take only modified tables
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

func matchTableDeltas(fromDeltas, toDeltas []TableDeltaEngine) (deltas []TableDeltaEngine, err error) {
	var matchedNames []string
	from := make(map[string]TableDeltaEngine, len(fromDeltas))
	for _, f := range fromDeltas {
		from[f.FromName] = f
	}

	to := make(map[string]TableDeltaEngine, len(toDeltas))
	for _, t := range toDeltas {
		to[t.ToName] = t
		if _, ok := from[t.ToName]; ok {
			matchedNames = append(matchedNames, t.ToName)
		}
	}

	match := func(t, f TableDeltaEngine) (TableDeltaEngine, error) {
		return TableDeltaEngine{
			TableDeltaBase: TableDeltaBase{
				FromName:         f.FromName,
				ToName:           t.ToName,
				FromSch:          f.FromSch,
				ToSch:            t.ToSch,
				FromFks:          f.FromFks,
				ToFks:            t.ToFks,
				FromFksParentSch: f.FromFksParentSch,
				ToFksParentSch:   t.ToFksParentSch,
			},
			FromTable: f.FromTable,
			ToTable:   t.ToTable,
		}, nil
	}

	deltas = make([]TableDeltaEngine, 0)

	for _, name := range matchedNames {
		t := to[name]
		tInfo := t.GetBaseInfo()
		f := from[name]
		fInfo := f.GetBaseInfo()
		if schemasOverlap(tInfo.ToSch, fInfo.FromSch) {
			matched, err := match(t, f)
			if err != nil {
				return nil, err
			}
			deltas = append(deltas, matched)
			delete(from, fInfo.FromName)
			delete(to, tInfo.ToName)
		}
	}

	for _, f := range from {
		for _, t := range to {
			tInfo := t.GetBaseInfo()
			fInfo := f.GetBaseInfo()
			if schemasOverlap(fInfo.FromSch, tInfo.ToSch) {
				matched, err := match(t, f)
				if err != nil {
					return nil, err
				}
				deltas = append(deltas, matched)
				delete(from, fInfo.FromName)
				delete(to, tInfo.ToName)
			}
		}
	}

	// append unmatched TableDeltas
	for _, f := range from {
		deltas = append(deltas, f)
	}
	for _, t := range to {
		deltas = append(deltas, t)
	}

	return deltas, nil
}

func schemasOverlap(from, to schema.Schema) bool {
	f := set.NewUint64Set(from.GetAllCols().Tags)
	t := set.NewUint64Set(to.GetAllCols().Tags)
	return f.Intersection(t).Size() > 0
}

func (td TableDeltaEngine) GetTableCreateStatement(ctx context.Context, isFromTable bool) (string, error) {
	var sch schema.Schema
	var name string

	if isFromTable {
		sch = td.FromSch
		name = td.FromName
	} else {
		sch = td.ToSch
		name = td.ToName
	}

	sb := strings.Builder{}
	sb.WriteString("CREATE TABLE ")
	sb.WriteString(name)
	sb.WriteString(" (\n")

	err := sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		sb.WriteString("\t")
		sb.WriteString(col.Name)
		sb.WriteString(" ")
		sb.WriteString(col.TypeInfo.ToSqlType().String())
		sb.WriteString(",\n")
		return false, nil
	})
	if err != nil {
		return "", err
	}

	sb.WriteString(");")

	createStatement := sb.String()
	return createStatement, nil
}

func (td TableDeltaEngine) GetBaseInfo() TableDeltaBase {
	return td.TableDeltaBase
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (td TableDeltaEngine) IsAdd() bool {
	return td.FromTable == nil && td.ToTable != nil
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (td TableDeltaEngine) IsDrop() bool {
	return td.FromTable != nil && td.ToTable == nil
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (td TableDeltaEngine) IsRename() bool {
	if td.IsAdd() || td.IsDrop() {
		return false
	}
	return td.FromName != td.ToName
}

// HasHashChanged returns true if the hash of the table content has changed between
// the fromRoot and toRoot.
func (td TableDeltaEngine) HasHashChanged() (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	toHash, err := td.ToTable.HashOf()
	if err != nil {
		return false, err
	}

	fromHash, err := td.FromTable.HashOf()
	if err != nil {
		return false, err
	}

	return !toHash.Equal(fromHash), nil
}

// HasSchemaChanged returns true if the table schema has changed between the
// fromRoot and toRoot.
func (td TableDeltaEngine) HasSchemaChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	if td.HasFKChanges() {
		return true, nil
	}

	fromSchemaHash, err := td.FromTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	toSchemaHash, err := td.ToTable.GetSchemaHash(ctx)
	if err != nil {
		return false, err
	}

	return !fromSchemaHash.Equal(toSchemaHash), nil
}

func (td TableDeltaEngine) HasDataChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() {
		isEmpty, err := isTableDataEmpty(ctx, td.ToTable)
		if err != nil {
			return false, err
		}

		return !isEmpty, nil
	}

	if td.IsDrop() {
		isEmpty, err := isTableDataEmpty(ctx, td.FromTable)
		if err != nil {
			return false, err
		}
		return !isEmpty, nil
	}

	fromRowDataHash, err := td.FromTable.GetRowDataHash(ctx)
	if err != nil {
		return false, err
	}

	toRowDataHash, err := td.ToTable.GetRowDataHash(ctx)
	if err != nil {
		return false, err
	}

	return !fromRowDataHash.Equal(toRowDataHash), nil
}

func (td TableDeltaEngine) HasPrimaryKeySetChanged() bool {
	return !schema.ArePrimaryKeySetsDiffable(td.Format(), td.FromSch, td.ToSch)
}

func (td TableDeltaEngine) HasChanges() (bool, error) {
	hashChanged, err := td.HasHashChanged()
	if err != nil {
		return false, err
	}

	return td.HasFKChanges() || td.IsRename() || td.HasPrimaryKeySetChanged() || hashChanged, nil
}

// CurName returns the most recent name of the table.
func (td TableDeltaEngine) CurName() string {
	if td.ToName != "" {
		return td.ToName
	}
	return td.FromName
}

func (td TableDeltaEngine) HasFKChanges() bool {
	if len(td.FromFks) != len(td.ToFks) {
		return true
	}

	sort.Slice(td.FromFks, func(i, j int) bool {
		return td.FromFks[i].Name < td.FromFks[j].Name
	})
	sort.Slice(td.ToFks, func(i, j int) bool {
		return td.ToFks[i].Name < td.ToFks[j].Name
	})

	fromSchemaMap := td.FromFksParentSch
	fromSchemaMap[td.FromName] = td.FromSch
	toSchemaMap := td.ToFksParentSch
	toSchemaMap[td.ToName] = td.ToSch

	for i := range td.FromFks {
		if !td.FromFks[i].Equals(td.ToFks[i], fromSchemaMap, toSchemaMap) {
			return true
		}
	}

	return false
}

// GetSchemas returns the table's schema at the fromRoot and toRoot, or schema.Empty if the table did not exist.
func (td TableDeltaEngine) GetSchemas(ctx context.Context) (from, to schema.Schema, err error) {
	if td.FromSch == nil {
		td.FromSch = schema.EmptySchema
	}
	if td.ToSch == nil {
		td.ToSch = schema.EmptySchema
	}
	return td.FromSch, td.ToSch, nil
}

// Format returns the format of the tables in this delta.
func (td TableDeltaEngine) Format() *types.NomsBinFormat {
	if td.FromTable != nil {
		return td.FromTable.Format()
	}
	return td.ToTable.Format()
}

func (td TableDeltaEngine) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	// nil table is neither keyless nor keyed
	from, to := schema.IsKeyless(f), schema.IsKeyless(t)
	if td.FromTable == nil {
		return to, nil
	} else if td.ToTable == nil {
		return from, nil
	} else {
		if from && to {
			return true, nil
		} else if !from && !to {
			return false, nil
		} else {
			return false, fmt.Errorf("mismatched keyless and keyed schemas for table %s", td.CurName())
		}
	}
}

// isTableDataEmpty return true if the table does not contain any data
func isTableDataEmpty(ctx context.Context, table *doltdb.Table) (bool, error) {
	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return false, err
	}

	return rowData.Empty()
}

// GetSummary returns a summary of the table delta.
func (td TableDeltaEngine) GetSummary(ctx context.Context) (*TableDeltaSummary, error) {
	dataChange, err := td.HasDataChanged(ctx)
	if err != nil {
		return nil, err
	}

	// Dropping a table is always a schema change, and also a data change if the table contained data
	if td.IsDrop() {
		return &TableDeltaSummary{
			TableName:     td.FromName,
			FromTableName: td.FromName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "dropped",
		}, nil
	}

	// Creating a table is always a schema change, and also a data change if data was inserted
	if td.IsAdd() {
		return &TableDeltaSummary{
			TableName:    td.ToName,
			ToTableName:  td.ToName,
			DataChange:   dataChange,
			SchemaChange: true,
			DiffType:     "added",
		}, nil
	}

	// Renaming a table is always a schema change, and also a data change if the table data differs
	if td.IsRename() {
		return &TableDeltaSummary{
			TableName:     td.ToName,
			FromTableName: td.FromName,
			ToTableName:   td.ToName,
			DataChange:    dataChange,
			SchemaChange:  true,
			DiffType:      "renamed",
		}, nil
	}

	schemaChange, err := td.HasSchemaChanged(ctx)
	if err != nil {
		return nil, err
	}

	return &TableDeltaSummary{
		TableName:     td.FromName,
		FromTableName: td.FromName,
		ToTableName:   td.ToName,
		DataChange:    dataChange,
		SchemaChange:  schemaChange,
		DiffType:      "modified",
	}, nil
}

// GetRowData returns the table's row data at the fromRoot and toRoot, or an empty map if the table did not exist.
func (td TableDeltaEngine) GetRowData(ctx context.Context) (from, to durable.Index, err error) {
	if td.FromTable == nil && td.ToTable == nil {
		return nil, nil, fmt.Errorf("both from and to tables are missing from table delta")
	}

	if td.FromTable != nil {
		from, err = td.FromTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |FromTable| use the |ToTable|'s schema to make the index.
		from, _ = durable.NewEmptyIndex(ctx, td.FromVRW, td.FromNodeStore, td.ToSch)
	}

	if td.ToTable != nil {
		to, err = td.ToTable.GetRowData(ctx)
		if err != nil {
			return from, to, err
		}
	} else {
		// If there is no |ToTable| use the |FromTable|'s schema to make the index.
		to, _ = durable.NewEmptyIndex(ctx, td.ToVRW, td.ToNodeStore, td.FromSch)
	}

	return from, to, nil
}

// SqlSchemaDiff returns a slice of DDL statements that will transform the schema in the from delta to the schema in
// the to delta.
func SqlSchemaDiff(ctx context.Context, td TableDelta, toSchemas map[string]schema.Schema) ([]string, error) {
	tdInfo := td.GetBaseInfo()
	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve schema for table %s, cause: %s", tdInfo.ToName, err.Error())
	}

	var ddlStatements []string
	if td.IsDrop() {
		ddlStatements = append(ddlStatements, sqlfmt.DropTableStmt(tdInfo.FromName))
	} else if td.IsAdd() {
		toPkSch, err := sqlutil.FromDoltSchema(tdInfo.ToName, tdInfo.ToSch)
		if err != nil {
			return nil, err
		}
		stmt, err := GenerateCreateTableStatement(tdInfo.ToName, tdInfo.ToSch, toPkSch, tdInfo.ToFks, tdInfo.ToFksParentSch)
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		ddlStatements = append(ddlStatements, stmt)
	} else {
		stmts, err := GetNonCreateNonDropTableSqlSchemaDiff(td, toSchemas, fromSch, toSch)
		if err != nil {
			return nil, err
		}
		ddlStatements = append(ddlStatements, stmts...)
	}

	return ddlStatements, nil
}

// GetNonCreateNonDropTableSqlSchemaDiff returns any schema diff in SQL statements that is NEITHER 'CREATE TABLE' NOR 'DROP TABLE' statements.
func GetNonCreateNonDropTableSqlSchemaDiff(td TableDelta, toSchemas map[string]schema.Schema, fromSch, toSch schema.Schema) ([]string, error) {
	if td.IsAdd() || td.IsDrop() {
		// use add and drop specific methods
		return nil, nil
	}

	tdInfo := td.GetBaseInfo()

	var ddlStatements []string
	if tdInfo.FromName != tdInfo.ToName {
		ddlStatements = append(ddlStatements, sqlfmt.RenameTableStmt(tdInfo.FromName, tdInfo.ToName))
	}

	eq := schema.SchemasAreEqual(fromSch, toSch)
	if eq && !td.HasFKChanges() {
		return ddlStatements, nil
	}

	colDiffs, unionTags := DiffSchColumns(fromSch, toSch)
	for _, tag := range unionTags {
		cd := colDiffs[tag]
		switch cd.DiffType {
		case SchDiffNone:
		case SchDiffAdded:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddColStmt(tdInfo.ToName, sqlfmt.GenerateCreateTableColumnDefinition(*cd.New)))
		case SchDiffRemoved:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropColStmt(tdInfo.ToName, cd.Old.Name))
		case SchDiffModified:
			// Ignore any primary key set changes here
			if cd.Old.IsPartOfPK != cd.New.IsPartOfPK {
				continue
			}
			if cd.Old.Name != cd.New.Name {
				ddlStatements = append(ddlStatements, sqlfmt.AlterTableRenameColStmt(tdInfo.ToName, cd.Old.Name, cd.New.Name))
			}
		}
	}

	// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
	if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
		ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropPks(tdInfo.ToName))
		if toSch.GetPKCols().Size() > 0 {
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddPrimaryKeys(tdInfo.ToName, toSch.GetPKCols()))
		}
	}

	for _, idxDiff := range DiffSchIndexes(fromSch, toSch) {
		switch idxDiff.DiffType {
		case SchDiffNone:
		case SchDiffAdded:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(tdInfo.ToName, idxDiff.To))
		case SchDiffRemoved:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(tdInfo.FromName, idxDiff.From))
		case SchDiffModified:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(tdInfo.FromName, idxDiff.From))
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(tdInfo.ToName, idxDiff.To))
		}
	}

	for _, fkDiff := range DiffForeignKeys(tdInfo.FromFks, tdInfo.ToFks) {
		switch fkDiff.DiffType {
		case SchDiffNone:
		case SchDiffAdded:
			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
		case SchDiffRemoved:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
		case SchDiffModified:
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))

			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
		}
	}

	return ddlStatements, nil
}

// GetDataDiffStatement returns any data diff in SQL statements for given table including INSERT, UPDATE and DELETE row statements.
func GetDataDiffStatement(tableName string, sch schema.Schema, row sql.Row, rowDiffType ChangeType, colDiffTypes []ChangeType) (string, error) {
	if len(row) != len(colDiffTypes) {
		return "", fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	switch rowDiffType {
	case Added:
		return sqlfmt.SqlRowAsInsertStmt(row, tableName, sch)
	case Removed:
		return sqlfmt.SqlRowAsDeleteStmt(row, tableName, sch, 0)
	case ModifiedNew:
		updatedCols := set.NewEmptyStrSet()
		for i, diffType := range colDiffTypes {
			if diffType != None {
				updatedCols.Add(sch.GetAllCols().GetByIndex(i).Name)
			}
		}
		return sqlfmt.SqlRowAsUpdateStmt(row, tableName, sch, updatedCols)
	case ModifiedOld:
		// do nothing, we only issue UPDATE for ModifiedNew
		return "", nil
	default:
		return "", fmt.Errorf("unexpected row diff type: %v", rowDiffType)
	}
}

// GenerateCreateTableStatement returns CREATE TABLE statement for given table. This function was made to share the same
// 'create table' statement logic as GMS. We initially were running `SHOW CREATE TABLE` query to get the statement;
// however, it cannot be done for cases that need this statement in sql shell mode. Dolt uses its own Schema and
// Column and other object types which are not directly compatible with GMS, so we try to use as much shared logic
// as possible with GMS to get 'create table' statement in Dolt.
func GenerateCreateTableStatement(tblName string, sch schema.Schema, pkSchema sql.PrimaryKeySchema, fks []doltdb.ForeignKey, fksParentSch map[string]schema.Schema) (string, error) {
	sqlSch := pkSchema.Schema
	colStmts := make([]string, len(sqlSch))

	// Statement creation parts for each column
	for i, col := range sch.GetAllCols().GetColumns() {
		colStmts[i] = sqlfmt.GenerateCreateTableIndentedColumnDefinition(col)
	}

	primaryKeyCols := sch.GetPKCols().GetColumnNames()
	if len(primaryKeyCols) > 0 {
		primaryKey := sql.GenerateCreateTablePrimaryKeyDefinition(primaryKeyCols)
		colStmts = append(colStmts, primaryKey)
	}

	indexes := sch.Indexes().AllIndexes()
	for _, index := range indexes {
		// The primary key may or may not be declared as an index by the table. Don't print it twice if it's here.
		if isPrimaryKeyIndex(index, sch) {
			continue
		}
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableIndexDefinition(index))
	}

	for _, fk := range fks {
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableForeignKeyDefinition(fk, sch, fksParentSch[fk.ReferencedTableName]))
	}

	for _, check := range sch.Checks().AllChecks() {
		colStmts = append(colStmts, sqlfmt.GenerateCreateTableCheckConstraintClause(check))
	}

	coll := sql.CollationID(sch.GetCollation())
	createTableStmt := sql.GenerateCreateTableStatement(tblName, colStmts, coll.CharacterSet().Name(), coll.Name())
	return fmt.Sprintf("%s;", createTableStmt), nil
}

// isPrimaryKeyIndex returns whether the index given matches the table's primary key columns. Order is not considered.
func isPrimaryKeyIndex(index schema.Index, sch schema.Schema) bool {
	var pks = sch.GetPKCols().GetColumns()
	var pkMap = make(map[string]struct{})
	for _, c := range pks {
		pkMap[c.Name] = struct{}{}
	}

	indexCols := index.ColumnNames()
	if len(indexCols) != len(pks) {
		return false
	}

	for _, c := range index.ColumnNames() {
		if _, ok := pkMap[c]; !ok {
			return false
		}
	}

	return true
}
