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
	"github.com/dolthub/dolt/go/libraries/utils/queries"
	"github.com/dolthub/dolt/go/store/types"
	"sort"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

// TableDelta represents the change of a single table between two roots.
// FromFKs and ToFKs contain Foreign Keys that constrain columns in this table,
// they do not contain Foreign Keys that reference this table.
type TableDeltaSql struct {
	TableDeltaBase
	Summary       TableDeltaSummary
	FromTableHash string
	ToTableHash   string

	fromRef  string
	toRef    string
	queryist *queries.Queryist
}

var _ TableDelta = &TableDeltaSql{}

type diffInfo struct {
	ToTableHash   string
	FromTableHash string
	DiffType      string
}

// GetStagedUnstagedTableDeltas represents staged and unstaged changes as TableDelta slices.
func GetStagedUnstagedTableDeltasFromSql(queryist queries.Queryist, sqlCtx *sql.Context) (staged, unstaged []TableDeltaSql, err error) {
	staged, err = GetTableDeltasFromSql(queryist, sqlCtx, "HEAD", "STAGED")
	if err != nil {
		return nil, nil, err
	}

	unstaged, err = GetTableDeltasFromSql(queryist, sqlCtx, "STAGED", "WORKING")
	if err != nil {
		return nil, nil, err
	}

	return staged, unstaged, nil
}

func getTableSchemaAtRef(queryist queries.Queryist, sqlCtx *sql.Context, tableName string, ref string) (schema.Schema, error) {
	q := fmt.Sprintf("select * from %s as of '%s' limit 1", tableName, ref)
	tableSchema, iter, err := queryist.Query(sqlCtx, q)
	if err != nil {
		return nil, err
	}

	// iterate over all returned rows
	_, err = sql.RowIterToRows(sqlCtx, tableSchema, iter)
	if err != nil {
		return nil, err
	}

	sch, err := sqlutil.ToDoltResultSchema(tableSchema)
	if err != nil {
		return nil, err
	}
	return sch, nil
}

func getChangedTablesBetweenRefs(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef string) ([]TableDeltaSummary, error) {
	q := fmt.Sprintf("select * from dolt_diff_summary('%s', '%s')", fromRef, toRef)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return nil, err
	}

	summaries := make([]TableDeltaSummary, len(rows))
	for i, row := range rows {
		fromTableName := row[0].(string)
		toTableName := row[1].(string)
		diffType := row[2].(string)
		dataChangeVal := row[3]
		schemaChangeVal := row[4]

		dataChange, err := queries.GetTinyIntColAsBool(dataChangeVal)
		if err != nil {
			return nil, err
		}
		schemaChange, err := queries.GetTinyIntColAsBool(schemaChangeVal)
		if err != nil {
			return nil, err
		}

		summary := TableDeltaSummary{
			DiffType:      diffType,
			DataChange:    dataChange,
			SchemaChange:  schemaChange,
			TableName:     toTableName, // TODO: should this be fromTableName?
			FromTableName: fromTableName,
			ToTableName:   toTableName,
		}
		summaries[i] = summary
	}
	return summaries, nil
}

func getTableDiffsBetweenRefs(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef, tableName string) ([]diffInfo, error) {
	diffs := make([]diffInfo, 0)
	q := fmt.Sprintf("select to_table_hash, from_table_hash, diff_type from dolt_diff('%s', '%s', '%s')", fromRef, toRef, tableName)
	rows, err := queries.GetRowsForSql(queryist, sqlCtx, q)
	if err != nil {
		return diffs, err
	}

	for _, row := range rows {
		toTableHash := row[0].(string)
		fromTableHash := row[1].(string)
		diffType := row[2].(string)
		d := diffInfo{ToTableHash: toTableHash, FromTableHash: fromTableHash, DiffType: diffType}
		diffs = append(diffs, d)
	}
	return diffs, nil
}

// GetTableDeltas returns a slice of TableDelta objects for each table that changed between fromRoot and toRoot.
// It matches tables across roots by finding Schemas with Column tags in common.
func GetTableDeltasFromSql(queryist queries.Queryist, sqlCtx *sql.Context, fromRef, toRef string) (deltas []TableDeltaSql, err error) {
	changedTables, err := getChangedTablesBetweenRefs(queryist, sqlCtx, fromRef, toRef)
	if err != nil {
		return nil, err
	}

	deltas = make([]TableDeltaSql, 0)
	for _, summary := range changedTables {
		fromTableName := summary.FromTableName
		toTableName := summary.ToTableName

		tableName := toTableName
		if tableName == "" {
			tableName = fromTableName
		}

		tableDiffs, err := getTableDiffsBetweenRefs(queryist, sqlCtx, fromRef, toRef, tableName)
		if err != nil {
			return nil, err
		}

		var toTableHash = ""
		var fromTableHash = ""
		for _, td := range tableDiffs {
			if fromTableHash == "" {
				fromTableHash = td.FromTableHash
			} else if fromTableHash != td.FromTableHash {
				// TODO: this should not happen, remove?
				return nil, fmt.Errorf("fromTableHash mismatch")
			}

			if toTableHash == "" {
				toTableHash = td.ToTableHash
			} else if toTableHash != td.ToTableHash {
				// TODO: this should not happen, remove?
				return nil, fmt.Errorf("toTableHash mismatch")
			}
		}

		var toTableSchema schema.Schema = nil
		var fromTableSchema schema.Schema = nil
		if toTableName != "" {
			toTableSchema, err = getTableSchemaAtRef(queryist, sqlCtx, toTableName, toRef)
			if err != nil {
				return nil, err
			}
		}
		if fromTableName != "" {
			fromTableSchema, err = getTableSchemaAtRef(queryist, sqlCtx, fromTableName, fromRef)
			if err != nil {
				return nil, err
			}
		}

		delta := TableDeltaSql{
			Summary: summary,
			TableDeltaBase: TableDeltaBase{
				ToName:   toTableName,
				FromName: fromTableName,

				ToSch:   toTableSchema,
				FromSch: fromTableSchema,
			},
			//ToTableHash:   toTableHash + "_pavel_test",   // TODO: remove _pavel_test
			//FromTableHash: fromTableHash + "_pavel_test", // TODO: remove _pavel_test
			ToTableHash:   toTableHash,
			FromTableHash: fromTableHash,

			queryist: &queryist,
			toRef:    toRef,
			fromRef:  fromRef,
		}
		deltas = append(deltas, delta)
	}

	deltas, err = filterUnmodifiedTableDeltasSql(deltas)
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

func filterUnmodifiedTableDeltasSql(deltas []TableDeltaSql) ([]TableDeltaSql, error) {
	var filtered []TableDeltaSql
	for _, d := range deltas {
		//if d.ToTable == nil || d.FromTable == nil {
		if d.FromName == "" || d.ToName == "" {
			// Table was added or dropped
			filtered = append(filtered, d)
			continue
		}

		hasChanges := d.Summary.SchemaChange || d.Summary.DataChange

		if hasChanges {
			// Take only modified tables
			filtered = append(filtered, d)
		}
	}

	return filtered, nil
}

func (td TableDeltaSql) GetBaseInfo() TableDeltaBase {
	return td.TableDeltaBase
}

// IsAdd returns true if the table was added between the fromRoot and toRoot.
func (td TableDeltaSql) IsAdd() bool {
	//return td.FromTable == nil && td.ToTable != nil
	return len(td.FromName) == 0 && len(td.ToName) > 0
}

// IsDrop returns true if the table was dropped between the fromRoot and toRoot.
func (td TableDeltaSql) IsDrop() bool {
	//return td.FromTable != nil && td.ToTable == nil
	return len(td.FromName) > 0 && len(td.ToName) == 0
}

// IsRename return true if the table was renamed between the fromRoot and toRoot.
func (td TableDeltaSql) IsRename() bool {
	if td.IsAdd() || td.IsDrop() {
		return false
	}
	return td.FromName != td.ToName
}

// HasHashChanged returns true if the hash of the table content has changed between
// the fromRoot and toRoot.
func (td TableDeltaSql) HasHashChanged() (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	toHash := td.ToTableHash
	fromHash := td.FromTableHash
	changed := toHash != fromHash

	return changed, nil
}

// HasSchemaChanged returns true if the table schema has changed between the
// fromRoot and toRoot.
func (td TableDeltaSql) HasSchemaChanged(ctx context.Context) (bool, error) {
	if td.IsAdd() || td.IsDrop() {
		return true, nil
	}

	return td.Summary.SchemaChange, nil

	//if td.HasFKChanges() {
	//	return true, nil
	//}
	//
	//fromSchemaHash, err := td.FromTable.GetSchemaHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//toSchemaHash, err := td.ToTable.GetSchemaHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//return !fromSchemaHash.Equal(toSchemaHash), nil
}

func (td TableDeltaSql) HasDataChanged(ctx context.Context) (bool, error) {
	return td.Summary.DataChange, nil

	//if td.IsAdd() {
	//	isEmpty, err := isTableDataEmpty(ctx, td.ToTable)
	//	if err != nil {
	//		return false, err
	//	}
	//
	//	return !isEmpty, nil
	//}
	//
	//if td.IsDrop() {
	//	isEmpty, err := isTableDataEmpty(ctx, td.FromTable)
	//	if err != nil {
	//		return false, err
	//	}
	//	return !isEmpty, nil
	//}
	//
	//fromRowDataHash, err := td.FromTable.GetRowDataHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//toRowDataHash, err := td.ToTable.GetRowDataHash(ctx)
	//if err != nil {
	//	return false, err
	//}
	//
	//return !fromRowDataHash.Equal(toRowDataHash), nil
}

func (td TableDeltaSql) GetTableCreateStatement(ctx context.Context, isFromTable bool) (string, error) {
	ref := td.toRef
	tableName := td.ToName
	if isFromTable {
		ref = td.fromRef
		tableName = td.FromName
	}
	query := fmt.Sprintf("SHOW CREATE TABLE '%s' as of '%s'", tableName, ref)
	sqlCtx := sql.NewContext(ctx)
	queryist := td.queryist
	rows, err := queries.GetRowsForSql(*queryist, sqlCtx, query)
	if err != nil {
		return "", err
	}
	if len(rows) != 1 {
		return "", fmt.Errorf("expected 1 row, got %d", len(rows))
	}
	statement := rows[0][1].(string)
	return statement, nil
}

func (td TableDeltaSql) HasPrimaryKeySetChanged() bool {
	return !schema.ArePrimaryKeySetsDiffable(types.Format_DOLT, td.FromSch, td.ToSch)
}

func (td TableDeltaSql) HasChanges() (bool, error) {
	hashChanged, err := td.HasHashChanged()
	if err != nil {
		return false, err
	}

	return td.HasFKChanges() || td.IsRename() || td.HasPrimaryKeySetChanged() || hashChanged, nil
}

// CurName returns the most recent name of the table.
func (td TableDeltaSql) CurName() string {
	if td.ToName != "" {
		return td.ToName
	}
	return td.FromName
}

func (td TableDeltaSql) HasFKChanges() bool {
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
func (td TableDeltaSql) GetSchemas(ctx context.Context) (from, to schema.Schema, err error) {
	if td.FromSch == nil {
		td.FromSch = schema.EmptySchema
	}
	if td.ToSch == nil {
		td.ToSch = schema.EmptySchema
	}
	return td.FromSch, td.ToSch, nil
}

func (td TableDeltaSql) IsKeyless(ctx context.Context) (bool, error) {
	f, t, err := td.GetSchemas(ctx)
	if err != nil {
		return false, err
	}

	// nil table is neither keyless nor keyed
	from, to := schema.IsKeyless(f), schema.IsKeyless(t)
	if td.FromName == "" {
		return to, nil
	} else if td.ToName == "" {
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

//// isTableDataEmpty return true if the table does not contain any data
//func isTableDataEmpty(ctx context.Context, table *doltdb.Table) (bool, error) {
//	rowData, err := table.GetRowData(ctx)
//	if err != nil {
//		return false, err
//	}
//
//	return rowData.Empty()
//}

// GetSummary returns a summary of the table delta.
func (td TableDeltaSql) GetSummary(ctx context.Context) (*TableDeltaSummary, error) {
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

//// GetRowData returns the table's row data at the fromRoot and toRoot, or an empty map if the table did not exist.
//func (td TableDelta) GetRowData(ctx context.Context) (from, to durable.Index, err error) {
//	if td.FromTable == nil && td.ToTable == nil {
//		return nil, nil, fmt.Errorf("both from and to tables are missing from table delta")
//	}
//
//	if td.FromTable != nil {
//		from, err = td.FromTable.GetRowData(ctx)
//		if err != nil {
//			return from, to, err
//		}
//	} else {
//		// If there is no |FromTable| use the |ToTable|'s schema to make the index.
//		from, _ = durable.NewEmptyIndex(ctx, td.FromVRW, td.FromNodeStore, td.ToSch)
//	}
//
//	if td.ToTable != nil {
//		to, err = td.ToTable.GetRowData(ctx)
//		if err != nil {
//			return from, to, err
//		}
//	} else {
//		// If there is no |ToTable| use the |FromTable|'s schema to make the index.
//		to, _ = durable.NewEmptyIndex(ctx, td.ToVRW, td.ToNodeStore, td.FromSch)
//	}
//
//	return from, to, nil
//}

//// SqlSchemaDiff returns a slice of DDL statements that will transform the schema in the from delta to the schema in
//// the to delta.
//func SqlSchemaDiff(ctx context.Context, td TableDelta, toSchemas map[string]schema.Schema) ([]string, error) {
//	fromSch, toSch, err := td.GetSchemas(ctx)
//	if err != nil {
//		return nil, fmt.Errorf("cannot retrieve schema for table %s, cause: %s", td.ToName, err.Error())
//	}
//
//	var ddlStatements []string
//	if td.IsDrop() {
//		ddlStatements = append(ddlStatements, sqlfmt.DropTableStmt(td.FromName))
//	} else if td.IsAdd() {
//		toPkSch, err := sqlutil.FromDoltSchema(td.ToName, td.ToSch)
//		if err != nil {
//			return nil, err
//		}
//		stmt, err := GenerateCreateTableStatement(td.ToName, td.ToSch, toPkSch, td.ToFks, td.ToFksParentSch)
//		if err != nil {
//			return nil, errhand.VerboseErrorFromError(err)
//		}
//		ddlStatements = append(ddlStatements, stmt)
//	} else {
//		stmts, err := GetNonCreateNonDropTableSqlSchemaDiff(td, toSchemas, fromSch, toSch)
//		if err != nil {
//			return nil, err
//		}
//		ddlStatements = append(ddlStatements, stmts...)
//	}
//
//	return ddlStatements, nil
//}
//
//// GetNonCreateNonDropTableSqlSchemaDiff returns any schema diff in SQL statements that is NEITHER 'CREATE TABLE' NOR 'DROP TABLE' statements.
//func GetNonCreateNonDropTableSqlSchemaDiff(td TableDelta, toSchemas map[string]schema.Schema, fromSch, toSch schema.Schema) ([]string, error) {
//	if td.IsAdd() || td.IsDrop() {
//		// use add and drop specific methods
//		return nil, nil
//	}
//
//	var ddlStatements []string
//	if td.FromName != td.ToName {
//		ddlStatements = append(ddlStatements, sqlfmt.RenameTableStmt(td.FromName, td.ToName))
//	}
//
//	eq := schema.SchemasAreEqual(fromSch, toSch)
//	if eq && !td.HasFKChanges() {
//		return ddlStatements, nil
//	}
//
//	colDiffs, unionTags := DiffSchColumns(fromSch, toSch)
//	for _, tag := range unionTags {
//		cd := colDiffs[tag]
//		switch cd.DiffType {
//		case SchDiffNone:
//		case SchDiffAdded:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddColStmt(td.ToName, sqlfmt.GenerateCreateTableColumnDefinition(*cd.New)))
//		case SchDiffRemoved:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropColStmt(td.ToName, cd.Old.Name))
//		case SchDiffModified:
//			// Ignore any primary key set changes here
//			if cd.Old.IsPartOfPK != cd.New.IsPartOfPK {
//				continue
//			}
//			if cd.Old.Name != cd.New.Name {
//				ddlStatements = append(ddlStatements, sqlfmt.AlterTableRenameColStmt(td.ToName, cd.Old.Name, cd.New.Name))
//			}
//		}
//	}
//
//	// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
//	if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
//		ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropPks(td.ToName))
//		if toSch.GetPKCols().Size() > 0 {
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddPrimaryKeys(td.ToName, toSch.GetPKCols()))
//		}
//	}
//
//	for _, idxDiff := range DiffSchIndexes(fromSch, toSch) {
//		switch idxDiff.DiffType {
//		case SchDiffNone:
//		case SchDiffAdded:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
//		case SchDiffRemoved:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
//		case SchDiffModified:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropIndexStmt(td.FromName, idxDiff.From))
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddIndexStmt(td.ToName, idxDiff.To))
//		}
//	}
//
//	for _, fkDiff := range DiffForeignKeys(td.FromFks, td.ToFks) {
//		switch fkDiff.DiffType {
//		case SchDiffNone:
//		case SchDiffAdded:
//			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
//		case SchDiffRemoved:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
//		case SchDiffModified:
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableDropForeignKeyStmt(fkDiff.From))
//
//			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
//			ddlStatements = append(ddlStatements, sqlfmt.AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
//		}
//	}
//
//	return ddlStatements, nil
//}
//
//// GetDataDiffStatement returns any data diff in SQL statements for given table including INSERT, UPDATE and DELETE row statements.
//func GetDataDiffStatement(tableName string, sch schema.Schema, row sql.Row, rowDiffType ChangeType, colDiffTypes []ChangeType) (string, error) {
//	if len(row) != len(colDiffTypes) {
//		return "", fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
//	}
//
//	switch rowDiffType {
//	case Added:
//		return sqlfmt.SqlRowAsInsertStmt(row, tableName, sch)
//	case Removed:
//		return sqlfmt.SqlRowAsDeleteStmt(row, tableName, sch, 0)
//	case ModifiedNew:
//		updatedCols := set.NewEmptyStrSet()
//		for i, diffType := range colDiffTypes {
//			if diffType != None {
//				updatedCols.Add(sch.GetAllCols().GetByIndex(i).Name)
//			}
//		}
//		return sqlfmt.SqlRowAsUpdateStmt(row, tableName, sch, updatedCols)
//	case ModifiedOld:
//		// do nothing, we only issue UPDATE for ModifiedNew
//		return "", nil
//	default:
//		return "", fmt.Errorf("unexpected row diff type: %v", rowDiffType)
//	}
//}
//
//// GenerateCreateTableStatement returns CREATE TABLE statement for given table. This function was made to share the same
//// 'create table' statement logic as GMS. We initially were running `SHOW CREATE TABLE` query to get the statement;
//// however, it cannot be done for cases that need this statement in sql shell mode. Dolt uses its own Schema and
//// Column and other object types which are not directly compatible with GMS, so we try to use as much shared logic
//// as possible with GMS to get 'create table' statement in Dolt.
//func GenerateCreateTableStatement(tblName string, sch schema.Schema, pkSchema sql.PrimaryKeySchema, fks []doltdb.ForeignKey, fksParentSch map[string]schema.Schema) (string, error) {
//	sqlSch := pkSchema.Schema
//	colStmts := make([]string, len(sqlSch))
//
//	// Statement creation parts for each column
//	for i, col := range sch.GetAllCols().GetColumns() {
//		colStmts[i] = sqlfmt.GenerateCreateTableIndentedColumnDefinition(col)
//	}
//
//	primaryKeyCols := sch.GetPKCols().GetColumnNames()
//	if len(primaryKeyCols) > 0 {
//		primaryKey := sql.GenerateCreateTablePrimaryKeyDefinition(primaryKeyCols)
//		colStmts = append(colStmts, primaryKey)
//	}
//
//	indexes := sch.Indexes().AllIndexes()
//	for _, index := range indexes {
//		// The primary key may or may not be declared as an index by the table. Don't print it twice if it's here.
//		if isPrimaryKeyIndex(index, sch) {
//			continue
//		}
//		colStmts = append(colStmts, sqlfmt.GenerateCreateTableIndexDefinition(index))
//	}
//
//	for _, fk := range fks {
//		colStmts = append(colStmts, sqlfmt.GenerateCreateTableForeignKeyDefinition(fk, sch, fksParentSch[fk.ReferencedTableName]))
//	}
//
//	for _, check := range sch.Checks().AllChecks() {
//		colStmts = append(colStmts, sqlfmt.GenerateCreateTableCheckConstraintClause(check))
//	}
//
//	coll := sql.CollationID(sch.GetCollation())
//	createTableStmt := sql.GenerateCreateTableStatement(tblName, colStmts, coll.CharacterSet().Name(), coll.Name())
//	return fmt.Sprintf("%s;", createTableStmt), nil
//}
//
//// isPrimaryKeyIndex returns whether the index given matches the table's primary key columns. Order is not considered.
//func isPrimaryKeyIndex(index schema.Index, sch schema.Schema) bool {
//	var pks = sch.GetPKCols().GetColumns()
//	var pkMap = make(map[string]struct{})
//	for _, c := range pks {
//		pkMap[c.Name] = struct{}{}
//	}
//
//	indexCols := index.ColumnNames()
//	if len(indexCols) != len(pks) {
//		return false
//	}
//
//	for _, c := range index.ColumnNames() {
//		if _, ok := pkMap[c]; !ok {
//			return false
//		}
//	}
//
//	return true
//}
