// Copyright 2020 Dolthub, Inc.
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

package sqlfmt

import (
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/set"
)

// GenerateDataDiffStatement returns any data diff in SQL statements for given table including INSERT, UPDATE and DELETE row statements.
func GenerateDataDiffStatement(tableName string, sch schema.Schema, row sql.Row, rowDiffType diff.ChangeType, colDiffTypes []diff.ChangeType) (string, error) {
	if len(row) != len(colDiffTypes) {
		return "", fmt.Errorf("expected the same size for columns and diff types, got %d and %d", len(row), len(colDiffTypes))
	}

	switch rowDiffType {
	case diff.Added:
		return SqlRowAsInsertStmt(row, tableName, sch)
	case diff.Removed:
		return SqlRowAsDeleteStmt(row, tableName, sch, 0)
	case diff.ModifiedNew:
		updatedCols := set.NewEmptyStrSet()
		for i, diffType := range colDiffTypes {
			if diffType != diff.None {
				updatedCols.Add(sch.GetAllCols().GetByIndex(i).Name)
			}
		}
		if updatedCols.Size() == 0 {
			return "", nil
		}
		return SqlRowAsUpdateStmt(row, tableName, sch, updatedCols)
	case diff.ModifiedOld:
		// do nothing, we only issue UPDATE for ModifiedNew
		return "", nil
	default:
		return "", fmt.Errorf("unexpected row diff type: %v", rowDiffType)
	}
}

// GenerateSqlPatchSchemaStatements examines the table schema changes in the specified TableDelta |td| and returns
// a slice of SQL path statements that represent the equivalent SQL DDL statements for those schema changes. The
// specified RootValue, |toRoot|, must be the RootValue that was used as the "To" root when computing the specified
// TableDelta.
func GenerateSqlPatchSchemaStatements(ctx *sql.Context, toRoot doltdb.RootValue, td diff.TableDelta) ([]string, error) {
	toSchemas, err := doltdb.GetAllSchemas(ctx, toRoot)
	if err != nil {
		return nil, fmt.Errorf("could not read schemas from toRoot, cause: %s", err.Error())
	}

	fromSch, toSch, err := td.GetSchemas(ctx)
	if err != nil {
		return nil, fmt.Errorf("cannot retrieve schema for table %s, cause: %s", td.ToName, err.Error())
	}

	var ddlStatements []string
	if td.IsDrop() {
		ddlStatements = append(ddlStatements, DropTableStmt(td.FromName.Name))
	} else if td.IsAdd() {
		stmt, err := GenerateCreateTableStatement(td.ToName.Name, td.ToSch, td.ToFks, nameMapFromTableNameMap(td.ToFksParentSch))
		if err != nil {
			return nil, errhand.VerboseErrorFromError(err)
		}
		ddlStatements = append(ddlStatements, stmt)
	} else {
		stmts, err := generateNonCreateNonDropTableSqlSchemaDiff(td, toSchemas, fromSch, toSch)
		if err != nil {
			return nil, err
		}
		ddlStatements = append(ddlStatements, stmts...)
	}

	return ddlStatements, nil
}

func nameMapFromTableNameMap(tableNameMap map[doltdb.TableName]schema.Schema) map[string]schema.Schema {
	nameMap := make(map[string]schema.Schema)
	for name := range tableNameMap {
		nameMap[name.Name] = tableNameMap[name]
	}
	return nameMap
}

// generateNonCreateNonDropTableSqlSchemaDiff returns any schema diff in SQL statements that is NEITHER 'CREATE TABLE' NOR 'DROP TABLE' statements.
// TODO: schema names
func generateNonCreateNonDropTableSqlSchemaDiff(td diff.TableDelta, toSchemas map[string]schema.Schema, fromSch, toSch schema.Schema) ([]string, error) {
	if td.IsAdd() || td.IsDrop() {
		// use add and drop specific methods
		return nil, nil
	}

	var ddlStatements []string
	if td.FromName != td.ToName {
		ddlStatements = append(ddlStatements, RenameTableStmt(td.FromName.Name, td.ToName.Name))
	}

	eq := schema.SchemasAreEqual(fromSch, toSch)
	if eq && !td.HasFKChanges() {
		return ddlStatements, nil
	}

	colDiffs, unionTags := diff.DiffSchColumns(fromSch, toSch)
	for _, tag := range unionTags {
		cd := colDiffs[tag]
		switch cd.DiffType {
		case diff.SchDiffNone:
		case diff.SchDiffAdded:
			ddlStatements = append(ddlStatements, AlterTableAddColStmt(td.ToName.Name, GenerateCreateTableColumnDefinition(*cd.New, sql.CollationID(td.ToSch.GetCollation()))))
		case diff.SchDiffRemoved:
			ddlStatements = append(ddlStatements, AlterTableDropColStmt(td.ToName.Name, cd.Old.Name))
		case diff.SchDiffModified:
			// Ignore any primary key set changes here
			if cd.Old.IsPartOfPK != cd.New.IsPartOfPK {
				continue
			}
			if cd.Old.Name != cd.New.Name {
				ddlStatements = append(ddlStatements, AlterTableRenameColStmt(td.ToName.Name, cd.Old.Name, cd.New.Name))
			}
			if !cd.Old.TypeInfo.Equals(cd.New.TypeInfo) {
				ddlStatements = append(ddlStatements, AlterTableModifyColStmt(td.ToName.Name,
					GenerateCreateTableColumnDefinition(*cd.New, sql.CollationID(td.ToSch.GetCollation()))))
			}
		}
	}

	// Print changes between a primary key set change. It contains an ALTER TABLE DROP and an ALTER TABLE ADD
	if !schema.ColCollsAreEqual(fromSch.GetPKCols(), toSch.GetPKCols()) {
		ddlStatements = append(ddlStatements, AlterTableDropPks(td.ToName.Name))
		if toSch.GetPKCols().Size() > 0 {
			ddlStatements = append(ddlStatements, AlterTableAddPrimaryKeys(td.ToName.Name, toSch.GetPKCols().GetColumnNames()))
		}
	}

	for _, idxDiff := range diff.DiffSchIndexes(fromSch, toSch) {
		switch idxDiff.DiffType {
		case diff.SchDiffNone:
		case diff.SchDiffAdded:
			ddlStatements = append(ddlStatements, AlterTableAddIndexStmt(td.ToName.Name, idxDiff.To))
		case diff.SchDiffRemoved:
			ddlStatements = append(ddlStatements, AlterTableDropIndexStmt(td.FromName.Name, idxDiff.From))
		case diff.SchDiffModified:
			ddlStatements = append(ddlStatements, AlterTableDropIndexStmt(td.FromName.Name, idxDiff.From))
			ddlStatements = append(ddlStatements, AlterTableAddIndexStmt(td.ToName.Name, idxDiff.To))
		}
	}

	for _, fkDiff := range diff.DiffForeignKeys(td.FromFks, td.ToFks) {
		switch fkDiff.DiffType {
		case diff.SchDiffNone:
		case diff.SchDiffAdded:
			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
			ddlStatements = append(ddlStatements, AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
		case diff.SchDiffRemoved:
			from := fkDiff.From
			ddlStatements = append(ddlStatements, AlterTableDropForeignKeyStmt(from.TableName, from.Name))
		case diff.SchDiffModified:
			from := fkDiff.From
			ddlStatements = append(ddlStatements, AlterTableDropForeignKeyStmt(from.TableName, from.Name))

			parentSch := toSchemas[fkDiff.To.ReferencedTableName]
			ddlStatements = append(ddlStatements, AlterTableAddForeignKeyStmt(fkDiff.To, toSch, parentSch))
		}
	}

	// Handle charset/collation changes
	toCollation := toSch.GetCollation()
	fromCollation := fromSch.GetCollation()
	if toCollation != fromCollation {
		ddlStatements = append(ddlStatements, AlterTableCollateStmt(td.ToName.Name, fromCollation, toCollation))
	}

	return ddlStatements, nil
}

// GenerateCreateTableColumnDefinition returns column definition for CREATE TABLE statement with no indentation
func GenerateCreateTableColumnDefinition(col schema.Column, tableCollation sql.CollationID) string {
	colStr := GenerateCreateTableIndentedColumnDefinition(col, tableCollation)
	return strings.TrimPrefix(colStr, "  ")
}

// GenerateCreateTableIndentedColumnDefinition returns column definition for CREATE TABLE statement with no indentation
func GenerateCreateTableIndentedColumnDefinition(col schema.Column, tableCollation sql.CollationID) string {
	var defaultVal, genVal, onUpdateVal *sql.ColumnDefaultValue
	if col.Default != "" {
		// hacky way to determine if column default is an expression
		if col.Default[0] != '(' && col.Default[len(col.Default)-1] != ')'  && col.Default[0] != '\'' && col.Default[len(col.Default)-1] != '\'' {
			col.Default = fmt.Sprintf("'%s'", col.Default)
		}
		defaultVal = sql.NewUnresolvedColumnDefaultValue(col.Default)
	}
	if col.Generated != "" {
		genVal = sql.NewUnresolvedColumnDefaultValue(col.Generated)
	}
	if col.OnUpdate != "" {
		onUpdateVal = sql.NewUnresolvedColumnDefaultValue(col.OnUpdate)
	}

	return sql.GenerateCreateTableColumnDefinition(
		&sql.Column{
			Name:          col.Name,
			Type:          col.TypeInfo.ToSqlType(),
			Default:       defaultVal,
			AutoIncrement: col.AutoIncrement,
			Nullable:      col.IsNullable(),
			Comment:       col.Comment,
			Generated:     genVal,
			Virtual:       col.Virtual,
			OnUpdate:      onUpdateVal,
		}, col.Default, col.OnUpdate, tableCollation)
}

// GenerateCreateTableIndexDefinition returns index definition for CREATE TABLE statement with indentation of 2 spaces
func GenerateCreateTableIndexDefinition(index schema.Index) string {
	return sql.GenerateCreateTableIndexDefinition(index.IsUnique(), index.IsSpatial(), index.IsFullText(), index.Name(),
		sql.QuoteIdentifiers(index.ColumnNames()), index.Comment())
}

// GenerateCreateTableForeignKeyDefinition returns foreign key definition for CREATE TABLE statement with indentation of 2 spaces
func GenerateCreateTableForeignKeyDefinition(fk doltdb.ForeignKey, sch, parentSch schema.Schema) string {
	var fkCols []string
	if fk.IsResolved() {
		for _, tag := range fk.TableColumns {
			c, _ := sch.GetAllCols().GetByTag(tag)
			fkCols = append(fkCols, c.Name)
		}
	} else {
		fkCols = append(fkCols, fk.UnresolvedFKDetails.TableColumns...)
	}

	var parentCols []string
	if parentSch != nil && fk.IsResolved() {
		for _, tag := range fk.ReferencedTableColumns {
			c, _ := parentSch.GetAllCols().GetByTag(tag)
			parentCols = append(parentCols, c.Name)
		}
	} else {
		// the referenced table is dropped, so the schema is nil or the foreign key is not resolved
		parentCols = append(parentCols, fk.UnresolvedFKDetails.ReferencedTableColumns...)
	}

	onDelete := ""
	if fk.OnDelete != doltdb.ForeignKeyReferentialAction_DefaultAction {
		onDelete = fk.OnDelete.String()
	}
	onUpdate := ""
	if fk.OnUpdate != doltdb.ForeignKeyReferentialAction_DefaultAction {
		onUpdate = fk.OnUpdate.String()
	}
	return sql.GenerateCreateTableForiegnKeyDefinition(fk.Name, fkCols, fk.ReferencedTableName, parentCols, onDelete, onUpdate)
}

// GenerateCreateTableCheckConstraintClause returns check constraint clause definition for CREATE TABLE statement with indentation of 2 spaces
func GenerateCreateTableCheckConstraintClause(check schema.Check) string {
	return sql.GenerateCreateTableCheckConstraintClause(check.Name(), check.Expression(), check.Enforced())
}

func DropTableStmt(tableName string) string {
	var b strings.Builder
	b.WriteString("DROP TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(";")
	return b.String()
}

func DropTableIfExistsStmt(tableName string) string {
	var b strings.Builder
	b.WriteString("DROP TABLE IF EXISTS ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(";")
	return b.String()
}

func AlterTableAddColStmt(tableName string, newColDef string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ADD ")
	b.WriteString(newColDef)
	b.WriteRune(';')
	return b.String()
}

func AlterTableModifyColStmt(tableName string, newColDef string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" MODIFY COLUMN ")
	b.WriteString(newColDef)
	b.WriteRune(';')
	return b.String()
}

func AlterTableDropColStmt(tableName string, oldColName string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" DROP ")
	b.WriteString(QuoteIdentifier(oldColName))
	b.WriteRune(';')
	return b.String()
}

func AlterTableRenameColStmt(tableName string, oldColName string, newColName string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" RENAME COLUMN ")
	b.WriteString(QuoteIdentifier(oldColName))
	b.WriteString(" TO ")
	b.WriteString(QuoteIdentifier(newColName))
	b.WriteRune(';')
	return b.String()
}

func AlterTableDropPks(tableName string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" DROP PRIMARY KEY")
	b.WriteRune(';')
	return b.String()
}

func AlterTableAddPrimaryKeys(tableName string, pkColNames []string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ADD PRIMARY KEY (")

	for i := 0; i < len(pkColNames); i++ {
		if i == 0 {
			b.WriteString(pkColNames[i])
		} else {
			b.WriteString("," + pkColNames[i])
		}
	}
	b.WriteRune(')')
	b.WriteRune(';')
	return b.String()
}

func RenameTableStmt(fromName string, toName string) string {
	var b strings.Builder
	b.WriteString("RENAME TABLE ")
	b.WriteString(QuoteIdentifier(fromName))
	b.WriteString(" TO ")
	b.WriteString(QuoteIdentifier(toName))
	b.WriteString(";")

	return b.String()
}

func AlterTableAddIndexStmt(tableName string, idx schema.Index) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ADD INDEX ")
	b.WriteString(QuoteIdentifier(idx.Name()))
	var cols []string
	for _, cn := range idx.ColumnNames() {
		cols = append(cols, QuoteIdentifier(cn))
	}
	b.WriteString("(" + strings.Join(cols, ",") + ");")
	return b.String()
}

func AlterTableDropIndexStmt(tableName string, idx schema.Index) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" DROP INDEX ")
	b.WriteString(QuoteIdentifier(idx.Name()))
	b.WriteRune(';')
	return b.String()
}

func AlterTableCollateStmt(tableName string, fromCollation, toCollation schema.Collation) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	toCollationId := sql.CollationID(toCollation)
	b.WriteString(" COLLATE=" + QuoteComment(toCollationId.Name()) + ";")
	return b.String()
}

func AlterDatabaseCollateStmt(dbName string, fromCollation, toCollation schema.Collation) string {
	var b strings.Builder
	b.WriteString("ALTER DATABASE ")
	b.WriteString(QuoteIdentifier(dbName))
	toCollationId := sql.CollationID(toCollation)
	b.WriteString(" COLLATE=" + QuoteComment(toCollationId.Name()) + ";")
	return b.String()
}

func AlterTableAddForeignKeyStmt(fk doltdb.ForeignKey, sch, parentSch schema.Schema) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(fk.TableName))
	b.WriteString(" ADD CONSTRAINT ")
	b.WriteString(QuoteIdentifier(fk.Name))
	b.WriteString(" FOREIGN KEY ")
	var childCols []string
	for _, tag := range fk.TableColumns {
		c, _ := sch.GetAllCols().GetByTag(tag)
		childCols = append(childCols, QuoteIdentifier(c.Name))
	}
	b.WriteString("(" + strings.Join(childCols, ",") + ")")
	b.WriteString(" REFERENCES ")
	var parentCols []string
	for _, tag := range fk.ReferencedTableColumns {
		c, _ := parentSch.GetAllCols().GetByTag(tag)
		parentCols = append(parentCols, QuoteIdentifier(c.Name))
	}
	b.WriteString(QuoteIdentifier(fk.ReferencedTableName))
	b.WriteString(" (" + strings.Join(parentCols, ",") + ");")
	return b.String()
}

func AlterTableDropForeignKeyStmt(tableName, fkName string) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" DROP FOREIGN KEY ")
	b.WriteString(QuoteIdentifier(fkName))
	b.WriteRune(';')
	return b.String()
}

// GenerateCreateTableStatement returns a CREATE TABLE statement for given table. This is a reasonable approximation of
// `SHOW CREATE TABLE` in the engine, but may have some differences. Callers are advised to use the engine when
// possible.
// TODO: schema names
func GenerateCreateTableStatement(
	tblName string,
	sch schema.Schema,
	fks []doltdb.ForeignKey,
	fksParentSch map[string]schema.Schema,
) (string, error) {
	colStmts := make([]string, sch.GetAllCols().Size())

	// Statement creation parts for each column
	for i, col := range sch.GetAllCols().GetColumns() {
		colStmts[i] = GenerateCreateTableIndentedColumnDefinition(col, sql.CollationID(sch.GetCollation()))
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
		colStmts = append(colStmts, GenerateCreateTableIndexDefinition(index))
	}

	for _, fk := range fks {
		colStmts = append(colStmts, GenerateCreateTableForeignKeyDefinition(fk, sch, fksParentSch[fk.ReferencedTableName]))
	}

	for _, check := range sch.Checks().AllChecks() {
		colStmts = append(colStmts, GenerateCreateTableCheckConstraintClause(check))
	}

	coll := sql.CollationID(sch.GetCollation())
	createTableStmt := sql.GenerateCreateTableStatement(tblName, colStmts, "", coll.CharacterSet().Name(), coll.Name(), sch.GetComment())
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
