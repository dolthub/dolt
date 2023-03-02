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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
)

// GenerateCreateTableColumnDefinition returns column definition for CREATE TABLE statement with no indentation
func GenerateCreateTableColumnDefinition(col schema.Column) string {
	colStr := sql.GenerateCreateTableColumnDefinition(col.Name, col.TypeInfo.ToSqlType(), col.IsNullable(), col.AutoIncrement, col.Default != "", col.Default, col.Comment)
	return strings.TrimPrefix(colStr, "  ")
}

// GenerateCreateTableIndentedColumnDefinition returns column definition for CREATE TABLE statement with no indentation
func GenerateCreateTableIndentedColumnDefinition(col schema.Column) string {
	return sql.GenerateCreateTableColumnDefinition(col.Name, col.TypeInfo.ToSqlType(), col.IsNullable(), col.AutoIncrement, col.Default != "", col.Default, col.Comment)
}

// GenerateCreateTableIndexDefinition returns index definition for CREATE TABLE statement with indentation of 2 spaces
func GenerateCreateTableIndexDefinition(index schema.Index) string {
	return sql.GenerateCreateTableIndexDefinition(index.IsUnique(), index.IsSpatial(), index.Name(), sql.QuoteIdentifiers(index.ColumnNames()), index.Comment())
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
		for _, col := range fk.UnresolvedFKDetails.TableColumns {
			fkCols = append(fkCols, col)
		}
	}

	var parentCols []string
	if fk.IsResolved() {
		for _, tag := range fk.ReferencedTableColumns {
			c, _ := parentSch.GetAllCols().GetByTag(tag)
			parentCols = append(parentCols, c.Name)
		}
	} else {
		for _, col := range fk.UnresolvedFKDetails.ReferencedTableColumns {
			parentCols = append(parentCols, col)
		}
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

func AlterTableAddPrimaryKeys(tableName string, pks *schema.ColCollection) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(tableName))
	b.WriteString(" ADD PRIMARY KEY (")

	for i := 0; i < pks.Size(); i++ {
		if i == 0 {
			b.WriteString(pks.GetByIndex(i).Name)
		} else {
			b.WriteString("," + pks.GetByIndex(i).Name)
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

func AlterTableDropForeignKeyStmt(fk doltdb.ForeignKey) string {
	var b strings.Builder
	b.WriteString("ALTER TABLE ")
	b.WriteString(QuoteIdentifier(fk.TableName))
	b.WriteString(" DROP FOREIGN KEY ")
	b.WriteString(QuoteIdentifier(fk.Name))
	b.WriteRune(';')
	return b.String()
}
