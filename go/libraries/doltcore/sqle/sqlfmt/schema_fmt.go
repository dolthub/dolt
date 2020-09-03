// Copyright 2020 Liquidata, Inc.
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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

//TODO: This is a relic from before `SHOW CREATE TABLE` was implemented. We should remove this file altogether.
const TagCommentPrefix = "tag:"

//  FmtCol converts a column to a string with a given indent space count, name width, and type width.  If nameWidth or
// typeWidth are 0 or less than the length of the name or type, then the length of the name or type will be used
func FmtCol(indent, nameWidth, typeWidth int, col schema.Column) string {
	sqlType := col.TypeInfo.ToSqlType()
	return FmtColWithNameAndType(indent, nameWidth, typeWidth, col.Name, sqlType.String(), col)
}

// FmtColWithTag follows the same logic as FmtCol, but includes the column's tag as a comment
func FmtColWithTag(indent, nameWidth, typeWidth int, col schema.Column) string {
	fc := FmtCol(indent, nameWidth, typeWidth, col)
	return fmt.Sprintf("%s COMMENT '%s'", fc, FmtColTagComment(col.Tag))
}

// FmtColWithNameAndType creates a string representing a column within a sql create table statement with a given indent
// space count, name width, and type width.  If nameWidth or typeWidth are 0 or less than the length of the name or
// type, then the length of the name or type will be used.
func FmtColWithNameAndType(indent, nameWidth, typeWidth int, colName, typeStr string, col schema.Column) string {
	colName = QuoteIdentifier(colName)
	fmtStr := fmt.Sprintf("%%%ds%%%ds %%%ds", indent, nameWidth, typeWidth)
	colStr := fmt.Sprintf(fmtStr, "", colName, typeStr)

	for _, cnst := range col.Constraints {
		switch cnst.GetConstraintType() {
		case schema.NotNullConstraintType:
			colStr += " NOT NULL"
		default:
			panic("FmtColWithNameAndType doesn't know how to format constraint type: " + cnst.GetConstraintType())
		}
	}

	if col.Default != "" {
		colStr += " DEFAULT " + col.Default
	}

	return colStr
}

// FmtColPrimaryKey creates a string representing a primary key constraint within a sql create table statement with a
// given indent.
func FmtColPrimaryKey(indent int, colStr string) string {
	fmtStr := fmt.Sprintf("%%%ds PRIMARY KEY (%s)\n", indent, colStr)
	return fmt.Sprintf(fmtStr, "")
}

func FmtColTagComment(tag uint64) string {
	return fmt.Sprintf("%s%d", TagCommentPrefix, tag)
}

func FmtIndex(index schema.Index) string {
	sb := strings.Builder{}
	if index.IsUnique() {
		sb.WriteString("UNIQUE ")
	}
	sb.WriteString("INDEX ")
	sb.WriteString(QuoteIdentifier(index.Name()))
	sb.WriteString(" (")
	for i, indexColName := range index.ColumnNames() {
		if i != 0 {
			sb.WriteRune(',')
		}
		sb.WriteString(QuoteIdentifier(indexColName))
	}
	sb.WriteRune(')')
	if len(index.Comment()) > 0 {
		sb.WriteString(" COMMENT ")
		sb.WriteString(QuoteComment(index.Comment()))
	}
	return sb.String()
}

func FmtForeignKey(fk doltdb.ForeignKey, sch, parentSch schema.Schema) string {
	sb := strings.Builder{}
	sb.WriteString("CONSTRAINT ")
	sb.WriteString(QuoteIdentifier(fk.Name))
	sb.WriteString(" FOREIGN KEY (")
	for i, tag := range fk.TableColumns {
		if i != 0 {
			sb.WriteRune(',')
		}
		c, _ := sch.GetAllCols().GetByTag(tag)
		sb.WriteString(QuoteIdentifier(c.Name))
	}
	sb.WriteString(")\n    REFERENCES ")
	sb.WriteString(QuoteIdentifier(fk.ReferencedTableName))
	sb.WriteString(" (")
	for i, tag := range fk.ReferencedTableColumns {
		if i != 0 {
			sb.WriteRune(',')
		}
		c, _ := parentSch.GetAllCols().GetByTag(tag)
		sb.WriteString(QuoteIdentifier(c.Name))
	}
	sb.WriteRune(')')
	if fk.OnDelete != doltdb.ForeignKeyReferenceOption_DefaultAction {
		sb.WriteString("\n    ON DELETE ")
		sb.WriteString(fk.OnDelete.String())
	}
	if fk.OnUpdate != doltdb.ForeignKeyReferenceOption_DefaultAction {
		sb.WriteString("\n    ON UPDATE ")
		sb.WriteString(fk.OnUpdate.String())
	}
	return sb.String()
}

// CreateTableStmtWithTags generates a SQL CREATE TABLE command
func CreateTableStmt(tableName string, sch schema.Schema, foreignKeys []doltdb.ForeignKey, parentSchs map[string]schema.Schema) string {
	return createTableStmt(tableName, sch, func(col schema.Column) string {
		return FmtCol(2, 0, 0, col)
	}, foreignKeys, parentSchs)
}

// CreateTableStmtWithTags generates a SQL CREATE TABLE command that includes the column tags as comments
func CreateTableStmtWithTags(tableName string, sch schema.Schema, foreignKeys []doltdb.ForeignKey, parentSchs map[string]schema.Schema) string {
	return createTableStmt(tableName, sch, func(col schema.Column) string {
		return FmtColWithTag(2, 0, 0, col)
	}, foreignKeys, parentSchs)
}

type fmtColFunc func(col schema.Column) string

func createTableStmt(tableName string, sch schema.Schema, fmtCol fmtColFunc, foreignKeys []doltdb.ForeignKey, parentSchs map[string]schema.Schema) string {

	sb := strings.Builder{}
	sb.WriteString(fmt.Sprintf("CREATE TABLE %s (\n", QuoteIdentifier(tableName)))

	firstLine := true
	_ = sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if firstLine {
			firstLine = false
		} else {
			sb.WriteString(",\n")
		}

		s := fmtCol(col)
		sb.WriteString(s)

		return false, nil
	})

	firstPK := true
	_ = sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if firstPK {
			sb.WriteString(",\n  PRIMARY KEY (")
			firstPK = false
		} else {
			sb.WriteRune(',')
		}
		sb.WriteString(QuoteIdentifier(col.Name))
		return false, nil
	})

	sb.WriteRune(')')

	for _, idx := range sch.Indexes().AllIndexes() {
		sb.WriteString(",\n  ")
		sb.WriteString(FmtIndex(idx))
	}

	for _, fk := range foreignKeys {
		sb.WriteString(",\n  ")
		sb.WriteString(FmtForeignKey(fk, sch, parentSchs[fk.ReferencedTableName]))
	}

	sb.WriteString("\n);")

	return sb.String()
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
