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

package sql

import (
	"fmt"
	"strings"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	dtypes "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle/types"
)

// SchemaAsCreateStmt takes a Schema and returns a string representing a SQL create table command that could be used to
// create this table
func SchemaAsCreateStmt(tableName string, sch schema.Schema) string {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "CREATE TABLE %s (\n", QuoteIdentifier(tableName))

	firstLine := true
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if firstLine {
			firstLine = false
		} else {
			sb.WriteString(",\n")
		}

		s := FmtCol(2, 0, 0, col)
		sb.WriteString(s)

		return false
	})

	firstPK := true
	err := sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		if firstPK {
			sb.WriteString(",\n  PRIMARY KEY (")
			firstPK = false
		} else {
			sb.WriteRune(',')
		}
		sb.WriteString(QuoteIdentifier(col.Name))
		return false, nil
	})

	// TODO: fix panics
	if err != nil {
		panic(err)
	}

	sb.WriteString(")\n);")
	return sb.String()
}

// FmtCol converts a column to a string with a given indent space count, name width, and type width.  If nameWidth or
// typeWidth are 0 or less than the length of the name or type, then the length of the name or type will be used
func FmtCol(indent, nameWidth, typeWidth int, col schema.Column) string {
	sqlTypeStr, err := dtypes.NomsKindToSqlTypeString(col.Kind)
	if err != nil {
		panic(err) // We can default or panic, as this would mean the type has no SQL interface
	}
	return FmtColWithNameAndType(indent, nameWidth, typeWidth, col.Name, sqlTypeStr, col)
}

// FmtColWithNameAndType creates a string representing a column within a sql create table statement with a given indent
// space count, name width, and type width.  If nameWidth or typeWidth are 0 or less than the length of the name or
// type, then the length of the name or type will be used.
func FmtColWithNameAndType(indent, nameWidth, typeWidth int, colName, typeStr string, col schema.Column) string {
	colName = "`" + colName + "`"
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

	return colStr + fmt.Sprintf(" COMMENT 'tag:%d'", col.Tag)
}

// Quotes the identifier given with backticks.
func QuoteIdentifier(s string) string {
	return "`" + s + "`"
}
