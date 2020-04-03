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

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
)

const TagCommentPrefix = "tag:"

// FmtCol converts a column to a string with a given indent space count, name width, and type width.  If nameWidth or
// typeWidth are 0 or less than the length of the name or type, then the length of the name or type will be used
func FmtCol(indent, nameWidth, typeWidth int, col schema.Column) string {
	sqlType := col.TypeInfo.ToSqlType()
	return FmtColWithNameAndType(indent, nameWidth, typeWidth, col.Name, sqlType.String(), col)
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

	return colStr + fmt.Sprintf(" COMMENT 'tag:%d'", col.Tag)
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
