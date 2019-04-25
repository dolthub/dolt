package sql

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"strings"
)

// SchemaAsCreateStmt takes a Schema and returns a string representing a SQL create table command that could be used to
// create this table
func SchemaAsCreateStmt(tableName string, sch schema.Schema) (string, error) {
	sb := &strings.Builder{}
	fmt.Fprintf(sb, "CREATE TABLE %s (\n", tableName)

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
	sch.GetPKCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		if firstPK {
			sb.WriteString(",\n  primary key (")
			firstPK = false
		} else {
			sb.WriteRune(',')
		}
		sb.WriteString(col.Name)
		return false
	})

	sb.WriteString(")\n);")
	return sb.String(), nil
}

func FmtCol(indent, nameWidth, typeWidth int, col schema.Column) string {
	return FmtColWithNameAndType(indent, nameWidth, typeWidth, col.Name, DoltToSQLType[col.Kind], col)
}

func FmtColWithNameAndType(indent, nameWidth, typeWidth int, colName, typeStr string, col schema.Column) string {
	fmtStr := fmt.Sprintf("%%%ds%%%ds %%%ds", indent, nameWidth, typeWidth)
	colStr := fmt.Sprintf(fmtStr, "", colName, typeStr)

	for _, cnst := range col.Constraints {
		switch cnst.GetConstraintType() {
		case schema.NotNullConstraintType:
			colStr += " not null"
		default:
			panic("SchemaAsCreateStmt doesn't know how to format constraint type: " + cnst.GetConstraintType())
		}
	}

	return colStr + fmt.Sprintf(" comment 'tag:%d'", col.Tag)
}
