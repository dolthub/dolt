package sql

import (
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"strings"
)

// SchemaAsCreateStmt takes a Schema and returns a string representing a SQL create table command that could be used to
// create this table
func SchemaAsCreateStmt(sch schema.Schema) (string, error) {
	sb := &strings.Builder{}
	sb.WriteString("CREATE TABLE %s (\n")

	firstLine := true
	sch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
		if firstLine {
			firstLine = false
		} else {
			sb.WriteString(",\n")
		}

		fmt.Fprintf(sb, "  %s %s", col.Name, DoltToSQLType[col.Kind])

		for _, cnst := range col.Constraints {
			switch cnst.GetConstraintType() {
			case schema.NotNullConstraintType:
				sb.WriteString(" not null")
			default:
				panic("SchemaAsCreateStmt doesn't know how to format constraint type: " + cnst.GetConstraintType())
			}
		}

		fmt.Fprintf(sb, " comment 'tag:%d'", tag)
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
