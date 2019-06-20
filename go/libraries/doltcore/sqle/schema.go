package sqle

import (
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/src-d/go-mysql-server/sql"
)

// doltSchemaToSqlSchema returns the sql.Schema corresponding to the dolt schema given.
func doltSchemaToSqlSchema(tableName string, sch schema.Schema) sql.Schema {
  cols := make([]*sql.Column, sch.GetAllCols().Size())

  var i int
	sch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
		cols[i] = doltColToSqlCol(tableName, col)
		i++
		return false
	})

  return cols
}

// doltColToSqlCol returns the SQL column corresponding to the dolt column given.
func doltColToSqlCol(tableName string, col schema.Column) *sql.Column {
		return &sql.Column{
			Name:     col.Name,
			Type:     nomsTypeToSqlType(col.Kind),
			Default:  nil,
			Nullable: col.IsNullable(),
			Source:   tableName,
		}
}