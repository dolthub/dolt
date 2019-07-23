package sqle

import (
	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
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

func SqlSchemaToDoltSchema(sqlSchema sql.Schema) schema.Schema {
	var cols []schema.Column
	for i, col := range sqlSchema {
		cols = append(cols, SqlColToDoltCol(uint64(i), false, col))
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		panic(err)
	}

	return schema.UnkeyedSchemaFromCols(colColl)
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

// doltColToSqlCol returns the dolt column corresponding to the SQL column given
func SqlColToDoltCol(tag uint64, isPk bool, col *sql.Column) schema.Column {
	// TODO: nullness constraint
	return schema.NewColumn(col.Name, tag, SqlTypeToNomsKind(col.Type), isPk)
}
