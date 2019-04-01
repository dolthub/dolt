package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/xwb1989/sqlparser"
)

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteCreate(db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (schema.Schema, error) {

	if ddl.Action != sqlparser.CreateStr {
		panic("expected create statement")
	}

	var tableName string
	tableNameExpr := ddl.Table
	tableName = tableNameExpr.Name.String()

	if root.HasTable(tableName) {
		return errCreate("error: table %ddl already defined", tableName)
	}

	spec := ddl.TableSpec
	sch, err := getSchema(spec)
	if err != nil {
		return nil, err
	}

	schVal, err := encoding.MarshalAsNomsValue(root.VRW(), sch)
	tbl := doltdb.NewTable(root.VRW(), schVal, types.NewMap(root.VRW()))
	root = root.PutTable(db, tableName, tbl)

	return sch, nil
}

func getSchema(spec *sqlparser.TableSpec) (schema.Schema, error) {
	cols := make([]schema.Column, len(spec.Columns))

	var tag uint64
	for i, colDef := range spec.Columns {
		col, err := getColumn(colDef, spec.Indexes, tag)
		if err != nil {
			return nil, err
		}
		cols[i] = col
		tag++
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(colColl), nil
}


func getColumn(colDef *sqlparser.ColumnDefinition, indexes []*sqlparser.IndexDefinition, tag uint64) (schema.Column, error) {
	columnType := colDef.Type
	isPkey := false

OuterLoop:
	for _, index := range indexes {
		if index.Info.Primary {
			for _, indexCol := range index.Columns {
				if indexCol.Column.Equal(colDef.Name) {
					isPkey = true
				}
				break OuterLoop
			}
		}
	}

	switch columnType.Type {

	// integer-like types
	case TINYINT:
		fallthrough
	case SMALLINT:
		fallthrough
	case MEDIUMINT:
		fallthrough
	case INT:
		fallthrough
	case INTEGER:
		fallthrough
	case BIGINT:
		return schema.NewColumn(colDef.Name.String(), tag, types.IntKind, isPkey), nil

	// string-like types
	case TEXT:
		fallthrough
	case TINYTEXT:
		fallthrough
	case MEDIUMTEXT:
		fallthrough
	case LONGTEXT:
		fallthrough
	case BLOB:
		fallthrough
	case TINYBLOB:
		fallthrough
	case MEDIUMBLOB:
		fallthrough
	case LONGBLOB:
		fallthrough
	case CHAR:
		fallthrough
	case VARCHAR:
		fallthrough
	case BINARY:
		fallthrough
	case VARBINARY:
		return schema.NewColumn(colDef.Name.String(), tag, types.StringKind, isPkey), nil

	// float-like types
	case FLOAT_TYPE:
		fallthrough
	case DOUBLE:
		fallthrough
	case DECIMAL:
		return schema.NewColumn(colDef.Name.String(), tag, types.FloatKind, isPkey), nil

	// bool-like types
	case BIT:
		fallthrough
	case BOOLEAN:
		return schema.NewColumn(colDef.Name.String(), tag, types.BoolKind, isPkey), nil

	// time-like types
	case DATE:
		fallthrough
	case TIME:
		fallthrough
	case DATETIME:
		fallthrough
	case TIMESTAMP:
		fallthrough
	case YEAR:
		return errColumn("Date and time types aren't supported")

	// unsupported types
	case ENUM:
		return errColumn("Unsupported column type %v", columnType.Type)
	case SET:
		return errColumn("Unsupported column type %v", columnType.Type)
	case JSON:
		return errColumn("Unsupported column type %v", columnType.Type)
	case GEOMETRY:
		return errColumn("Unsupported column type %v", columnType.Type)
	case POINT:
		return errColumn("Unsupported column type %v", columnType.Type)
	case LINESTRING:
		return errColumn("Unsupported column type %v", columnType.Type)
	case POLYGON:
		return errColumn("Unsupported column type %v", columnType.Type)
	case GEOMETRYCOLLECTION:
		return errColumn("Unsupported column type %v", columnType.Type)
	case MULTIPOINT:
		return errColumn("Unsupported column type %v", columnType.Type)
	case MULTILINESTRING:
		return errColumn("Unsupported column type %v", columnType.Type)
	case MULTIPOLYGON:
		return errColumn("Unsupported column type %v", columnType.Type)
	default:
		return errColumn("Unrecognized column type %v", columnType.Type)
	}
}

func errColumn(errFmt string, args... interface{}) (schema.Column, error) {
	return schema.Column{}, errors.New(fmt.Sprintf(errFmt, args...))
}

func errCreate(errFmt string, args... interface{}) (schema.Schema, error) {
	return nil, errors.New(fmt.Sprintf(errFmt, args...))
}
