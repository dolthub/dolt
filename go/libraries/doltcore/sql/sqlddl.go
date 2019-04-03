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

// For some reason these constants are private in the sql parser library, so we need to either fork that package or
// duplicate them here. Not quite ready to fork it, so duplicating for now.
const (
	colKeyNone sqlparser.ColumnKeyOption = iota
	colKeyPrimary
	colKeySpatialKey
	colKeyUnique
	colKeyUniqueKey
	colKey
)

// ExecuteCreate executes the given create statement and returns the new root value of the database and its
// accompanying schema.
func ExecuteCreate(db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, schema.Schema, error) {
	if ddl.Action != sqlparser.CreateStr {
		panic("expected create statement")
	}

	// Unlike other SQL statements, DDL statements can have an error but still return a statement from Parse().
	// Callers should call ParseStrictDDL themselves if they want to verify a DDL statement parses correctly.
	_, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		return &doltdb.RootValue{}, nil, err
	}

	var tableName string
	tableNameExpr := ddl.NewName
	tableName = tableNameExpr.Name.String()

	if root.HasTable(tableName) {
		return errCreate("error: table %v already defined", tableName)
	}

	spec := ddl.TableSpec
	sch, err := getSchema(spec)
	if err != nil {
		return nil, nil, err
	}

	schVal, err := encoding.MarshalAsNomsValue(root.VRW(), sch)
	tbl := doltdb.NewTable(root.VRW(), schVal, types.NewMap(root.VRW()))
	root = root.PutTable(db, tableName, tbl)

	return root, sch, nil
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
	isPkey := colDef.Type.KeyOpt == colKeyPrimary

	if !isPkey {
	OuterLoop:
		for _, index := range indexes {
			if index.Info.Primary {
				for _, indexCol := range index.Columns {
					if indexCol.Column.Equal(colDef.Name) {
						isPkey = true
						break OuterLoop
					}
				}
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
	case BOOLEAN: // not actually implemented at the moment (should be a synonym for bit)
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

func errCreate(errFmt string, args... interface{}) (*doltdb.RootValue, schema.Schema, error) {
	return nil, nil, errors.New(fmt.Sprintf(errFmt, args...))
}
