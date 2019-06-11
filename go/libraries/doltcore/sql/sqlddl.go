package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/xwb1989/sqlparser"
	"strconv"
	"strings"
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

var ErrNoPrimaryKeyColumns = errors.New("at least one primary key column must be specified")
var tagCommentPrefix = "tag:"

// ExecuteCreate executes the given create statement and returns the new root value of the database and its
// accompanying schema.
func ExecuteCreate(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, schema.Schema, error) {
	if ddl.Action != sqlparser.CreateStr {
		panic("expected create statement")
	}

	// Unlike other SQL statements, DDL statements can have an error but still return a statement from Parse().
	// Callers should call ParseStrictDDL themselves if they want to verify a DDL statement parses correctly.
	_, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		return nil, nil, err
	}

	tableName := ddl.Table.Name.String()
	if !doltdb.IsValidTableName(tableName) {
		return errCreate("Invalid table name: '%v'", tableName)
	}

	if root.HasTable(ctx, tableName) {
		return errCreate("Table %v already exists", tableName)
	}

	spec := ddl.TableSpec

	sch, err := getSchema(spec)
	if err != nil {
		return nil, nil, err
	}

	schVal, err := encoding.MarshalAsNomsValue(ctx, root.VRW(), sch)
	tbl := doltdb.NewTable(ctx, root.VRW(), schVal, types.NewMap(ctx, root.VRW()))
	root = root.PutTable(ctx, db, tableName, tbl)

	return root, sch, nil
}

// ExecuteAlter executes the given create statement and returns the new root value of the database and its
// accompanying schema.
func ExecuteAlter(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, schema.Schema, error) {
	if ddl.Action != sqlparser.AlterStr {
		panic("expected alter statement")
	}

	// Unlike other SQL statements, DDL statements can have an error but still return a statement from Parse().
	// Callers should call ParseStrictDDL themselves if they want to verify a DDL statement parses correctly.
	_, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		return nil, nil, err
	}

	tableName := ddl.Table.Name.String()
	if !doltdb.IsValidTableName(tableName) {
		return errCreate("Invalid table name: '%v'", tableName)
	}

	if !root.HasTable(ctx, tableName) {
		return nil, nil, errFmt(UnknownTableErrFmt, tableName)
	}

	switch ddl.ColumnAction {
	case sqlparser.AddStr:
		return ExecuteAddColumn(ctx, db, root, tableName, ddl.TableSpec)
	case sqlparser.DropStr:
		return ExecuteDropColumn(ctx, db, root, tableName, ddl.Column)
	case sqlparser.RenameStr:
		return ExecuteRenameColumn(ctx, db, root, tableName, ddl.Column, ddl.ToColumn)
	default:
		return nil, nil, errFmt("Unsupported alter table statement: '%v'", nodeToString(ddl))
	}
}

// ExecuteRenameColumn renames the column named. Returns the new root value and new schema, or an error if one occurs.
func ExecuteRenameColumn(ctx context.Context, db *doltdb.DoltDB, value *doltdb.RootValue, tableName string, fromCol sqlparser.ColIdent, toCol sqlparser.ColIdent) (*doltdb.RootValue, schema.Schema, error) {

	return nil, nil, nil
}

// Drops the column named from the table named. Returns the new root value and new schema, or an error if one occurs.
func ExecuteDropColumn(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, tableName string, col sqlparser.ColIdent) (*doltdb.RootValue, schema.Schema, error) {
	table, _ := root.GetTable(ctx, tableName)

	updatedTable, err := alterschema.DropColumn(ctx, db, table, col.String())
	if err != nil {
		if err == schema.ErrColNotFound {
			return nil, nil, errFmt(UnknownColumnErrFmt, col.String())
		}
		return nil, nil, err
	}

	root = root.PutTable(ctx, db, tableName, updatedTable)
	return root, updatedTable.GetSchema(ctx), nil
}

// Adds the column given to the table named. Returns the new root value and new schema, or an error if one occurs.
func ExecuteAddColumn(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, tableName string, spec *sqlparser.TableSpec) (*doltdb.RootValue, schema.Schema, error) {
	table, _ := root.GetTable(ctx, tableName)
	sch := table.GetSchema(ctx)
	tag := schema.AutoGenerateTag(sch)

	colDef := spec.Columns[0]
	col, defaultVal, err := getColumn(colDef, spec.Indexes, tag)
	if err != nil {
		return nil, nil, err
	}
	if col.IsPartOfPK {
		return nil, nil, errFmt("Adding primary keys is not supported")
	}

	nullable := alterschema.NotNull
	if col.IsNullable() {
		nullable = alterschema.Null
	}

	updatedTable, err := alterschema.AddColumnToTable(ctx, db, table, col.Tag, col.Name, col.Kind, nullable, defaultVal)
	if err != nil {
		return nil, nil, err
	}

	root = root.PutTable(ctx, db, tableName, updatedTable)
	return root, updatedTable.GetSchema(ctx), nil
}

func getSchema(spec *sqlparser.TableSpec) (schema.Schema, error) {
	cols := make([]schema.Column, len(spec.Columns))

	var tag uint64
	var seenPk bool
	for i, colDef := range spec.Columns {
		// TODO: support default value
		col, _, err := getColumn(colDef, spec.Indexes, tag)
		if err != nil {
			return nil, err
		}
		if col.IsPartOfPK {
			seenPk = true
		}
		cols[i] = col
		tag++
	}
	if !seenPk {
		return nil, ErrNoPrimaryKeyColumns
	}

	colColl, err := schema.NewColCollection(cols...)
	if err != nil {
		return nil, err
	}

	return schema.SchemaFromCols(colColl), nil
}

// fakeResolver satisfies the TagResolver interface to let us fetch a value from a RowValGetter, without needing an
// actual row. This only works for literal values.
type fakeResolver struct {
	TagResolver
}

func (fakeResolver) ResolveTag(tableName string, columnName string) (uint64, error) {
	return schema.InvalidTag, errors.New("Fake ResolveTag called")
}

// getColumn returns the column given by the definition, indexes, and tag given, as well as its default value if
// specified by the definition. The tag may be overridden if the column definition includes a tag already.
func getColumn(colDef *sqlparser.ColumnDefinition, indexes []*sqlparser.IndexDefinition, tag uint64) (schema.Column, types.Value, error) {
	columnType := colDef.Type

	// Primary key info can either be specified in the column's type info (for in-line declarations), or in a slice of
	// indexes attached to the table def. We have to check both places to find if a column is part of the primary key
	isPkey := colDef.Type.KeyOpt == colKeyPrimary
	notNull := bool(colDef.Type.NotNull)

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

	var constraints []schema.ColConstraint
	if isPkey || notNull {
		constraints = append(constraints, schema.NotNullConstraint{})
	}

	commentTag := extractTag(columnType)
	if commentTag != schema.InvalidTag {
		tag = commentTag
	}

	var colKind types.NomsKind
	switch columnType.Type {

	// integer-like types
	case TINYINT, SMALLINT, MEDIUMINT, INT, INTEGER, BIGINT:
		kind := types.IntKind
		if columnType.Unsigned {
			kind = types.UintKind
		}
		colKind = kind

	// UUID type
	case UUID:
		colKind = types.UUIDKind

	// string-like types
	// TODO: enforce length constraints for string types
	// TODO: support different charsets
	case TEXT, TINYTEXT, MEDIUMTEXT, LONGTEXT, CHAR, VARCHAR:
		colKind = types.StringKind

	// blob-like types
	case BLOB, TINYBLOB, MEDIUMBLOB, LONGBLOB:
		colKind = types.BlobKind

	// float-like types
	case FLOAT_TYPE, DOUBLE, DECIMAL:
		colKind = types.FloatKind

	// bool-like types
	case BIT, BOOLEAN, BOOL:
		colKind = types.BoolKind

	// time-like types (not yet supported in noms, but should be)
	case DATE, TIME, DATETIME, TIMESTAMP, YEAR:
		return errColumn("Date and time types aren't supported")

	// binary string types, need to support differently from normal strings
	case BINARY, VARBINARY:
		return errColumn("BINARY and VARBINARY types are not supported")

	// unsupported types
	case ENUM, SET, JSON, GEOMETRY, POINT, LINESTRING, POLYGON, GEOMETRYCOLLECTION, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON:
		return errColumn("Unsupported column type %v", columnType.Type)

	// unrecognized types
	default:
		return errColumn("Unrecognized column type %v", columnType.Type)
	}

	column := schema.NewColumn(colDef.Name.String(), tag, colKind, isPkey, constraints...)

	if colDef.Type.Default == nil {
		return column, nil, nil
	}

	// Get the default value. This can be any expression (usually a literal value). We aren't using the simpler semantics
	// of extractNomsValueFromSQLVal here, that doesn't cover the full range of expressions permitted by SQL (like -1.0,
	// 2+2, CONCAT("a", "b")).
	getter, err := getterFor(colDef.Type.Default, nil, NewAliases())
	if err != nil {
		return schema.InvalidCol, nil, err
	}

	if getter.NomsKind != colKind {
		return errColumn("Type mismatch for default value of column %v: '%v'", column.Name, nodeToString(colDef.Type.Default))
	}

	if err = getter.Init(fakeResolver{}); err != nil {
		return errColumn("Unsupported default expression for column %v: '%v'", column.Name, nodeToString(colDef.Type.Default))
	}

	var defaultVal types.Value
	// Extracting the default value this way requires us to be prepared to panic, since we're using a nil row to extract
	// the value. This should work fine for literal expressions, but might panic otherwise.
	func() {
		defer func() {
			rp := recover()
			if rp != nil {
				err = errFmt("Unsupported default expression for column %v: '%v'", column.Name, nodeToString(colDef.Type.Default))
			}
		}()

		defaultVal = getter.Get(nil)
	}()

	if err != nil {
		return schema.InvalidCol, nil, err
	}

	return column, defaultVal, nil
}

// Extracts the optional comment tag from a column type defn, or InvalidTag if it can't be extracted
func extractTag(columnType sqlparser.ColumnType) uint64 {
	if columnType.Comment == nil {
		return schema.InvalidTag
	}

	sqlVal := columnType.Comment
	if sqlVal.Type != sqlparser.StrVal {
		return schema.InvalidTag
	}

	commentString := string(sqlVal.Val)
	i := strings.Index(commentString, tagCommentPrefix)
	if i >= 0 {
		startIdx := i + len(tagCommentPrefix)
		tag, err := strconv.ParseUint(commentString[startIdx:], 10, 64)
		if err != nil {
			return schema.InvalidTag
		}
		return tag
	}

	return schema.InvalidTag
}

func errColumn(errFmt string, args ...interface{}) (schema.Column, types.Value, error) {
	return schema.InvalidCol, nil, errors.New(fmt.Sprintf(errFmt, args...))
}

func errCreate(errFmt string, args ...interface{}) (*doltdb.RootValue, schema.Schema, error) {
	return nil, nil, errors.New(fmt.Sprintf(errFmt, args...))
}
