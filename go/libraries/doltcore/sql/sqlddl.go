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
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
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

// ExecuteDrop executes the given drop statement and returns the new root value of the database.
func ExecuteDrop(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	if ddl.Action != sqlparser.DropStr {
		panic("expected drop statement")
	}

	// Unlike other SQL statements, DDL statements can have an error but still return a statement from Parse().
	// Callers should call ParseStrictDDL themselves if they want to verify a DDL statement parses correctly.
	if _, err := sqlparser.ParseStrictDDL(query); err != nil {
		return nil, err
	}

	if len(ddl.FromTables) == 0 {
		panic("FromTables empty")
	}

	tablesToDrop := make([]string, len(ddl.FromTables))
	for i, tableName := range ddl.FromTables {
		tablesToDrop[i] = tableName.Name.String()
	}

	var filtered []string
	for _, tableName := range tablesToDrop {
		if has, err := root.HasTable(ctx, tableName); err != nil {
			return nil, err
		} else if !has {
			if ddl.IfExists {
				continue
			} else {
				return nil, errFmt(UnknownTableErrFmt, tableName)
			}
		} else {
			filtered = append(filtered, tableName)
		}
	}

	var err error
	if root, err = root.RemoveTables(ctx, filtered...); err != nil {
		return nil, err
	}

	return root, nil
}

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
		return nil, nil, errFmt("Invalid table name: '%v'", tableName)
	}

	if has, err := root.HasTable(ctx, tableName); err != nil {
		return nil, nil, err
	} else if has {
		if ddl.IfNotExists {
			table, _, err := root.GetTable(ctx, tableName)

			if err != nil {
				return nil, nil, err
			}

			sch, err := table.GetSchema(ctx)

			if err != nil {
				return nil, nil, err
			}

			return root, sch, nil
		}
		return nil, nil, errFmt("Table '%v' already exists", tableName)
	}

	spec := ddl.TableSpec

	sch, err := getSchema(spec)
	if err != nil {
		return nil, nil, err
	}

	schVal, err := encoding.MarshalAsNomsValue(ctx, root.VRW(), sch)
	m, err := types.NewMap(ctx, root.VRW())

	if err != nil {
		return nil, nil, err
	}

	tbl, err := doltdb.NewTable(ctx, root.VRW(), schVal, m)

	if err != nil {
		return nil, nil, err
	}

	root, err = root.PutTable(ctx, tableName, tbl)

	if err != nil {
		return nil, nil, err
	}

	return root, sch, nil
}

// ExecuteAlter executes the given alter table statement and returns the new root value of the database.
func ExecuteAlter(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	// Unlike other SQL statements, DDL statements can have an error but still return a statement from Parse().
	// Callers should call ParseStrictDDL themselves if they want to verify a DDL statement parses correctly.
	_, err := sqlparser.ParseStrictDDL(query)
	if err != nil {
		return nil, err
	}

	switch ddl.Action {
	case sqlparser.AlterStr:
		return executeAlter(ctx, db, root, ddl, query)
	case sqlparser.RenameStr:
		return executeRename(ctx, db, root, ddl, query)
	default:
		return nil, errFmt("Unsupported alter statement: '%v'", query)
	}
}

// executeRename renames a set of tables and returns the new root value.
func executeRename(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	if len(ddl.FromTables) != len(ddl.ToTables) {
		panic("Expected from tables and to tables of equal length")
	}

	for i := range ddl.FromTables {
		fromTable := ddl.FromTables[i]
		toTable := ddl.ToTables[i]
		if err := validateTable(ctx, root, fromTable.Name.String()); err != nil {
			return nil, err
		}

		var err error
		if root, err = alterschema.RenameTable(ctx, db, root, fromTable.Name.String(), toTable.Name.String()); err != nil {
			if err == doltdb.ErrTableExists {
				return nil, errFmt("A table with the name '%v' already exists", toTable.Name.String())
			}
			return nil, err
		}
	}

	return root, nil
}

// validateTable returns an error if the given table name is invalid or if the table doesn't exist
func validateTable(ctx context.Context, root *doltdb.RootValue, tableName string) error {
	if !doltdb.IsValidTableName(tableName) {
		return errFmt("Invalid table name: '%v'", tableName)
	}

	if has, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if !has {
		return errFmt(UnknownTableErrFmt, tableName)
	}

	return nil
}

// executeAlter executes an alter statement and returns the updated root
func executeAlter(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, ddl *sqlparser.DDL, query string) (*doltdb.RootValue, error) {
	tableName := ddl.Table.Name.String()
	if err := validateTable(ctx, root, tableName); err != nil {
		return nil, err
	}

	switch ddl.ColumnAction {
	case sqlparser.AddStr:
		return addColumn(ctx, db, root, tableName, ddl.TableSpec)
	case sqlparser.DropStr:
		return dropColumn(ctx, db, root, tableName, ddl.Column)
	case sqlparser.RenameStr:
		return renameColumn(ctx, db, root, tableName, ddl.Column, ddl.ToColumn)
	default:
		return nil, errFmt("Unsupported alter table statement: '%v'", query)
	}
}

// renameColumn renames the column named. Returns the new root value and new schema, or an error if one occurs.
func renameColumn(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, tableName string, fromCol, toCol sqlparser.ColIdent) (*doltdb.RootValue, error) {
	table, _, err := root.GetTable(ctx, tableName)

	if err != nil {
		return nil, err
	}

	updatedTable, err := alterschema.RenameColumn(ctx, db, table, fromCol.String(), toCol.String())
	if err != nil {
		if err == schema.ErrColNotFound {
			return nil, errFmt(UnknownColumnErrFmt, fromCol.String())
		} else if err == schema.ErrColNameCollision {
			return nil, errFmt("A column with the name '%v' already exists", toCol.String())
		}
		return nil, err
	}

	return root.PutTable(ctx, tableName, updatedTable)
}

// dropColumn drops the column named from the table named. Returns the new root value and new schema, or an error if one occurs.
func dropColumn(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, tableName string, col sqlparser.ColIdent) (*doltdb.RootValue, error) {
	table, _, err := root.GetTable(ctx, tableName)

	if err != nil {
		return nil, err
	}

	updatedTable, err := alterschema.DropColumn(ctx, db, table, col.String())
	if err != nil {
		if err == schema.ErrColNotFound {
			return nil, errFmt(UnknownColumnErrFmt, col.String())
		}
		return nil, err
	}

	return root.PutTable(ctx, tableName, updatedTable)
}

// addColumn adds the column given to the table named. Returns the new root value and new schema, or an error if one occurs.
func addColumn(ctx context.Context, db *doltdb.DoltDB, root *doltdb.RootValue, tableName string, spec *sqlparser.TableSpec) (*doltdb.RootValue, error) {
	table, _, err := root.GetTable(ctx, tableName)

	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)

	if err != nil {
		return nil, err
	}

	tag := schema.AutoGenerateTag(sch)

	colDef := spec.Columns[0]
	col, defaultVal, err := getColumn(colDef, spec.Indexes, tag)
	if err != nil {
		return nil, err
	}
	if col.IsPartOfPK {
		return nil, errFmt("Adding primary keys is not supported")
	}

	nullable := alterschema.NotNull
	if col.IsNullable() {
		nullable = alterschema.Null
	}

	updatedTable, err := alterschema.AddColumnToTable(ctx, db, table, col.Tag, col.Name, col.Kind, nullable, defaultVal)
	if err != nil {
		return nil, err
	}

	return root.PutTable(ctx, tableName, updatedTable)
}

// getSchema returns the schema corresponding to the TableSpec given
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

	// TODO: type conversion. This doesn't work at all for uint columns (parser always thinks integer literals are int,
	//  not uint)
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
