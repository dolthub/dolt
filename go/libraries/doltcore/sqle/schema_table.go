// Copyright 2020 Dolthub, Inc.
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

package sqle

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
)

const (
	viewFragment    = "view"
	triggerFragment = "trigger"
	eventFragment   = "event"
)

type Extra struct {
	CreatedAt int64
}

type SchemaTable struct {
	backingTable *WritableDoltTable
}

func (st *SchemaTable) Name() string {
	return doltdb.SchemasTableName
}

func (st *SchemaTable) String() string {
	return doltdb.SchemasTableName
}

func (st *SchemaTable) Schema() sql.Schema {
	return SchemaTableSqlSchema().Schema
}

func (st *SchemaTable) Collation() sql.CollationID {
	return sql.Collation_Default
}

func (st *SchemaTable) Partitions(ctx *sql.Context) (sql.PartitionIter, error) {
	if st.backingTable == nil {
		return index.SinglePartitionIterFromNomsMap(nil), nil
	}
	return st.backingTable.Partitions(ctx)
}

func (st *SchemaTable) PartitionRows(ctx *sql.Context, partition sql.Partition) (sql.RowIter, error) {
	if st.backingTable == nil {
		return sql.RowsToRowIter(), nil
	}
	return st.backingTable.PartitionRows(ctx, partition)
}

func (st *SchemaTable) LockedToRoot(ctx *sql.Context, root doltdb.RootValue) (sql.IndexAddressableTable, error) {
	if st.backingTable == nil {
		return st, nil
	}
	return st.backingTable.LockedToRoot(ctx, root)
}

func (st *SchemaTable) IndexedAccess(lookup sql.IndexLookup) sql.IndexedTable {
	// Never reached. Interface required for LockedToRoot to be implemented.
	panic("Unreachable")
}

func (st *SchemaTable) GetIndexes(ctx *sql.Context) ([]sql.Index, error) {
	return nil, nil
}

func (st *SchemaTable) PreciseMatch() bool {
	return true
}

func (st *SchemaTable) UnWrap() *WritableDoltTable {
	return st.backingTable
}

var _ sql.Table = (*SchemaTable)(nil)
var _ dtables.VersionableTable = (*SchemaTable)(nil)
var _ sql.IndexAddressableTable = (*SchemaTable)(nil)
var _ WritableDoltTableWrapper = (*SchemaTable)(nil)

func SchemaTableSqlSchema() sql.PrimaryKeySchema {
	sqlSchema, err := sqlutil.FromDoltSchema("", doltdb.SchemasTableName, SchemaTableSchema())
	if err != nil {
		panic(err) // should never happen
	}
	return sqlSchema
}

func mustNewColWithTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, defaultVal string, autoIncrement bool, comment string, constraints ...schema.ColConstraint) schema.Column {
	col, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo, partOfPK, defaultVal, autoIncrement, comment, constraints...)
	if err != nil {
		panic(err)
	}
	return col
}

func mustCreateStringType(baseType query.Type, length int64, collation sql.CollationID) sql.StringType {
	ti, err := gmstypes.CreateString(baseType, length, collation)
	if err != nil {
		panic(err)
	}
	return ti
}

// dolt_schemas columns
func SchemaTableSchema() schema.Schema {
	var schemasTableCols = schema.NewColCollection(
		mustNewColWithTypeInfo(doltdb.SchemasTablesTypeCol, schema.DoltSchemasTypeTag, typeinfo.CreateVarStringTypeFromSqlType(mustCreateStringType(query.Type_VARCHAR, 64, sql.Collation_utf8mb4_0900_ai_ci)), true, "", false, ""),
		mustNewColWithTypeInfo(doltdb.SchemasTablesNameCol, schema.DoltSchemasNameTag, typeinfo.CreateVarStringTypeFromSqlType(mustCreateStringType(query.Type_VARCHAR, 64, sql.Collation_utf8mb4_0900_ai_ci)), true, "", false, ""),
		mustNewColWithTypeInfo(doltdb.SchemasTablesFragmentCol, schema.DoltSchemasFragmentTag, typeinfo.CreateVarStringTypeFromSqlType(gmstypes.LongText), false, "", false, ""),
		mustNewColWithTypeInfo(doltdb.SchemasTablesExtraCol, schema.DoltSchemasExtraTag, typeinfo.JSONType, false, "", false, ""),
		mustNewColWithTypeInfo(doltdb.SchemasTablesSqlModeCol, schema.DoltSchemasSqlModeTag, typeinfo.CreateVarStringTypeFromSqlType(mustCreateStringType(query.Type_VARCHAR, 256, sql.Collation_utf8mb4_0900_ai_ci)), false, "", false, ""),
	)

	return schema.MustSchemaFromCols(schemasTableCols)
}

func NewEmptySchemaTable() sql.Table {
	return &SchemaTable{}
}

func NewSchemaTable(backingTable *WritableDoltTable) sql.Table {
	return &SchemaTable{backingTable: backingTable}
}

// getOrCreateDoltSchemasTable returns the `dolt_schemas` table in `db`, creating it if it does not already exist.
// Also migrates data to the correct format if necessary.
func getOrCreateDoltSchemasTable(ctx *sql.Context, db Database) (retTbl *WritableDoltTable, retErr error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	schemaName := doltdb.DefaultSchemaName
	if resolve.UseSearchPath {
		if db.schemaName == "" {
			schemaName, err = resolve.FirstExistingSchemaOnSearchPath(ctx, root)
			if err != nil {
				return nil, err
			}
			db.schemaName = schemaName
		} else {
			schemaName = db.schemaName
		}
	}

	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return nil, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}

	if wrapper.backingTable != nil {
		schemasTable := wrapper.backingTable
		// Old schemas are missing the `extra` column. Very ancient. Provide error message and bail.
		if !schemasTable.Schema().Contains(doltdb.SchemasTablesExtraCol, doltdb.SchemasTableName) {
			return nil, fmt.Errorf("cannot migrate dolt_schemas table from v0.19.1 or earlier")
		} else {
			return schemasTable, nil
		}
	}

	// Create new empty table
	err = db.createDoltTable(ctx, doltdb.SchemasTableName, schemaName, root, SchemaTableSchema())
	if err != nil {
		return nil, err
	}
	tbl, _, err = db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok = tbl.(*SchemaTable)
	if !ok {
		return nil, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}
	if wrapper.backingTable == nil {
		return nil, sql.ErrTableNotFound.New(doltdb.SchemasTableName)
	}

	return wrapper.backingTable, nil
}

// fragFromSchemasTable returns the row with the given schema fragment if it exists.
func fragFromSchemasTable(ctx *sql.Context, tbl *WritableDoltTable, fragType string, name string) (r sql.Row, found bool, rerr error) {
	fragType, name = strings.ToLower(fragType), strings.ToLower(name)

	// This performs a full table scan in the worst case, but it's only used when adding or dropping a trigger or view
	iter, err := SqlTableToRowIter(ctx, tbl.DoltTable, nil)
	if err != nil {
		return nil, false, err
	}

	defer func(iter sql.RowIter, ctx *sql.Context) {
		err := iter.Close(ctx)
		if err != nil && rerr == nil {
			rerr = err
		}
	}(iter, ctx)

	// The dolt_schemas table has undergone various changes over time and multiple possible schemas for it exist, so we
	// need to get the column indexes from the current schema
	nameIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesNameCol)
	typeIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesTypeCol)

	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, false, err
		}

		// These columns are case insensitive, make sure to do a case-insensitive comparison
		if strings.ToLower(sqlRow[typeIdx].(string)) == fragType && strings.ToLower(sqlRow[nameIdx].(string)) == name {
			return sqlRow, true, nil
		}
	}

	return nil, false, nil
}

type schemaFragment struct {
	name     string
	fragment string
	created  time.Time
	// sqlMode indicates the SQL_MODE that was used when this schema fragment was initially parsed. SQL_MODE settings
	// such as ANSI_QUOTES control customized parsing behavior needed for some schema fragments.
	sqlMode string
}

func getSchemaFragmentsOfType(ctx *sql.Context, tbl *WritableDoltTable, fragType string) (sf []schemaFragment, rerr error) {
	iter, err := SqlTableToRowIter(ctx, tbl.DoltTable, nil)
	if err != nil {
		return nil, err
	}

	// The dolt_schemas table has undergone various changes over time and multiple possible schemas for it exist, so we
	// need to get the column indexes from the current schema
	nameIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesNameCol)
	typeIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesTypeCol)
	fragmentIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesFragmentCol)
	extraIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesExtraCol)
	sqlModeIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesSqlModeCol)

	defer func(iter sql.RowIter, ctx *sql.Context) {
		err := iter.Close(ctx)
		if err != nil && rerr == nil {
			rerr = err
		}
	}(iter, ctx)

	var frags []schemaFragment
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if sqlRow[typeIdx] != fragType {
			continue
		}

		sqlModeString := ""
		if sqlModeIdx >= 0 {
			if s, ok := sqlRow[sqlModeIdx].(string); ok {
				sqlModeString = s
			}
		} else {
			defaultSqlMode, err := loadDefaultSqlMode()
			if err != nil {
				return nil, err
			}
			sqlModeString = defaultSqlMode
		}

		// For older tables, use 1 as the trigger creation time
		if extraIdx < 0 || sqlRow[extraIdx] == nil {
			frags = append(frags, schemaFragment{
				name:     sqlRow[nameIdx].(string),
				fragment: sqlRow[fragmentIdx].(string),
				created:  time.Unix(1, 0).UTC(), // TablePlus editor thinks 0 is out of range
				sqlMode:  sqlModeString,
			})
			continue
		}

		// Extract Created Time from JSON column
		createdTime, err := getCreatedTime(ctx, sqlRow[extraIdx].(sql.JSONWrapper))
		if err != nil {
			return nil, err
		}

		frags = append(frags, schemaFragment{
			name:     sqlRow[nameIdx].(string),
			fragment: sqlRow[fragmentIdx].(string),
			created:  time.Unix(createdTime, 0).UTC(),
			sqlMode:  sqlModeString,
		})
	}

	return frags, nil
}

// loadDefaultSqlMode loads the default value for the @@SQL_MODE system variable and returns it, along
// with any unexpected errors encountered while reading the default value.
func loadDefaultSqlMode() (string, error) {
	global, _, ok := sql.SystemVariables.GetGlobal("SQL_MODE")
	if !ok {
		return "", fmt.Errorf("unable to load default @@SQL_MODE")
	}
	s, ok := global.GetDefault().(string)
	if !ok {
		return "", fmt.Errorf("unexpected type for @@SQL_MODE default value: %T", global.GetDefault())
	}
	return s, nil
}

func getCreatedTime(ctx *sql.Context, extraCol sql.JSONWrapper) (int64, error) {
	doc, err := extraCol.ToInterface()
	if err != nil {
		return 0, err
	}

	err = fmt.Errorf("value %v does not contain creation time", doc)

	obj, ok := doc.(map[string]interface{})
	if !ok {
		return 0, err
	}

	v, ok := obj["CreatedAt"]
	if !ok {
		return 0, err
	}

	f, ok := v.(float64)
	if !ok {
		return 0, err
	}
	return int64(f), nil
}
