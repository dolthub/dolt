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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/index"
)

var errDoltSchemasTableFormat = fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)

const (
	viewFragment    = "view"
	triggerFragment = "trigger"
)

type Extra struct {
	CreatedAt int64
}

func mustNewColWithTypeInfo(name string, tag uint64, typeInfo typeinfo.TypeInfo, partOfPK bool, defaultVal string, autoIncrement bool, comment string, constraints ...schema.ColConstraint) schema.Column {
	col, err := schema.NewColumnWithTypeInfo(name, tag, typeInfo,partOfPK, defaultVal, autoIncrement, comment, constraints...)
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
var schemasTableCols = schema.NewColCollection(
	mustNewColWithTypeInfo(doltdb.SchemasTablesTypeCol, schema.DoltSchemasTypeTag, typeinfo.CreateVarStringTypeFromSqlType(mustCreateStringType(query.Type_VARCHAR, 64, sql.Collation_utf8mb4_0900_ai_ci)), true, "", false, ""),
	mustNewColWithTypeInfo(doltdb.SchemasTablesNameCol, schema.DoltSchemasNameTag, typeinfo.CreateVarStringTypeFromSqlType(mustCreateStringType(query.Type_VARCHAR, 64, sql.Collation_utf8mb4_0900_ai_ci)), true, "", false, ""),
	mustNewColWithTypeInfo(doltdb.SchemasTablesFragmentCol, schema.DoltSchemasFragmentTag, typeinfo.CreateVarStringTypeFromSqlType(gmstypes.LongText), false, "", false, ""),
	mustNewColWithTypeInfo(doltdb.SchemasTablesExtraCol, schema.DoltSchemasExtraTag, typeinfo.JSONType, false, "", false, ""),
)

var schemaTableSchema = schema.MustSchemaFromCols(schemasTableCols)
var schemaTableKd = schemaTableSchema.GetKeyDescriptor()
var schemaTableVd = schemaTableSchema.GetValueDescriptor()

func SchemasTableSchema() schema.Schema {
	return schemaTableSchema
}

// GetOrCreateDoltSchemasTable returns the `dolt_schemas` table in `db`, creating it if it does not already exist.
func GetOrCreateDoltSchemasTable(ctx *sql.Context, db Database) (retTbl *WritableDoltTable, retErr error) {
	tbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}

	if found {
		return tbl.(*WritableDoltTable), nil
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}
	
	// Create new empty table
	err = db.createDoltTable(ctx, doltdb.SchemasTableName, root, schemaTableSchema)
	if err != nil {
		return nil, err
	}
	tbl, found, err = db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sql.ErrTableNotFound.New("dolt_schemas")
	}

	return tbl.(*WritableDoltTable), nil
}

// fragFromSchemasTable returns the row with the given schema fragment if it exists.
func fragFromSchemasTable(ctx *sql.Context, tbl *WritableDoltTable, fragType string, name string) (sql.Row, bool, error) {
	// indexes, err := tbl.GetIndexes(ctx)
	// if err != nil {
	// 	return nil, false, err
	// }
	var fragNameIndex sql.Index
	// TODO: replace with primary key lookup
	// for _, index := range indexes {
	// 	if index.ID() == doltdb.SchemasTablesIndexName {
	// 		fragNameIndex = index
	// 		break
	// 	}
	// }
	// if fragNameIndex == nil {
	// 	return nil, false, noSchemaIndexDefined
	// }

	exprs := fragNameIndex.Expressions()
	lookup, err := sql.NewIndexBuilder(fragNameIndex).Equals(ctx, exprs[0], fragType).Equals(ctx, exprs[1], name).Build(ctx)
	if err != nil {
		return nil, false, err
	}

	iter, err := index.RowIterForIndexLookup(ctx, tbl.DoltTable, lookup, tbl.sqlSch, nil)
	if err != nil {
		return nil, false, err
	}

	defer func() {
		if cerr := iter.Close(ctx); cerr != nil {
			err = cerr
		}
	}()

	// todo(andy): use filtered reader?
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			return nil, false, nil
		}
		if err != nil {
			return nil, false, err
		}
		if sqlRow[0] != fragType || sqlRow[1] != name {
			continue
		}
		return sqlRow, true, nil
	}
}

type schemaFragment struct {
	name     string
	fragment string
	created  time.Time
}

func getSchemaFragmentsOfType(ctx *sql.Context, tbl *WritableDoltTable, fragType string) ([]schemaFragment, error) {
	iter, err := SqlTableToRowIter(ctx, tbl.DoltTable, nil)
	if err != nil {
		return nil, err
	}

	// The dolt_schemas table has undergone various changes over time and multiple possible schemas for it exist, so we 
	// need to get the column indexes from the current schema
	nameIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesNameCol)
	fragmentIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesFragmentCol)
	extraIdx := tbl.sqlSchema().IndexOfColName(doltdb.SchemasTablesExtraCol)
	
	var frags []schemaFragment
	for {
		sqlRow, err := iter.Next(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if sqlRow[fragmentIdx] != fragType {
			continue
		}

		// For older tables, use 1 as the trigger creation time
		if sqlRow[extraIdx] == nil {
			frags = append(frags, schemaFragment{
				name:     sqlRow[nameIdx].(string),
				fragment: sqlRow[fragmentIdx].(string),
				created:  time.Unix(1, 0).UTC(), // TablePlus editor thinks 0 is out of range
			})
			continue
		}

		// Extract Created Time from JSON column
		createdTime, err := getCreatedTime(ctx, sqlRow[extraIdx].(gmstypes.JSONValue))

		frags = append(frags, schemaFragment{
			name:     sqlRow[nameIdx].(string),
			fragment: sqlRow[fragmentIdx].(string),
			created:  time.Unix(createdTime, 0).UTC(),
		})
	}
	return frags, nil
}

func getCreatedTime(ctx *sql.Context, extraCol gmstypes.JSONValue) (int64, error) {
	doc, err := extraCol.Unmarshall(ctx)
	if err != nil {
		return 0, err
	}

	err = fmt.Errorf("value %v does not contain creation time", doc.Val)

	obj, ok := doc.Val.(map[string]interface{})
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
