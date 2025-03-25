// Copyright 2024 Dolthub, Inc.
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

package resolve

import (
	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// Table returns the schema-qualified name of the table given in the root provided, along with the table itself
// and whether it exists.
func Table(
	ctx *sql.Context,
	root doltdb.RootValue,
	tableName string,
) (doltdb.TableName, *doltdb.Table, bool, error) {
	if UseSearchPath {
		return TableWithSearchPath(ctx, root, tableName)
	}

	tName := doltdb.TableName{Schema: doltdb.DefaultSchemaName, Name: tableName}
	tbl, correctedTableName, tblExists, err := doltdb.GetTableInsensitive(ctx, root, tName)
	tName.Name = correctedTableName
	return tName, tbl, tblExists, err
}

// TableName returns the schema-qualified name of the table given in the root provided, along with whether it exists.
func TableName(
	ctx *sql.Context,
	root doltdb.RootValue,
	tableName string,
) (doltdb.TableName, bool, error) {
	if UseSearchPath {
		return TableNameWithSearchPath(ctx, root, tableName)
	}

	tName := doltdb.TableName{Schema: doltdb.DefaultSchemaName, Name: tableName}
	resolvedName, ok, err := root.ResolveTableName(ctx, tName)
	if err != nil || !ok {
		return doltdb.TableName{}, false, err
	}
	return doltdb.TableName{Schema: doltdb.DefaultSchemaName, Name: resolvedName}, true, nil
}

// TablesOnSearchPath returns all the tables in the root value given that are in a schema in the search path
func TablesOnSearchPath(ctx *sql.Context, root doltdb.RootValue) ([]doltdb.TableName, error) {
	schemasToSearch, err := SearchPath(ctx)
	if err != nil {
		return nil, err
	}

	var tableNames []doltdb.TableName
	for _, schemaName := range schemasToSearch {
		names, err := root.GetTableNames(ctx, schemaName)
		if err != nil {
			return nil, err
		}
		tableNames = append(tableNames, doltdb.ToTableNames(names, schemaName)...)
	}

	return tableNames, nil
}

// TableNameWithSearchPath resolves a table name in the root value, searching through the schemas in the search path.
func TableNameWithSearchPath(
	ctx *sql.Context,
	root doltdb.RootValue,
	tableName string,
) (doltdb.TableName, bool, error) {
	schemasToSearch, err := SearchPath(ctx)
	if err != nil {
		return doltdb.TableName{}, false, err
	}

	for _, schemaName := range schemasToSearch {
		tablesInSchema, err := root.GetTableNames(ctx, schemaName)
		if err != nil {
			return doltdb.TableName{}, false, err
		}
		correctedTableName, ok := sql.GetTableNameInsensitive(tableName, tablesInSchema)
		if !ok {
			continue
		}

		candidate := doltdb.TableName{Name: correctedTableName, Schema: schemaName}
		ok, err = root.HasTable(ctx, candidate)
		if err != nil {
			return doltdb.TableName{}, false, err
		} else if !ok {
			// Should be impossible
			return doltdb.TableName{}, false, nil
		}

		return candidate, true, nil
	}

	return doltdb.TableName{}, false, nil
}

// TableWithSearchPath resolves a table name to a table in the root value, searching through the schemas in the search path.
func TableWithSearchPath(
	ctx *sql.Context,
	root doltdb.RootValue,
	tableName string,
) (doltdb.TableName, *doltdb.Table, bool, error) {
	correctedName, ok, err := TableNameWithSearchPath(ctx, root, tableName)
	if err != nil || !ok {
		return doltdb.TableName{}, nil, false, err
	}
	tbl, ok, err := root.GetTable(ctx, correctedName)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	} else if !ok {
		// Should be impossible
		return doltdb.TableName{}, nil, false, nil
	}
	return correctedName, tbl, true, nil
}
