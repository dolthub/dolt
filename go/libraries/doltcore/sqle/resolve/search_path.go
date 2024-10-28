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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
)

// UseSearchPath is a global variable that determines whether or not to use the search path when resolving table names.
// Currently used by Doltgres
var UseSearchPath = false

// SearchPath returns all the schemas in the search_path setting, with elements like "$user" expanded
func SearchPath(ctx *sql.Context) ([]string, error) {
	searchPathVar, err := ctx.GetSessionVariable(ctx, "search_path")
	if err != nil {
		return nil, err
	}

	pathElems := strings.Split(searchPathVar.(string), ",")
	path := make([]string, len(pathElems))
	for i, pathElem := range pathElems {
		path[i] = normalizeSearchPathSchema(ctx, pathElem)
	}

	return path, nil
}

func normalizeSearchPathSchema(ctx *sql.Context, schemaName string) string {
	schemaName = strings.Trim(schemaName, " ")
	if schemaName == "\"$user\"" {
		client := ctx.Session.Client()
		return client.User
	}
	return schemaName
}

// FirstExistingSchemaOnSearchPath returns the first schema in the search path that exists in the database.
func FirstExistingSchemaOnSearchPath(ctx *sql.Context, root doltdb.RootValue) (string, error) {
	schemas, err := SearchPath(ctx)
	if err != nil {
		return "", err
	}

	schemaName := ""
	for _, s := range schemas {
		var exists bool
		schemaName, exists, err = doltdb.ResolveDatabaseSchema(ctx, root, s)
		if err != nil {
			return "", err
		}

		if exists {
			break
		}
	}

	// No existing schema found in the search_path and none specified in the statement means we can't create the table
	if schemaName == "" {
		return "", sql.ErrDatabaseNoDatabaseSchemaSelectedCreate.New()
	}

	return schemaName, nil
}

// IsSystemTable returns whether a table is a system table or not
func IsSystemTable(ctx *sql.Context, tableName doltdb.TableName, root doltdb.RootValue) (bool, error) {
	if tableName.Schema == "dolt" {
		return true, nil
	}

	schemasToSearch, err := SearchPath(ctx)
	if err != nil {
		return false, nil
	}
	for _, schemaName := range schemasToSearch {
		if schemaName == "dolt" {
			return true, nil
		}

		tablesInSchema, err := root.GetTableNames(ctx, schemaName)
		if err != nil {
			return false, err
		}
		for _, table := range tablesInSchema {
			if strings.EqualFold(table, tableName.Name) {
				return false, nil
			}
		}
	}

	return false, nil
}
