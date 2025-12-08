// Copyright 2025 Dolthub, Inc.
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
	"context"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/adapters"
)

// GetGeneratedSystemTables returns table names of all generated system tables.
func GetGeneratedSystemTables(ctx context.Context, root doltdb.RootValue) ([]doltdb.TableName, error) {
	s := doltdb.NewTableNameSet(nil)

	// Depending on whether the search path is used, the generated system tables will either be in the dolt namespace
	// or the empty (default) namespace.
	for _, tableName := range doltdb.GeneratedSystemTableNames() {
		adapter, ok := adapters.DoltTableAdapterRegistry.Adapters[tableName]
		if ok {
			tableName = adapter.TableName()
		}

		tableUnique := doltdb.TableName{Name: tableName}
		if UseSearchPath {
			tableUnique.Schema = doltdb.DoltNamespace
		}

		s.Add(tableUnique)
	}

	schemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}

	// For dolt there are no stored schemas, search the default (empty string) schema
	if len(schemas) == 0 {
		schemas = append(schemas, schema.DatabaseSchema{Name: doltdb.DefaultSchemaName})
	}

	for _, schema := range schemas {
		tableNames, err := root.GetTableNames(ctx, schema.Name, false)
		if err != nil {
			return nil, err
		}

		for _, pre := range doltdb.GeneratedSystemTablePrefixes {
			for _, tableName := range tableNames {
				s.Add(doltdb.TableName{
					Name:   pre + tableName,
					Schema: schema.Name,
				})
			}
		}

		// For doltgres, we also support the legacy dolt_ table names, addressable in any user schema
		if UseSearchPath && schema.Name != "pg_catalog" && schema.Name != doltdb.DoltNamespace {
			for _, name := range doltdb.DoltGeneratedTableNames {
				s.Add(doltdb.TableName{
					Name:   name,
					Schema: schema.Name,
				})
			}
		}
	}

	return s.AsSlice(), nil
}
