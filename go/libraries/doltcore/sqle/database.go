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

package sqle

import (
	"context"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	sql.Database
	name string
	root *doltdb.RootValue
}

// NewDatabase returns a new dolt databae to use in queries.
func NewDatabase(name string, root *doltdb.RootValue) *Database {
	return &Database{
		name: name,
		root: root,
	}
}

// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

// Tables returns the tables in this database, currently exactly the same tables as in the current working root.
func (db *Database) Tables() map[string]sql.Table {
	ctx := context.Background()

	tables := make(map[string]sql.Table)
	tableNames := db.root.GetTableNames(ctx)
	for _, name := range tableNames {
		table, ok := db.root.GetTable(ctx, name)
		if !ok {
			panic("Error loading table " + name)
		}
		tables[name] = &DoltTable{name: name, table: table, sch: table.GetSchema(ctx)}
	}

	return tables
}
