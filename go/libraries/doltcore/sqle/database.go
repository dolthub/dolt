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
	"fmt"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	sql.Database
	name string
	root *doltdb.RootValue
	dEnv *env.DoltEnv
}

// NewDatabase returns a new dolt databae to use in queries.
func NewDatabase(name string, root *doltdb.RootValue, dEnv *env.DoltEnv) *Database {
	return &Database{
		name: name,
		root: root,
		dEnv: dEnv,
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
	tableNames, err := db.root.GetTableNames(ctx)

	// TODO: fix panics
	if err != nil {
		panic(err)
	}

	for _, name := range tableNames {
		table, ok, err := db.root.GetTable(ctx, name)

		// TODO: fix panics
		if err != nil {
			panic(err)
		}

		if !ok {
			panic("Error loading table " + name)
		}

		sch, err := table.GetSchema(ctx)
		// TODO: fix panics
		if err != nil {
			panic(err)
		}

		tables[name] = &DoltTable{name: name, table: table, sch: sch, db: db}

		//if db.dEnv != nil {
		//	dfTbl := NewDiffTable(name, db.dEnv)
		//	tables[dfTbl.Name()] = dfTbl
		//}
	}

	tables[LogTableName] = NewLogTable(db.dEnv)

	return tables
}

// Root returns the root value for the database.
func (db *Database) Root() *doltdb.RootValue {
	return db.root
}

// DropTable drops the table with the name given
func (db *Database) DropTable(ctx *sql.Context, tableName string) error {
	tableExists, err := db.root.HasTable(ctx, tableName)
	if err != nil {
		return err
	}

	if !tableExists {
		return sql.ErrTableNotFound.New(tableName)
	}

	newRoot, err := db.root.RemoveTables(ctx, tableName)
	if err != nil {
		return err
	}

	// TODO: races
	db.root = newRoot

	return nil
}

// CreateTable creates a table with the name and schema given.
func (db *Database) CreateTable(ctx *sql.Context, tableName string, schema sql.Schema) error {

	if !doltdb.IsValidTableName(tableName) {
		return fmt.Errorf("Invalid table name: '%v'", tableName)
	}

	if exists, err := db.root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	doltSch, err := SqlSchemaToDoltSchema(schema)
	if err != nil {
		return err
	}

	schVal, err := encoding.MarshalAsNomsValue(ctx, db.root.VRW(), doltSch)
	if err != nil {
		return err
	}

	m, err := types.NewMap(ctx, db.root.VRW())
	if err != nil {
		return err
	}

	tbl, err := doltdb.NewTable(ctx, db.root.VRW(), schVal, m)
	if err != nil {
		return err
	}

	newRoot, err := db.root.PutTable(ctx, tableName, tbl)
	if err != nil {
		return err
	}

	// TODO: races
	db.root = newRoot

	return nil
}
