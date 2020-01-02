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
	"strings"

	"github.com/src-d/go-mysql-server/sql"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/encoding"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var _ sql.Database = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)

type batchMode bool

const (
	batched batchMode = true
	single  batchMode = false
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	name      string
	root      *doltdb.RootValue
	ddb       *doltdb.DoltDB
	rs        *env.RepoState
	batchMode batchMode
	tables    map[string]*DoltTable
}

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rs *env.RepoState) *Database {
	return &Database{
		name:      name,
		root:      root,
		ddb:       ddb,
		rs:        rs,
		batchMode: single,
		tables:    make(map[string]*DoltTable),
	}
}

// NewBatchedDatabase returns a new dolt database executing in batch insert mode. Integrators must call Flush() to
// commit any outstanding edits.
func NewBatchedDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rs *env.RepoState) *Database {
	return &Database{
		name:      name,
		root:      root,
		ddb:       ddb,
		rs:        rs,
		batchMode: batched,
		tables:    make(map[string]*DoltTable),
	}
}

// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

func (db *Database) GetTableInsensitive(ctx context.Context, tblName string) (sql.Table, bool, error) {
	lwrName := strings.ToLower(tblName)
	if strings.HasPrefix(lwrName, DoltDiffTablePrefix) {
		tblName = tblName[len(DoltDiffTablePrefix):]
		dt, err := NewDiffTable(ctx, tblName, db.ddb, db.rs)

		if err != nil {
			return nil, false, err
		}

		return dt, true, nil
	}

	if strings.HasPrefix(lwrName, DoltHistoryTablePrefix) {
		tblName = tblName[len(DoltHistoryTablePrefix):]
		dh, err := NewHistoryTable(ctx, tblName, db.ddb)

		if err != nil {
			return nil, false, err
		}

		return dh, true, nil
	}

	if lwrName == LogTableName {
		return NewLogTable(db.ddb, db.rs), true, nil
	}

	tableNames, err := db.GetTableNames(ctx)

	if err != nil {
		return nil, false, err
	}

	exactName, ok := sql.GetTableNameInsensitive(tblName, tableNames)

	if !ok {
		return nil, false, nil
	}

	if table, ok := db.tables[exactName]; ok {
		return table, true, nil
	}

	tbl, ok, err := db.root.GetTable(ctx, exactName)

	if err != nil {
		return nil, false, err
	} else if !ok {
		panic("Name '" + exactName + "' had already been verified... This is a bug")
	}

	sch, err := tbl.GetSchema(ctx)

	if err != nil {
		return nil, false, err
	}

	table := &DoltTable{name: exactName, table: tbl, sch: sch, db: db}
	db.tables[exactName] = table
	return table, true, nil
}

func (db *Database) GetTableNames(ctx context.Context) ([]string, error) {
	tblNames, err := db.root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	for i, tbl := range tblNames {
		if tbl == doltdb.DocTableName {
			tblNames = append(tblNames[:i], tblNames[i+1:]...)
			break
		}
	}
	return tblNames, nil
}

// Root returns the root value for the database.
func (db *Database) Root() *doltdb.RootValue {
	return db.root
}

// Set a new root value for the database. Can be used if the dolt working
// set value changes outside of the basic SQL execution engine.
func (db *Database) SetRoot(newRoot *doltdb.RootValue) {
	// TODO: races
	db.root = newRoot
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

	delete(db.tables, tableName)

	db.SetRoot(newRoot)

	return nil
}

// CreateTable creates a table with the name and schema given.
func (db *Database) CreateTable(ctx *sql.Context, tableName string, schema sql.Schema) error {

	if !doltdb.IsValidTableName(tableName) || tableName == doltdb.DocTableName {
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

	db.SetRoot(newRoot)

	return nil
}

// RenameTable implements sql.TableRenamer
func (db *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	root, err := alterschema.RenameTable(ctx, db.Root(), oldName, newName)
	if err != nil {
		return err
	}

	delete(db.tables, oldName)
	db.SetRoot(root)

	return nil
}

// Flushes the current batch of outstanding changes and returns any errors.
func (db *Database) Flush(ctx context.Context) error {
	for name, table := range db.tables {
		if err := table.flushBatchedEdits(ctx); err != nil {
			return err
		}
		delete(db.tables, name)
	}
	return nil
}
