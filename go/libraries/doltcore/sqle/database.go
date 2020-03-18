// Copyright 2019-2020 Liquidata, Inc.
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
	"io"
	"strings"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/parse"
	"github.com/src-d/go-mysql-server/sql/plan"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
)

type batchMode bool

var ErrInvalidTableName = errors.NewKind("Invalid table name %s. Table names must match the regular expression " + doltdb.TableNameRegexStr)
var ErrReservedTableName = errors.NewKind("Invalid table name %s. Table names beginning with `dolt_` are reserved for internal use")
var ErrSystemTableAlter = errors.NewKind("Cannot alter table %s: system tables cannot be dropped or altered")

const (
	batched batchMode = true
	single  batchMode = false
)

// Database implements sql.Database for a dolt DB.
type Database struct {
	name      string
	root      *doltdb.RootValue
	ddb       *doltdb.DoltDB
	rsr       env.RepoStateReader
	batchMode batchMode
	tables    map[*doltdb.RootValue]map[string]sql.Table
}

var _ sql.Database = (*Database)(nil)
var _ sql.VersionedDatabase = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rsr env.RepoStateReader) *Database {
	return &Database{
		name:      name,
		root:      root,
		ddb:       ddb,
		rsr:       rsr,
		batchMode: single,
		tables:    initTableMap(root),
	}
}

func initTableMap(root *doltdb.RootValue) map[*doltdb.RootValue]map[string]sql.Table {
	tablesForRoot := make(map[string]sql.Table)
	tables := make(map[*doltdb.RootValue]map[string]sql.Table)
	tables[root] = tablesForRoot
	return tables
}

// NewBatchedDatabase returns a new dolt database executing in batch insert mode. Integrators must call Flush() to
// commit any outstanding edits.
func NewBatchedDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rsr env.RepoStateReader) *Database {
	return &Database{
		name:      name,
		root:      root,
		ddb:       ddb,
		rsr:       rsr,
		batchMode: batched,
		tables:    initTableMap(root),
	}
}

// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

// GetTableInsensitive is used when resolving tables in queries. It returns a best-effort case-insensitive match for
// the table name given.
func (db *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	lwrName := strings.ToLower(tblName)
	if strings.HasPrefix(lwrName, DoltDiffTablePrefix) {
		tblName = tblName[len(DoltDiffTablePrefix):]
		dt, err := NewDiffTable(ctx, tblName, db.ddb, db.rsr)

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
		return NewLogTable(db.ddb, db.rsr), true, nil
	}

	return db.getTable(ctx, db.root, tblName)
}

// GetTableInsensitiveAsOf implements sql.VersionedDatabase
func (db *Database) GetTableInsensitiveAsOf(ctx *sql.Context, tableName string, asOf interface{}) (sql.Table, bool, error) {
	root, err := db.rootAsOf(ctx, asOf)
	if err != nil {
		return nil, false, err
	}

	return db.getTable(ctx, root, tableName)
}

func (db *Database) rootAsOf(ctx *sql.Context, asOf interface{}) (*doltdb.RootValue, error) {

	switch x := asOf.(type) {
	case string:
		return db.getRootForCommitRef(ctx, x)
	case time.Time:
		return db.getRootForTime(ctx, x)
	default:
		panic("unsupported AS OF type")
	}
}

func (db *Database) getRootForTime(ctx *sql.Context, asOf time.Time) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec("HEAD", db.rsr.CWBHeadRef().String())
	if err != nil {
		return nil, err
	}

	cm, err := db.ddb.Resolve(ctx, cs)
	if err != nil {
		return nil, err
	}

	cmItr := doltdb.CommitItrForRoots(db.ddb, cm)
	var curr *doltdb.Commit
	var prev *doltdb.Commit
	for {
		if curr != nil {
			meta, err := curr.GetCommitMeta()
			if err != nil {
				return nil, err
			}
			if meta.Time().Before(asOf) {
				return prev.GetRootValue()
			}
		}

		_, curr, err = cmItr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		prev = curr
	}

	return curr.GetRootValue()
}

func (db *Database) getRootForCommitRef(ctx *sql.Context, commitRef string) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(commitRef, db.rsr.CWBHeadRef().String())
	if err != nil {
		return nil, err
	}

	cm, err := db.ddb.Resolve(ctx, cs)
	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue()
	if err != nil {
		return nil, err
	}

	return root, nil
}

// GetTableNamesAsOf implements sql.VersionedDatabase
func (db *Database) GetTableNamesAsOf(ctx *sql.Context, time interface{}) ([]string, error) {
	root, err := db.rootAsOf(ctx, time)
	if err != nil {
		return nil, err
	}

	tblNames, err := root.GetTableNames(ctx)
	if err != nil {
		return nil, err
	}
	return filterDoltInternalTables(tblNames), nil
}

// getTable gets the table with the exact name given at the root value given. The database caches tables for all root
// values to avoid doing schema lookups on every table lookup, which are expensive.
func (db *Database) getTable(ctx context.Context, root *doltdb.RootValue, tableName string) (sql.Table, bool, error) {
	if tablesForRoot, ok := db.tables[root]; ok {
		if table, ok := tablesForRoot[tableName]; ok {
			return table, true, nil
		}
	}

	tableNames, err := getAllTableNames(ctx, root)
	if err != nil {
		return nil, true, err
	}

	tableName, ok := sql.GetTableNameInsensitive(tableName, tableNames)
	if !ok {
		return nil, false, nil
	}

	tbl, ok, err := root.GetTable(ctx, tableName)
	if err != nil {
		return nil, false, err
	} else if !ok {
		// Should be impossible
		return nil, false, doltdb.ErrTableNotFound
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}

	var table sql.Table

	readonlyTable := DoltTable{name: tableName, table: tbl, sch: sch, db: db}
	if doltdb.IsSystemTable(tableName) {
		table = &readonlyTable
	} else if doltdb.HasDoltPrefix(tableName) {
		table = &WritableDoltTable{DoltTable: readonlyTable}
	} else {
		table = &AlterableDoltTable{WritableDoltTable{DoltTable: readonlyTable}}
	}

	if db.tables[root] == nil {
		db.tables[root] = make(map[string]sql.Table)
	}

	db.tables[root][tableName] = table

	return table, true, nil
}

// GetTableNames returns the names of all user tables. System tables in user space (e.g. dolt_docs, dolt_query_catalog)
// are filtered out. This method is used for queries that examine the schema of the database, e.g. show tables. Table
// name resolution in queries is handled by GetTableInsensitive. Use GetAllTableNames for an unfiltered list of all
// tables in user space.
func (db *Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames, err := db.GetAllTableNames(ctx)
	if err != nil {
		return nil, err
	}
	return filterDoltInternalTables(tblNames), nil
}

// GetAllTableNames returns all user-space tables, including system tables in user space
// (e.g. dolt_docs, dolt_query_catalog).
func (db *Database) GetAllTableNames(ctx *sql.Context) ([]string, error) {
	return getAllTableNames(ctx, db.root)
}

func getAllTableNames(ctx context.Context, root *doltdb.RootValue) ([]string, error) {
	return root.GetTableNames(ctx)
}

func filterDoltInternalTables(tblNames []string) []string {
	result := []string{}
	for _, tbl := range tblNames {
		if !doltdb.HasDoltPrefix(tbl) {
			result = append(result, tbl)
		}
	}
	return result
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
	if doltdb.IsSystemTable(tableName) {
		return ErrSystemTableAlter.New(tableName)
	}

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

	delete(db.tables[db.root], tableName)

	db.SetRoot(newRoot)

	return nil
}

// CreateTable creates a table with the name and schema given.
func (db *Database) CreateTable(ctx *sql.Context, tableName string, schema sql.Schema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createTable(ctx, tableName, schema)
}

// Unlike the exported version, createTable doesn't enforce any table name checks.
func (db *Database) createTable(ctx *sql.Context, tableName string, schema sql.Schema) error {
	if exists, err := db.root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	doltSch, err := SqlSchemaToDoltSchema(schema)
	if err != nil {
		return err
	}

	newRoot, err := db.root.CreateEmptyTable(ctx, tableName, doltSch)
	if err != nil {
		return err
	}

	db.SetRoot(newRoot)

	return nil
}

// RenameTable implements sql.TableRenamer
func (db *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	if doltdb.IsSystemTable(oldName) {
		return ErrSystemTableAlter.New(oldName)
	}

	if doltdb.HasDoltPrefix(newName) {
		return ErrReservedTableName.New(newName)
	}

	if !doltdb.IsValidTableName(newName) {
		return ErrInvalidTableName.New(newName)
	}

	root, err := alterschema.RenameTable(ctx, db.Root(), oldName, newName)
	if err != nil {
		return err
	}

	delete(db.tables[db.root], oldName)
	db.SetRoot(root)

	return nil
}

// Flush flushes the current batch of outstanding changes and returns any errors.
func (db *Database) Flush(ctx context.Context) error {
	for name, table := range db.tables[db.root] {
		if writable, ok := table.(*WritableDoltTable); ok {
			if err := writable.flushBatchedEdits(ctx); err != nil {
				return err
			}
		} else if alterable, ok := table.(*AlterableDoltTable); ok {
			if err := alterable.flushBatchedEdits(ctx); err != nil {
				return err
			}
		}
		delete(db.tables[db.root], name)
	}
	return nil
}

// CreateView implements sql.ViewCreator. Persists the view in the dolt database, so
// it can exist in a sql session later. Returns sql.ErrExistingView if a view
// with that name already exists.
func (db *Database) CreateView(ctx *sql.Context, name string, definition string) error {
	tbl, err := GetOrCreateDoltSchemasTable(ctx, db)
	if err != nil {
		return err
	}

	exists, err := viewExistsInSchemasTable(ctx, tbl, name)
	if err != nil {
		return err
	}
	if exists {
		return sql.ErrExistingView.New(name)
	}

	// It does not exist; insert it.
	row := sql.Row{"view", name, definition}
	inserter := tbl.Inserter(ctx)
	err = inserter.Insert(ctx, row)
	if err != nil {
		return err
	}
	return inserter.Close(ctx)
}

// DropView implements sql.ViewDropper. Removes a view from persistence in the
// dolt database. Returns sql.ErrNonExistingView if the view did not
// exist.
func (db *Database) DropView(ctx *sql.Context, name string) error {
	stbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return err
	}
	if !found {
		return sql.ErrNonExistingView.New(name)
	}

	tbl := stbl.(*WritableDoltTable)
	exists, err := viewExistsInSchemasTable(ctx, tbl, name)
	if err != nil {
		return err
	}
	if !exists {
		return sql.ErrNonExistingView.New(name)
	}

	// It exists. delete it from the table.
	row := sql.Row{"view", name}
	deleter := tbl.Deleter(ctx)
	err = deleter.Delete(ctx, row)
	if err != nil {
		return err
	}

	return deleter.Close(ctx)
}

// RegisterSchemaFragments register SQL schema fragments that are persisted in the given
// `Database` with the provided `sql.Catalog`. Returns an error if
// there are I/O issues, but currently silently fails to register some
// schema fragments if they don't parse, or if registries within the
// `catalog` return errors.
func RegisterSchemaFragments(ctx *sql.Context, catalog *sql.Catalog, db *Database) error {
	stbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	tbl := stbl.(*WritableDoltTable)
	if err != nil {
		return err
	}
	iter, err := newRowIterator(&tbl.DoltTable, ctx)
	if err != nil {
		return err
	}
	defer iter.Close()

	var parseErrors []error

	vr := catalog.ViewRegistry
	r, err := iter.Next()
	for err == nil {
		if err != nil {
			break
		}
		if r[0] == "view" {
			name := r[1].(string)
			definition := r[2].(string)
			cv, err := parse.Parse(sql.NewContext(ctx), fmt.Sprintf("create view %s as %s", dsql.QuoteIdentifier(name), definition))
			if err != nil {
				parseErrors = append(parseErrors, err)
			} else {
				vr.Register(db.Name(), sql.NewView(name, cv.(*plan.CreateView).Definition))
			}
		}
		r, err = iter.Next()
	}
	if err != io.EOF {
		return err
	}

	if len(parseErrors) > 0 {
		// TODO: Warning for uncreated views...
	}

	return nil
}
