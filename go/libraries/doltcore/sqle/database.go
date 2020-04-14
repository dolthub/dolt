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
	"sync"
	"time"

	"github.com/src-d/go-mysql-server/sql"
	"github.com/src-d/go-mysql-server/sql/parse"
	"github.com/src-d/go-mysql-server/sql/plan"
	"gopkg.in/src-d/go-errors.v1"
	"vitess.io/vitess/go/vt/proto/query"

	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema/alterschema"
	dsql "github.com/liquidata-inc/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

type commitBehavior int8

var ErrInvalidTableName = errors.NewKind("Invalid table name %s. Table names must match the regular expression " + doltdb.TableNameRegexStr)
var ErrReservedTableName = errors.NewKind("Invalid table name %s. Table names beginning with `dolt_` are reserved for internal use")
var ErrSystemTableAlter = errors.NewKind("Cannot alter table %s: system tables cannot be dropped or altered")

const (
	batched commitBehavior = iota
	single
	autoCommit
)

type tableCache struct {
	mu     *sync.Mutex
	tables map[*doltdb.RootValue]map[string]sql.Table
}

func (tc *tableCache) Get(tableName string, root *doltdb.RootValue) (sql.Table, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if !ok {
		return nil, false
	}

	tbl, ok := tablesForRoot[tableName]

	return tbl, ok
}

func (tc *tableCache) Put(tableName string, root *doltdb.RootValue, tbl sql.Table) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if !ok {
		tablesForRoot = make(map[string]sql.Table)
		tc.tables[root] = tablesForRoot
	}

	tablesForRoot[tableName] = tbl
}

func (tc *tableCache) AllForRoot(root *doltdb.RootValue) (map[string]sql.Table, bool) {
	tc.mu.Lock()
	defer tc.mu.Unlock()

	tablesForRoot, ok := tc.tables[root]

	if ok {
		copyOf := make(map[string]sql.Table, len(tablesForRoot))
		for name, tbl := range tablesForRoot {
			copyOf[name] = tbl
		}

		return copyOf, true
	}

	return nil, false
}

// Database implements sql.Database for a dolt DB.
type Database struct {
	name      string
	defRoot   *doltdb.RootValue
	ddb       *doltdb.DoltDB
	rsr       env.RepoStateReader
	rsw       env.RepoStateWriter
	batchMode commitBehavior
	tc        *tableCache
}

var _ sql.Database = (*Database)(nil)
var _ sql.VersionedDatabase = (*Database)(nil)
var _ sql.TableDropper = (*Database)(nil)
var _ sql.TableCreator = (*Database)(nil)
var _ sql.TableRenamer = (*Database)(nil)

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(name string, defRoot *doltdb.RootValue, ddb *doltdb.DoltDB, rsr env.RepoStateReader) *Database {
	return &Database{
		name:      name,
		defRoot:   defRoot,
		ddb:       ddb,
		rsr:       rsr,
		batchMode: single,
		tc:        &tableCache{&sync.Mutex{}, make(map[*doltdb.RootValue]map[string]sql.Table)},
	}
}

// NewBatchedDatabase returns a new dolt database executing in batch insert mode. Integrators must call Flush() to
// commit any outstanding edits.
func NewBatchedDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rsr env.RepoStateReader) *Database {
	return &Database{
		name:      name,
		defRoot:   root,
		ddb:       ddb,
		rsr:       rsr,
		batchMode: batched,
		tc:        &tableCache{&sync.Mutex{}, make(map[*doltdb.RootValue]map[string]sql.Table)},
	}
}

// NewAutoCommitDatabase returns a new dolt database executing in autocommit mode. Every write operation will update
// the working set with the new root value.
func NewAutoCommitDatabase(name string, root *doltdb.RootValue, ddb *doltdb.DoltDB, rsr env.RepoStateReader, rsw env.RepoStateWriter) *Database {
	return &Database{
		name:      name,
		defRoot:   root,
		ddb:       ddb,
		rsr:       rsr,
		rsw:       rsw,
		batchMode: autoCommit,
		tc:        &tableCache{&sync.Mutex{}, make(map[*doltdb.RootValue]map[string]sql.Table)},
	}
}


// Name returns the name of this database, set at creation time.
func (db *Database) Name() string {
	return db.name
}

// GetDefaultRoot returns the default root of the database that is used by new sessions.
func (db *Database) GetDefaultRoot() *doltdb.RootValue {
	return db.defRoot
}

// GetDoltDB gets the underlying DoltDB of the Database
func (db *Database) GetDoltDB() *doltdb.DoltDB {
	return db.ddb
}

// GetTableInsensitive is used when resolving tables in queries. It returns a best-effort case-insensitive match for
// the table name given.
func (db *Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return nil, false, err
	}

	return db.GetTableInsensitiveWithRoot(ctx, root, tblName)
}

func (db *Database) GetTableInsensitiveWithRoot(ctx context.Context, root *doltdb.RootValue, tblName string) (sql.Table, bool, error) {
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
		dh, err := NewHistoryTable(ctx, tblName, db.ddb, db.rsr)

		if err != nil {
			return nil, false, err
		}

		return dh, true, nil
	}

	if lwrName == LogTableName {
		return NewLogTable(db.ddb, db.rsr), true, nil
	}

	return db.getTable(ctx, root, tblName)
}

// GetTableInsensitiveAsOf implements sql.VersionedDatabase
func (db *Database) GetTableInsensitiveAsOf(ctx *sql.Context, tableName string, asOf interface{}) (sql.Table, bool, error) {
	root, err := db.rootAsOf(ctx, asOf)
	if err != nil {
		return nil, false, err
	} else if root == nil {
		return nil, false, nil
	}

	return db.getTable(ctx, root, tableName)
}

// rootAsOf returns the root of the DB as of the expression given, which may be nil in the case that it refers to an
// expression before the first commit.
func (db *Database) rootAsOf(ctx *sql.Context, asOf interface{}) (*doltdb.RootValue, error) {
	switch x := asOf.(type) {
	case string:
		return db.getRootForCommitRef(ctx, x)
	case time.Time:
		return db.getRootForTime(ctx, x)
	default:
		panic(fmt.Sprintf("unsupported AS OF type %T", asOf))
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

	hash, err := cm.HashOf()
	if err != nil {
		return nil, err
	}

	cmItr, err := commitwalk.GetTopologicalOrderIterator(ctx, db.ddb, hash)
	if err != nil {
		return nil, err
	}

	for {
		_, curr, err := cmItr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		meta, err := curr.GetCommitMeta()
		if err != nil {
			return nil, err
		}

		if meta.Time().Equal(asOf) || meta.Time().Before(asOf) {
			return curr.GetRootValue()
		}
	}

	return nil, nil
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
	} else if root == nil {
		return nil, nil
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
	if table, ok := db.tc.Get(tableName, root); ok {
		return table, true, nil
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

	db.tc.Put(tableName, root, table)

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
	root, err := db.GetRoot(ctx)

	if err != nil {
		return nil, err
	}

	return getAllTableNames(ctx, root)
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

func (db *Database) headKeyForDB() string {
	return fmt.Sprintf("%s_head", db.name)
}

var hashType = sql.MustCreateString(query.Type_TEXT, 32, sql.Collation_ascii_bin)

func (db *Database) GetRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	dsess := DSessFromSess(ctx.Session)
	currRoot, dbRootOk := dsess.dbRoots[db.name]

	key := db.headKeyForDB()
	typ, val := ctx.Session.Get(key)

	if val == nil {
		if !dbRootOk {
			return nil, fmt.Errorf("value for '%s' not found", key)
		} else {
			err := db.SetRoot(ctx, currRoot.root)

			if err != nil {
				return nil, err
			}

			return currRoot.root, nil
		}
	} else {
		if typ.Type() != query.Type_TEXT {
			return nil, fmt.Errorf("invalid value for '%s'", key)
		}

		hashStr := val.(string)
		h, ok := hash.MaybeParse(hashStr)

		if !ok {
			return nil, fmt.Errorf("invalid hash '%s' stored in '%s'", hashStr, key)
		}

		if dbRootOk {
			if hashStr == currRoot.hashStr {
				return currRoot.root, nil
			}
		}

		newRoot, err := db.ddb.ReadRootValue(ctx, h)

		if err != nil {
			return nil, err
		}

		dsess.dbRoots[db.name] = dbRoot{hashStr, newRoot, db.ddb, db.rsw}
		return newRoot, nil
	}
}

// Set a new root value for the database. Can be used if the dolt working
// set value changes outside of the basic SQL execution engine.
func (db *Database) SetRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	h, err := newRoot.HashOf()

	if err != nil {
		return err
	}

	hashStr := h.String()
	key := db.headKeyForDB()
	ctx.Session.Set(key, hashType, hashStr)

	dsess := DSessFromSess(ctx.Session)
	dsess.dbRoots[db.name] = dbRoot{hashStr, newRoot, db.ddb, db.rsw}

	// if db.batchMode == autoCommit {
	// 	h, err := db.ddb.WriteRootValue(ctx, newRoot)
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	db.defRoot = newRoot
	// 	return db.rsw.SetWorkingHash(ctx, h)
	// }

	return nil
}

// DropTable drops the table with the name given
func (db *Database) DropTable(ctx *sql.Context, tableName string) error {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	if doltdb.IsSystemTable(tableName) {
		return ErrSystemTableAlter.New(tableName)
	}

	tableExists, err := root.HasTable(ctx, tableName)
	if err != nil {
		return err
	}

	if !tableExists {
		return sql.ErrTableNotFound.New(tableName)
	}

	newRoot, err := root.RemoveTables(ctx, tableName)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// CreateTable creates a table with the name and schema given.
func (db *Database) CreateTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	for _, col := range sch {
		commentTag := extractTag(col)
		if commentTag == schema.InvalidTag {
			// we'll replace this invalid tag
			continue
		}
		if commentTag >= schema.ReservedTagMin {
			return fmt.Errorf("tag %d is within the reserved tag space", commentTag)
		}
	}

	return db.createTable(ctx, tableName, sch)
}

// Unlike the exported version, createTable doesn't enforce any table name checks.
func (db *Database) createTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	doltSch, err := SqlSchemaToDoltSchema(ctx, db.defRoot, tableName, sch)
	if err != nil {
		return err
	}

	tt, err := root.TablesNamesForTags(ctx, doltSch.GetAllCols().Tags...)
	if err != nil {
		return err
	}
	if len(tt) > 0 {
		var ee []string
		_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
			if collisionTable, tagExists := tt[tag]; tagExists {
				ee = append(ee, schema.ErrTagPrevUsed(tag, col.Name, collisionTable).Error())
			}
			return false, nil
		})
		return fmt.Errorf(strings.Join(ee, "\n"))
	}

	newRoot, err := root.CreateEmptyTable(ctx, tableName, doltSch)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// RenameTable implements sql.TableRenamer
func (db *Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	if doltdb.IsSystemTable(oldName) {
		return ErrSystemTableAlter.New(oldName)
	}

	if doltdb.HasDoltPrefix(newName) {
		return ErrReservedTableName.New(newName)
	}

	if !doltdb.IsValidTableName(newName) {
		return ErrInvalidTableName.New(newName)
	}

	newRoot, err := alterschema.RenameTable(ctx, root, oldName, newName)

	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// Flush flushes the current batch of outstanding changes and returns any errors.
func (db *Database) Flush(ctx *sql.Context) error {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	tables, ok := db.tc.AllForRoot(root)

	if ok {
		for _, table := range tables {
			if writable, ok := table.(*WritableDoltTable); ok {
				if err := writable.flushBatchedEdits(ctx); err != nil {
					return err
				}
			} else if alterable, ok := table.(*AlterableDoltTable); ok {
				if err := alterable.flushBatchedEdits(ctx); err != nil {
					return err
				}
			}
		}
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
// `Database` with the provided `sql.ViewRegistry`. Returns an error if
// there are I/O issues, but currently silently fails to register some
// schema fragments if they don't parse, or if registries within the
// `catalog` return errors.
func RegisterSchemaFragments(ctx *sql.Context, db *Database, root *doltdb.RootValue) error {
	stbl, found, err := db.GetTableInsensitiveWithRoot(ctx, root, doltdb.SchemasTableName)
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

	r, err := iter.Next()
	for err == nil {
		if err != nil {
			break
		}
		if r[0] == "view" {
			name := r[1].(string)
			definition := r[2].(string)
			cv, err := parse.Parse(ctx, fmt.Sprintf("create view %s as %s", dsql.QuoteIdentifier(name), definition))
			if err != nil {
				parseErrors = append(parseErrors, err)
			} else {
				ctx.Register(db.Name(), cv.(*plan.CreateView).Definition.AsView())
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
