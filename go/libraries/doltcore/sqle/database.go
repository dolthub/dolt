// Copyright 2019-2020 Dolthub, Inc.
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
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
)

var ErrInvalidTableName = errors.NewKind("Invalid table name %s. Table names must match the regular expression " + doltdb.TableNameRegexStr)
var ErrReservedTableName = errors.NewKind("Invalid table name %s. Table names beginning with `dolt_` are reserved for internal use")
var ErrSystemTableAlter = errors.NewKind("Cannot alter table %s: system tables cannot be dropped or altered")

type SqlDatabase interface {
	sql.Database
	GetRoot(*sql.Context) (*doltdb.RootValue, error)
	DbData() env.DbData
	Name() string

	StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error)
	Flush(*sql.Context) error
	EditOptions() editor.Options
}

func DbsAsDSQLDBs(dbs []sql.Database) []SqlDatabase {
	dsqlDBs := make([]SqlDatabase, 0, len(dbs))
	for _, db := range dbs {
		var sqlDb SqlDatabase
		if sqlDatabase, ok := db.(SqlDatabase); ok {
			sqlDb = sqlDatabase
		} else if privDatabase, ok := db.(mysql_db.PrivilegedDatabase); ok {
			if sqlDatabase, ok := privDatabase.Unwrap().(SqlDatabase); ok {
				sqlDb = sqlDatabase
			}
		}
		if sqlDb == nil {
			continue
		}
		switch v := sqlDb.(type) {
		case ReadReplicaDatabase, Database:
			dsqlDBs = append(dsqlDBs, v)
		case ReadOnlyDatabase, *UserSpaceDatabase:
		default:
			// esoteric analyzer errors occur if we silently drop databases, usually caused by pointer receivers
			panic("cannot cast to SqlDatabase")
		}
	}
	return dsqlDBs
}

// Database implements sql.Database for a dolt DB.
type Database struct {
	name     string
	ddb      *doltdb.DoltDB
	rsr      env.RepoStateReader
	rsw      env.RepoStateWriter
	drw      env.DocsReadWriter
	gs       globalstate.GlobalState
	editOpts editor.Options
}

func (db Database) EditOptions() editor.Options {
	return db.editOpts
}

var _ sql.Database = Database{}
var _ sql.TableCreator = Database{}
var _ sql.ViewDatabase = Database{}
var _ sql.TemporaryTableCreator = Database{}
var _ sql.TemporaryTableDatabase = Database{}

type ReadOnlyDatabase struct {
	Database
}

var _ sql.ReadOnlyDatabase = ReadOnlyDatabase{}

func (r ReadOnlyDatabase) IsReadOnly() bool {
	return true
}

func (db Database) StartTransaction(ctx *sql.Context, tCharacteristic sql.TransactionCharacteristic) (sql.Transaction, error) {
	dsession := dsess.DSessFromSess(ctx.Session)

	if !dsession.HasDB(ctx, db.Name()) {
		init, err := GetInitialDBState(ctx, db)
		if err != nil {
			return nil, err
		}

		err = dsession.AddDB(ctx, init)
		if err != nil {
			return nil, err
		}
	}

	return dsession.StartTransaction(ctx, db.Name(), tCharacteristic)
}

func (db Database) CommitTransaction(ctx *sql.Context, tx sql.Transaction) error {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.CommitTransaction(ctx, db.name, tx)
}

func (db Database) Rollback(ctx *sql.Context, tx sql.Transaction) error {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.RollbackTransaction(ctx, db.name, tx)
}

func (db Database) CreateSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.CreateSavepoint(ctx, name, db.name, tx)
}

func (db Database) RollbackToSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.RollbackToSavepoint(ctx, name, db.name, tx)
}

func (db Database) ReleaseSavepoint(ctx *sql.Context, tx sql.Transaction, name string) error {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.ReleaseSavepoint(ctx, name, db.name, tx)
}

var _ SqlDatabase = Database{}
var _ sql.VersionedDatabase = Database{}
var _ sql.TableDropper = Database{}
var _ sql.TableCreator = Database{}
var _ sql.TemporaryTableCreator = Database{}
var _ sql.TableRenamer = Database{}
var _ sql.TriggerDatabase = Database{}
var _ sql.StoredProcedureDatabase = Database{}
var _ sql.ExternalStoredProcedureDatabase = Database{}
var _ sql.TransactionDatabase = Database{}
var _ globalstate.StateProvider = Database{}

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(name string, dbData env.DbData, editOpts editor.Options) Database {
	return Database{
		name:     name,
		ddb:      dbData.Ddb,
		rsr:      dbData.Rsr,
		rsw:      dbData.Rsw,
		drw:      dbData.Drw,
		gs:       globalstate.NewGlobalStateStore(),
		editOpts: editOpts,
	}
}

// GetInitialDBState returns the InitialDbState for |db|.
func GetInitialDBState(ctx context.Context, db SqlDatabase) (dsess.InitialDbState, error) {
	rsr := db.DbData().Rsr
	ddb := db.DbData().Ddb

	var retainedErr error

	headCommit, err := ddb.Resolve(ctx, rsr.CWBHeadSpec(), rsr.CWBHeadRef())
	if err == doltdb.ErrBranchNotFound {
		retainedErr = err
		err = nil
	}
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	var ws *doltdb.WorkingSet
	if retainedErr == nil {
		ws, err = env.WorkingSet(ctx, ddb, rsr)
		if err != nil {
			return dsess.InitialDbState{}, err
		}
	}

	remotes, err := rsr.GetRemotes()
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	backups, err := rsr.GetBackups()
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	branches, err := rsr.GetBranches()
	if err != nil {
		return dsess.InitialDbState{}, err
	}

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     db.DbData(),
		Remotes:    remotes,
		Branches:   branches,
		Backups:    backups,
		Err:        retainedErr,
	}, nil
}

// Name returns the name of this database, set at creation time.
func (db Database) Name() string {
	return db.name
}

// GetDoltDB gets the underlying DoltDB of the Database
func (db Database) GetDoltDB() *doltdb.DoltDB {
	return db.ddb
}

// GetStateReader gets the RepoStateReader for a Database
func (db Database) GetStateReader() env.RepoStateReader {
	return db.rsr
}

// GetStateWriter gets the RepoStateWriter for a Database
func (db Database) GetStateWriter() env.RepoStateWriter {
	return db.rsw
}

func (db Database) GetDocsReadWriter() env.DocsReadWriter {
	return db.drw
}

func (db Database) DbData() env.DbData {
	return env.DbData{
		Ddb: db.ddb,
		Rsw: db.rsw,
		Rsr: db.rsr,
		Drw: db.drw,
	}
}

func (db Database) GetGlobalState() globalstate.GlobalState {
	return db.gs
}

// GetTableInsensitive is used when resolving tables in queries. It returns a best-effort case-insensitive match for
// the table name given.
func (db Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	// We start by first checking whether the input table is a temporary table. Temporary tables with name `x` take
	// priority over persisted tables of name `x`.
	ds := dsess.DSessFromSess(ctx.Session)
	if tbl, ok := ds.GetTemporaryTable(ctx, db.Name(), tblName); ok {
		return tbl, ok, nil
	}

	root, err := db.GetRoot(ctx)

	if err != nil {
		return nil, false, err
	}

	return db.GetTableInsensitiveWithRoot(ctx, root, tblName)
}

func (db Database) GetTableInsensitiveWithRoot(ctx *sql.Context, root *doltdb.RootValue, tblName string) (sql.Table, bool, error) {
	lwrName := strings.ToLower(tblName)

	sess := dsess.DSessFromSess(ctx.Session)

	// NOTE: system tables are not suitable for caching
	// TODO: these tables that cache a root value at construction time should not, they need to get it from the session
	//  at runtime
	switch {
	case strings.HasPrefix(lwrName, doltdb.DoltDiffTablePrefix):
		suffix := tblName[len(doltdb.DoltDiffTablePrefix):]
		head, err := sess.GetHeadCommit(ctx, db.name)
		if err != nil {
			return nil, false, err
		}
		dt, err := dtables.NewDiffTable(ctx, suffix, db.ddb, root, head)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	case strings.HasPrefix(lwrName, doltdb.DoltCommitDiffTablePrefix):
		suffix := tblName[len(doltdb.DoltCommitDiffTablePrefix):]
		dt, err := dtables.NewCommitDiffTable(ctx, suffix, db.ddb, root)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	case strings.HasPrefix(lwrName, doltdb.DoltHistoryTablePrefix):
		suffix := tblName[len(doltdb.DoltHistoryTablePrefix):]
		head, err := sess.GetHeadCommit(ctx, db.name)
		if err != nil {
			return nil, false, err
		}
		dt, err := dtables.NewHistoryTable(ctx, suffix, db.ddb, root, head)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	case strings.HasPrefix(lwrName, doltdb.DoltConfTablePrefix):
		suffix := tblName[len(doltdb.DoltConfTablePrefix):]
		dt, err := dtables.NewConflictsTable(ctx, suffix, root, dtables.RootSetter(db))
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	case strings.HasPrefix(lwrName, doltdb.DoltConstViolTablePrefix):
		suffix := tblName[len(doltdb.DoltConstViolTablePrefix):]
		dt, err := dtables.NewConstraintViolationsTable(ctx, suffix, root, dtables.RootSetter(db))
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	}

	// NOTE: system tables are not suitable for caching
	var dt sql.Table
	found := false
	switch lwrName {
	case doltdb.LogTableName:
		head, err := sess.GetHeadCommit(ctx, db.name)
		if err != nil {
			return nil, false, err
		}
		dt, found = dtables.NewLogTable(ctx, db.ddb, head), true
	case doltdb.DiffTableName:
		head, err := sess.GetHeadCommit(ctx, db.name)
		if err != nil {
			return nil, false, err
		}
		dt, found = dtables.NewUnscopedDiffTable(ctx, db.ddb, head), true
	case doltdb.TableOfTablesInConflictName:
		dt, found = dtables.NewTableOfTablesInConflict(ctx, db.name, db.ddb), true
	case doltdb.TableOfTablesWithViolationsName:
		dt, found = dtables.NewTableOfTablesConstraintViolations(ctx, root), true
	case doltdb.BranchesTableName:
		dt, found = dtables.NewBranchesTable(ctx, db.ddb), true
	case doltdb.RemotesTableName:
		dt, found = dtables.NewRemotesTable(ctx, db.ddb), true
	case doltdb.CommitsTableName:
		dt, found = dtables.NewCommitsTable(ctx, db.ddb), true
	case doltdb.CommitAncestorsTableName:
		dt, found = dtables.NewCommitAncestorsTable(ctx, db.ddb), true
	case doltdb.StatusTableName:
		dt, found = dtables.NewStatusTable(ctx, db.name, db.ddb, dsess.NewSessionStateAdapter(sess.Session, db.name, map[string]env.Remote{}, map[string]env.BranchConfig{}, map[string]env.Remote{}), db.drw), true
	}
	if found {
		return dt, found, nil
	}

	return db.getTable(ctx, root, tblName)
}

// GetTableInsensitiveAsOf implements sql.VersionedDatabase
func (db Database) GetTableInsensitiveAsOf(ctx *sql.Context, tableName string, asOf interface{}) (sql.Table, bool, error) {
	root, err := db.rootAsOf(ctx, asOf)

	if err != nil {
		return nil, false, err
	} else if root == nil {
		return nil, false, nil
	}

	table, ok, err := db.getTable(ctx, root, tableName)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	switch table := table.(type) {
	case *DoltTable:
		return table.LockedToRoot(root), true, nil
	case *AlterableDoltTable:
		return table.LockedToRoot(root), true, nil
	case *WritableDoltTable:
		return table.LockedToRoot(root), true, nil
	default:
		panic(fmt.Sprintf("unexpected table type %T", table))
	}
}

// rootAsOf returns the root of the DB as of the expression given, which may be nil in the case that it refers to an
// expression before the first commit.
func (db Database) rootAsOf(ctx *sql.Context, asOf interface{}) (*doltdb.RootValue, error) {
	switch x := asOf.(type) {
	case string:
		return db.getRootForCommitRef(ctx, x)
	case time.Time:
		return db.getRootForTime(ctx, x)
	default:
		panic(fmt.Sprintf("unsupported AS OF type %T", asOf))
	}
}

func (db Database) getRootForTime(ctx *sql.Context, asOf time.Time) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return nil, err
	}

	cm, err := db.ddb.Resolve(ctx, cs, db.rsr.CWBHeadRef())
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

		meta, err := curr.GetCommitMeta(ctx)
		if err != nil {
			return nil, err
		}

		if meta.Time().Equal(asOf) || meta.Time().Before(asOf) {
			return curr.GetRootValue(ctx)
		}
	}

	return nil, nil
}

func (db Database) getRootForCommitRef(ctx *sql.Context, commitRef string) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(commitRef)
	if err != nil {
		return nil, err
	}

	cm, err := db.ddb.Resolve(ctx, cs, db.rsr.CWBHeadRef())
	if err != nil {
		return nil, err
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	return root, nil
}

// GetTableNamesAsOf implements sql.VersionedDatabase
func (db Database) GetTableNamesAsOf(ctx *sql.Context, time interface{}) ([]string, error) {
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
func (db Database) getTable(ctx *sql.Context, root *doltdb.RootValue, tableName string) (sql.Table, bool, error) {
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

	readonlyTable, err := NewDoltTable(tableName, sch, tbl, db, db.editOpts)
	if err != nil {
		return nil, false, err
	}
	if doltdb.IsReadOnlySystemTable(tableName) {
		table = readonlyTable
	} else if doltdb.HasDoltPrefix(tableName) {
		table = &WritableDoltTable{DoltTable: readonlyTable, db: db}
	} else {
		table = &AlterableDoltTable{WritableDoltTable{DoltTable: readonlyTable, db: db}}
	}

	return table, true, nil
}

// GetTableNames returns the names of all user tables. System tables in user space (e.g. dolt_docs, dolt_query_catalog)
// are filtered out. This method is used for queries that examine the schema of the database, e.g. show tables. Table
// name resolution in queries is handled by GetTableInsensitive. Use GetAllTableNames for an unfiltered list of all
// tables in user space.
func (db Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	tblNames, err := db.GetAllTableNames(ctx)
	if err != nil {
		return nil, err
	}
	return filterDoltInternalTables(tblNames), nil
}

// GetAllTableNames returns all user-space tables, including system tables in user space
// (e.g. dolt_docs, dolt_query_catalog).
func (db Database) GetAllTableNames(ctx *sql.Context) ([]string, error) {
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

var hashType = sql.MustCreateString(query.Type_TEXT, 32, sql.Collation_ascii_bin)

// GetRoot returns the root value for this database session
func (db Database) GetRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.Name())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no root value found in session")
	}

	return dbState.GetRoots().Working, nil
}

func (db Database) GetWorkingSet(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.Name())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no root value found in session")
	}
	return dbState.WorkingSet, nil
}

// SetRoot should typically be called on the Session, which is where this state lives. But it's available here as a
// convenience.
func (db Database) SetRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.SetRoot(ctx, db.name, newRoot)
}

// GetHeadRoot returns root value for the current session head
func (db Database) GetHeadRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	head, err := sess.GetHeadCommit(ctx, db.name)
	if err != nil {
		return nil, err
	}
	return head.GetRootValue(ctx)
}

// DropTable drops the table with the name given.
// The planner returns the correct case sensitive name in tableName
func (db Database) DropTable(ctx *sql.Context, tableName string) error {
	if doltdb.IsReadOnlySystemTable(tableName) {
		return ErrSystemTableAlter.New(tableName)
	}

	ds := dsess.DSessFromSess(ctx.Session)
	if _, ok := ds.GetTemporaryTable(ctx, db.Name(), tableName); ok {
		ds.DropTemporaryTable(ctx, db.Name(), tableName)
		return nil
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}

	tableExists, err := root.HasTable(ctx, tableName)
	if err != nil {
		return err
	}

	if !tableExists {
		return sql.ErrTableNotFound.New(tableName)
	}

	newRoot, err := root.RemoveTables(ctx, true, false, tableName)
	if err != nil {
		return err
	}

	err = db.dropTableFromAiTracker(ctx, tableName)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// dropTableFromAiTracker grabs the auto increment tracker and removes the table named tableName from it.
func (db Database) dropTableFromAiTracker(ctx *sql.Context, tableName string) error {
	sess := dsess.DSessFromSess(ctx.Session)
	ws, err := sess.WorkingSet(ctx, db.Name())

	if err != nil {
		return err
	}

	ait, err := db.gs.GetAutoIncrementTracker(ctx, ws)
	if err != nil {
		return err
	}
	ait.DropTable(tableName)

	return nil
}

// CreateTable creates a table with the name and schema given.
func (db Database) CreateTable(ctx *sql.Context, tableName string, sch sql.PrimaryKeySchema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createSqlTable(ctx, tableName, sch)
}

// Unlike the exported version CreateTable, createSqlTable doesn't enforce any table name checks.
func (db Database) createSqlTable(ctx *sql.Context, tableName string, sch sql.PrimaryKeySchema) error {
	ws, err := db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}
	root := ws.WorkingRoot()

	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	headRoot, err := db.GetHeadRoot(ctx)
	if err != nil {
		return err
	}

	doltSch, err := sqlutil.ToDoltSchema(ctx, root, tableName, sch, headRoot)
	if err != nil {
		return err
	}

	// Prevent any tables that use Spatial Types as Primary Key from being created
	if schema.IsUsingSpatialColAsKey(doltSch) {
		return schema.ErrUsingSpatialKey.New(tableName)
	}

	if schema.HasAutoIncrement(doltSch) {
		ait, err := db.gs.GetAutoIncrementTracker(ctx, ws)
		if err != nil {
			return err
		}
		ait.AddNewTable(tableName)
	}

	return db.createDoltTable(ctx, tableName, root, doltSch)
}

// createDoltTable creates a table on the database using the given dolt schema while not enforcing table name checks.
func (db Database) createDoltTable(ctx *sql.Context, tableName string, root *doltdb.RootValue, doltSch schema.Schema) error {
	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	var conflictingTbls []string
	_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		_, tbl, exists, err := root.GetTableByColTag(ctx, tag)
		if err != nil {
			return true, err
		}
		if exists && tbl != tableName {
			errStr := schema.ErrTagPrevUsed(tag, col.Name, tbl).Error()
			conflictingTbls = append(conflictingTbls, errStr)
		}
		return false, nil
	})

	if len(conflictingTbls) > 0 {
		return fmt.Errorf(strings.Join(conflictingTbls, "\n"))
	}

	newRoot, err := root.CreateEmptyTable(ctx, tableName, doltSch)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// CreateTemporaryTable creates a table that only exists the length of a session.
func (db Database) CreateTemporaryTable(ctx *sql.Context, tableName string, pkSch sql.PrimaryKeySchema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	tmp, err := NewTempTable(ctx, db.ddb, pkSch, tableName, db.name, db.editOpts)
	if err != nil {
		return err
	}

	ds := dsess.DSessFromSess(ctx.Session)
	ds.AddTemporaryTable(ctx, db.Name(), tmp)
	return nil
}

// RenameTable implements sql.TableRenamer
func (db Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	if doltdb.IsReadOnlySystemTable(oldName) {
		return ErrSystemTableAlter.New(oldName)
	}

	if doltdb.HasDoltPrefix(newName) {
		return ErrReservedTableName.New(newName)
	}

	if !doltdb.IsValidTableName(newName) {
		return ErrInvalidTableName.New(newName)
	}

	if _, ok, _ := db.GetTableInsensitive(ctx, newName); ok {
		return sql.ErrTableAlreadyExists.New(newName)
	}

	newRoot, err := renameTable(ctx, root, oldName, newName)

	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// Flush flushes the current batch of outstanding changes and returns any errors.
func (db Database) Flush(ctx *sql.Context) error {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, _, err := sess.LookupDbState(ctx, db.Name())
	if err != nil {
		return err
	}
	editSession := dbState.WriteSession

	ws, err := editSession.Flush(ctx)
	if err != nil {
		return err
	}
	return db.SetRoot(ctx, ws.WorkingRoot())
}

// GetView implements sql.ViewDatabase
func (db Database) GetView(ctx *sql.Context, viewName string) (string, bool, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return "", false, err
	}

	lwrViewName := strings.ToLower(viewName)
	switch {
	case strings.HasPrefix(lwrViewName, doltdb.DoltBlameViewPrefix):
		tableName := lwrViewName[len(doltdb.DoltBlameViewPrefix):]

		view, err := dtables.NewBlameView(ctx, tableName, root)
		if err != nil {
			return "", false, err
		}
		return view, true, nil
	}

	tbl, ok, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return "", false, err
	}
	if !ok {
		return "", false, nil
	}

	fragments, err := getSchemaFragmentsOfType(ctx, tbl.(*WritableDoltTable), viewFragment)
	if err != nil {
		return "", false, err
	}

	for _, fragment := range fragments {
		if strings.ToLower(fragment.name) == strings.ToLower(viewName) {
			return fragment.fragment, true, nil
		}
	}

	return "", false, nil
}

// AllViews implements sql.ViewDatabase
func (db Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	tbl, ok, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	frags, err := getSchemaFragmentsOfType(ctx, tbl.(*WritableDoltTable), viewFragment)
	if err != nil {
		return nil, err
	}

	var views []sql.ViewDefinition
	for _, frag := range frags {
		views = append(views, sql.ViewDefinition{
			Name:           frag.name,
			TextDefinition: frag.fragment,
		})
	}
	if err != nil {
		return nil, err
	}

	return views, nil
}

// CreateView implements sql.ViewCreator. Persists the view in the dolt database, so
// it can exist in a sql session later. Returns sql.ErrExistingView if a view
// with that name already exists.
func (db Database) CreateView(ctx *sql.Context, name string, definition string) error {
	err := sql.ErrExistingView.New(db.name, name)
	return db.addFragToSchemasTable(ctx, "view", name, definition, time.Unix(0, 0).UTC(), err)
}

// DropView implements sql.ViewDropper. Removes a view from persistence in the
// dolt database. Returns sql.ErrNonExistingView if the view did not
// exist.
func (db Database) DropView(ctx *sql.Context, name string) error {
	err := sql.ErrViewDoesNotExist.New(db.name, name)
	return db.dropFragFromSchemasTable(ctx, "view", name, err)
}

// GetTriggers implements sql.TriggerDatabase.
func (db Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	tbl, ok, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	frags, err := getSchemaFragmentsOfType(ctx, tbl.(*WritableDoltTable), triggerFragment)
	if err != nil {
		return nil, err
	}

	var triggers []sql.TriggerDefinition
	for _, frag := range frags {
		triggers = append(triggers, sql.TriggerDefinition{
			Name:            frag.name,
			CreateStatement: frag.fragment,
			CreatedAt:       frag.created,
		})
	}
	if err != nil {
		return nil, err
	}

	return triggers, nil
}

// CreateTrigger implements sql.TriggerDatabase.
func (db Database) CreateTrigger(ctx *sql.Context, definition sql.TriggerDefinition) error {
	return db.addFragToSchemasTable(ctx,
		"trigger",
		definition.Name,
		definition.CreateStatement,
		definition.CreatedAt,
		fmt.Errorf("triggers `%s` already exists", definition.Name), //TODO: add a sql error and return that instead
	)
}

// DropTrigger implements sql.TriggerDatabase.
func (db Database) DropTrigger(ctx *sql.Context, name string) error {
	//TODO: add a sql error and use that as the param error instead
	return db.dropFragFromSchemasTable(ctx, "trigger", name, sql.ErrTriggerDoesNotExist.New(name))
}

// GetStoredProcedures implements sql.StoredProcedureDatabase.
func (db Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return DoltProceduresGetAll(ctx, db)
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return DoltProceduresAddProcedure(ctx, db, spd)
}

// DropStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return DoltProceduresDropProcedure(ctx, db, name)
}

// GetExternalStoredProcedures implements sql.ExternalStoredProcedureDatabase.
func (db Database) GetExternalStoredProcedures(ctx *sql.Context) ([]sql.ExternalStoredProcedureDetails, error) {
	return dprocedures.DoltProcedures, nil
}

func (db Database) addFragToSchemasTable(ctx *sql.Context, fragType, name, definition string, created time.Time, existingErr error) (retErr error) {
	tbl, err := GetOrCreateDoltSchemasTable(ctx, db)
	if err != nil {
		return err
	}

	_, exists, err := fragFromSchemasTable(ctx, tbl, fragType, name)
	if err != nil {
		return err
	}
	if exists {
		return existingErr
	}

	ts, err := db.TableEditSession(ctx)
	if err != nil {
		return err
	}

	ws, err := ts.Flush(ctx)
	if err != nil {
		return err
	}

	// If rows exist, then grab the highest id and add 1 to get the new id
	idx, err := nextSchemasTableIndex(ctx, ws.WorkingRoot())
	if err != nil {
		return err
	}

	// Insert the new row into the db
	inserter := tbl.Inserter(ctx)
	defer func() {
		err := inserter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	// Encode createdAt time to JSON
	extra := Extra{
		CreatedAt: created.Unix(),
	}
	extraJSON, err := json.Marshal(extra)
	if err != nil {
		return err
	}
	return inserter.Insert(ctx, sql.Row{fragType, name, definition, idx, extraJSON})
}

func (db Database) dropFragFromSchemasTable(ctx *sql.Context, fragType, name string, missingErr error) error {
	stbl, found, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return err
	}
	if !found {
		return missingErr
	}

	tbl := stbl.(*WritableDoltTable)
	row, exists, err := fragFromSchemasTable(ctx, tbl, fragType, name)
	if err != nil {
		return err
	}
	if !exists {
		return missingErr
	}
	deleter := tbl.Deleter(ctx)
	err = deleter.Delete(ctx, row)
	if err != nil {
		return err
	}

	return deleter.Close(ctx)
}

// TableEditSession returns the TableEditSession for this database from the given context.
func (db Database) TableEditSession(ctx *sql.Context) (writer.WriteSession, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, _, err := sess.LookupDbState(ctx, db.Name())
	if err != nil {
		return nil, err
	}
	return dbState.WriteSession, nil
}

// GetAllTemporaryTables returns all temporary tables
func (db Database) GetAllTemporaryTables(ctx *sql.Context) ([]sql.Table, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.GetAllTemporaryTables(ctx, db.Name())
}
