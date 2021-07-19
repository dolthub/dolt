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
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/alterschema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/types"
)

var ErrInvalidTableName = errors.NewKind("Invalid table name %s. Table names must match the regular expression " + doltdb.TableNameRegexStr)
var ErrReservedTableName = errors.NewKind("Invalid table name %s. Table names beginning with `dolt_` are reserved for internal use")
var ErrSystemTableAlter = errors.NewKind("Cannot alter table %s: system tables cannot be dropped or altered")

type SqlDatabase interface {
	sql.Database
	GetRoot(*sql.Context) (*doltdb.RootValue, error)
	GetTemporaryTablesRoot(*sql.Context) (*doltdb.RootValue, bool)
}

// Database implements sql.Database for a dolt DB.
type Database struct {
	name string
	ddb  *doltdb.DoltDB
	rsr  env.RepoStateReader
	rsw  env.RepoStateWriter
	drw  env.DocsReadWriter
}

var _ sql.Database = Database{}
var _ sql.TableCreator = Database{}
var _ sql.TemporaryTableCreator = Database{}
var _ sql.TemporaryTableDatabase = Database{}

type ReadOnlyDatabase struct {
	Database
}

var _ sql.ReadOnlyDatabase = ReadOnlyDatabase{}

func (r ReadOnlyDatabase) IsReadOnly() bool {
	return true
}

func (db Database) StartTransaction(ctx *sql.Context) (sql.Transaction, error) {
	dsession := dsess.DSessFromSess(ctx.Session)
	return dsession.StartTransaction(ctx, db.Name())
}

func (db Database) setHeadHash(ctx *sql.Context, ref ref.WorkingSetRef) error {
	// TODO: use the session HEAD ref here instead of the repo state one
	// headRef, err := ref.ToHeadRef()
	// if err != nil {
	// 	return err
	// }

	headCommit, err := db.ddb.Resolve(ctx, db.rsr.CWBHeadSpec(), db.rsr.CWBHeadRef())
	if err != nil {
		return err
	}
	headHash, err := headCommit.HashOf()
	if err != nil {
		return err
	}
	if doltSession, ok := ctx.Session.(*dsess.Session); ok {
		return doltSession.SetSessionVarDirectly(ctx, dsess.HeadKey(db.name), headHash.String())
	} else {
		return ctx.SetSessionVariable(ctx, dsess.HeadKey(db.name), headHash.String())
	}
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
var _ sql.TransactionDatabase = Database{}

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(name string, dbData env.DbData) Database {
	return Database{
		name: name,
		ddb:  dbData.Ddb,
		rsr:  dbData.Rsr,
		rsw:  dbData.Rsw,
		drw:  dbData.Drw,
	}
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

// GetTableInsensitive is used when resolving tables in queries. It returns a best-effort case-insensitive match for
// the table name given.
func (db Database) GetTableInsensitive(ctx *sql.Context, tblName string) (sql.Table, bool, error) {
	// We start by first checking whether the input table is a temporary table. Temporary tables with name `x` take
	// priority over persisted tables of name `x`.
	tempTableRootValue, tempRootExists := db.GetTemporaryTablesRoot(ctx)
	if tempRootExists {
		tbl, tempTableFound, err := db.getTable(ctx, tempTableRootValue, tblName, true)
		if err != nil {
			return nil, false, err
		}

		if tempTableFound {
			return tbl, true, nil
		}
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
	case doltdb.TableOfTablesInConflictName:
		dt, found = dtables.NewTableOfTablesInConflict(ctx, db.ddb, root), true
	case doltdb.TableOfTablesWithViolationsName:
		dt, found = dtables.NewTableOfTablesConstraintViolations(ctx, root), true
	case doltdb.BranchesTableName:
		dt, found = dtables.NewBranchesTable(ctx, db.ddb), true
	case doltdb.CommitsTableName:
		dt, found = dtables.NewCommitsTable(ctx, db.ddb), true
	case doltdb.CommitAncestorsTableName:
		dt, found = dtables.NewCommitAncestorsTable(ctx, db.ddb), true
	case doltdb.StatusTableName:
		dt, found = dtables.NewStatusTable(ctx, db.name, db.ddb, dsess.NewSessionStateAdapter(sess, db.name), db.drw), true
	}
	if found {
		return dt, found, nil
	}

	return db.getTable(ctx, root, tblName, false)
}

// GetTableInsensitiveAsOf implements sql.VersionedDatabase
func (db Database) GetTableInsensitiveAsOf(ctx *sql.Context, tableName string, asOf interface{}) (sql.Table, bool, error) {
	root, err := db.rootAsOf(ctx, asOf)

	if err != nil {
		return nil, false, err
	} else if root == nil {
		return nil, false, nil
	}

	table, ok, err := db.getTable(ctx, root, tableName, false)
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

func (db Database) getRootForCommitRef(ctx *sql.Context, commitRef string) (*doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec(commitRef)
	if err != nil {
		return nil, err
	}

	cm, err := db.ddb.Resolve(ctx, cs, db.rsr.CWBHeadRef())
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
func (db Database) getTable(ctx *sql.Context, root *doltdb.RootValue, tableName string, temporary bool) (sql.Table, bool, error) {
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

	readonlyTable := NewDoltTable(tableName, sch, tbl, db, temporary)
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
	dbState, dbRootOk := sess.DbStates[db.name]
	if !dbRootOk {
		return nil, fmt.Errorf("no root value found in session")
	}

	return dbState.WorkingSet.WorkingRoot(), nil
}

func (db Database) GetTemporaryTablesRoot(ctx *sql.Context) (*doltdb.RootValue, bool) {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.GetTempTableRootValue(ctx, db.Name())
}

// SetRoot should typically be called on the Session, which is where this state lives. But it's available here as a
// convenience.
func (db Database) SetRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.SetRoot(ctx, db.name, newRoot)
}

// SetTemporaryRoot sets the root value holding temporary tables not persisted to the repo state after the session.
func (db Database) SetTemporaryRoot(ctx *sql.Context, newRoot *doltdb.RootValue) error {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.SetTempTableRoot(ctx, db.name, newRoot)
}

// GetHeadRoot returns root value for the current session head
func (db Database) GetHeadRoot(ctx *sql.Context) (*doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	head, err := sess.GetHeadCommit(ctx, db.name)
	if err != nil {
		return nil, err
	}
	return head.GetRootValue()
}

// DropTable drops the table with the name given.
// The planner returns the correct case sensitive name in tableName
func (db Database) DropTable(ctx *sql.Context, tableName string) error {
	if doltdb.IsReadOnlySystemTable(tableName) {
		return ErrSystemTableAlter.New(tableName)
	}

	// Temporary Tables Get Precedence over schema tables
	tempTableRoot, tempRootExists := db.GetTemporaryTablesRoot(ctx)
	if tempRootExists {
		tempTableExists, err := tempTableRoot.HasTable(ctx, tableName)
		if err != nil {
			return err
		}

		if tempTableExists {
			newRoot, err := tempTableRoot.RemoveTables(ctx, tableName)
			if err != nil {
				return err
			}

			return db.SetTemporaryRoot(ctx, newRoot)
		}
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

	newRoot, err := root.RemoveTables(ctx, tableName)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// CreateTable creates a table with the name and schema given.
func (db Database) CreateTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createSqlTable(ctx, tableName, sch)
}

// Unlike the exported version CreateTable, createSqlTable doesn't enforce any table name checks.
func (db Database) createSqlTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}

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
func (db Database) CreateTemporaryTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	if doltdb.HasDoltPrefix(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createTempSQLTable(ctx, tableName, sch)
}

func (db Database) createTempSQLTable(ctx *sql.Context, tableName string, sch sql.Schema) error {
	// Get temporary root value
	sess := dsess.DSessFromSess(ctx.Session)
	tempTableRootValue, exists := db.GetTemporaryTablesRoot(ctx)

	// create the root value only when needed.
	if !exists {
		err := sess.CreateTemporaryTablesRoot(ctx, db.Name(), db.GetDoltDB())
		if err != nil {
			return err
		}

		tempTableRootValue, _ = db.GetTemporaryTablesRoot(ctx)
	}

	doltSch, err := sqlutil.ToDoltSchema(ctx, tempTableRootValue, tableName, sch, nil)
	if err != nil {
		return err
	}

	return db.createTempDoltTable(ctx, tableName, tempTableRootValue, doltSch, sess)
}

func (db Database) createTempDoltTable(ctx *sql.Context, tableName string, root *doltdb.RootValue, doltSch schema.Schema, dsess *dsess.Session) error {
	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		_, tbl, exists, err := root.GetTableByColTag(ctx, tag)
		if err != nil {
			return true, err
		}
		if exists && tbl != tableName {
			panic("Table's tags are associated with a different table name")
		}
		return false, nil
	})

	newRoot, err := root.CreateEmptyTable(ctx, tableName, doltSch)
	if err != nil {
		return err
	}

	return dsess.SetTempTableRoot(ctx, db.Name(), newRoot)
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

	newRoot, err := alterschema.RenameTable(ctx, root, oldName, newName)

	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// Flush flushes the current batch of outstanding changes and returns any errors.
func (db Database) Flush(ctx *sql.Context) error {
	sess := dsess.DSessFromSess(ctx.Session)
	editSession := sess.DbStates[db.name].EditSession

	newRoot, err := editSession.Flush(ctx)
	if err != nil {
		return err
	}

	err = db.SetRoot(ctx, newRoot)
	if err != nil {
		return nil
	}

	// Flush any changes made to temporary tables
	// TODO: Shouldn't always be updating both roots. Needs to update either both roots or neither of them, atomically
	tempTableEditSession := sess.DbStates[db.name].TempTableEditSession
	if tempTableEditSession != nil {
		newTempTableRoot, err := tempTableEditSession.Flush(ctx)
		if err != nil {
			return nil
		}

		return sess.SetTempTableRoot(ctx, db.Name(), newTempTableRoot)
	}

	return nil
}

// CreateView implements sql.ViewCreator. Persists the view in the dolt database, so
// it can exist in a sql session later. Returns sql.ErrExistingView if a view
// with that name already exists.
func (db Database) CreateView(ctx *sql.Context, name string, definition string) error {
	return db.addFragToSchemasTable(ctx, "view", name, definition, sql.ErrExistingView.New(name))
}

// DropView implements sql.ViewDropper. Removes a view from persistence in the
// dolt database. Returns sql.ErrNonExistingView if the view did not
// exist.
func (db Database) DropView(ctx *sql.Context, name string) error {
	return db.dropFragFromSchemasTable(ctx, "view", name, sql.ErrNonExistingView.New(name))
}

// GetTriggers implements sql.TriggerDatabase.
func (db Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	tbl, ok, err := root.GetTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	typeCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesTypeCol)
	if !ok {
		return nil, fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)
	}
	nameCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesNameCol)
	if !ok {
		return nil, fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)
	}
	fragCol, ok := sch.GetAllCols().GetByName(doltdb.SchemasTablesFragmentCol)
	if !ok {
		return nil, fmt.Errorf("`%s` schema in unexpected format", doltdb.SchemasTableName)
	}

	rowData, err := tbl.GetRowData(ctx)
	if err != nil {
		return nil, err
	}
	var triggers []sql.TriggerDefinition
	err = rowData.Iter(ctx, func(key, val types.Value) (stop bool, err error) {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return true, err
		}
		if typeColVal, ok := dRow.GetColVal(typeCol.Tag); ok && typeColVal.Equals(types.String("trigger")) {
			name, ok := dRow.GetColVal(nameCol.Tag)
			if !ok {
				taggedVals, _ := dRow.TaggedValues()
				return true, fmt.Errorf("missing `%s` value for trigger row: (%s)", doltdb.SchemasTablesNameCol, taggedVals)
			}
			createStmt, ok := dRow.GetColVal(fragCol.Tag)
			if !ok {
				taggedVals, _ := dRow.TaggedValues()
				return true, fmt.Errorf("missing `%s` value for trigger row: (%s)", doltdb.SchemasTablesFragmentCol, taggedVals)
			}
			triggers = append(triggers, sql.TriggerDefinition{
				Name:            string(name.(types.String)),
				CreateStatement: string(createStmt.(types.String)),
			})
		}
		return false, nil
	})
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
	missingValue := errors.NewKind("missing `%s` value for procedure row: (%s)")

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	table, ok, err := root.GetTable(ctx, doltdb.ProceduresTableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}

	rowData, err := table.GetRowData(ctx)
	if err != nil {
		return nil, err
	}

	sch, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	var spds []sql.StoredProcedureDetails
	err = rowData.Iter(ctx, func(key, val types.Value) (stop bool, err error) {
		dRow, err := row.FromNoms(sch, key.(types.Tuple), val.(types.Tuple))
		if err != nil {
			return true, err
		}
		taggedVals, err := dRow.TaggedValues()
		if err != nil {
			return true, err
		}

		name, ok := dRow.GetColVal(schema.DoltProceduresNameTag)
		if !ok {
			return true, missingValue.New(doltdb.ProceduresTableNameCol, taggedVals)
		}
		createStmt, ok := dRow.GetColVal(schema.DoltProceduresCreateStmtTag)
		if !ok {
			return true, missingValue.New(doltdb.ProceduresTableCreateStmtCol, taggedVals)
		}
		createdAt, ok := dRow.GetColVal(schema.DoltProceduresCreatedAtTag)
		if !ok {
			return true, missingValue.New(doltdb.ProceduresTableCreatedAtCol, taggedVals)
		}
		modifiedAt, ok := dRow.GetColVal(schema.DoltProceduresModifiedAtTag)
		if !ok {
			return true, missingValue.New(doltdb.ProceduresTableModifiedAtCol, taggedVals)
		}
		spds = append(spds, sql.StoredProcedureDetails{
			Name:            string(name.(types.String)),
			CreateStatement: string(createStmt.(types.String)),
			CreatedAt:       time.Time(createdAt.(types.Timestamp)),
			ModifiedAt:      time.Time(modifiedAt.(types.Timestamp)),
		})
		return false, nil
	})
	if err != nil {
		return nil, err
	}
	return spds, nil
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	return DoltProceduresAddProcedure(ctx, db, spd)
}

// DropStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	return DoltProceduresDropProcedure(ctx, db, name)
}

func (db Database) addFragToSchemasTable(ctx *sql.Context, fragType, name, definition string, existingErr error) (retErr error) {
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

	// If rows exist, then grab the highest id and add 1 to get the new id
	indexToUse := int64(1)
	te, err := db.TableEditSession(ctx, tbl.IsTemporary()).GetTableEditor(ctx, doltdb.SchemasTableName, tbl.sch)
	if err != nil {
		return err
	}
	dTable, err := te.Table(ctx)
	if err != nil {
		return err
	}
	rowData, err := dTable.GetRowData(ctx)
	if err != nil {
		return err
	}
	if rowData.Len() > 0 {
		keyTpl, _, err := rowData.Last(ctx)
		if err != nil {
			return err
		}
		if keyTpl != nil {
			key, err := keyTpl.(types.Tuple).Get(1)
			if err != nil {
				return err
			}
			indexToUse = int64(key.(types.Int)) + 1
		}
	}

	// Insert the new row into the db
	inserter := tbl.Inserter(ctx)
	defer func() {
		err := inserter.Close(ctx)
		if retErr == nil {
			retErr = err
		}
	}()
	return inserter.Insert(ctx, sql.Row{fragType, name, definition, indexToUse})
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
func (db Database) TableEditSession(ctx *sql.Context, isTemporary bool) *editor.TableEditSession {
	if isTemporary {
		return dsess.DSessFromSess(ctx.Session).DbStates[db.name].TempTableEditSession
	}
	return dsess.DSessFromSess(ctx.Session).DbStates[db.name].EditSession
}

// GetAllTemporaryTables returns all temporary tables
func (db Database) GetAllTemporaryTables(ctx *sql.Context) ([]sql.Table, error) {
	sess := dsess.DSessFromSess(ctx.Session)

	tables := make([]sql.Table, 0)

	root := sess.DbStates[db.name].TempTableRoot
	if root != nil {
		tNames, err := root.GetTableNames(ctx)
		if err != nil {
			return nil, err
		}

		for _, tName := range tNames {
			tbl, ok, err := db.GetTableInsensitive(ctx, tName)
			if err != nil {
				return nil, err
			}

			if ok {
				tables = append(tables, tbl)
			}
		}
	}

	return tables, nil
}

// RegisterSchemaFragments register SQL schema fragments that are persisted in the given
// `Database` with the provided `sql.ViewRegistry`. Returns an error if
// there are I/O issues, but currently silently fails to register some
// schema fragments if they don't parse, or if registries within the
// `catalog` return errors.
func RegisterSchemaFragments(ctx *sql.Context, db Database, root *doltdb.RootValue) error {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}

	tbl, found, err := root.GetTable(ctx, doltdb.SchemasTableName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	rowData, err := tbl.GetRowData(ctx)

	if err != nil {
		return err
	}

	iter, err := newRowIterator(ctx, tbl, nil, &doltTablePartition{rowData: rowData, end: NoUpperBound})
	if err != nil {
		return err
	}
	defer iter.Close(ctx)

	var parseErrors []error

	r, err := iter.Next()
	for err == nil {
		if r[0] == "view" {
			name := r[1].(string)
			definition := r[2].(string)
			cv, err := parse.Parse(ctx, fmt.Sprintf("create view %s as %s", sqlfmt.QuoteIdentifier(name), definition))
			if err != nil {
				parseErrors = append(parseErrors, err)
			} else {
				err = ctx.Register(db.Name(), cv.(*plan.CreateView).Definition.AsView())
				if err != nil {
					return err
				}
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
