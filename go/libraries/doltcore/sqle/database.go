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

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/analyzer/analyzererrors"
	"github.com/dolthub/go-mysql-server/sql/expression"
	"github.com/dolthub/go-mysql-server/sql/fulltext"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/doltcore/rebase"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtables"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/resolve"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/concurrentmap"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrInvalidTableName = errors.NewKind("Invalid table name %s.")
var ErrReservedTableName = errors.NewKind("Invalid table name %s. Table names beginning with `dolt_` are reserved for internal use")
var ErrReservedDiffTableName = errors.NewKind("Invalid table name %s. Table names beginning with `__DATABASE__` are reserved for internal use")
var ErrSystemTableAlter = errors.NewKind("Cannot alter table %s: system tables cannot be dropped or altered")

// Database implements sql.Database for a dolt DB.
type Database struct {
	baseName      string
	requestedName string
	schemaName    string
	ddb           *doltdb.DoltDB
	rsr           env.RepoStateReader
	rsw           env.RepoStateWriter
	gs            dsess.GlobalStateImpl
	editOpts      editor.Options
	revision      string
	revType       dsess.RevisionType
}

var _ dsess.SqlDatabase = Database{}
var _ dsess.RevisionDatabase = Database{}
var _ globalstate.GlobalStateProvider = Database{}
var _ sql.CollatedDatabase = Database{}
var _ sql.Database = Database{}
var _ sql.StoredProcedureDatabase = Database{}
var _ sql.TableCreator = Database{}
var _ sql.IndexedTableCreator = Database{}
var _ sql.TableDropper = Database{}
var _ sql.TableRenamer = Database{}
var _ sql.TemporaryTableCreator = Database{}
var _ sql.TemporaryTableDatabase = Database{}
var _ sql.TriggerDatabase = Database{}
var _ sql.VersionedDatabase = Database{}
var _ sql.ViewDatabase = Database{}
var _ sql.EventDatabase = Database{}
var _ sql.AliasedDatabase = Database{}
var _ fulltext.Database = Database{}
var _ rebase.RebasePlanDatabase = Database{}
var _ sql.SchemaValidator = Database{}
var _ sql.SchemaDatabase = Database{}
var _ sql.DatabaseSchema = Database{}

type ReadOnlyDatabase struct {
	Database
}

var _ sql.ReadOnlyDatabase = ReadOnlyDatabase{}
var _ dsess.SqlDatabase = ReadOnlyDatabase{}

func (r ReadOnlyDatabase) IsReadOnly() bool {
	return true
}

func (r ReadOnlyDatabase) InitialDBState(ctx *sql.Context) (dsess.InitialDbState, error) {
	return initialDBState(ctx, r, r.revision)
}

func (r ReadOnlyDatabase) WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (dsess.SqlDatabase, error) {
	revDb, err := r.Database.WithBranchRevision(requestedName, branchSpec)
	if err != nil {
		return nil, err
	}

	r.Database = revDb.(Database)
	return r, nil
}

func (db Database) WithBranchRevision(requestedName string, branchSpec dsess.SessionDatabaseBranchSpec) (dsess.SqlDatabase, error) {
	db.rsr, db.rsw = branchSpec.RepoState, branchSpec.RepoState
	db.revision = branchSpec.Branch
	db.revType = dsess.RevisionTypeBranch
	db.requestedName = requestedName

	return db, nil
}

func (db Database) ValidateSchema(sch sql.Schema) error {
	if rowLen := schema.MaxRowStorageSize(sch); rowLen > int64(val.MaxTupleDataSize) {
		// |val.MaxTupleDataSize| is less than |types.MaxRowLength| to account for
		// serial message metadata
		return analyzererrors.ErrInvalidRowLength.New(val.MaxTupleDataSize, rowLen)
	}
	return nil
}

// Revision implements dsess.RevisionDatabase
func (db Database) Revision() string {
	return db.revision
}

func (db Database) Versioned() bool {
	return true
}

func (db Database) RevisionType() dsess.RevisionType {
	return db.revType
}

func (db Database) EditOptions() editor.Options {
	return db.editOpts
}

func (db Database) DoltDatabases() []*doltdb.DoltDB {
	return []*doltdb.DoltDB{db.ddb}
}

// NewDatabase returns a new dolt database to use in queries.
func NewDatabase(ctx context.Context, name string, dbData env.DbData, editOpts editor.Options) (Database, error) {
	globalState, err := dsess.NewGlobalStateStoreForDb(ctx, name, dbData.Ddb)
	if err != nil {
		return Database{}, err
	}

	return Database{
		baseName:      name,
		requestedName: name,
		ddb:           dbData.Ddb,
		rsr:           dbData.Rsr,
		rsw:           dbData.Rsw,
		gs:            globalState,
		editOpts:      editOpts,
	}, nil
}

// initialDBState returns the InitialDbState for |db|. Other implementations of SqlDatabase outside this file should
// implement their own method for an initial db state and not rely on this method.
func initialDBState(ctx *sql.Context, db dsess.SqlDatabase, branch string) (dsess.InitialDbState, error) {
	if len(db.Revision()) > 0 {
		return initialStateForRevisionDb(ctx, db)
	}

	return initialDbState(ctx, db, branch)
}

func (db Database) InitialDBState(ctx *sql.Context) (dsess.InitialDbState, error) {
	return initialDBState(ctx, db, db.revision)
}

// Name returns the name of this database, set at creation time.
func (db Database) Name() string {
	return db.RequestedName()
}

// Schema returns the name of the schema that this database represents.
func (db Database) Schema() string {
	return db.schemaName
}

// AliasedName is what allows databases named e.g. `mydb/b1` to work with the grant and info schema tables that expect
// a base (no revision qualifier) db name
func (db Database) AliasedName() string {
	return db.baseName
}

// RevisionQualifiedName returns the name of this database including its revision qualifier, if any. This method should
// be used whenever accessing internal state of a database and its tables.
func (db Database) RevisionQualifiedName() string {
	if db.revision == "" {
		return db.baseName
	}
	return db.baseName + dsess.DbRevisionDelimiter + db.revision
}

func (db Database) RequestedName() string {
	return db.requestedName
}

// GetDoltDB gets the underlying doltDB of the Database
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

func (db Database) DbData() env.DbData {
	return env.DbData{
		Ddb: db.ddb,
		Rsw: db.rsw,
		Rsr: db.rsr,
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

	return db.getTableInsensitive(ctx, nil, ds, root, tblName, "")
}

// GetTableInsensitiveAsOf implements sql.VersionedDatabase
func (db Database) GetTableInsensitiveAsOf(ctx *sql.Context, tableName string, asOf interface{}) (sql.Table, bool, error) {
	if asOf == nil {
		return db.GetTableInsensitive(ctx, tableName)
	}
	head, root, err := resolveAsOf(ctx, db, asOf)
	if err != nil {
		return nil, false, err
	} else if root == nil {
		return nil, false, nil
	}

	sess := dsess.DSessFromSess(ctx.Session)

	table, ok, err := db.getTableInsensitive(ctx, head, sess, root, tableName, asOf)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	if doltdb.IsReadOnlySystemTable(doltdb.TableName{Name: tableName, Schema: db.schemaName}) {
		// currently, system tables do not need to be "locked to root"
		//  see comment below in getTableInsensitive
		return table, ok, nil
	}

	switch t := table.(type) {
	case dtables.VersionableTable:
		versionedTable, err := t.LockedToRoot(ctx, root)
		if err != nil {
			return nil, false, err
		}
		return versionedTable, true, nil

	case *plan.EmptyTable:
		// getTableInsensitive returns *plan.EmptyTable if the table doesn't exist in the data root, but
		// schemas have been locked to a commit where the table does exist. Since the table is empty,
		// there's no need to lock it to a root.
		return t, true, nil

	default:
		return nil, false, fmt.Errorf("unexpected table type %T", table)
	}
}

func (db Database) getTableInsensitive(ctx *sql.Context, head *doltdb.Commit, ds *dsess.DoltSession, root doltdb.RootValue, tblName string, asOf interface{}) (sql.Table, bool, error) {
	lwrName := strings.ToLower(tblName)

	// TODO: these tables that cache a root value at construction time should not, they need to get it from the session
	//  at runtime
	switch {
	case strings.HasPrefix(lwrName, doltdb.DoltDiffTablePrefix):
		if head == nil {
			var err error
			head, err = ds.GetHeadCommit(ctx, db.RevisionQualifiedName())

			if err != nil {
				return nil, false, err
			}
		}

		baseTableName := tblName[len(doltdb.DoltDiffTablePrefix):]
		tname := doltdb.TableName{Name: baseTableName, Schema: db.schemaName}
		if resolve.UseSearchPath && db.schemaName == "" {
			var err error
			tname, _, _, err = resolve.Table(ctx, root, baseTableName)
			if err != nil {
				return nil, false, err
			}
		}

		dt, err := dtables.NewDiffTable(ctx, db.Name(), tname, db.ddb, root, head)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil

	case strings.HasPrefix(lwrName, doltdb.DoltCommitDiffTablePrefix):
		baseTableName := tblName[len(doltdb.DoltCommitDiffTablePrefix):]
		tname := doltdb.TableName{Name: baseTableName, Schema: db.schemaName}
		if resolve.UseSearchPath && db.schemaName == "" {
			var err error
			tname, _, _, err = resolve.Table(ctx, root, baseTableName)
			if err != nil {
				return nil, false, err
			}
		}

		// Grab the staged root, if we have a valid working set, so we can show the staged changes
		// in the system table, too. If we're in a detached head mode, just reuse the working root.
		stagedRoot, err := workingSetStagedRoot(ctx, db.RevisionQualifiedName())
		if err == doltdb.ErrOperationNotSupportedInDetachedHead {
			stagedRoot = root
		} else if err != nil {
			return nil, false, err
		}

		dt, err := dtables.NewCommitDiffTable(ctx, db.Name(), tname, db.ddb, root, stagedRoot)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil

	case strings.HasPrefix(lwrName, doltdb.DoltHistoryTablePrefix):
		baseTableName := tblName[len(doltdb.DoltHistoryTablePrefix):]
		baseTable, ok, err := db.getTable(ctx, root, baseTableName)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			return nil, false, nil
		}

		if head == nil {
			var err error
			head, err = ds.GetHeadCommit(ctx, db.RevisionQualifiedName())
			if err != nil {
				return nil, false, err
			}
		}

		switch t := baseTable.(type) {
		case *AlterableDoltTable:
			return NewHistoryTable(t.DoltTable, db.ddb, head), true, nil
		case *WritableDoltTable:
			return NewHistoryTable(t.DoltTable, db.ddb, head), true, nil
		default:
			return nil, false, fmt.Errorf("expected Alterable or WritableDoltTable, found %T", baseTable)
		}

	case strings.HasPrefix(lwrName, doltdb.DoltConfTablePrefix):
		baseTableName := tblName[len(doltdb.DoltConfTablePrefix):]
		tname := doltdb.TableName{Name: baseTableName, Schema: db.schemaName}
		if resolve.UseSearchPath && db.schemaName == "" {
			var err error
			tname, _, _, err = resolve.Table(ctx, root, baseTableName)
			if err != nil {
				return nil, false, err
			}
		}

		srcTable, ok, err := db.getTableInsensitive(ctx, head, ds, root, tname.Name, asOf)
		if err != nil {
			return nil, false, err
		} else if !ok {
			return nil, false, nil
		}
		dt, err := dtables.NewConflictsTable(ctx, tname, srcTable, root, dtables.RootSetter(db))
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil

	case strings.HasPrefix(lwrName, doltdb.DoltConstViolTablePrefix):
		baseTableName := tblName[len(doltdb.DoltConstViolTablePrefix):]
		tname := doltdb.TableName{Name: baseTableName, Schema: db.schemaName}
		if resolve.UseSearchPath && db.schemaName == "" {
			var err error
			tname, _, _, err = resolve.Table(ctx, root, baseTableName)
			if err != nil {
				return nil, false, err
			}
		}

		dt, err := dtables.NewConstraintViolationsTable(ctx, tname, root, dtables.RootSetter(db))
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	case strings.HasPrefix(lwrName, doltdb.DoltWorkspaceTablePrefix):
		sess := dsess.DSessFromSess(ctx.Session)

		ws, err := sess.WorkingSet(ctx, db.RevisionQualifiedName())
		if err != nil {
			return nil, false, err
		}

		roots, _ := sess.GetRoots(ctx, db.RevisionQualifiedName())
		head := roots.Head

		baseTableName := tblName[len(doltdb.DoltWorkspaceTablePrefix):]
		tname := doltdb.TableName{Name: baseTableName, Schema: db.schemaName}
		if resolve.UseSearchPath && db.schemaName == "" {
			var err error
			baseName, _, exists, err := resolve.Table(ctx, root, baseTableName)
			if err != nil {
				return nil, false, err
			}
			// Only set tname if table exists so that emptyWorkspaceTable is used if the table does not exist
			if exists {
				tname = baseName
			}
		}

		dt, err := dtables.NewWorkspaceTable(ctx, tblName, tname, head, ws)
		if err != nil {
			return nil, false, err
		}
		return dt, true, nil
	}

	var dt sql.Table
	found := false
	tname := doltdb.TableName{Name: lwrName, Schema: db.schemaName}
	switch lwrName {
	case doltdb.GetLogTableName(), doltdb.LogTableName:
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			if head == nil {
				var err error
				head, err = ds.GetHeadCommit(ctx, db.RevisionQualifiedName())
				if err != nil {
					return nil, false, err
				}
			}

			dt, found = dtables.NewLogTable(ctx, db.Name(), lwrName, db.ddb, head), true
		}
	case doltdb.DiffTableName, doltdb.GetDiffTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			if head == nil {
				var err error
				head, err = ds.GetHeadCommit(ctx, db.RevisionQualifiedName())
				if err != nil {
					return nil, false, err
				}
			}

			dt, found = dtables.NewUnscopedDiffTable(ctx, db.Name(), lwrName, db.ddb, head), true
		}
	case doltdb.ColumnDiffTableName, doltdb.GetColumnDiffTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			if head == nil {
				var err error
				head, err = ds.GetHeadCommit(ctx, db.RevisionQualifiedName())
				if err != nil {
					return nil, false, err
				}
			}

			dt, found = dtables.NewColumnDiffTable(ctx, db.Name(), lwrName, db.ddb, head), true
		}
	case doltdb.TableOfTablesInConflictName, doltdb.GetTableOfTablesInConflictName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewTableOfTablesInConflict(ctx, db.RevisionQualifiedName(), lwrName, db.ddb), true
		}
	case doltdb.TableOfTablesWithViolationsName, doltdb.GetTableOfTablesWithViolationsName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewTableOfTablesConstraintViolations(ctx, lwrName, root), true
		}
	case doltdb.SchemaConflictsTableName, doltdb.GetSchemaConflictsTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewSchemaConflictsTable(ctx, db.RevisionQualifiedName(), lwrName, db.ddb), true
		}
	case doltdb.GetBranchesTableName(), doltdb.BranchesTableName:
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewBranchesTable(ctx, db, lwrName), true
		}
	case doltdb.RemoteBranchesTableName, doltdb.GetRemoteBranchesTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewRemoteBranchesTable(ctx, db, lwrName), true
		}
	case doltdb.RemotesTableName, doltdb.GetRemotesTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewRemotesTable(ctx, db.ddb, lwrName), true
		}
	case doltdb.CommitsTableName, doltdb.GetCommitsTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewCommitsTable(ctx, db.Name(), lwrName, db.ddb), true
		}
	case doltdb.CommitAncestorsTableName, doltdb.GetCommitAncestorsTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewCommitAncestorsTable(ctx, db.Name(), lwrName, db.ddb), true
		}
	case doltdb.GetStatusTableName(), doltdb.StatusTableName:
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			sess := dsess.DSessFromSess(ctx.Session)
			adapter := dsess.NewSessionStateAdapter(
				sess, db.RevisionQualifiedName(),
				concurrentmap.New[string, env.Remote](),
				concurrentmap.New[string, env.BranchConfig](),
				concurrentmap.New[string, env.Remote]())
			ws, err := sess.WorkingSet(ctx, db.RevisionQualifiedName())
			if err != nil {
				return nil, false, err
			}

			dt, found = dtables.NewStatusTable(ctx, lwrName, db.ddb, ws, adapter), true
		}
	case doltdb.MergeStatusTableName, doltdb.GetMergeStatusTableName():
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewMergeStatusTable(db.RevisionQualifiedName(), lwrName), true
		}
	case doltdb.GetTagsTableName(), doltdb.TagsTableName:
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			dt, found = dtables.NewTagsTable(ctx, lwrName, db.ddb), true
		}
	case dtables.AccessTableName:
		basCtx := branch_control.GetBranchAwareSession(ctx)
		if basCtx != nil {
			if controller := basCtx.GetController(); controller != nil {
				dt, found = dtables.NewBranchControlTable(controller.Access), true
			}
		}
	case dtables.NamespaceTableName:
		basCtx := branch_control.GetBranchAwareSession(ctx)
		if basCtx != nil {
			if controller := basCtx.GetController(); controller != nil {
				dt, found = dtables.NewBranchNamespaceControlTable(controller.Namespace), true
			}
		}
	case doltdb.IgnoreTableName:
		if resolve.UseSearchPath && db.schemaName == "" {
			schemaName, err := resolve.FirstExistingSchemaOnSearchPath(ctx, root)
			if err != nil {
				return nil, false, err
			}
			db.schemaName = schemaName
		}

		backingTable, _, err := db.getTable(ctx, root, doltdb.IgnoreTableName)
		if err != nil {
			return nil, false, err
		}
		if backingTable == nil {
			dt, found = dtables.NewEmptyIgnoreTable(ctx, db.schemaName), true
		} else {
			versionableTable := backingTable.(dtables.VersionableTable)
			dt, found = dtables.NewIgnoreTable(ctx, versionableTable, db.schemaName), true
		}
	case doltdb.GetDocTableName(), doltdb.DocTableName:
		isDoltgresSystemTable, err := resolve.IsDoltgresSystemTable(ctx, tname, root)
		if err != nil {
			return nil, false, err
		}
		if !resolve.UseSearchPath || isDoltgresSystemTable {
			if resolve.UseSearchPath && lwrName == doltdb.DocTableName {
				db.schemaName = doltdb.DoltNamespace
			}
			backingTable, _, err := db.getTable(ctx, root, doltdb.GetDocTableName())
			if err != nil {
				return nil, false, err
			}
			if backingTable == nil {
				dt, found = dtables.NewEmptyDocsTable(ctx), true
			} else {
				versionableTable := backingTable.(dtables.VersionableTable)
				dt, found = dtables.NewDocsTable(ctx, versionableTable), true
			}
		}
	case doltdb.StatisticsTableName:
		if resolve.UseSearchPath && db.schemaName == "" {
			schemaName, err := resolve.FirstExistingSchemaOnSearchPath(ctx, root)
			if err != nil {
				return nil, false, err
			}
			db.schemaName = schemaName
		}

		var tables []string
		var err error
		branch, ok := asOf.(string)
		if ok && branch != "" {
			tables, err = db.GetTableNamesAsOf(ctx, branch)
		} else {
			tables, err = db.GetTableNames(ctx)
		}
		if err != nil {
			return nil, false, err
		}
		dt, found = dtables.NewStatisticsTable(ctx, db.Name(), db.schemaName, branch, tables), true
	case doltdb.ProceduresTableName:
		found = true
		backingTable, _, err := db.getTable(ctx, root, doltdb.ProceduresTableName)
		if err != nil {
			return nil, false, err
		}
		if backingTable == nil {
			dt = NewEmptyProceduresTable()
		} else {
			writeTable := backingTable.(*WritableDoltTable)
			dt = NewProceduresTable(writeTable)
		}
	case doltdb.SchemasTableName:
		found = true
		backingTable, _, err := db.getTable(ctx, root, doltdb.SchemasTableName)
		if err != nil {
			return nil, false, err
		}
		dt = NewSchemaTable(backingTable)
	}

	if found {
		return dt, found, nil
	}

	// Converts dolt_rebase to dolt.rebase for doltgres compatibility
	if resolve.UseSearchPath && lwrName == doltdb.RebaseTableName {
		db.schemaName = doltdb.DoltNamespace
		tblName = doltdb.GetRebaseTableName()
	}

	// TODO: this should reuse the root, not lookup the db state again
	table, found, err := db.getTable(ctx, root, tblName)
	if err != nil {
		return nil, false, err
	}
	if found {
		return table, found, err
	}

	// If the table wasn't found in the specified data root, check if there is an overridden
	// schema commit that contains it and return an empty table if so.
	return resolveOverriddenNonexistentTable(ctx, tblName, db)
}

// workingSetStagedRoot returns the staged root for the current session in the database
// named |dbName|. If a working set is not available (e.g. if a commit or tag is checked
// out), this function returns an ErrOperationNotSupportedInDetachedHead error.
func workingSetStagedRoot(ctx *sql.Context, dbName string) (doltdb.RootValue, error) {
	ds := dsess.DSessFromSess(ctx.Session)
	ws, err := ds.WorkingSet(ctx, dbName)
	if err != nil {
		return nil, err
	}
	return ws.StagedRoot(), nil
}

// resolveAsOf resolves given expression to a commit, if one exists.
func resolveAsOf(ctx *sql.Context, db Database, asOf interface{}) (*doltdb.Commit, doltdb.RootValue, error) {
	head, err := db.rsr.CWBHeadRef()
	if err != nil {
		return nil, nil, err
	}
	switch x := asOf.(type) {
	case time.Time:
		return resolveAsOfTime(ctx, db.ddb, head, x)
	case string:
		return resolveAsOfCommitRef(ctx, db, head, x)
	default:
		return nil, nil, fmt.Errorf("unsupported AS OF type %T", asOf)
	}
}

func resolveAsOfTime(ctx *sql.Context, ddb *doltdb.DoltDB, head ref.DoltRef, asOf time.Time) (*doltdb.Commit, doltdb.RootValue, error) {
	cs, err := doltdb.NewCommitSpec("HEAD")
	if err != nil {
		return nil, nil, err
	}

	optCmt, err := ddb.Resolve(ctx, cs, head)
	if err != nil {
		return nil, nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, nil, doltdb.ErrGhostCommitEncountered
	}

	h, err := cm.HashOf()
	if err != nil {
		return nil, nil, err
	}

	cmItr, err := commitwalk.GetTopologicalOrderIterator(ctx, ddb, []hash.Hash{h}, nil)
	if err != nil {
		return nil, nil, err
	}

	for {
		_, optCmt, err := cmItr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, nil, err
		}
		curr, ok := optCmt.ToCommit()
		if !ok {
			return nil, nil, doltdb.ErrGhostCommitEncountered
		}

		meta, err := curr.GetCommitMeta(ctx)
		if err != nil {
			return nil, nil, err
		}

		if meta.Time().Equal(asOf) || meta.Time().Before(asOf) {
			root, err := curr.GetRootValue(ctx)
			if err != nil {
				return nil, nil, err
			}
			return curr, root, nil
		}
	}
	return nil, nil, nil
}

func resolveAsOfCommitRef(ctx *sql.Context, db Database, head ref.DoltRef, commitRef string) (*doltdb.Commit, doltdb.RootValue, error) {
	ddb := db.ddb

	if commitRef == doltdb.Working || commitRef == doltdb.Staged {
		sess := dsess.DSessFromSess(ctx.Session)
		root, _, _, err := sess.ResolveRootForRef(ctx, ctx.GetCurrentDatabase(), commitRef)
		if err != nil {
			return nil, nil, err
		}

		cm, err := ddb.ResolveCommitRef(ctx, head)
		if err != nil {
			return nil, nil, err
		}
		return cm, root, nil
	}

	cs, err := doltdb.NewCommitSpec(commitRef)

	if err != nil {
		return nil, nil, err
	}

	nomsRoot, err := dsess.TransactionRoot(ctx, db)
	if err != nil {
		return nil, nil, err
	}

	optCmt, err := ddb.ResolveByNomsRoot(ctx, cs, head, nomsRoot)
	if err != nil {
		return nil, nil, err
	}
	cm, ok := optCmt.ToCommit()
	if !ok {
		return nil, nil, doltdb.ErrGhostCommitEncountered
	}

	root, err := cm.GetRootValue(ctx)
	if err != nil {
		return nil, nil, err
	}

	return cm, root, nil
}

// GetTableNamesAsOf implements sql.VersionedDatabase
func (db Database) GetTableNamesAsOf(ctx *sql.Context, time interface{}) ([]string, error) {
	_, root, err := resolveAsOf(ctx, db, time)
	if err != nil {
		return nil, err
	} else if root == nil {
		return nil, nil
	}

	showSystemTablesVar, err := ctx.GetSessionVariable(ctx, dsess.ShowSystemTables)
	if err != nil {
		return nil, err
	}

	showSystemTables := showSystemTablesVar.(int8) == 1

	tblNames, err := db.getAllTableNames(ctx, root, showSystemTables)
	if err != nil {
		return nil, err
	}

	return filterDoltInternalTables(ctx, tblNames, db.schemaName), nil
}

// getTable returns the user table with the given baseName from the root given
func (db Database) getTable(ctx *sql.Context, root doltdb.RootValue, tableName string) (sql.Table, bool, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, fmt.Errorf("no state for database %s", db.RevisionQualifiedName())
	}

	overriddenSchemaRoot, err := resolveOverriddenSchemaRoot(ctx, db)
	if err != nil {
		return nil, false, err
	}

	// If schema hasn't been overridden, we can use a cached table if one exists
	if overriddenSchemaRoot == nil {
		key, err := doltdb.NewDataCacheKey(root)
		if err != nil {
			return nil, false, err
		}

		cachedTable, ok := dbState.SessionCache().GetCachedTable(key, dsess.TableCacheKey{Name: tableName, Schema: db.schemaName})
		if ok {
			return cachedTable, true, nil
		}
	}

	t, tblExists, err := db.checkForPgCatalogTable(ctx, tableName)
	if err != nil {
		return nil, false, err
	} else if tblExists {
		return t, tblExists, nil
	}

	tblName, tbl, tblExists, err := db.resolveUserTable(ctx, root, tableName)
	if err != nil {
		return nil, false, err
	} else if !tblExists {
		return nil, false, nil
	}

	tableName = tblName.Name
	// for remainder of this operation, all db operations will use the name resolved here
	db.schemaName = tblName.Schema

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return nil, false, err
	}

	if overriddenSchemaRoot != nil {
		err = overrideSchemaForTable(ctx, tableName, tbl, overriddenSchemaRoot)
		if err != nil {
			return nil, false, err
		}
	}

	table, err := db.newDoltTable(tableName, sch, tbl)
	if err != nil {
		return nil, false, err
	}

	// If the schema hasn't been overridden, cache the table
	if overriddenSchemaRoot == nil {
		key, err := doltdb.NewDataCacheKey(root)
		if err != nil {
			return nil, false, err
		}
		dbState.SessionCache().CacheTable(key, dsess.TableCacheKey{Name: tableName, Schema: db.schemaName}, table)
	}

	return table, true, nil
}

// checkForPgCatalogTable checks if the table is of pg_catalog schema
// when the schema is not defined and the table name start with 'pg_'.
func (db Database) checkForPgCatalogTable(ctx *sql.Context, tableName string) (sql.Table, bool, error) {
	if resolve.UseSearchPath && db.schemaName == "" && strings.HasPrefix(strings.ToLower(tableName), "pg_") {
		sdb, foundSch, err := db.GetSchema(ctx, "pg_catalog")
		if err != nil {
			return nil, false, err
		}
		if foundSch {
			tbl, foundTbl, err := sdb.GetTableInsensitive(ctx, tableName)
			if err != nil {
				return nil, false, err
			}
			if foundTbl {
				return tbl, foundTbl, nil
			}
		}
	}
	return nil, false, nil
}

// resolveUserTable returns the table with the given name from the root given. The table name is resolved in a
// case-insensitive manner. The table is returned along with its case-sensitive matched name. An error is returned if
// no such table exists.
func (db Database) resolveUserTable(ctx *sql.Context, root doltdb.RootValue, tableName string) (doltdb.TableName, *doltdb.Table, bool, error) {
	if resolve.UseSearchPath && db.schemaName == "" {
		return resolve.TableWithSearchPath(ctx, root, tableName)
	} else {
		return db.tableInsensitive(ctx, root, tableName)
	}
}

// tableInsensitive returns the name of this table in the root given with the db's schema name, if it exists.
// Name matching is applied in a case-insensitive manner, and the table's case-corrected name is returned as the
// first result.
func (db Database) tableInsensitive(ctx *sql.Context, root doltdb.RootValue, tableName string) (doltdb.TableName, *doltdb.Table, bool, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	}
	if !ok {
		return doltdb.TableName{}, nil, false, fmt.Errorf("no state for database %s", db.RevisionQualifiedName())
	}

	if tableListKey := root.TableListHash(); tableListKey != 0 {
		tableList, ok := dbState.SessionCache().GetCachedTableMap(tableListKey)
		if ok {
			tname, ok := tableList[strings.ToLower(tableName)]
			if ok {
				tblName := doltdb.TableName{Name: tname, Schema: db.schemaName}
				tbl, _, err := root.GetTable(ctx, tblName)
				if err != nil {
					return doltdb.TableName{}, nil, false, err
				}
				return tblName, tbl, true, nil
			} else {
				return doltdb.TableName{}, nil, false, nil
			}
		}
	}

	tableNames, err := db.getAllTableNames(ctx, root, true)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	}

	if tableListKey := root.TableListHash(); tableListKey != 0 {
		tableMap := make(map[string]string)
		for _, table := range tableNames {
			tableMap[strings.ToLower(table)] = table
		}
		dbState.SessionCache().CacheTableMap(tableListKey, tableMap)
	}

	tableName, ok = sql.GetTableNameInsensitive(tableName, tableNames)
	if !ok {
		return doltdb.TableName{}, nil, false, nil
	}

	// TODO: should we short-circuit the schema name for system tables?
	tname := doltdb.TableName{Name: tableName, Schema: db.schemaName}
	tbl, ok, err := root.GetTable(ctx, tname)
	if err != nil {
		return doltdb.TableName{}, nil, false, err
	} else if !ok {
		// Should be impossible
		return doltdb.TableName{}, nil, false, doltdb.ErrTableNotFound
	}

	return tname, tbl, true, nil
}

func (db Database) newDoltTable(tableName string, sch schema.Schema, tbl *doltdb.Table) (sql.Table, error) {
	readonlyTable, err := NewDoltTable(tableName, sch, tbl, db, db.editOpts)
	if err != nil {
		return nil, err
	}

	tname := doltdb.TableName{Name: tableName, Schema: db.schemaName}
	var table sql.Table
	if doltdb.IsReadOnlySystemTable(tname) {
		table = readonlyTable
	} else if doltdb.IsDoltCITable(tableName) && !doltdb.IsFullTextTable(tableName) {
		table = &AlterableDoltTable{WritableDoltTable{DoltTable: readonlyTable, db: db}}
	} else if doltdb.IsSystemTable(tname) && !doltdb.IsFullTextTable(tableName) {
		table = &WritableDoltTable{DoltTable: readonlyTable, db: db}
	} else {
		table = &AlterableDoltTable{WritableDoltTable{DoltTable: readonlyTable, db: db}}
	}

	return table, nil
}

// GetTableNames returns the names of all user tables. System tables in user space (e.g. dolt_docs, dolt_query_catalog)
// are filtered out. This method is used for queries that examine the schema of the database, e.g. show tables. Table
// name resolution in queries is handled by GetTableInsensitive. Use GetAllTableNames for an unfiltered list of all
// tables in user space.
func (db Database) GetTableNames(ctx *sql.Context) ([]string, error) {
	showSystemTablesVar, err := ctx.GetSessionVariable(ctx, dsess.ShowSystemTables)
	if err != nil {
		return nil, err
	}

	showSystemTables := showSystemTablesVar.(int8) == 1
	tblNames, err := db.GetAllTableNames(ctx, showSystemTables)
	if err != nil {
		return nil, err
	}

	if showSystemTables {
		return tblNames, nil
	}

	// TODO: Figure out way to remove filterDoltInternalTables
	return filterDoltInternalTables(ctx, tblNames, db.schemaName), nil
}

func (db Database) SchemaName() string {
	return db.schemaName
}

// GetAllTableNames returns all user-space tables, including system tables in user space
// (e.g. dolt_docs, dolt_query_catalog).
func (db Database) GetAllTableNames(ctx *sql.Context, showSystemTables bool) ([]string, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	return db.getAllTableNames(ctx, root, showSystemTables)
}

func (db Database) getAllTableNames(ctx *sql.Context, root doltdb.RootValue, showSystemTables bool) ([]string, error) {
	var err error
	var result []string
	// If we are in a schema-enabled session and the schema name is not set, we need to union all table names in all
	// schemas in the search_path
	if resolve.UseSearchPath && db.schemaName == "" {
		names, err := resolve.TablesOnSearchPath(ctx, root)
		if err != nil {
			return nil, err
		}
		// TODO: this method should probably return TableNames, but need to iron out the effective schema for system
		//  tables first
		result = doltdb.FlattenTableNames(names)
	} else {
		result, err = root.GetTableNames(ctx, db.schemaName)
		if err != nil {
			return nil, err
		}
	}

	if showSystemTables {
		systemTables, err := doltdb.GetGeneratedSystemTables(ctx, root)
		if err != nil {
			return nil, err
		}
		result = append(result, systemTables...)
	}

	return result, nil
}

func filterDoltInternalTables(ctx *sql.Context, tblNames []string, schemaName string) []string {
	result := []string{}

	for _, tbl := range tblNames {
		if doltdb.IsDoltCITable(tbl) {
			if doltdb.DoltCICanBypass(ctx) {
				result = append(result, tbl)
			}
		} else if !doltdb.IsSystemTable(doltdb.TableName{Name: tbl, Schema: schemaName}) {
			result = append(result, tbl)
		}
	}
	return result
}

// GetRoot returns the root value for this database session
func (db Database) GetRoot(ctx *sql.Context) (doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no root value found in session")
	}

	return dbState.WorkingRoot(), nil
}

// GetWorkingSet gets the current working set for the database.
// If there is no working set (most likely because the DB is in Detached Head mode, return an error.
// If a command needs to work while in Detached Head, that command should call sess.LookupDbState directly.
// TODO: This is a temporary measure to make sure that new commands that call GetWorkingSet don't unexpectedly receive
// a null pointer. In the future, we should replace all uses of dbState.WorkingSet, including this, with a new interface
// where users avoid handling the WorkingSet directly.
func (db Database) GetWorkingSet(ctx *sql.Context) (*doltdb.WorkingSet, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	dbState, ok, err := sess.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("no root value found in session")
	}
	if dbState.WorkingSet() == nil {
		return nil, doltdb.ErrOperationNotSupportedInDetachedHead
	}
	return dbState.WorkingSet(), nil
}

// SetRoot should typically be called on the Session, which is where this state lives. But it's available here as a
// convenience.
func (db Database) SetRoot(ctx *sql.Context, newRoot doltdb.RootValue) error {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.SetWorkingRoot(ctx, db.RevisionQualifiedName(), newRoot)
}

// GetHeadRoot returns root value for the current session head
func (db Database) GetHeadRoot(ctx *sql.Context) (doltdb.RootValue, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	head, err := sess.GetHeadCommit(ctx, db.RevisionQualifiedName())
	if err != nil {
		return nil, err
	}
	return head.GetRootValue(ctx)
}

// DropTable drops the table with the name given.
// The planner returns the correct case sensitive name in tableName
func (db Database) DropTable(ctx *sql.Context, tableName string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	if doltdb.IsNonAlterableSystemTable(doltdb.TableName{Name: tableName, Schema: db.schemaName}) {
		return ErrSystemTableAlter.New(tableName)
	}

	return db.dropTable(ctx, tableName)
}

// dropTable drops the table with the baseName given, without any business logic checks
func (db Database) dropTable(ctx *sql.Context, tableName string) error {
	_, tblExists, err := db.checkForPgCatalogTable(ctx, tableName)
	if err != nil {
		return err
	} else if tblExists {
		return sql.ErrDropTableNotSupported.New("pg_catalog")
	}

	ds := dsess.DSessFromSess(ctx.Session)
	if _, ok := ds.GetTemporaryTable(ctx, db.Name(), tableName); ok {
		ds.DropTemporaryTable(ctx, db.Name(), tableName)
		return nil
	}

	ws, err := db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}

	root := ws.WorkingRoot()
	tblName, tbl, tblExists, err := db.resolveUserTable(ctx, root, tableName)
	if err != nil {
		return err
	} else if !tblExists {
		return sql.ErrTableNotFound.New(tableName)
	}

	tableName = tblName.Name
	// for remainder of this operation, all db operations will use the name resolved here
	db.schemaName = tblName.Schema

	newRoot, err := root.RemoveTables(ctx, true, false, tblName)
	if err != nil {
		return err
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return err
	}

	if schema.HasAutoIncrement(sch) {
		ddb, _ := ds.GetDoltDB(ctx, db.RevisionQualifiedName())
		err = db.removeTableFromAutoIncrementTracker(ctx, tableName, ddb, ws.Ref())
		if err != nil {
			return err
		}
	}

	return db.SetRoot(ctx, newRoot)
}

// removeTableFromAutoIncrementTracker updates the global auto increment tracking as necessary to deal with the table
// given being dropped or truncated. The auto increment value for this table after this operation will either be reset
// back to 1 if this table only exists in the working set given, or to the highest value in all other working sets
// otherwise. This operation is expensive if the
func (db Database) removeTableFromAutoIncrementTracker(
	ctx *sql.Context,
	tableName string,
	ddb *doltdb.DoltDB,
	ws ref.WorkingSetRef,
) error {
	branches, err := ddb.GetBranches(ctx)
	if err != nil {
		return err
	}

	var wses []*doltdb.WorkingSet
	for _, b := range branches {
		wsRef, err := ref.WorkingSetRefForHead(b)
		if err != nil {
			return err
		}

		if wsRef == ws {
			// skip this branch, we've deleted it here
			continue
		}

		ws, err := ddb.ResolveWorkingSet(ctx, wsRef)
		if err == doltdb.ErrWorkingSetNotFound {
			// skip, continue working on other branches
			continue
		} else if err != nil {
			return err
		}

		wses = append(wses, ws)
	}

	ait, err := db.gs.AutoIncrementTracker(ctx)
	if err != nil {
		return err
	}

	err = ait.DropTable(ctx, tableName, wses...)
	if err != nil {
		return err
	}

	return nil
}

// CreateTable creates a table with the name and schema given.
func (db Database) CreateTable(ctx *sql.Context, tableName string, sch sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	if doltdb.IsSystemTable(doltdb.TableName{Name: tableName, Schema: db.schemaName}) && !doltdb.IsFullTextTable(tableName) {
		return ErrReservedTableName.New(tableName)
	}

	if doltdb.HasDoltCIPrefix(tableName) {
		if !doltdb.DoltCICanBypass(ctx) {
			return ErrReservedTableName.New(tableName)
		}
	}

	if strings.HasPrefix(tableName, diff.DBPrefix) {
		return ErrReservedDiffTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createSqlTable(ctx, tableName, db.schemaName, sch, collation, comment)
}

// CreateIndexedTable creates a table with the name and schema given.
func (db Database) CreateIndexedTable(ctx *sql.Context, tableName string, sch sql.PrimaryKeySchema, idxDef sql.IndexDef, collation sql.CollationID) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	if doltdb.IsSystemTable(doltdb.TableName{Name: tableName, Schema: db.schemaName}) {
		return ErrReservedTableName.New(tableName)
	}

	if doltdb.HasDoltCIPrefix(tableName) {
		if !doltdb.DoltCICanBypass(ctx) {
			return ErrReservedTableName.New(tableName)
		}
	}

	if strings.HasPrefix(tableName, diff.DBPrefix) {
		return ErrReservedDiffTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	return db.createIndexedSqlTable(ctx, tableName, db.schemaName, sch, idxDef, collation)
}

// CreateFulltextTableNames returns a set of names that will be used to create Full-Text pseudo-index tables.
func (db Database) CreateFulltextTableNames(ctx *sql.Context, parentTableName string, parentIndexName string) (fulltext.IndexTableNames, error) {
	allTableNames, err := db.GetAllTableNames(ctx, true)
	if err != nil {
		return fulltext.IndexTableNames{}, err
	}
	var tablePrefix string
OuterLoop:
	for i := uint64(0); true; i++ {
		tablePrefix = strings.ToLower(fmt.Sprintf("dolt_%s_%s_%d", parentTableName, parentIndexName, i))
		for _, tableName := range allTableNames {
			if strings.HasPrefix(strings.ToLower(tableName), tablePrefix) {
				continue OuterLoop
			}
		}
		break
	}
	return fulltext.IndexTableNames{
		Config:      fmt.Sprintf("dolt_%s_fts_config", parentTableName),
		Position:    fmt.Sprintf("%s_fts_position", tablePrefix),
		DocCount:    fmt.Sprintf("%s_fts_doc_count", tablePrefix),
		GlobalCount: fmt.Sprintf("%s_fts_global_count", tablePrefix),
		RowCount:    fmt.Sprintf("%s_fts_row_count", tablePrefix),
	}, nil
}

// createSqlTable is the private version of CreateTable. It doesn't enforce any table name checks.
func (db Database) createSqlTable(ctx *sql.Context, table string, schemaName string, sch sql.PrimaryKeySchema, collation sql.CollationID, comment string) error {
	ws, err := db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}
	root := ws.WorkingRoot()

	if resolve.UseSearchPath && db.schemaName == "" {
		schemaName, err = resolve.FirstExistingSchemaOnSearchPath(ctx, root)
		if err != nil {
			return err
		}
		db.schemaName = schemaName
	}

	tableName := doltdb.TableName{Name: table, Schema: schemaName}
	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(table)
	}

	headRoot, err := db.GetHeadRoot(ctx)
	if err != nil {
		return err
	}

	doltSch, err := sqlutil.ToDoltSchema(ctx, root, tableName, sch, headRoot, collation)
	if err != nil {
		return err
	}
	doltSch.SetComment(comment)

	// Prevent any tables that use Spatial Types as Primary Key from being created
	if schema.IsUsingSpatialColAsKey(doltSch) {
		return schema.ErrUsingSpatialKey.New(tableName.Name)
	}

	// Prevent any tables that use BINARY, CHAR, VARBINARY, VARCHAR prefixes

	if schema.HasAutoIncrement(doltSch) {
		ait, err := db.gs.AutoIncrementTracker(ctx)
		if err != nil {
			return err
		}
		ait.AddNewTable(tableName.Name)
	}

	return db.createDoltTable(ctx, tableName.Name, tableName.Schema, root, doltSch)
}

// createIndexedSqlTable is the private version of createSqlTable. It doesn't enforce any table name checks.
func (db Database) createIndexedSqlTable(ctx *sql.Context, table string, schemaName string, sch sql.PrimaryKeySchema, idxDef sql.IndexDef, collation sql.CollationID) error {
	ws, err := db.GetWorkingSet(ctx)
	if err != nil {
		return err
	}
	root := ws.WorkingRoot()

	if resolve.UseSearchPath && db.schemaName == "" {
		schemaName, err = resolve.FirstExistingSchemaOnSearchPath(ctx, root)
		if err != nil {
			return err
		}
		db.schemaName = schemaName
	}

	tableName := doltdb.TableName{Name: table, Schema: schemaName}
	if exists, err := root.HasTable(ctx, tableName); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName.Name)
	}

	headRoot, err := db.GetHeadRoot(ctx)
	if err != nil {
		return err
	}

	doltSch, err := sqlutil.ToDoltSchema(ctx, root, tableName, sch, headRoot, collation)
	if err != nil {
		return err
	}

	// Prevent any tables that use Spatial Types as Primary Key from being created
	if schema.IsUsingSpatialColAsKey(doltSch) {
		return schema.ErrUsingSpatialKey.New(tableName.Name)
	}

	// Prevent any tables that use BINARY, CHAR, VARBINARY, VARCHAR prefixes in Primary Key
	for _, idxCol := range idxDef.Columns {
		col := sch.Schema[sch.Schema.IndexOfColName(idxCol.Name)]
		if col.PrimaryKey && types.IsText(col.Type) && idxCol.Length > 0 {
			return sql.ErrUnsupportedIndexPrefix.New(col.Name)
		}
	}

	if schema.HasAutoIncrement(doltSch) {
		ait, err := db.gs.AutoIncrementTracker(ctx)
		if err != nil {
			return err
		}
		ait.AddNewTable(tableName.Name)
	}

	return db.createDoltTable(ctx, tableName.Name, tableName.Schema, root, doltSch)
}

// createDoltTable creates a table on the database using the given dolt schema while not enforcing table baseName checks.
func (db Database) createDoltTable(ctx *sql.Context, tableName string, schemaName string, root doltdb.RootValue, doltSch schema.Schema) error {
	if exists, err := root.HasTable(ctx, doltdb.TableName{Name: tableName, Schema: schemaName}); err != nil {
		return err
	} else if exists {
		return sql.ErrTableAlreadyExists.New(tableName)
	}

	var conflictingTbls []string
	_ = doltSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool, err error) {
		_, oldTableName, exists, err := doltdb.GetTableByColTag(ctx, root, tag)
		if err != nil {
			return true, err
		}
		if exists && oldTableName.Name != tableName {
			errStr := schema.ErrTagPrevUsed(tag, col.Name, tableName, oldTableName.Name).Error()
			conflictingTbls = append(conflictingTbls, errStr)
		}
		return false, nil
	})

	if len(conflictingTbls) > 0 {
		return fmt.Errorf(strings.Join(conflictingTbls, "\n"))
	}

	newRoot, err := doltdb.CreateEmptyTable(ctx, root, doltdb.TableName{Name: tableName, Schema: schemaName}, doltSch)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// CreateTemporaryTable creates a table that only exists the length of a session.
func (db Database) CreateTemporaryTable(ctx *sql.Context, tableName string, pkSch sql.PrimaryKeySchema, collation sql.CollationID) error {
	if doltdb.IsSystemTable(doltdb.TableName{Name: tableName, Schema: db.schemaName}) {
		return ErrReservedTableName.New(tableName)
	}

	if doltdb.HasDoltCIPrefix(tableName) {
		if !doltdb.DoltCICanBypass(ctx) {
			return ErrReservedTableName.New(tableName)
		}
	}

	if strings.HasPrefix(tableName, diff.DBPrefix) {
		return ErrReservedDiffTableName.New(tableName)
	}

	if !doltdb.IsValidTableName(tableName) {
		return ErrInvalidTableName.New(tableName)
	}

	tmp, err := NewTempTable(ctx, db.ddb, pkSch, tableName, db.Name(), db.editOpts, collation)
	if err != nil {
		return err
	}

	ds := dsess.DSessFromSess(ctx.Session)
	ds.AddTemporaryTable(ctx, db.Name(), tmp)
	return nil
}

// CreateSchema implements sql.SchemaDatabase
func (db Database) CreateSchema(ctx *sql.Context, schemaName string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}

	_, exists, err := doltdb.ResolveDatabaseSchema(ctx, root, schemaName)
	if err != nil {
		return err
	}

	if exists {
		return sql.ErrDatabaseSchemaExists.New(schemaName)
	}

	root, err = root.CreateDatabaseSchema(ctx, schema.DatabaseSchema{
		Name: schemaName,
	})
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, root)
}

// GetSchema implements sql.SchemaDatabase
func (db Database) GetSchema(ctx *sql.Context, schemaName string) (sql.DatabaseSchema, bool, error) {
	// For doltgres, the information_schema database should be a schema.
	if schemaName == sql.InformationSchemaDatabaseName {
		return newInformationSchemaDatabase(db.Name()), true, nil
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, false, err
	}

	schemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, false, err
	}

	for _, schema := range schemas {
		if strings.EqualFold(schema.Name, schemaName) {
			db.schemaName = schema.Name
			handledSchema, err := HandleSchema(ctx, schemaName, db)
			if err != nil {
				return nil, false, err
			}
			return handledSchema, true, nil
		}
	}

	// For a temporary backwards compatibility solution, always pretend the public schema exists.
	// We create it explicitly for new databases.
	if strings.EqualFold(schemaName, "public") {
		db.schemaName = "public"
		return db, true, nil
	}

	return nil, false, nil
}

// HandleSchema is used by Doltgres to intercept a database for the purposes of system tables. In Dolt, this just
// returns the given database.
var HandleSchema = func(ctx *sql.Context, schemaName string, db Database) (sql.DatabaseSchema, error) {
	return db, nil
}

// AllSchemas implements sql.SchemaDatabase
func (db Database) AllSchemas(ctx *sql.Context) ([]sql.DatabaseSchema, error) {
	if !resolve.UseSearchPath {
		return []sql.DatabaseSchema{db}, nil
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, err
	}

	schemas, err := root.GetDatabaseSchemas(ctx)
	if err != nil {
		return nil, err
	}

	dbSchemas := make([]sql.DatabaseSchema, len(schemas)+1)
	for i, schema := range schemas {
		sdb := db
		sdb.schemaName = schema.Name
		handledDb, err := HandleSchema(ctx, schema.Name, sdb)
		if err != nil {
			return nil, err
		}
		dbSchemas[i] = handledDb
	}

	// For doltgres, the information_schema database should be a schema.
	dbSchemas[len(schemas)] = newInformationSchemaDatabase(db.Name())

	return dbSchemas, nil
}

// RenameTable implements sql.TableRenamer
func (db Database) RenameTable(ctx *sql.Context, oldName, newName string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	root, err := db.GetRoot(ctx)

	if err != nil {
		return err
	}

	if doltdb.IsNonAlterableSystemTable(doltdb.TableName{Name: oldName, Schema: db.schemaName}) {
		return ErrSystemTableAlter.New(oldName)
	}

	if doltdb.IsSystemTable(doltdb.TableName{Name: newName, Schema: db.schemaName}) {
		return ErrReservedTableName.New(newName)
	}

	if doltdb.HasDoltCIPrefix(newName) {
		if !doltdb.DoltCICanBypass(ctx) {
			return ErrReservedTableName.New(newName)
		}
	}

	if strings.HasPrefix(newName, diff.DBPrefix) {
		return ErrReservedDiffTableName.New(newName)
	}

	if !doltdb.IsValidTableName(newName) {
		return ErrInvalidTableName.New(newName)
	}

	oldNameWithSchema, _, exists, err := resolve.Table(ctx, root, oldName)
	if err != nil {
		return err
	}

	if !exists {
		return sql.ErrTableNotFound.New(oldName)
	}

	// TODO: we have no way to rename a table to a different schema, need to change the GMS interface for that
	newNameWithSchema := doltdb.TableName{Schema: oldNameWithSchema.Schema, Name: newName}
	_, exists, err = root.ResolveTableName(ctx, newNameWithSchema)
	if exists {
		return sql.ErrTableAlreadyExists.New(newName)
	}

	newRoot, err := renameTable(ctx, root, oldNameWithSchema, newNameWithSchema)
	if err != nil {
		return err
	}

	return db.SetRoot(ctx, newRoot)
}

// GetViewDefinition implements sql.ViewDatabase
func (db Database) GetViewDefinition(ctx *sql.Context, viewName string) (sql.ViewDefinition, bool, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}
	// attempts to define the db schema name if applicable
	if resolve.UseSearchPath && db.schemaName == "" {
		if schemaName, _ := resolve.FirstExistingSchemaOnSearchPath(ctx, root); schemaName != "" {
			db.schemaName = schemaName
		}
	}

	lwrViewName := strings.ToLower(viewName)
	switch {
	case strings.HasPrefix(lwrViewName, doltdb.DoltBlameViewPrefix):
		tableName := lwrViewName[len(doltdb.DoltBlameViewPrefix):]

		blameViewTextDef, err := dtables.NewBlameView(ctx, doltdb.TableName{Name: tableName, Schema: db.schemaName}, root)
		if err != nil {
			return sql.ViewDefinition{}, false, err
		}
		return sql.ViewDefinition{Name: viewName, TextDefinition: blameViewTextDef, CreateViewStatement: fmt.Sprintf("CREATE VIEW `%s` AS %s", viewName, blameViewTextDef)}, true, nil
	}

	schTblHash, ok, err := root.GetTableHash(ctx, doltdb.TableName{Name: doltdb.SchemasTableName, Schema: db.schemaName})
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}
	if !ok {
		return sql.ViewDefinition{}, false, nil
	}

	key := doltdb.DataCacheKey{Hash: schTblHash}

	ds := dsess.DSessFromSess(ctx.Session)
	dbState, _, err := ds.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}

	if dbState.SessionCache().ViewsCached(key) {
		view, ok := dbState.SessionCache().GetCachedViewDefinition(key, dsess.TableCacheKey{Name: viewName, Schema: db.schemaName})
		if ok {
			return view, ok, nil
		}
	}

	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}

	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return sql.ViewDefinition{}, false, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}

	if wrapper.backingTable == nil {
		dbState.SessionCache().CacheViews(key, nil, db.schemaName)
		return sql.ViewDefinition{}, false, nil
	}

	views, viewDef, found, err := getViewDefinitionFromSchemaFragmentsOfView(ctx, wrapper.backingTable, viewName)
	if err != nil {
		return sql.ViewDefinition{}, false, err
	}

	// TODO: only cache views from a single schema here
	dbState.SessionCache().CacheViews(key, views, db.schemaName)

	return viewDef, found, nil
}

func getViewDefinitionFromSchemaFragmentsOfView(ctx *sql.Context, tbl *WritableDoltTable, viewName string) ([]sql.ViewDefinition, sql.ViewDefinition, bool, error) {
	fragments, err := getSchemaFragmentsOfType(ctx, tbl, viewFragment)
	if err != nil {
		return nil, sql.ViewDefinition{}, false, err
	}

	var found = false
	var viewDef sql.ViewDefinition
	var views = make([]sql.ViewDefinition, len(fragments))
	for i, fragment := range fragments {
		if strings.HasPrefix(strings.ToLower(fragments[i].fragment), "select") {
			// older versions
			views[i] = sql.ViewDefinition{
				Name:                fragments[i].name,
				TextDefinition:      fragments[i].fragment,
				CreateViewStatement: fmt.Sprintf("CREATE VIEW %s AS %s", fragments[i].name, fragments[i].fragment),
			}
		} else {
			views[i] = sql.ViewDefinition{
				Name: fragments[i].name,
				// TODO: need to define TextDefinition
				CreateViewStatement: fragments[i].fragment,
				SqlMode:             fragment.sqlMode,
			}
		}

		if strings.EqualFold(fragment.name, viewName) {
			found = true
			viewDef = views[i]
		}
	}

	return views, viewDef, found, nil
}

// AllViews implements sql.ViewDatabase
func (db Database) AllViews(ctx *sql.Context) ([]sql.ViewDefinition, error) {
	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return nil, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}
	if wrapper.backingTable == nil {
		return nil, nil
	}

	views, _, _, err := getViewDefinitionFromSchemaFragmentsOfView(ctx, wrapper.backingTable, "")
	if err != nil {
		return nil, err
	}

	return views, nil
}

// CreateView implements sql.ViewCreator. Persists the view in the dolt database, so
// it can exist in a sql session later. Returns sql.ErrExistingView if a view
// with that name already exists.
func (db Database) CreateView(ctx *sql.Context, name string, selectStatement, createViewStmt string) error {
	err := sql.ErrExistingView.New(db.Name(), name)
	return db.addFragToSchemasTable(ctx, "view", name, createViewStmt, time.Unix(0, 0).UTC(), err)
}

// DropView implements sql.ViewDropper. Removes a view from persistence in the
// dolt database. Returns sql.ErrNonExistingView if the view did not
// exist.
func (db Database) DropView(ctx *sql.Context, name string) error {
	err := sql.ErrViewDoesNotExist.New(db.baseName, name)
	return db.dropFragFromSchemasTable(ctx, "view", name, err)
}

// GetTriggers implements sql.TriggerDatabase.
func (db Database) GetTriggers(ctx *sql.Context) ([]sql.TriggerDefinition, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return nil, nil
	}

	key, err := doltdb.NewDataCacheKey(root)
	if err != nil {
		return nil, err
	}

	ds := dsess.DSessFromSess(ctx.Session)
	dbState, _, err := ds.LookupDbState(ctx, db.RevisionQualifiedName())
	if err != nil {
		return nil, nil
	}

	var triggers []sql.TriggerDefinition
	var ok bool
	if triggers, ok = dbState.SessionCache().GetCachedTriggers(key, db.schemaName); ok {
		return triggers, nil
	}

	defer func() {
		dbState.SessionCache().CacheTriggers(key, triggers, db.schemaName)
	}()

	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, err
	}

	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return nil, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}

	if wrapper.backingTable == nil {
		return nil, nil
	}

	frags, err := getSchemaFragmentsOfType(ctx, wrapper.backingTable, triggerFragment)
	if err != nil {
		return nil, err
	}

	for _, frag := range frags {
		triggers = append(triggers, sql.TriggerDefinition{
			Name:            frag.name,
			CreateStatement: frag.fragment,
			CreatedAt:       frag.created,
			SqlMode:         frag.sqlMode,
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

// GetEvent implements sql.EventDatabase.
func (db Database) GetEvent(ctx *sql.Context, name string) (sql.EventDefinition, bool, error) {
	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return sql.EventDefinition{}, false, err
	}

	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return sql.EventDefinition{}, false, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}
	if wrapper.backingTable == nil {
		return sql.EventDefinition{}, false, nil
	}

	frags, err := getSchemaFragmentsOfType(ctx, wrapper.backingTable, eventFragment)
	if err != nil {
		return sql.EventDefinition{}, false, err
	}

	for _, frag := range frags {
		if strings.EqualFold(frag.name, name) {
			event, err := db.createEventDefinitionFromFragment(ctx, frag)
			if err != nil {
				return sql.EventDefinition{}, false, err
			}
			return *event, true, nil
		}
	}
	return sql.EventDefinition{}, false, nil
}

// GetEvents implements sql.EventDatabase.
func (db Database) GetEvents(ctx *sql.Context) (events []sql.EventDefinition, token interface{}, err error) {
	tbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return nil, nil, err
	}
	wrapper, ok := tbl.(*SchemaTable)
	if !ok {
		return nil, nil, fmt.Errorf("expected a SchemaTable, but found %T", tbl)
	}

	if wrapper.backingTable == nil {
		// If the dolt_schemas table doesn't exist, it's not an error, just no events
		return nil, nil, nil
	}

	frags, err := getSchemaFragmentsOfType(ctx, wrapper.backingTable, eventFragment)
	if err != nil {
		return nil, nil, err
	}

	for _, frag := range frags {
		event, err := db.createEventDefinitionFromFragment(ctx, frag)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, *event)
	}

	// Grab a hash of the dolt_schemas table to use as the identifying token
	// to track if events need to be reloaded.
	tableHash, err := db.doltSchemaTableHash(ctx)
	if err != nil {
		return nil, nil, err
	}

	return events, tableHash, nil
}

// NeedsToReloadEvents implements sql.EventDatabase.
func (db Database) NeedsToReloadEvents(ctx *sql.Context, token interface{}) (bool, error) {
	// A nil token means no events in this db. If the dolt_schemas table doesn't exist, it will have a zero hash below
	// as well, meaning we don't reload events in that case.
	if token == nil {
		token = hash.Hash{}
	}

	hash, ok := token.(hash.Hash)
	if !ok {
		return false, fmt.Errorf("expected token to be hash.Hash, but received %T", token)
	}

	tableHash, err := db.doltSchemaTableHash(ctx)
	if err != nil {
		return false, err
	}

	// If the current hash doesn't match what we last loaded, then we
	// need to reload event definitions
	return !tableHash.Equal(hash), nil
}

// doltSchemaTableHash returns the hash of the dolt_schemas table, or any error encountered along the way.
func (db Database) doltSchemaTableHash(ctx *sql.Context) (hash.Hash, error) {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return hash.Hash{}, err
	}

	tableHash, _, err := root.GetTableHash(ctx, doltdb.TableName{Name: doltdb.SchemasTableName})
	return tableHash, err
}

// createEventDefinitionFromFragment creates an EventDefinition instance from the schema fragment |frag|.
func (db Database) createEventDefinitionFromFragment(ctx *sql.Context, frag schemaFragment) (*sql.EventDefinition, error) {
	b := planbuilder.New(ctx, db.getCatalog(ctx), db.getEventScheduler(ctx), nil)
	b.SetParserOptions(sql.NewSqlModeFromString(frag.sqlMode).ParserOptions())
	parsed, _, _, _, err := b.Parse(updateEventStatusTemporarilyForNonDefaultBranch(db.revision, frag.fragment), nil, false)
	if err != nil {
		return nil, err
	}

	eventPlan, ok := parsed.(*plan.CreateEvent)
	if !ok {
		return nil, fmt.Errorf("unexpected type %T for create event statement", eventPlan)
	}

	// NOTE: Time fields for events are assumed to be specified in the session's timezone, which defaults to the
	//       system's timezone. When we store them, we store them at UTC, and when we send them back to a caller
	//       we convert them back to the caller's session timezone.
	//       Here we are loading the events from disk, so they are already in UTC and don't need any other
	//       timezone applied, so we specify "+00:00".
	event, err := eventPlan.GetEventDefinition(ctx, frag.created, frag.created, frag.created, "+00:00")
	if err != nil {
		return nil, err
	}
	event.SqlMode = frag.sqlMode

	return &event, nil
}

// getCatalog creates and returns the analyzer.Catalog instance for this database.
func (db Database) getCatalog(ctx *sql.Context) *analyzer.Catalog {
	doltSession := dsess.DSessFromSess(ctx.Session)
	return sqle.NewDefault(doltSession.Provider()).Analyzer.Catalog
}

// getEventScheduler retrieves the EventScheduler for this database
func (db Database) getEventScheduler(ctx *sql.Context) sql.EventScheduler {
	doltSession := dsess.DSessFromSess(ctx.Session)
	return sqle.NewDefault(doltSession.Provider()).EventScheduler
}

// SaveEvent implements sql.EventDatabase.
func (db Database) SaveEvent(ctx *sql.Context, event sql.EventDefinition) (bool, error) {
	// If the database is NOT on the DefaultInitBranch, then we disable the event, since
	// events only run from a single branch. We check this by looking at the database's
	// revision and ensuring it either matches DefaultInitBranch or is empty.
	// TODO: need better way to determine the default branch; currently it checks only 'main'
	if (db.revision != env.DefaultInitBranch && db.revision != "") && event.Status == sql.EventStatus_Enable.String() {
		ctx.GetLogger().Debugf("disabling event %s (db.revision == %s)", event.Name, db.revision)
		event.Status = sql.EventStatus_Disable.String()
		ctx.Session.Warn(&sql.Warning{
			Level:   "Warning",
			Code:    1105,
			Message: fmt.Sprintf("Event status cannot be enabled for revision database."),
		})
	}

	// TODO: store LastAltered, LastExecuted and TimezoneOffset in appropriate place
	return event.Status == sql.EventStatus_Enable.String(), db.addFragToSchemasTable(ctx,
		eventFragment,
		event.Name,
		event.CreateEventStatement(),
		event.CreatedAt,
		sql.ErrEventAlreadyExists.New(event.Name),
	)
}

// DropEvent implements sql.EventDatabase.
func (db Database) DropEvent(ctx *sql.Context, name string) error {
	return db.dropFragFromSchemasTable(ctx, eventFragment, name, sql.ErrEventDoesNotExist.New(name))
}

// UpdateEvent implements sql.EventDatabase.
func (db Database) UpdateEvent(ctx *sql.Context, originalName string, event sql.EventDefinition) (bool, error) {
	// TODO: any EVENT STATUS change should also update the branch-specific event scheduling
	err := db.DropEvent(ctx, originalName)
	if err != nil {
		return false, err
	}
	return db.SaveEvent(ctx, event)
}

// UpdateLastExecuted implements sql.EventDatabase
func (db Database) UpdateLastExecuted(ctx *sql.Context, eventName string, lastExecuted time.Time) error {
	// TODO: update LastExecuted in appropriate place
	return nil
}

// updateEventStatusTemporarilyForNonDefaultBranch updates the event status from ENABLE to DISABLE if it's not default branch.
// The event status metadata is not updated in storage, but only for display purposes we return event status as 'DISABLE'.
// This function is used temporarily to implement logic of only allowing enabled events to be executed on default branch.
func updateEventStatusTemporarilyForNonDefaultBranch(revision, createStmt string) string {
	// TODO: need better way to determine the default branch; currently it checks only 'main'

	if revision == "" || revision == env.DefaultInitBranch {
		return createStmt
	}
	return strings.Replace(createStmt, "ENABLE", "DISABLE", 1)
}

// GetStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) GetStoredProcedure(ctx *sql.Context, name string) (sql.StoredProcedureDetails, bool, error) {
	procedures, err := DoltProceduresGetAll(ctx, db, strings.ToLower(name))
	if err != nil {
		return sql.StoredProcedureDetails{}, false, nil
	}
	if len(procedures) == 1 {
		return procedures[0], true, nil
	}
	return sql.StoredProcedureDetails{}, false, nil
}

// GetStoredProcedures implements sql.StoredProcedureDatabase.
func (db Database) GetStoredProcedures(ctx *sql.Context) ([]sql.StoredProcedureDetails, error) {
	return DoltProceduresGetAll(ctx, db, "")
}

// SaveStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) SaveStoredProcedure(ctx *sql.Context, spd sql.StoredProcedureDetails) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	return DoltProceduresAddProcedure(ctx, db, spd)
}

// DropStoredProcedure implements sql.StoredProcedureDatabase.
func (db Database) DropStoredProcedure(ctx *sql.Context, name string) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	return DoltProceduresDropProcedure(ctx, db, name)
}

func (db Database) addFragToSchemasTable(ctx *sql.Context, fragType, name, definition string, created time.Time, existingErr error) (err error) {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	tbl, err := getOrCreateDoltSchemasTable(ctx, db)
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

	// Insert the new row into the db
	inserter := tbl.Inserter(ctx)
	defer func() {
		cErr := inserter.Close(ctx)
		if err == nil {
			err = cErr
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

	sqlMode := sql.LoadSqlMode(ctx)

	return inserter.Insert(ctx, sql.Row{fragType, name, definition, extraJSON, sqlMode.String()})
}

func (db Database) dropFragFromSchemasTable(ctx *sql.Context, fragType, name string, missingErr error) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}

	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}
	if resolve.UseSearchPath && db.schemaName == "" {
		schemaName, err := resolve.FirstExistingSchemaOnSearchPath(ctx, root)
		if err != nil {
			return err
		}
		db.schemaName = schemaName
	}

	stbl, _, err := db.GetTableInsensitive(ctx, doltdb.SchemasTableName)
	if err != nil {
		return err
	}

	swrapper, ok := stbl.(*SchemaTable)
	if !ok {
		return fmt.Errorf("expected a SchemaTable, but found %T", stbl)
	}
	if swrapper.backingTable == nil {
		return missingErr
	}

	tbl := swrapper.backingTable
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

	err = deleter.Close(ctx)
	if err != nil {
		return err
	}

	// If the dolt schemas table is now empty, drop it entirely. This is necessary to prevent the creation and
	// immediate dropping of views or triggers, when none previously existed, from changing the database state.
	return db.dropTableIfEmpty(ctx, doltdb.SchemasTableName)
}

// dropTableIfEmpty drops the table named if it exists and has at least one row.
func (db Database) dropTableIfEmpty(ctx *sql.Context, tableName string) error {
	stbl, found, err := db.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return err
	}
	if !found {
		return nil
	}

	if wrapped, ok := stbl.(WritableDoltTableWrapper); ok {
		stbl = wrapped.UnWrap()
		if stbl == nil {
			return nil
		}
	}

	table, err := stbl.(*WritableDoltTable).DoltTable.DoltTable(ctx)
	if err != nil {
		return err
	}

	rows, err := table.GetRowData(ctx)
	if err != nil {
		return err
	}

	numRows, err := rows.Count()
	if err != nil {
		return err
	}

	if numRows == 0 {
		return db.dropTable(ctx, tableName)
	}

	return nil
}

// GetAllTemporaryTables returns all temporary tables
func (db Database) GetAllTemporaryTables(ctx *sql.Context) ([]sql.Table, error) {
	sess := dsess.DSessFromSess(ctx.Session)
	return sess.GetAllTemporaryTables(ctx, db.Name())
}

// GetCollation implements the interface sql.CollatedDatabase.
func (db Database) GetCollation(ctx *sql.Context) sql.CollationID {
	root, err := db.GetRoot(ctx)
	if err != nil {
		return sql.Collation_Default
	}
	collation, err := root.GetCollation(ctx)
	if err != nil {
		return sql.Collation_Default
	}
	return sql.CollationID(collation)
}

// SetCollation implements the interface sql.CollatedDatabase.
func (db Database) SetCollation(ctx *sql.Context, collation sql.CollationID) error {
	if err := dsess.CheckAccessForDb(ctx, db, branch_control.Permissions_Write); err != nil {
		return err
	}
	if collation == sql.Collation_Unspecified {
		collation = sql.Collation_Default
	}
	root, err := db.GetRoot(ctx)
	if err != nil {
		return err
	}
	newRoot, err := root.SetCollation(ctx, schema.Collation(collation))
	if err != nil {
		return err
	}
	return db.SetRoot(ctx, newRoot)
}

// ConvertRowToRebasePlanStep converts a sql.Row to RebasePlanStep. This is used by Doltgres to convert
// from a sql.Row considering the correct types.
var ConvertRowToRebasePlanStep = convertRowToRebasePlanStep

func convertRowToRebasePlanStep(row sql.Row) (rebase.RebasePlanStep, error) {
	i, ok := row[1].(uint16)
	if !ok {
		return rebase.RebasePlanStep{}, fmt.Errorf("invalid enum value in rebase plan: %v (%T)", row[1], row[1])
	}

	rebaseAction, ok := dprocedures.RebaseActionEnumType.At(int(i))
	if !ok {
		return rebase.RebasePlanStep{}, fmt.Errorf("invalid enum value in rebase plan: %v (%T)", row[1], row[1])
	}

	return rebase.RebasePlanStep{
		RebaseOrder: row[0].(decimal.Decimal),
		Action:      rebaseAction,
		CommitHash:  row[2].(string),
		CommitMsg:   row[3].(string),
	}, nil
}

// LoadRebasePlan implements the rebase.RebasePlanDatabase interface
func (db Database) LoadRebasePlan(ctx *sql.Context) (*rebase.RebasePlan, error) {
	if resolve.UseSearchPath {
		db.schemaName = doltdb.DoltNamespace
	}
	table, ok, err := db.GetTableInsensitive(ctx, doltdb.GetRebaseTableName())
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unable to find dolt_rebase table")
	}
	resolvedTable := plan.NewResolvedTable(table, db, nil)
	rebaseSchema := dprocedures.GetDoltRebaseSystemTableSchema()
	sort := plan.NewSort([]sql.SortField{{
		Column: expression.NewGetField(0, rebaseSchema[0].Type, "rebase_order", false),
		Order:  sql.Ascending,
	}}, resolvedTable)
	iter, err := rowexec.DefaultBuilder.Build(ctx, sort, nil)
	if err != nil {
		return nil, err
	}

	var rebasePlan rebase.RebasePlan
	for {
		row, err := iter.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		newRebasePlan, err := ConvertRowToRebasePlanStep(row)
		if err != nil {
			return nil, err
		}

		rebasePlan.Steps = append(rebasePlan.Steps, newRebasePlan)
	}

	return &rebasePlan, nil
}

// ConvertRebasePlanStepToRow converts a RebasePlanStep to sql.Row. This is used by Doltgres to convert
// to a sql.Row with the correct types.
var ConvertRebasePlanStepToRow = convertRebasePlanStepToRow

func convertRebasePlanStepToRow(planMember rebase.RebasePlanStep) (sql.Row, error) {
	actionEnumValue := dprocedures.RebaseActionEnumType.IndexOf(strings.ToLower(planMember.Action))
	if actionEnumValue == -1 {
		return nil, fmt.Errorf("invalid rebase action: %s", planMember.Action)
	}
	return sql.Row{
		planMember.RebaseOrder,
		uint16(actionEnumValue),
		planMember.CommitHash,
		planMember.CommitMsg,
	}, nil
}

// SaveRebasePlan implements the rebase.RebasePlanDatabase interface
func (db Database) SaveRebasePlan(ctx *sql.Context, plan *rebase.RebasePlan) error {
	if resolve.UseSearchPath {
		db.schemaName = doltdb.DoltNamespace
	}

	pkSchema := sql.NewPrimaryKeySchema(dprocedures.GetDoltRebaseSystemTableSchema())
	// we use createSqlTable, instead of CreateTable to avoid the "dolt_" reserved prefix table name check
	err := db.createSqlTable(ctx, doltdb.GetRebaseTableName(), db.schemaName, pkSchema, sql.Collation_Default, "")
	if err != nil {
		return err
	}

	table, ok, err := db.GetTableInsensitive(ctx, doltdb.GetRebaseTableName())
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("unable to find %s table", doltdb.GetRebaseTableName())
	}

	writeableDoltTable, ok := table.(*WritableDoltTable)
	if !ok {
		return fmt.Errorf("expected a *sqle.WritableDoltTable, but got %T", table)
	}

	inserter := writeableDoltTable.Inserter(ctx)
	for _, planMember := range plan.Steps {
		row, err := ConvertRebasePlanStepToRow(planMember)
		if err != nil {
			return err
		}

		err = inserter.Insert(ctx, row)
		if err != nil {
			return err
		}
	}

	return inserter.Close(ctx)
}

// noopRepoStateWriter is a minimal implementation of RepoStateWriter that does nothing
type noopRepoStateWriter struct{}

func (n noopRepoStateWriter) UpdateStagedRoot(ctx context.Context, newRoot doltdb.RootValue) error {
	return nil
}

func (n noopRepoStateWriter) UpdateWorkingRoot(ctx context.Context, newRoot doltdb.RootValue) error {
	return nil
}

func (n noopRepoStateWriter) SetCWBHeadRef(ctx context.Context, marshalableRef ref.MarshalableRef) error {
	return nil
}

func (n noopRepoStateWriter) AddRemote(r env.Remote) error {
	return nil
}

func (n noopRepoStateWriter) AddBackup(r env.Remote) error {
	return nil
}

func (n noopRepoStateWriter) RemoveRemote(ctx context.Context, name string) error {
	return nil
}

func (n noopRepoStateWriter) RemoveBackup(ctx context.Context, name string) error {
	return nil
}

func (n noopRepoStateWriter) TempTableFilesDir() (string, error) {
	return "", nil
}

func (n noopRepoStateWriter) UpdateBranch(name string, new env.BranchConfig) error {
	return nil
}
