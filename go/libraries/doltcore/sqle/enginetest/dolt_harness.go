// Copyright 2020 Dolthub, Inc.
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

package enginetest

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltHarness struct {
	t              *testing.T
	multiRepoEnv   *env.MultiRepoEnv
	createdEnvs    map[string]*env.DoltEnv
	session        *dsess.DoltSession
	branchControl  *branch_control.Controller
	databases      []sqle.Database
	hashes         []string
	parallelism    int
	skippedQueries []string
	setupData      []setup.SetupScript
	resetData      []setup.SetupScript
	initDbs        map[string]struct{}
	autoInc        bool
	engine         *gms.Engine
}

var _ enginetest.Harness = (*DoltHarness)(nil)
var _ enginetest.SkippingHarness = (*DoltHarness)(nil)
var _ enginetest.ClientHarness = (*DoltHarness)(nil)
var _ enginetest.IndexHarness = (*DoltHarness)(nil)
var _ enginetest.VersionedDBHarness = (*DoltHarness)(nil)
var _ enginetest.ForeignKeyHarness = (*DoltHarness)(nil)
var _ enginetest.KeylessTableHarness = (*DoltHarness)(nil)
var _ enginetest.ReadOnlyDatabaseHarness = (*DoltHarness)(nil)
var _ enginetest.ValidatingHarness = (*DoltHarness)(nil)

func newDoltHarness(t *testing.T) *DoltHarness {
	dEnv := dtestutils.CreateTestEnv()
	mrEnv, err := env.MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	require.NoError(t, err)
	b := env.GetDefaultInitBranch(dEnv.Config)
	pro, err := sqle.NewDoltDatabaseProvider(b, mrEnv.FileSystem())
	require.NoError(t, err)
	pro = pro.WithDbFactoryUrl(doltdb.InMemDoltDB)

	localConfig := dEnv.Config.WriteableConfig()
	branchControl := branch_control.CreateDefaultController()
	session, err := dsess.NewDoltSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), pro,
		localConfig, branchControl)
	require.NoError(t, err)
	dh := &DoltHarness{
		t:              t,
		session:        session,
		skippedQueries: defaultSkippedQueries,
		multiRepoEnv:   mrEnv,
		createdEnvs:    make(map[string]*env.DoltEnv),
		branchControl:  branchControl,
	}

	return dh
}

var defaultSkippedQueries = []string{
	"show variables",             // we set extra variables
	"show create table fk_tbl",   // we create an extra key for the FK that vanilla gms does not
	"show indexes from",          // we create / expose extra indexes (for foreign keys)
	"show global variables like", // we set extra variables
}

// Setup sets the setup scripts for this DoltHarness's engine
func (d *DoltHarness) Setup(setupData ...[]setup.SetupScript) {
	d.engine = nil
	d.setupData = nil
	for i := range setupData {
		d.setupData = append(d.setupData, setupData[i]...)
	}
}

// resetScripts returns a set of queries that will reset the given database
// names. If [autoInc], the queries for resetting autoincrement tables are
// included.
func (d *DoltHarness) resetScripts() []setup.SetupScript {
	ctx := enginetest.NewContext(d)
	_, res := enginetest.MustQuery(ctx, d.engine, "select schema_name from information_schema.schemata where schema_name not in ('information_schema');")
	var dbs []string
	for i := range res {
		dbs = append(dbs, res[i][0].(string))
	}

	var resetCmds []setup.SetupScript
	for i := range dbs {
		db := dbs[i]
		resetCmds = append(resetCmds, setup.SetupScript{fmt.Sprintf("use %s", db)})

		// Any auto increment tables must be dropped and recreated to get a fresh state for the global auto increment
		// sequence trackers
		_, aiTables := enginetest.MustQuery(ctx, d.engine,
			fmt.Sprintf("select distinct table_name from information_schema.columns where extra = 'auto_increment' and table_schema = '%s';", db))

		for _, tableNameRow := range aiTables {
			tableName := tableNameRow[0].(string)

			// special handling for auto_increment_tbl, which is expected to start with particular values
			if strings.ToLower(tableName) == "auto_increment_tbl" {
				resetCmds = append(resetCmds, setup.AutoincrementData...)
				continue
			}

			resetCmds = append(resetCmds, setup.SetupScript{fmt.Sprintf("drop table %s", tableName)})

			ctx := enginetest.NewContext(d).WithCurrentDB(db)
			_, showCreateResult := enginetest.MustQuery(ctx, d.engine, fmt.Sprintf("show create table %s;", tableName))
			var createTableStatement strings.Builder
			for _, row := range showCreateResult {
				createTableStatement.WriteString(row[1].(string))
			}

			resetCmds = append(resetCmds, setup.SetupScript{createTableStatement.String()})
		}

		resetCmds = append(resetCmds, setup.SetupScript{"call dclean()"})
		resetCmds = append(resetCmds, setup.SetupScript{"call dreset('--hard', 'head')"})
	}

	resetCmds = append(resetCmds, setup.SetupScript{"use mydb"})
	return resetCmds
}

// commitScripts returns a set of queries that will commit the workingsets
// of the given database names
func commitScripts(dbs []string) []setup.SetupScript {
	var commitCmds setup.SetupScript
	for i := range dbs {
		db := dbs[i]
		commitCmds = append(commitCmds, fmt.Sprintf("use %s", db))
		commitCmds = append(commitCmds, "call dolt_add('.')")
		commitCmds = append(commitCmds, fmt.Sprintf("call dolt_commit('--allow-empty', '-am', 'checkpoint enginetest database %s', '--date', '1970-01-01T12:00:00')", db))
	}
	commitCmds = append(commitCmds, "use mydb")
	return []setup.SetupScript{commitCmds}
}

// NewEngine creates a new *gms.Engine or calls reset and clear scripts on the existing
// engine for reuse.
func (d *DoltHarness) NewEngine(t *testing.T) (*gms.Engine, error) {
	if d.engine == nil {
		d.branchControl = branch_control.CreateDefaultController()

		pro := d.NewDatabaseProvider(information_schema.NewUpdatableInformationSchemaDatabase())
		doltProvider, ok := pro.(sqle.DoltDatabaseProvider)
		require.True(t, ok)

		var err error
		d.session, err = dsess.NewDoltSession(sql.NewEmptyContext(), enginetest.NewBaseSession(), doltProvider,
			d.multiRepoEnv.Config(), d.branchControl)
		require.NoError(t, err)

		e, err := enginetest.NewEngineWithProviderSetup(t, d, pro, d.setupData)
		if err != nil {
			return nil, err
		}
		d.engine = e

		var res []sql.Row
		ctx := enginetest.NewContext(d)
		_, res = enginetest.MustQuery(ctx, e, "select schema_name from information_schema.schemata where schema_name not in ('information_schema');")
		var dbs []string
		for i := range res {
			dbs = append(dbs, res[i][0].(string))
		}

		e, err = enginetest.RunEngineScripts(ctx, e, commitScripts(dbs), d.SupportsNativeIndexCreation())
		if err != nil {
			return nil, err
		}

		return e, nil
	}

	// grants are files that can only be manually reset
	d.engine.Analyzer.Catalog.MySQLDb = mysql_db.CreateEmptyMySQLDb()
	d.engine.Analyzer.Catalog.MySQLDb.AddRootAccount()

	//todo(max): easier if tests specify their databases ahead of time
	ctx := enginetest.NewContext(d)
	e, err := enginetest.RunEngineScripts(ctx, d.engine, d.resetScripts(), d.SupportsNativeIndexCreation())

	return e, err
}

// WithParallelism returns a copy of the harness with parallelism set to the given number of threads. A value of 0 or
// less means to use the system parallelism settings.
func (d *DoltHarness) WithParallelism(parallelism int) *DoltHarness {
	nd := *d
	nd.parallelism = parallelism
	return &nd
}

// WithSkippedQueries returns a copy of the harness with the given queries skipped
func (d *DoltHarness) WithSkippedQueries(queries []string) *DoltHarness {
	nd := *d
	nd.skippedQueries = append(d.skippedQueries, queries...)
	return &nd
}

// SkipQueryTest returns whether to skip a query
func (d *DoltHarness) SkipQueryTest(query string) bool {
	lowerQuery := strings.ToLower(query)
	for _, skipped := range d.skippedQueries {
		if strings.Contains(lowerQuery, strings.ToLower(skipped)) {
			return true
		}
	}

	return false
}

func (d *DoltHarness) Parallelism() int {
	if d.parallelism <= 0 {

		// always test with some parallelism
		parallelism := runtime.NumCPU()

		if parallelism <= 1 {
			parallelism = 2
		}

		return parallelism
	}

	return d.parallelism
}

func (d *DoltHarness) NewContext() *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(d.session))
}

func (d *DoltHarness) NewContextWithClient(client sql.Client) *sql.Context {
	return sql.NewContext(context.Background(), sql.WithSession(d.newSessionWithClient(client)))
}

func (d *DoltHarness) NewSession() *sql.Context {
	d.session = d.newSessionWithClient(sql.Client{Address: "localhost", User: "root"})
	return d.NewContext()
}

func (d *DoltHarness) newSessionWithClient(client sql.Client) *dsess.DoltSession {
	states := make([]dsess.InitialDbState, len(d.databases))
	for i, db := range d.databases {
		env := d.multiRepoEnv.GetEnv(db.Name())
		states[i] = getDbState(d.t, db, env)
	}
	localConfig := d.multiRepoEnv.Config()
	pro := d.session.Provider()

	dSession, err := dsess.NewDoltSession(
		enginetest.NewContext(d),
		sql.NewBaseSessionWithClientServer("address", client, 1),
		pro.(dsess.DoltDatabaseProvider),
		localConfig,
		d.branchControl,
		states...,
	)
	require.NoError(d.t, err)
	return dSession
}

func (d *DoltHarness) SupportsNativeIndexCreation() bool {
	return true
}

func (d *DoltHarness) SupportsForeignKeys() bool {
	return true
}

func (d *DoltHarness) SupportsKeylessTables() bool {
	return true
}

func (d *DoltHarness) NewDatabase(name string) sql.Database {
	return d.NewDatabases(name)[0]
}

func (d *DoltHarness) NewDatabases(names ...string) []sql.Database {
	d.databases = nil
	for _, name := range names {
		dEnv := dtestutils.CreateTestEnvWithName(name)

		store := dEnv.DoltDB.ValueReadWriter().(*types.ValueStore)
		store.SetValidateContentAddresses(true)

		tmpDir, err := dEnv.TempTableFilesDir()
		require.NoError(d.t, err)
		opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
		db, err := sqle.NewDatabase(context.Background(), name, dEnv.DbData(), opts)
		require.NoError(d.t, err)

		d.databases = append(d.databases, db)

		d.multiRepoEnv.AddOrReplaceEnv(name, dEnv)
		d.createdEnvs[db.Name()] = dEnv
	}

	// TODO(zachmu): it should be safe to reuse a session with a new database, but it isn't in all cases. Particularly, if you
	//  have a database that only ever receives read queries, and then you re-use its session for a new database with
	//  the same name, the first write query will panic on dangling references in the noms layer. Not sure why this is
	//  happening, but it only happens as a result of this test setup.
	_ = d.NewSession()

	return dsqleDBsAsSqlDBs(d.databases)
}

func (d *DoltHarness) NewReadOnlyDatabases(names ...string) (dbs []sql.ReadOnlyDatabase) {
	for _, db := range d.NewDatabases(names...) {
		dbs = append(dbs, sqle.ReadOnlyDatabase{Database: db.(sqle.Database)})
	}
	return
}

func (d *DoltHarness) NewDatabaseProvider(dbs ...sql.Database) sql.MutableDatabaseProvider {
	// When NewDatabaseProvider is called, we create a new MultiRepoEnv in order to ensure
	// that only the specified sql.Databases are available for tests to use. Because
	// NewDatabases must be called before NewDatabaseProvider, we grab the DoltEnvs
	// previously created by NewDatabases and re-add them to the new MultiRepoEnv.
	dEnv := dtestutils.CreateTestEnv()
	mrEnv, err := env.MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	require.NoError(d.t, err)
	d.multiRepoEnv = mrEnv
	locations := make([]filesys.Filesys, len(dbs))
	for i, db := range dbs {
		if strings.ToLower(db.Name()) != sql.InformationSchemaDatabaseName {
			d.multiRepoEnv.AddEnv(db.Name(), d.createdEnvs[db.Name()])
			locations[i] = d.createdEnvs[db.Name()].FS
		}
	}

	b := env.GetDefaultInitBranch(d.multiRepoEnv.Config())
	pro, err := sqle.NewDoltDatabaseProviderWithDatabases(b, d.multiRepoEnv.FileSystem(), dbs, locations)
	require.NoError(d.t, err)

	return pro.WithDbFactoryUrl(doltdb.InMemDoltDB)
}

func getDbState(t *testing.T, db sqle.Database, dEnv *env.DoltEnv) dsess.InitialDbState {
	ctx := context.Background()

	head := dEnv.RepoStateReader().CWBHeadSpec()
	headCommit, err := dEnv.DoltDB.Resolve(ctx, head, dEnv.RepoStateReader().CWBHeadRef())
	require.NoError(t, err)

	ws, err := dEnv.WorkingSet(ctx)
	require.NoError(t, err)

	return dsess.InitialDbState{
		Db:         db,
		HeadCommit: headCommit,
		WorkingSet: ws,
		DbData:     dEnv.DbData(),
		Remotes:    dEnv.RepoState.Remotes,
	}
}

func (d *DoltHarness) NewTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	var err error
	if ro, ok := db.(sqle.ReadOnlyDatabase); ok {
		err = ro.CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema, sql.Collation_Default)
	} else {
		err = db.(sqle.Database).CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema, sql.Collation_Default)
	}
	if err != nil {
		return nil, err
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d).WithCurrentDB(db.Name()), name)
	require.NoError(d.t, err)
	require.True(d.t, ok, "table %s not found after creation", name)
	return table, nil
}

// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.PrimaryKeySchema, asOf interface{}) sql.Table {
	table, err := d.NewTable(db, name, schema)
	if err != nil {
		require.True(d.t, sql.ErrTableAlreadyExists.Is(err))
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d), name)
	require.NoError(d.t, err)
	require.True(d.t, ok)

	return table
}

// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) SnapshotTable(db sql.VersionedDatabase, name string, asOf interface{}) error {
	switch db.(type) {
	case sqle.ReadOnlyDatabase:
		// TODO: insert query to dolt_branches table (below)
		// can't be performed against ReadOnlyDatabase
		d.t.Skip("can't create SnaphotTables for ReadOnlyDatabases")
	case sqle.Database:
	default:
		panic("not a Dolt SQL Database")
	}

	e := enginetest.NewEngineWithDbs(d.t, d, []sql.Database{db})

	asOfString, ok := asOf.(string)
	require.True(d.t, ok)

	ctx := enginetest.NewContext(d)
	_, iter, err := e.Query(ctx,
		"CALL DOLT_ADD('.')")
	_, iter, err = e.Query(ctx,
		"SELECT COMMIT('-am', 'test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(d.t, err)

	headHash, err := ctx.GetSessionVariable(ctx, dsess.HeadKey(db.Name()))
	require.NoError(d.t, err)

	ctx = enginetest.NewContext(d)
	// TODO: there's a bug in test setup with transactions, where the HEAD session var gets overwritten on transaction
	//  start, so we quote it here instead
	// query := "insert into dolt_branches (name, hash) values ('" + asOfString + "', @@" + dsess.HeadKey(ddb.Name()) + ")"
	query := "CALL dolt_branch('" + asOfString + "', '" + headHash.(string) + "')"

	_, iter, err = e.Query(ctx,
		query)
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(d.t, err)

	return nil
}

func (d *DoltHarness) ValidateEngine(ctx *sql.Context, e *gms.Engine) (err error) {
	for _, db := range e.Analyzer.Catalog.AllDatabases(ctx) {
		if err = ValidateDatabase(ctx, db); err != nil {
			return err
		}
	}
	return
}

func dsqleDBsAsSqlDBs(dbs []sqle.Database) []sql.Database {
	sqlDbs := make([]sql.Database, 0, len(dbs))
	for _, db := range dbs {
		sqlDbs = append(sqlDbs, db)
	}
	return sqlDbs
}
