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
	"time"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/enginetest/scriptgen/setup"
	"github.com/dolthub/go-mysql-server/memory"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/kvexec"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltHarness struct {
	t                     *testing.T
	provider              dsess.DoltDatabaseProvider
	statsPro              *statspro.StatsController
	multiRepoEnv          *env.MultiRepoEnv
	session               *dsess.DoltSession
	branchControl         *branch_control.Controller
	gcSafepointController *dsess.GCSafepointController
	parallelism           int
	skippedQueries        []string
	setupData             []setup.SetupScript
	resetData             []setup.SetupScript
	engine                *gms.Engine
	setupDbs              map[string]struct{}
	skipSetupCommit       bool
	configureStats        bool
	useLocalFilesystem    bool
	setupTestProcedures   bool
}

func (d *DoltHarness) UseLocalFileSystem() {
	d.useLocalFilesystem = true
}

func (d *DoltHarness) Session() *dsess.DoltSession {
	return d.session
}

func (d *DoltHarness) WithConfigureStats(configureStats bool) DoltEnginetestHarness {
	nd := *d
	nd.configureStats = configureStats
	return &nd
}

func (d *DoltHarness) NewHarness(t *testing.T) DoltEnginetestHarness {
	return newDoltHarness(t)
}

type DoltEnginetestHarness interface {
	enginetest.Harness
	enginetest.SkippingHarness
	enginetest.ClientHarness
	enginetest.IndexHarness
	enginetest.VersionedDBHarness
	enginetest.ForeignKeyHarness
	enginetest.KeylessTableHarness
	enginetest.ReadOnlyDatabaseHarness
	enginetest.ValidatingHarness

	// NewHarness returns a new uninitialized harness of the same type
	NewHarness(t *testing.T) DoltEnginetestHarness

	// WithSkippedQueries returns a copy of the harness with the given queries skipped
	WithSkippedQueries(skipped []string) DoltEnginetestHarness

	// WithParallelism returns a copy of the harness with parallelism set to the given number of threads
	// Deprecated: parallelism currently no-ops
	WithParallelism(parallelism int) DoltEnginetestHarness

	// WithConfigureStats returns a copy of the harness with the given configureStats value
	WithConfigureStats(configureStats bool) DoltEnginetestHarness

	// SkipSetupCommit configures to harness to skip the commit after setup scripts are run
	SkipSetupCommit()

	// UseLocalFileSystem configures the harness to use the local filesystem for all storage, instead of in-memory versions
	UseLocalFileSystem()

	// Close closes the harness, freeing up any resources it may have allocated
	Close()

	Engine() *gms.Engine

	Session() *dsess.DoltSession
}

var _ DoltEnginetestHarness = &DoltHarness{}

// newDoltHarness creates a new harness for testing Dolt, using an in-memory filesystem and an in-memory blob store.
func newDoltHarness(t *testing.T) *DoltHarness {
	dh := &DoltHarness{
		t:              t,
		skippedQueries: defaultSkippedQueries,
		parallelism:    1,
	}

	return dh
}

func newDoltEnginetestHarness(t *testing.T) DoltEnginetestHarness {
	return newDoltHarness(t)
}

// newDoltHarnessForLocalFilesystem creates a new harness for testing Dolt, using
// the local filesystem for all storage, instead of in-memory versions. This setup
// is useful for testing functionality that requires a real filesystem.
func newDoltHarnessForLocalFilesystem(t *testing.T) *DoltHarness {
	dh := newDoltHarness(t)
	dh.useLocalFilesystem = true
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
	d.closeProvider()
	d.engine = nil
	d.provider = nil
	d.setupData = nil
	for i := range setupData {
		d.setupData = append(d.setupData, setupData[i]...)
	}
}

func (d *DoltHarness) SkipSetupCommit() {
	d.skipSetupCommit = true
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
	resetCmds = append(resetCmds, setup.SetupScript{"SET foreign_key_checks=0;"})
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
			if strings.EqualFold(tableName, "auto_increment_tbl") {
				resetCmds = append(resetCmds, setup.AutoincrementData...)
				continue
			}

			resetCmds = append(resetCmds, setup.SetupScript{fmt.Sprintf("drop table %s", tableName)})
		}

		resetCmds = append(resetCmds, setup.SetupScript{"call dolt_clean()"})
		resetCmds = append(resetCmds, setup.SetupScript{"call dolt_reset('--hard', 'head')"})
	}

	resetCmds = append(resetCmds, setup.SetupScript{"SET foreign_key_checks=1;"})
	for _, db := range dbs {
		if _, ok := d.setupDbs[db]; !ok && db != "mydb" {
			resetCmds = append(resetCmds, setup.SetupScript{fmt.Sprintf("drop database if exists %s", db)})
		}
	}
	resetCmds = append(resetCmds, setup.SetupScript{"use mydb"})
	return resetCmds
}

// commitScripts returns a set of queries that will commit the working sets of the given database names
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
func (d *DoltHarness) NewEngine(t *testing.T) (enginetest.QueryEngine, error) {
	initializeEngine := d.engine == nil
	if initializeEngine {
		ctx := context.Background()
		d.branchControl = branch_control.CreateDefaultController(ctx)

		pro := d.newProvider(ctx)
		if d.setupTestProcedures {
			pro = d.newProviderWithProcedures(ctx)
		}
		doltProvider, ok := pro.(*sqle.DoltDatabaseProvider)
		require.True(t, ok)

		d.provider = doltProvider

		d.gcSafepointController = dsess.NewGCSafepointController()

		bThreads := sql.NewBackgroundThreads()

		ctxGen := func(ctx context.Context) (*sql.Context, error) {
			client := sql.Client{Address: "localhost", User: "root"}
			return sql.NewContext(context.Background(), sql.WithSession(d.newSessionWithClient(client))), nil
		}
		statsPro := statspro.NewStatsController(doltProvider, ctxGen, logrus.StandardLogger(), d.multiRepoEnv.GetEnv(d.multiRepoEnv.GetFirstDatabase()))
		d.statsPro = statsPro

		var err error
		d.session, err = dsess.NewDoltSession(enginetest.NewBaseSession(), d.provider, d.multiRepoEnv.Config(), d.branchControl, d.statsPro, writer.NewWriteSession, d.gcSafepointController)
		require.NoError(t, err)

		e, err := enginetest.NewEngine(t, d, d.provider, d.setupData, d.statsPro)
		if err != nil {
			return nil, err
		}
		e.Analyzer.ExecBuilder = rowexec.NewOverrideBuilder(kvexec.Builder{})
		d.engine = e

		sqlCtx := enginetest.NewContext(d)
		databases := pro.AllDatabases(sqlCtx)

		d.setupDbs = make(map[string]struct{})
		var dbs []string
		for _, db := range databases {
			dbName := db.Name()
			dbs = append(dbs, dbName)
			d.setupDbs[dbName] = struct{}{}
		}

		if !d.skipSetupCommit {
			e, err = enginetest.RunSetupScripts(sqlCtx, e, commitScripts(dbs), d.SupportsNativeIndexCreation())
			if err != nil {
				return nil, err
			}

			// Get a fresh session after running setup scripts, since some setup scripts can change the session state
			d.session, err = dsess.NewDoltSession(enginetest.NewBaseSession(), d.provider, d.multiRepoEnv.Config(), d.branchControl, d.statsPro, writer.NewWriteSession, nil)
			require.NoError(t, err)
		}

		e = e.WithBackgroundThreads(bThreads)

		if d.configureStats {
			err = statsPro.Init(ctx, databases)
			if err != nil {
				return nil, err
			}
			statsPro.SetTimers(int64(1*time.Nanosecond), int64(1*time.Second))

			err = statsPro.Restart()
			if err != nil {
				return nil, err
			}

			statsOnlyQueries := filterStatsOnlyQueries(d.setupData)
			e, err = enginetest.RunSetupScripts(sqlCtx, e, statsOnlyQueries, d.SupportsNativeIndexCreation())
			if err != nil {
				return nil, err
			}

			finalizeStatsAfterSetup := []setup.SetupScript{{"call dolt_stats_wait()"}}
			e, err = enginetest.RunSetupScripts(sqlCtx, d.engine, finalizeStatsAfterSetup, d.SupportsNativeIndexCreation())
			require.NoError(t, err)
		}

		return e, nil
	}

	// Reset the mysql DB table to a clean state for this new engine
	ctx := enginetest.NewContext(d)

	d.engine.Analyzer.Catalog.MySQLDb = mysql_db.CreateEmptyMySQLDb()
	d.engine.Analyzer.Catalog.MySQLDb.AddRootAccount()

	e, err := enginetest.RunSetupScripts(ctx, d.engine, d.resetScripts(), d.SupportsNativeIndexCreation())
	require.NoError(t, err)

	if d.configureStats {
		finalizeStatsAfterSetup := []setup.SetupScript{{"call dolt_stats_wait()"}}
		e, err = enginetest.RunSetupScripts(ctx, d.engine, finalizeStatsAfterSetup, d.SupportsNativeIndexCreation())
		require.NoError(t, err)
	}

	// Get a fresh session after running setup scripts, since some setup scripts can change the session state
	d.session, err = dsess.NewDoltSession(enginetest.NewBaseSession(), d.provider, d.multiRepoEnv.Config(), d.branchControl, d.statsPro, writer.NewWriteSession, nil)
	require.NoError(t, err)

	return e, err
}

func filterStatsOnlyQueries(scripts []setup.SetupScript) []setup.SetupScript {
	var ret []string
	for i := range scripts {
		for _, s := range scripts[i] {
			if strings.HasPrefix(s, "analyze table") {
				ret = append(ret, s)
			}
		}
	}
	return []setup.SetupScript{ret}
}

// WithParallelism returns a copy of the harness with parallelism set to the given number of threads. A value of 0 or
// less means to use the system parallelism settings.
func (d *DoltHarness) WithParallelism(parallelism int) DoltEnginetestHarness {
	nd := *d
	nd.parallelism = parallelism
	return &nd
}

// WithSkippedQueries returns a copy of the harness with the given queries skipped
func (d *DoltHarness) WithSkippedQueries(queries []string) DoltEnginetestHarness {
	nd := *d
	nd.skippedQueries = append(d.skippedQueries, queries...)
	return &nd
}

func (d *DoltHarness) Engine() *gms.Engine {
	return d.engine
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
	localConfig := d.multiRepoEnv.Config()
	pro := d.session.Provider()

	dSession, err := dsess.NewDoltSession(sql.NewBaseSessionWithClientServer("address", client, 1), pro.(dsess.DoltDatabaseProvider), localConfig, d.branchControl, d.statsPro, writer.NewWriteSession, nil)
	dSession.SetCurrentDatabase("mydb")
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

func (d *DoltHarness) NewDatabases(names ...string) []sql.Database {
	ctx := enginetest.NewContext(d)
	d.closeProvider()
	d.engine = nil
	d.provider = nil

	d.branchControl = branch_control.CreateDefaultController(context.Background())

	pro := d.newProvider(ctx)
	doltProvider, ok := pro.(*sqle.DoltDatabaseProvider)
	require.True(d.t, ok)
	d.provider = doltProvider

	var err error
	d.session, err = dsess.NewDoltSession(enginetest.NewBaseSession(), doltProvider, d.multiRepoEnv.Config(), d.branchControl, d.statsPro, writer.NewWriteSession, nil)
	require.NoError(d.t, err)

	// TODO: the engine tests should do this for us
	d.session.SetCurrentDatabase("mydb")

	e := enginetest.NewEngineWithProvider(d.t, d, d.provider)
	require.NoError(d.t, err)
	d.engine = e

	for _, name := range names {
		err := d.provider.CreateDatabase(enginetest.NewContext(d), name)
		require.NoError(d.t, err)
	}

	databases := pro.AllDatabases(ctx)

	// It's important that we return the databases in the same order as the names argument
	var dbs []sql.Database
	for _, name := range names {
		for _, db := range databases {
			if db.Name() == name {
				dbs = append(dbs, db)
				break
			}
		}
	}

	return dbs
}

func (d *DoltHarness) NewReadOnlyEngine(provider sql.DatabaseProvider) (enginetest.QueryEngine, error) {
	ddp, ok := provider.(*sqle.DoltDatabaseProvider)
	if !ok {
		return nil, fmt.Errorf("expected a DoltDatabaseProvider")
	}

	allDatabases := ddp.AllDatabases(d.NewContext())
	dbs := make([]dsess.SqlDatabase, len(allDatabases))
	locations := make([]filesys.Filesys, len(allDatabases))

	for i, db := range allDatabases {
		dbs[i] = sqle.ReadOnlyDatabase{Database: db.(sqle.Database)}
		loc, err := ddp.FileSystemForDatabase(db.Name())
		if err != nil {
			return nil, err
		}

		locations[i] = loc
	}

	readOnlyProvider, err := sqle.NewDoltDatabaseProviderWithDatabases("main", ddp.FileSystem(), dbs, locations)
	if err != nil {
		return nil, err
	}

	// reset the session as well since we have swapped out the database provider, which invalidates caching assumptions
	d.session, err = dsess.NewDoltSession(enginetest.NewBaseSession(), readOnlyProvider, d.multiRepoEnv.Config(), d.branchControl, d.statsPro, writer.NewWriteSession, d.gcSafepointController)
	require.NoError(d.t, err)

	return enginetest.NewEngineWithProvider(nil, d, readOnlyProvider), nil
}

func (d *DoltHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return d.provider
}

func (d *DoltHarness) Close() {
	d.closeProvider()
	if d.statsPro != nil {
		d.statsPro.Close()
	}
	sql.SystemVariables.SetGlobal(dsess.DoltStatsEnabled, int8(0))
}

func (d *DoltHarness) closeProvider() {
	if d.provider != nil {
		dbs := d.provider.AllDatabases(sql.NewEmptyContext())
		for _, db := range dbs {
			require.NoError(d.t, db.(dsess.SqlDatabase).DbData().Ddb.Close())
		}
	}
}

func (d *DoltHarness) newProvider(ctx context.Context) sql.MutableDatabaseProvider {
	d.closeProvider()

	var dEnv *env.DoltEnv
	if d.useLocalFilesystem {
		dEnv = dtestutils.CreateTestEnvForLocalFilesystem()
	} else {
		dEnv = dtestutils.CreateTestEnv()
	}
	defer dEnv.DoltDB(ctx).Close()

	store := dEnv.DoltDB(ctx).ValueReadWriter().(*types.ValueStore)
	store.SetValidateContentAddresses(true)

	mrEnv, err := env.MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	require.NoError(d.t, err)
	d.multiRepoEnv = mrEnv

	b := env.GetDefaultInitBranch(d.multiRepoEnv.Config())
	pro, err := sqle.NewDoltDatabaseProvider(b, d.multiRepoEnv.FileSystem())
	require.NoError(d.t, err)

	return pro
}

func (d *DoltHarness) newProviderWithProcedures(ctx context.Context) sql.MutableDatabaseProvider {
	pro := d.newProvider(ctx)
	provider, ok := pro.(*sqle.DoltDatabaseProvider)
	require.True(d.t, ok)
	for _, esp := range memory.ExternalStoredProcedures {
		provider.Register(esp)
	}
	return provider
}

func (d *DoltHarness) newTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	tc := db.(sql.TableCreator)

	ctx := enginetest.NewContext(d)
	ctx.Session.SetCurrentDatabase(db.Name())
	err := tc.CreateTable(ctx, name, schema, sql.Collation_Default, "")
	if err != nil {
		return nil, err
	}

	ctx = enginetest.NewContext(d)
	ctx.Session.SetCurrentDatabase(db.Name())
	table, ok, err := db.GetTableInsensitive(ctx, name)
	require.NoError(d.t, err)
	require.True(d.t, ok, "table %s not found after creation", name)
	return table, nil
}

// NewTableAsOf implements enginetest.VersionedHarness
// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) NewTableAsOf(db sql.VersionedDatabase, name string, schema sql.PrimaryKeySchema, asOf interface{}) sql.Table {
	table, err := d.newTable(db, name, schema)
	if err != nil {
		require.True(d.t, sql.ErrTableAlreadyExists.Is(err))
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d), name)
	require.NoError(d.t, err)
	require.True(d.t, ok)

	return table
}

// SnapshotTable implements enginetest.VersionedHarness
// Dolt doesn't version tables per se, just the entire database. So ignore the name and schema and just create a new
// branch with the given name.
func (d *DoltHarness) SnapshotTable(db sql.VersionedDatabase, tableName string, asOf interface{}) error {
	e := enginetest.NewEngineWithProvider(d.t, d, d.NewDatabaseProvider())

	asOfString, ok := asOf.(string)
	require.True(d.t, ok)

	ctx := enginetest.NewContext(d)

	_, iter, _, err := e.Query(ctx,
		"CALL DOLT_COMMIT('-Am', 'test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
	require.NoError(d.t, err)

	// Create a new branch at this commit with the given identifier
	ctx = enginetest.NewContext(d)
	query := "CALL dolt_branch('" + asOfString + "')"

	_, iter, _, err = e.Query(ctx,
		query)
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, iter)
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
