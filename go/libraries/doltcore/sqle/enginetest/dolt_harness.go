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
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/dtestutils"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

type DoltHarness struct {
	t               *testing.T
	provider        dsess.DoltDatabaseProvider
	multiRepoEnv    *env.MultiRepoEnv
	session         *dsess.DoltSession
	branchControl   *branch_control.Controller
	parallelism     int
	skippedQueries  []string
	setupData       []setup.SetupScript
	resetData       []setup.SetupScript
	engine          *gms.Engine
	skipSetupCommit bool
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
	dh := &DoltHarness{
		t:              t,
		skippedQueries: defaultSkippedQueries,
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

	resetCmds = append(resetCmds, setup.SetupScript{"SET foreign_key_checks=1;"})
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
func (d *DoltHarness) NewEngine(t *testing.T) (*gms.Engine, error) {
	if d.engine == nil {
		d.branchControl = branch_control.CreateDefaultController()

		pro := d.newProvider()
		doltProvider, ok := pro.(sqle.DoltDatabaseProvider)
		require.True(t, ok)
		d.provider = doltProvider

		var err error
		d.session, err = dsess.NewDoltSession(
			sql.NewEmptyContext(),
			enginetest.NewBaseSession(),
			doltProvider,
			d.multiRepoEnv.Config(),
			d.branchControl,
		)
		require.NoError(t, err)

		e, err := enginetest.NewEngine(t, d, d.provider, d.setupData)
		if err != nil {
			return nil, err
		}
		d.engine = e

		ctx := enginetest.NewContext(d)
		databases := pro.AllDatabases(ctx)
		var dbs []string
		for _, db := range databases {
			dbs = append(dbs, db.Name())
		}

		if !d.skipSetupCommit {
			e, err = enginetest.RunSetupScripts(ctx, e, commitScripts(dbs), d.SupportsNativeIndexCreation())
			if err != nil {
				return nil, err
			}
		}

		return e, nil
	}

	// Reset the mysql DB table to a clean state for this new engine
	d.engine.Analyzer.Catalog.MySQLDb = mysql_db.CreateEmptyMySQLDb()
	d.engine.Analyzer.Catalog.MySQLDb.AddRootAccount()

	ctx := enginetest.NewContext(d)
	e, err := enginetest.RunSetupScripts(ctx, d.engine, d.resetScripts(), d.SupportsNativeIndexCreation())

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
	localConfig := d.multiRepoEnv.Config()
	pro := d.session.Provider()

	dSession, err := dsess.NewDoltSession(
		enginetest.NewContext(d),
		sql.NewBaseSessionWithClientServer("address", client, 1),
		pro.(dsess.DoltDatabaseProvider),
		localConfig,
		d.branchControl,
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

func (d *DoltHarness) NewDatabases(names ...string) []sql.Database {
	d.engine = nil
	d.provider = nil

	d.branchControl = branch_control.CreateDefaultController()

	pro := d.newProvider()
	doltProvider, ok := pro.(sqle.DoltDatabaseProvider)
	require.True(d.t, ok)
	d.provider = doltProvider

	var err error
	d.session, err = dsess.NewDoltSession(
		sql.NewEmptyContext(),
		enginetest.NewBaseSession(),
		doltProvider,
		d.multiRepoEnv.Config(),
		d.branchControl,
	)
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

	ctx := enginetest.NewContext(d)
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

func (d *DoltHarness) NewReadOnlyEngine(provider sql.DatabaseProvider) (*gms.Engine, error) {
	ddp, ok := provider.(sqle.DoltDatabaseProvider)
	if !ok {
		return nil, fmt.Errorf("expected a DoltDatabaseProvider")
	}

	allDatabases := ddp.AllDatabases(d.NewContext())
	dbs := make([]sqle.SqlDatabase, len(allDatabases))
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

	return enginetest.NewEngineWithProvider(nil, d, readOnlyProvider), nil
}

func (d *DoltHarness) NewDatabaseProvider() sql.MutableDatabaseProvider {
	return d.provider
}

func (d *DoltHarness) newProvider() sql.MutableDatabaseProvider {
	dEnv := dtestutils.CreateTestEnv()

	store := dEnv.DoltDB.ValueReadWriter().(*types.ValueStore)
	store.SetValidateContentAddresses(true)

	mrEnv, err := env.MultiEnvForDirectory(context.Background(), dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	require.NoError(d.t, err)
	d.multiRepoEnv = mrEnv

	b := env.GetDefaultInitBranch(d.multiRepoEnv.Config())
	pro, err := sqle.NewDoltDatabaseProvider(b, d.multiRepoEnv.FileSystem())
	require.NoError(d.t, err)

	return pro.WithDbFactoryUrl(doltdb.InMemDoltDB)
}

func (d *DoltHarness) newTable(db sql.Database, name string, schema sql.PrimaryKeySchema) (sql.Table, error) {
	tc := db.(sql.TableCreator)

	err := tc.CreateTable(enginetest.NewContext(d).WithCurrentDB(db.Name()), name, schema, sql.Collation_Default)
	if err != nil {
		return nil, err
	}

	table, ok, err := db.GetTableInsensitive(enginetest.NewContext(d).WithCurrentDB(db.Name()), name)
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

	_, iter, err := e.Query(ctx,
		"CALL DOLT_COMMIT('-Am', 'test commit');")
	require.NoError(d.t, err)
	_, err = sql.RowIterToRows(ctx, nil, iter)
	require.NoError(d.t, err)

	// Create a new branch at this commit with the given identifier
	ctx = enginetest.NewContext(d)
	query := "CALL dolt_branch('" + asOfString + "')"

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
