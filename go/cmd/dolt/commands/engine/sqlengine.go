// Copyright 2021 Dolthub, Inc.
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

package engine

import (
	"context"
	"os"
	"strconv"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/gcctx"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	dblr "github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/kvexec"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/mysql_file_handler"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/valctx"
)

// SqlEngine packages up the context necessary to run sql queries against dsqle.
type SqlEngine struct {
	provider       *dsqle.DoltDatabaseProvider
	ContextFactory sql.ContextFactory
	dsessFactory   sessionFactory
	engine         *gms.Engine
	fs             filesys.Filesys
}

type sessionFactory func(mysqlSess *sql.BaseSession, pro sql.DatabaseProvider) (*dsess.DoltSession, error)
type contextFactory func(ctx context.Context, session sql.Session) (*sql.Context, error)

type SystemVariables map[string]interface{}

type SqlEngineConfig struct {
	IsReadOnly                 bool
	IsServerLocked             bool
	DoltCfgDirPath             string
	PrivFilePath               string
	BranchCtrlFilePath         string
	ServerUser                 string
	ServerPass                 string
	ServerHost                 string
	SkipRootUserInitialization bool
	Autocommit                 bool
	DoltTransactionCommit      bool
	Bulk                       bool
	JwksConfig                 []servercfg.JwksConfig
	SystemVariables            SystemVariables
	ClusterController          *cluster.Controller
	AutoGCController           *dsqle.AutoGCController
	BinlogReplicaController    binlogreplication.BinlogReplicaController
	EventSchedulerStatus       eventscheduler.SchedulerStatus
}

// NewSqlEngine returns a SqlEngine
func NewSqlEngine(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	config *SqlEngineConfig,
) (*SqlEngine, error) {
	// Context validation is a testing mode that we run Dolt in
	// during integration tests. It asserts that `context.Context`
	// instances which reach the storage layer have gone through
	// GC session lifecycle callbacks. This is only relevant in
	// sql mode, so we only enable it here. This is potentially
	// relevant in non-sql-server contexts, because things like
	// replication and events can still cause concurrency during a
	// GC, so we put this here instead of in sql-server.
	const contextValidationEnabledEnvVar = "DOLT_CONTEXT_VALIDATION_ENABLED"
	if val := os.Getenv(contextValidationEnabledEnvVar); val != "" && val != "0" && strings.ToLower(val) != "false" {
		valctx.EnableContextValidation()
	}

	gcSafepointController := gcctx.NewGCSafepointController()
	ctx = gcctx.WithGCSafepointController(ctx, gcSafepointController)

	defer gcctx.SessionEnd(ctx)
	gcctx.SessionCommandBegin(ctx)
	defer gcctx.SessionCommandEnd(ctx)

	dbs, locations, err := CollectDBs(ctx, mrEnv, config.Bulk)
	if err != nil {
		return nil, err
	}

	bThreads := sql.NewBackgroundThreads()
	var runAsyncThreads sqle.RunAsyncThreads
	dbs, runAsyncThreads, err = dsqle.ApplyReplicationConfig(ctx, mrEnv, cli.CliOut, dbs...)
	if err != nil {
		return nil, err
	}

	config.ClusterController.ManageSystemVariables(sql.SystemVariables)

	err = config.ClusterController.ApplyStandbyReplicationConfig(ctx, mrEnv, dbs...)
	if err != nil {
		return nil, err
	}

	err = applySystemVariables(sql.SystemVariables, config.SystemVariables)
	if err != nil {
		return nil, err
	}

	// Make a copy of the databases. |all| is going to be provided
	// as the set of all initial databases to dsqle
	// DatabaseProvider. |dbs| is only the databases that came
	// from MultiRepoEnv, and they are all real databases based on
	// DoltDB instances. |all| is going to include some extension,
	// informational databases like |dolt_cluster| sometimes,
	// depending on config.
	all := make([]dsess.SqlDatabase, len(dbs))
	copy(all, dbs)

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	clusterDB := config.ClusterController.ClusterDatabase()
	if clusterDB != nil {
		all = append(all, clusterDB.(dsess.SqlDatabase))
		locations = append(locations, nil)
	}

	b := env.GetDefaultInitBranch(mrEnv.Config())
	pro, err := dsqle.NewDoltDatabaseProviderWithDatabases(b, mrEnv.FileSystem(), all, locations)
	if err != nil {
		return nil, err
	}
	pro = pro.WithRemoteDialer(mrEnv.RemoteDialProvider())

	config.ClusterController.RegisterStoredProcedures(pro)
	if config.ClusterController != nil {
		pro.InitDatabaseHooks = append(pro.InitDatabaseHooks, cluster.NewInitDatabaseHook(config.ClusterController, bThreads))
		pro.DropDatabaseHooks = append(pro.DropDatabaseHooks, config.ClusterController.DropDatabaseHook())
		config.ClusterController.SetDropDatabase(pro.DropDatabase)
	}

	sqlEngine := &SqlEngine{}

	// Create the engine
	engine := gms.New(analyzer.NewBuilder(pro).Build(), &gms.Config{
		IsReadOnly:     config.IsReadOnly,
		IsServerLocked: config.IsServerLocked,
	}).WithBackgroundThreads(bThreads)

	if err := configureBinlogPrimaryController(engine); err != nil {
		return nil, err
	}

	config.ClusterController.SetIsStandbyCallback(func(isStandby bool) {
		pro.SetIsStandby(isStandby)

		// Standbys are read only, primaries are not.
		// We only change this here if the server was not forced read
		// only by its startup config.
		if !config.IsReadOnly {
			engine.ReadOnly.Store(isStandby)
		}
	})

	// Load in privileges from file, if it exists
	var persister cluster.MySQLDbPersister
	persister = mysql_file_handler.NewPersister(config.PrivFilePath, config.DoltCfgDirPath)

	persister = config.ClusterController.HookMySQLDbPersister(persister, engine.Analyzer.Catalog.MySQLDb)
	data, err := persister.LoadData(ctx)
	if err != nil {
		return nil, err
	}

	// Load the branch control permissions, if they exist
	var bcController *branch_control.Controller
	if bcController, err = branch_control.LoadData(ctx, config.BranchCtrlFilePath, config.DoltCfgDirPath); err != nil {
		return nil, err
	}
	config.ClusterController.HookBranchControlPersistence(bcController, mrEnv.FileSystem())

	// Setup the engine.
	engine.Analyzer.Catalog.MySQLDb.SetPersister(persister)

	engine.Analyzer.Catalog.MySQLDb.SetPlugins(map[string]mysql_db.PlaintextAuthPlugin{
		"authentication_dolt_jwt": NewAuthenticateDoltJWTPlugin(config.JwksConfig),
	})

	if config.AutoGCController != nil {
		err = config.AutoGCController.RunBackgroundThread(bThreads, sqlEngine.NewDefaultContext)
		if err != nil {
			return nil, err
		}
		config.AutoGCController.ApplyCommitHooks(ctx, mrEnv, dbs...)
		pro.InitDatabaseHooks = append(pro.InitDatabaseHooks, config.AutoGCController.InitDatabaseHook())
		pro.DropDatabaseHooks = append(pro.DropDatabaseHooks, config.AutoGCController.DropDatabaseHook())
		// XXX: We force session aware safepoint controller if auto_gc is on.
		dprocedures.UseSessionAwareSafepointController = true
		sql.SystemVariables.AssignValues(map[string]interface{}{
			dsess.DoltAutoGCEnabled: int8(1),
		})
	} else {
		sql.SystemVariables.AssignValues(map[string]interface{}{
			dsess.DoltAutoGCEnabled: int8(0),
		})
	}

	var statsPro sql.StatsProvider
	_, enabled, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsEnabled)
	if enabled.(int8) == 1 {
		statsPro = statspro.NewStatsController(logrus.StandardLogger(), bThreads, mrEnv.GetEnv(mrEnv.GetFirstDatabase()))
	} else {
		statsPro = statspro.StatsNoop{}
	}

	engine.Analyzer.Catalog.StatsProvider = statsPro

	engine.Analyzer.ExecBuilder = rowexec.NewOverrideBuilder(kvexec.Builder{})
	sessFactory := doltSessionFactory(pro, statsPro, mrEnv.Config(), bcController, gcSafepointController, config.Autocommit)
	sqlEngine.provider = pro
	sqlEngine.dsessFactory = sessFactory
	sqlEngine.ContextFactory = sqlContextFactory
	sqlEngine.engine = engine
	sqlEngine.fs = pro.FileSystem()

	pro.InstallReplicationInitDatabaseHook(bThreads, sqlEngine.NewDefaultContext)
	if err = config.ClusterController.RunCommitHooks(bThreads, sqlEngine.NewDefaultContext); err != nil {
		return nil, err
	}
	if runAsyncThreads != nil {
		if err = runAsyncThreads(bThreads, sqlEngine.NewDefaultContext); err != nil {
			return nil, err
		}
	}

	sqlCtx, err := sqlEngine.NewDefaultContext(ctx)
	if err != nil {
		return nil, err
	}

	// Load MySQL Db information
	if err = engine.Analyzer.Catalog.MySQLDb.LoadData(sqlCtx, data); err != nil {
		return nil, err
	}

	if dbg, ok := os.LookupEnv(dconfig.EnvSqlDebugLog); ok && strings.EqualFold(dbg, "true") {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv(dconfig.EnvSqlDebugLogVerbose); ok && strings.EqualFold(verbose, "true") {
			engine.Analyzer.Verbose = true
		}
	}

	err = sql.SystemVariables.SetGlobal(sqlCtx, dsess.DoltCommitOnTransactionCommit, config.DoltTransactionCommit)
	if err != nil {
		return nil, err
	}

	if engine.EventScheduler == nil {
		err = configureEventScheduler(config, engine, sqlEngine.ContextFactory, sessFactory, pro)
		if err != nil {
			return nil, err
		}
	}

	if config.BinlogReplicaController != nil {
		binLogSession, err := sessFactory(sql.NewBaseSession(), pro)
		if err != nil {
			return nil, err
		}

		err = configureBinlogReplicaController(config, engine, sqlEngine.ContextFactory, binLogSession)
		if err != nil {
			return nil, err
		}
	}

	return sqlEngine, nil
}

// NewRebasedSqlEngine returns a smalled rebased engine primarily used in filterbranch.
// TODO: migrate to provider
func NewRebasedSqlEngine(engine *gms.Engine, dbs map[string]dsess.SqlDatabase) *SqlEngine {
	return &SqlEngine{
		engine: engine,
	}
}

func applySystemVariables(vars sql.SystemVariableRegistry, cfg SystemVariables) error {
	if cfg != nil {
		return vars.AssignValues(cfg)
	}
	return nil
}

// InitStats initalizes stats threads. We separate construction
// from initialization because the session provider needs the
// *StatsCoordinator handle (stats and provider are cyclically
// dependent), but several other initialization steps race on
// session variables if stats threads are started too early.
// xxx: separating provider/stats dependency is tough because
// the session is the runtime source of truth for both.
func (se *SqlEngine) InitStats(ctx context.Context) error {
	// configuring stats depends on sessionBuilder
	// sessionBuilder needs ref to statsProv
	pro := se.GetUnderlyingEngine().Analyzer.Catalog.DbProvider.(*sqle.DoltDatabaseProvider)
	sqlCtx, err := se.NewLocalContext(ctx)
	if err != nil {
		return err
	}
	defer sql.SessionEnd(sqlCtx.Session)
	sql.SessionCommandBegin(sqlCtx.Session)
	defer sql.SessionCommandEnd(sqlCtx.Session)
	dbs := pro.AllDatabases(sqlCtx)
	statsPro := se.GetUnderlyingEngine().Analyzer.Catalog.StatsProvider
	if sc, ok := statsPro.(*statspro.StatsController); ok {
		_, memOnly, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsMemoryOnly)
		sc.SetMemOnly(memOnly.(int8) == 1)

		pro.InitDatabaseHooks = append(pro.InitDatabaseHooks, statspro.NewInitDatabaseHook(sc))
		pro.DropDatabaseHooks = append(pro.DropDatabaseHooks, statspro.NewDropDatabaseHook(sc))

		var sqlDbs []sql.Database
		for _, db := range dbs {
			sqlDbs = append(sqlDbs, db)
		}

		err = sc.Init(sqlCtx, pro, se.NewDefaultContext, sqlDbs)
		if err != nil {
			return err
		}

		if _, paused, _ := sql.SystemVariables.GetGlobal(dsess.DoltStatsPaused); paused.(int8) == 0 {
			if err = sc.Restart(sqlCtx); err != nil {
				return err
			}
		}
	}
	return nil
}

// Databases returns a slice of all databases in the engine
func (se *SqlEngine) Databases(ctx *sql.Context) []dsess.SqlDatabase {
	databases := se.provider.AllDatabases(ctx)
	dbs := make([]dsess.SqlDatabase, len(databases))
	for i := range databases {
		dbs[i] = databases[i].(dsess.SqlDatabase)
	}

	return nil
}

// NewContext returns a new sql.Context with the given session.
func (se *SqlEngine) NewContext(ctx context.Context, session sql.Session) (*sql.Context, error) {
	return se.ContextFactory(ctx, sql.WithSession(session)), nil
}

// NewDefaultContext returns a new sql.Context with a new default dolt session.
func (se *SqlEngine) NewDefaultContext(ctx context.Context) (*sql.Context, error) {
	session, err := se.NewDoltSession(ctx, sql.NewBaseSession())
	if err != nil {
		return nil, err
	}
	return se.ContextFactory(ctx, sql.WithSession(session)), nil
}

// NewLocalContext returns a new |sql.Context| with its client set to |root|
func (se *SqlEngine) NewLocalContext(ctx context.Context) (*sql.Context, error) {
	sqlCtx, err := se.NewDefaultContext(ctx)
	if err != nil {
		return nil, err
	}

	sqlCtx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})
	return sqlCtx, nil
}

// NewDoltSession creates a new DoltSession from a BaseSession
func (se *SqlEngine) NewDoltSession(_ context.Context, mysqlSess *sql.BaseSession) (*dsess.DoltSession, error) {
	return se.dsessFactory(mysqlSess, se.provider)
}

// Query execute a SQL statement and return values for printing.
func (se *SqlEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return se.engine.Query(ctx, query)
}

func (se *SqlEngine) QueryWithBindings(ctx *sql.Context, query string, parsed sqlparser.Statement, bindings map[string]sqlparser.Expr, qFlags *sql.QueryFlags) (sql.Schema, sql.RowIter, *sql.QueryFlags, error) {
	return se.engine.QueryWithBindings(ctx, query, parsed, bindings, qFlags)
}

// Analyze analyzes a node.
func (se *SqlEngine) Analyze(ctx *sql.Context, n sql.Node, qFlags *sql.QueryFlags) (sql.Node, error) {
	return se.engine.Analyzer.Analyze(ctx, n, nil, qFlags)
}

func (se *SqlEngine) GetUnderlyingEngine() *gms.Engine {
	return se.engine
}

func (se *SqlEngine) FileSystem() filesys.Filesys {
	return se.fs
}

func (se *SqlEngine) Close() error {
	var err error
	if se.engine != nil {
		if se.engine.Analyzer.Catalog.BinlogReplicaController != nil {
			dblr.DoltBinlogReplicaController.Close()
		}
		err = se.engine.Close()
	}
	if se.provider != nil {
		se.provider.Close()
	}
	return err
}

// configureBinlogReplicaController configures the binlog replication controller with the |engine|.
func configureBinlogReplicaController(config *SqlEngineConfig, engine *gms.Engine, ctxFactory sql.ContextFactory, session *dsess.DoltSession) error {
	executionCtx := ctxFactory(context.Background(), sql.WithSession(session))
	dblr.DoltBinlogReplicaController.SetExecutionContext(executionCtx)
	dblr.DoltBinlogReplicaController.SetEngine(engine)
	engine.Analyzer.Catalog.BinlogReplicaController = config.BinlogReplicaController

	return nil
}

// configureBinlogPrimaryController configures the |engine| to use the default Dolt binlog primary controller, as well
// as enabling the binlog producer if @@log_bin has been set to 1.
//
// NOTE: By default, binary logging for Dolt is not enabled, which differs from MySQL's @@log_bin default. Dolt's
// binary logging is initially an opt-in feature, but we may change that after measuring and tuning the
// performance hit that binary logging adds.
func configureBinlogPrimaryController(engine *gms.Engine) error {
	primaryController := dblr.NewDoltBinlogPrimaryController()
	engine.Analyzer.Catalog.BinlogPrimaryController = primaryController
	return nil
}

// configureEventScheduler configures the event scheduler with the |engine| for executing events, a |sessFactory|
// for creating sessions, and a DoltDatabaseProvider, |pro|.
func configureEventScheduler(config *SqlEngineConfig, engine *gms.Engine, ctxFactory sql.ContextFactory, sessFactory sessionFactory, pro *dsqle.DoltDatabaseProvider) error {
	// getCtxFunc is used to create new session with a new context for event scheduler.
	getCtxFunc := func() (*sql.Context, error) {
		sess, err := sessFactory(sql.NewBaseSession(), pro)
		if err != nil {
			return nil, err
		}
		return ctxFactory(context.Background(), sql.WithSession(sess)), nil
	}

	// A hidden env var allows overriding the event scheduler period for testing. This option is not
	// exposed via configuration because we do not want to encourage customers to use it. If the value
	// is equal to or less than 0, then the period is ignored and the default period, 30s, is used.
	eventSchedulerPeriod := 0
	eventSchedulerPeriodEnvVar := "DOLT_EVENT_SCHEDULER_PERIOD"
	if s, ok := os.LookupEnv(eventSchedulerPeriodEnvVar); ok {
		i, err := strconv.Atoi(s)
		if err != nil {
			logrus.Warnf("unable to parse value '%s' from env var '%s' as an integer", s, eventSchedulerPeriodEnvVar)
		} else {
			logrus.Warnf("overriding Dolt event scheduler period to %d seconds", i)
			eventSchedulerPeriod = i
		}
	}

	return engine.InitializeEventScheduler(getCtxFunc, config.EventSchedulerStatus, eventSchedulerPeriod)
}

// sqlContextFactory returns a contextFactory that creates a new sql.Context with the given session
func sqlContextFactory(ctx context.Context, opts ...sql.ContextOption) *sql.Context {
	ctx = valctx.WithContextValidation(ctx)
	sqlCtx := sql.NewContext(ctx, opts...)
	if sqlCtx.Session != nil {
		valctx.SetContextValidation(ctx, dsess.DSessFromSess(sqlCtx.Session).Validate)
	}
	return sqlCtx
}

// doltSessionFactory returns a sessionFactory that creates a new DoltSession
func doltSessionFactory(pro *dsqle.DoltDatabaseProvider, statsPro sql.StatsProvider, config config.ReadWriteConfig, bc *branch_control.Controller, gcSafepointController *gcctx.GCSafepointController, autocommit bool) sessionFactory {
	return func(mysqlSess *sql.BaseSession, provider sql.DatabaseProvider) (*dsess.DoltSession, error) {
		doltSession, err := dsess.NewDoltSession(mysqlSess, pro, config, bc, statsPro, writer.NewWriteSession, gcSafepointController)
		if err != nil {
			return nil, err
		}

		// nil ctx is actually fine in this context, not used in setting a session variable. Creating a new context isn't
		// free, and would be throwaway work, since we need to create a session before creating a sql.Context for user work.
		err = doltSession.SetSessionVariable(nil, sql.AutoCommitSessionVar, autocommit)
		if err != nil {
			return nil, err
		}

		return doltSession, nil
	}
}

type ConfigOption func(*SqlEngineConfig)

// NewSqlEngineForEnv returns a SqlEngine configured for the environment provided, with a single root user.
// Returns the new engine, the first database name, and any error that occurred.
func NewSqlEngineForEnv(ctx context.Context, dEnv *env.DoltEnv, options ...ConfigOption) (*SqlEngine, string, error) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return nil, "", err
	}

	config := &SqlEngineConfig{
		ServerUser: "root",
		ServerHost: "localhost",
	}
	for _, opt := range options {
		opt(config)
	}

	engine, err := NewSqlEngine(
		ctx,
		mrEnv,
		config,
	)
	if err != nil {
		return nil, "", err
	}

	if err := engine.InitStats(ctx); err != nil {
		return nil, "", err
	}

	return engine, mrEnv.GetFirstDatabase(), err
}
