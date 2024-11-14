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
	"fmt"
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
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	dblr "github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/kvexec"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/mysql_file_handler"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statsnoms"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/statspro"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/utils/config"
)

// SqlEngine packages up the context necessary to run sql queries against dsqle.
type SqlEngine struct {
	provider       sql.DatabaseProvider
	contextFactory contextFactory
	dsessFactory   sessionFactory
	engine         *gms.Engine
}

type sessionFactory func(mysqlSess *sql.BaseSession, pro sql.DatabaseProvider) (*dsess.DoltSession, error)
type contextFactory func(ctx context.Context, session sql.Session) (*sql.Context, error)

type SystemVariables map[string]interface{}

type SqlEngineConfig struct {
	IsReadOnly              bool
	IsServerLocked          bool
	DoltCfgDirPath          string
	PrivFilePath            string
	BranchCtrlFilePath      string
	ServerUser              string
	ServerPass              string
	ServerHost              string
	Autocommit              bool
	DoltTransactionCommit   bool
	Bulk                    bool
	JwksConfig              []servercfg.JwksConfig
	SystemVariables         SystemVariables
	ClusterController       *cluster.Controller
	BinlogReplicaController binlogreplication.BinlogReplicaController
	EventSchedulerStatus    eventscheduler.SchedulerStatus
}

// NewSqlEngine returns a SqlEngine
func NewSqlEngine(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	config *SqlEngineConfig,
) (*SqlEngine, error) {
	dbs, locations, err := CollectDBs(ctx, mrEnv, config.Bulk)
	if err != nil {
		return nil, err
	}

	bThreads := sql.NewBackgroundThreads()
	dbs, err = dsqle.ApplyReplicationConfig(ctx, bThreads, mrEnv, cli.CliOut, dbs...)
	if err != nil {
		return nil, err
	}

	config.ClusterController.ManageSystemVariables(sql.SystemVariables)

	err = config.ClusterController.ApplyStandbyReplicationConfig(ctx, bThreads, mrEnv, dbs...)
	if err != nil {
		return nil, err
	}

	err = applySystemVariables(sql.SystemVariables, config.SystemVariables)
	if err != nil {
		return nil, err
	}

	all := dbs[:]

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

	statsPro := statspro.NewProvider(pro, statsnoms.NewNomsStatsFactory(mrEnv.RemoteDialProvider()))
	engine.Analyzer.Catalog.StatsProvider = statsPro

	engine.Analyzer.ExecBuilder = rowexec.NewOverrideBuilder(kvexec.Builder{})
	sessFactory := doltSessionFactory(pro, statsPro, mrEnv.Config(), bcController, config.Autocommit)
	sqlEngine.provider = pro
	sqlEngine.contextFactory = sqlContextFactory()
	sqlEngine.dsessFactory = sessFactory
	sqlEngine.engine = engine

	// configuring stats depends on sessionBuilder
	// sessionBuilder needs ref to statsProv
	if err = statsPro.Configure(ctx, sqlEngine.NewDefaultContext, bThreads, dbs); err != nil {
		fmt.Fprintln(cli.CliErr, err)
	}

	// Load MySQL Db information
	if err = engine.Analyzer.Catalog.MySQLDb.LoadData(sql.NewEmptyContext(), data); err != nil {
		return nil, err
	}

	if dbg, ok := os.LookupEnv(dconfig.EnvSqlDebugLog); ok && strings.EqualFold(dbg, "true") {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv(dconfig.EnvSqlDebugLogVerbose); ok && strings.EqualFold(verbose, "true") {
			engine.Analyzer.Verbose = true
		}
	}

	err = sql.SystemVariables.SetGlobal(dsess.DoltCommitOnTransactionCommit, config.DoltTransactionCommit)
	if err != nil {
		return nil, err
	}

	if engine.EventScheduler == nil {
		err = configureEventScheduler(config, engine, sessFactory, pro)
		if err != nil {
			return nil, err
		}
	}

	if config.BinlogReplicaController != nil {
		binLogSession, err := sessFactory(sql.NewBaseSession(), pro)
		if err != nil {
			return nil, err
		}

		err = configureBinlogReplicaController(config, engine, binLogSession)
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
	return se.contextFactory(ctx, session)
}

// NewDefaultContext returns a new sql.Context with a new default dolt session.
func (se *SqlEngine) NewDefaultContext(ctx context.Context) (*sql.Context, error) {
	session, err := se.NewDoltSession(ctx, sql.NewBaseSession())
	if err != nil {
		return nil, err
	}
	return se.contextFactory(ctx, session)
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

func (se *SqlEngine) Close() error {
	if se.engine != nil {
		return se.engine.Close()
	}
	return nil
}

// configureBinlogReplicaController configures the binlog replication controller with the |engine|.
func configureBinlogReplicaController(config *SqlEngineConfig, engine *gms.Engine, session *dsess.DoltSession) error {
	ctxFactory := sqlContextFactory()
	executionCtx, err := ctxFactory(context.Background(), session)
	if err != nil {
		return err
	}
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
func configureEventScheduler(config *SqlEngineConfig, engine *gms.Engine, sessFactory sessionFactory, pro *dsqle.DoltDatabaseProvider) error {
	// need to give correct user, use the definer as user to run the event definition queries
	ctxFactory := sqlContextFactory()

	// getCtxFunc is used to create new session context for event scheduler.
	// It starts a transaction that needs to be committed using the function returned.
	getCtxFunc := func() (*sql.Context, func() error, error) {
		sess, err := sessFactory(sql.NewBaseSession(), pro)
		if err != nil {
			return nil, func() error { return nil }, err
		}

		newCtx, err := ctxFactory(context.Background(), sess)
		if err != nil {
			return nil, func() error { return nil }, err
		}

		ts, ok := newCtx.Session.(sql.TransactionSession)
		if !ok {
			return nil, func() error { return nil }, nil
		}

		tr, err := sess.StartTransaction(newCtx, sql.ReadWrite)
		if err != nil {
			return nil, func() error { return nil }, err
		}

		ts.SetTransaction(tr)

		return newCtx, func() error {
			return ts.CommitTransaction(newCtx, tr)
		}, nil
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

// sqlContextFactory returns a contextFactory that creates a new sql.Context with the initial database provided
func sqlContextFactory() contextFactory {
	return func(ctx context.Context, session sql.Session) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
		return sqlCtx, nil
	}
}

// doltSessionFactory returns a sessionFactory that creates a new DoltSession
func doltSessionFactory(pro *dsqle.DoltDatabaseProvider, statsPro sql.StatsProvider, config config.ReadWriteConfig, bc *branch_control.Controller, autocommit bool) sessionFactory {
	return func(mysqlSess *sql.BaseSession, provider sql.DatabaseProvider) (*dsess.DoltSession, error) {
		doltSession, err := dsess.NewDoltSession(mysqlSess, pro, config, bc, statsPro, writer.NewWriteSession)
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

// NewSqlEngineForEnv returns a SqlEngine configured for the environment provided, with a single root user.
// Returns the new engine, the first database name, and any error that occurred.
func NewSqlEngineForEnv(ctx context.Context, dEnv *env.DoltEnv) (*SqlEngine, string, error) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv)
	if err != nil {
		return nil, "", err
	}

	engine, err := NewSqlEngine(
		ctx,
		mrEnv,
		&SqlEngineConfig{
			ServerUser: "root",
			ServerHost: "localhost",
		},
	)

	return engine, mrEnv.GetFirstDatabase(), err
}
