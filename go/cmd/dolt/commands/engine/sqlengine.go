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
	"runtime"
	"strings"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	_ "github.com/dolthub/go-mysql-server/sql/variables"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/branch_control"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	dblr "github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/mysql_file_handler"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/types"
)

// SqlEngine packages up the context necessary to run sql queries against dsqle.
type SqlEngine struct {
	provider       sql.DatabaseProvider
	contextFactory contextFactory
	dsessFactory   sessionFactory
	engine         *gms.Engine
	resultFormat   PrintResultFormat
}

type sessionFactory func(mysqlSess *sql.BaseSession, pro sql.DatabaseProvider) (*dsess.DoltSession, error)
type contextFactory func(ctx context.Context, session sql.Session) (*sql.Context, error)

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
	Bulk                    bool
	JwksConfig              []JwksConfig
	ClusterController       *cluster.Controller
	BinlogReplicaController binlogreplication.BinlogReplicaController
}

// NewSqlEngine returns a SqlEngine
func NewSqlEngine(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	format PrintResultFormat,
	config *SqlEngineConfig,
) (*SqlEngine, error) {
	if ok, _ := mrEnv.IsLocked(); ok {
		config.IsServerLocked = true
	}

	dbs, locations, err := CollectDBs(ctx, mrEnv, config.Bulk)
	if err != nil {
		return nil, err
	}

	nbf := types.Format_Default
	if len(dbs) > 0 {
		nbf = dbs[0].DbData().Ddb.Format()
	}
	parallelism := runtime.GOMAXPROCS(0)
	if types.IsFormat_DOLT(nbf) {
		parallelism = 1
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

	all := append(dbs)

	clusterDB := config.ClusterController.ClusterDatabase()
	if clusterDB != nil {
		all = append(all, clusterDB.(dsqle.SqlDatabase))
		locations = append(locations, nil)
	}

	b := env.GetDefaultInitBranch(mrEnv.Config())
	pro, err := dsqle.NewDoltDatabaseProviderWithDatabases(b, mrEnv.FileSystem(), all, locations)
	if err != nil {
		return nil, err
	}
	pro = pro.WithRemoteDialer(mrEnv.RemoteDialProvider())

	config.ClusterController.RegisterStoredProcedures(pro)
	pro.InitDatabaseHook = cluster.NewInitDatabaseHook(config.ClusterController, bThreads, pro.InitDatabaseHook)
	config.ClusterController.ManageDatabaseProvider(pro)

	// Load in privileges from file, if it exists
	persister := mysql_file_handler.NewPersister(config.PrivFilePath, config.DoltCfgDirPath)
	data, err := persister.LoadData()
	if err != nil {
		return nil, err
	}

	// Load the branch control permissions, if they exist
	var bcController *branch_control.Controller
	if bcController, err = branch_control.LoadData(config.BranchCtrlFilePath, config.DoltCfgDirPath); err != nil {
		return nil, err
	}

	// Set up engine
	engine := gms.New(analyzer.NewBuilder(pro).WithParallelism(parallelism).Build(), &gms.Config{
		IsReadOnly:     config.IsReadOnly,
		IsServerLocked: config.IsServerLocked,
	}).WithBackgroundThreads(bThreads)
	engine.Analyzer.Catalog.MySQLDb.SetPersister(persister)

	engine.Analyzer.Catalog.MySQLDb.SetPlugins(map[string]mysql_db.PlaintextAuthPlugin{
		"authentication_dolt_jwt": NewAuthenticateDoltJWTPlugin(config.JwksConfig),
	})

	// Load MySQL Db information
	if err = engine.Analyzer.Catalog.MySQLDb.LoadData(sql.NewEmptyContext(), data); err != nil {
		return nil, err
	}

	if dbg, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG"); ok && strings.ToLower(dbg) == "true" {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG_VERBOSE"); ok && strings.ToLower(verbose) == "true" {
			engine.Analyzer.Verbose = true
		}
	}

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	sessionFactory := doltSessionFactory(pro, mrEnv.Config(), bcController, config.Autocommit)

	if config.BinlogReplicaController != nil {
		binLogSession, err := sessionFactory(sql.NewBaseSession(), pro)
		if err != nil {
			return nil, err
		}

		err = configureBinlogReplicaController(config, engine, binLogSession)
		if err != nil {
			return nil, err
		}
	}

	return &SqlEngine{
		provider:       pro,
		contextFactory: sqlContextFactory(),
		dsessFactory:   sessionFactory,
		engine:         engine,
		resultFormat:   format,
	}, nil
}

// NewRebasedSqlEngine returns a smalled rebased engine primarily used in filterbranch.
// TODO: migrate to provider
func NewRebasedSqlEngine(engine *gms.Engine, dbs map[string]dsqle.SqlDatabase) *SqlEngine {
	return &SqlEngine{
		engine: engine,
	}
}

// Databases returns a slice of all databases in the engine
func (se *SqlEngine) Databases(ctx *sql.Context) []dsqle.SqlDatabase {
	databases := se.provider.AllDatabases(ctx)
	dbs := make([]dsqle.SqlDatabase, len(databases))
	for i := range databases {
		dbs[i] = databases[i].(dsqle.SqlDatabase)
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

// GetResultFormat returns the printing format of the engine. The format isn't used by the engine internally, only
// stored for reference by clients who wish to use it to print results.
func (se *SqlEngine) GetResultFormat() PrintResultFormat {
	return se.resultFormat
}

// Query execute a SQL statement and return values for printing.
func (se *SqlEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return se.engine.Query(ctx, query)
}

// Analyze analyzes a node.
func (se *SqlEngine) Analyze(ctx *sql.Context, n sql.Node) (sql.Node, error) {
	return se.engine.Analyzer.Analyze(ctx, n, nil)
}

// TODO: All of this logic should be moved to the engine...
func (se *SqlEngine) Dbddl(ctx *sql.Context, dbddl *sqlparser.DBDDL, query string) (sql.Schema, sql.RowIter, error) {
	action := strings.ToLower(dbddl.Action)
	var rowIter sql.RowIter = nil
	var err error = nil

	if action != sqlparser.CreateStr && action != sqlparser.DropStr {
		return nil, nil, fmt.Errorf("Unhandled DBDDL action %v in Query %v", action, query)
	}

	if action == sqlparser.DropStr {
		// Should not be allowed to delete repo name and information schema
		if dbddl.DBName == sql.InformationSchemaDatabaseName {
			return nil, nil, fmt.Errorf("DROP DATABASE isn't supported for database %s", sql.InformationSchemaDatabaseName)
		}
	}

	sch, rowIter, err := se.Query(ctx, query)

	if rowIter != nil {
		err = rowIter.Close(ctx)
		if err != nil {
			return nil, nil, err
		}
	}

	if err != nil {
		return nil, nil, err
	}

	return sch, nil, nil
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
	contextFactory := sqlContextFactory()

	executionCtx, err := contextFactory(context.Background(), session)
	if err != nil {
		return err
	}
	executionCtx.SetClient(sql.Client{
		User:    "root",
		Address: "localhost",
	})

	dblr.DoltBinlogReplicaController.SetExecutionContext(executionCtx)
	engine.Analyzer.BinlogReplicaController = config.BinlogReplicaController

	return nil
}

// sqlContextFactory returns a contextFactory that creates a new sql.Context with the initial database provided
func sqlContextFactory() contextFactory {
	return func(ctx context.Context, session sql.Session) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx, sql.WithSession(session))
		return sqlCtx, nil
	}
}

// doltSessionFactory returns a sessionFactory that creates a new DoltSession
func doltSessionFactory(pro dsqle.DoltDatabaseProvider, config config.ReadWriteConfig, bc *branch_control.Controller, autocommit bool) sessionFactory {
	return func(mysqlSess *sql.BaseSession, provider sql.DatabaseProvider) (*dsess.DoltSession, error) {
		dsess, err := dsess.NewDoltSession(mysqlSess, pro, config, bc)
		if err != nil {
			return nil, err
		}

		// nil ctx is actually fine in this context, not used in setting a session variable. Creating a new context isn't
		// free, and would be throwaway work, since we need to create a session before creating a sql.Context for user work.
		err = dsess.SetSessionVariable(nil, sql.AutoCommitSessionVar, autocommit)
		if err != nil {
			return nil, err
		}

		return dsess, nil
	}
}

// NewSqlEngineForEnv returns a SqlEngine configured for the environment provided, with a single root user.
// Returns the new engine, the first database name, and any error that occurred.
func NewSqlEngineForEnv(ctx context.Context, dEnv *env.DoltEnv) (*SqlEngine, string, error) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	if err != nil {
		return nil, "", err
	}

	engine, err := NewSqlEngine(
		ctx,
		mrEnv,
		FormatCsv,
		&SqlEngineConfig{
			ServerUser: "root",
			ServerHost: "localhost",
		},
	)

	return engine, mrEnv.GetFirstDatabase(), err
}
