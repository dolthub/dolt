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
)

// SqlEngine packages up the context necessary to run sql queries against dsqle.
type SqlEngine struct {
	provider       sql.DatabaseProvider
	contextFactory contextFactory
	dsessFactory   sessionFactory
	engine         *gms.Engine
	resultFormat   PrintResultFormat
}

type sessionFactory func(ctx *sql.Context, mysqlSess *sql.BaseSession, pro sql.DatabaseProvider) (*dsess.DoltSession, error)
type contextFactory func(ctx context.Context) (*sql.Context, error)

type SqlEngineConfig struct {
	InitialDb               string
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

	parallelism := runtime.GOMAXPROCS(0)

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

	sess, err := dsess.NewDoltSession(sql.NewEmptyContext(), sql.NewBaseSession(), pro, mrEnv.Config(), bcController)
	if err != nil {
		return nil, err
	}

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	// TODO: this should just be the session default like it is with MySQL
	err = sess.SetSessionVariable(sql.NewContext(ctx), sql.AutoCommitSessionVar, config.Autocommit)
	if err != nil {
		return nil, err
	}

	configureBinlogReplicaController(config, engine, sess)

	return &SqlEngine{
		provider:       pro,
		contextFactory: newSqlContext(sess, config.InitialDb),
		dsessFactory:   newDoltSession(pro, mrEnv.Config(), config.Autocommit, bcController),
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

// Databases() returns a list of all databases in the engine
func (se *SqlEngine) Databases(ctx *sql.Context) []dsqle.SqlDatabase {
	databases := se.provider.AllDatabases(ctx)
	dbs := make([]dsqle.SqlDatabase, len(databases))
	for i := range databases {
		dbs[i] = databases[i].(dsqle.SqlDatabase)
	}

	return nil
}

// NewContext converts a context.Context to a sql.Context.
// TODO: investigate uses of this
func (se *SqlEngine) NewContext(ctx context.Context) (*sql.Context, error) {
	return se.contextFactory(ctx)
}

func (se *SqlEngine) NewDoltSession(ctx context.Context, mysqlSess *sql.BaseSession) (*dsess.DoltSession, error) {
	// TODO: this seems wasteful, we are creating a context for very little work here
	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}
	return se.dsessFactory(sqlCtx, mysqlSess, se.provider)
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

// configureBinlogReplicaController examines the specified |config| and if a binlog replica controller is provided,
// it creates a new context from the specified |sess| for the replia's applier to use, and it configures the
// binlog replica controller with the |engine|.
func configureBinlogReplicaController(config *SqlEngineConfig, engine *gms.Engine, sess *dsess.DoltSession) error {
	if config.BinlogReplicaController == nil {
		return nil
	}

	contextFactory := newSqlContext(sess, config.InitialDb)
	newCtx, err := contextFactory(context.Background())
	if err != nil {
		return err
	}
	newCtx.SetClient(sql.Client{
		User:    "root",
		Address: "localhost",
	})
	dblr.DoltBinlogReplicaController.SetExecutionContext(newCtx)
	engine.Analyzer.BinlogReplicaController = config.BinlogReplicaController

	return nil
}

func newSqlContext(sess *dsess.DoltSession, initialDb string) func(ctx context.Context) (*sql.Context, error) {
	return func(ctx context.Context) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx, sql.WithSession(sess))

		// If the session was already updated with a database then continue using it in the new session. Otherwise
		// use the initial one.
		if sessionDB := sess.GetCurrentDatabase(); sessionDB != "" {
			sqlCtx.SetCurrentDatabase(sessionDB)
		} else {
			sqlCtx.SetCurrentDatabase(initialDb)
		}

		return sqlCtx, nil
	}
}

// TODO: this should not require autocommit, that should be handled by the session default
func newDoltSession(
	pro dsqle.DoltDatabaseProvider,
	config config.ReadWriteConfig,
	autocommit bool,
	bc *branch_control.Controller,
) sessionFactory {
	return func(ctx *sql.Context, mysqlSess *sql.BaseSession, provider sql.DatabaseProvider) (*dsess.DoltSession, error) {

		dsess, err := dsess.NewDoltSession(sql.NewEmptyContext(), mysqlSess, pro, config, bc)
		if err != nil {
			return nil, err
		}

		// TODO: this should just be the session default like it is with MySQL
		err = dsess.SetSessionVariable(sql.NewContext(ctx), sql.AutoCommitSessionVar, autocommit)
		if err != nil {
			return nil, err
		}

		return dsess, nil
	}
}

// NewSqlEngineForEnv returns a SqlEngine configured for the environment provided, with a single root user
func NewSqlEngineForEnv(ctx context.Context, dEnv *env.DoltEnv) (*SqlEngine, error) {
	mrEnv, err := env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), dEnv.FS, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
	if err != nil {
		return nil, err
	}

	// Choose the first DB as the current one. This will be the DB in the working dir if there was one there
	var dbName string
	err = mrEnv.Iter(func(name string, _ *env.DoltEnv) (stop bool, err error) {
		dbName = name
		return true, nil
	})
	if err != nil {
		return nil, err
	}

	return NewSqlEngine(
		ctx,
		mrEnv,
		FormatCsv,
		&SqlEngineConfig{
			InitialDb:  dbName,
			IsReadOnly: false,
			ServerUser: "root",
			ServerPass: "",
			ServerHost: "localhost",
			Autocommit: false,
		},
	)
}

// NewLocalSqlContext returns a new |sql.Context| using the engine provided, with its client set to |root|
func NewLocalSqlContext(ctx context.Context, se *SqlEngine) (*sql.Context, error) {
	sqlCtx, err := se.NewContext(ctx)
	if err != nil {
		return nil, err
	}

	sqlCtx.Session.SetClient(sql.Client{User: "root", Address: "%", Capabilities: 0})
	return sqlCtx, nil
}
