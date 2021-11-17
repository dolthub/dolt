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

package cliengine

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"strings"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/vitess/go/vt/sqlparser"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
)

// SqlEngine packages up the context necessary to run sql queries against dsqle.
type SqlEngine struct {
	dbs            map[string]dsqle.SqlDatabase
	sess           *dsess.DoltSession
	contextFactory func(ctx context.Context) (*sql.Context, error)
	engine         *sqle.Engine
	resultFormat   PrintResultFormat
}

// NewSqlEngine returns a SqlEngine
func NewSqlEngine(
	ctx context.Context,
	mrEnv *env.MultiRepoEnv,
	format PrintResultFormat,
	initialDb string) (*SqlEngine, error) {
	au := new(auth.None)

	parallelism := runtime.GOMAXPROCS(0)

	dbs, err := CollectDBs(ctx, mrEnv)
	if err != nil {
		return nil, err
	}

	infoDB := information_schema.NewInformationSchemaDatabase()
	all := append(dsqleDBsAsSqlDBs(dbs), infoDB)

	pro := dsqle.NewDoltDatabaseProvider(mrEnv.Config(), mrEnv.FileSystem(), all...)

	engine := sqle.New(analyzer.NewBuilder(pro).WithParallelism(
		parallelism).Build(), &sqle.Config{Auth: au})

	if dbg, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG"); ok && strings.ToLower(dbg) == "true" {
		engine.Analyzer.Debug = true
		if verbose, ok := os.LookupEnv("DOLT_SQL_DEBUG_LOG_VERBOSE"); ok && strings.ToLower(verbose) == "true" {
			engine.Analyzer.Verbose = true
		}
	}

	nameToDB := make(map[string]dsqle.SqlDatabase)
	var dbStates []dsess.InitialDbState
	for _, db := range dbs {
		nameToDB[db.Name()] = db

		dbState, err := dsqle.GetInitialDBState(ctx, db)
		if err != nil {
			return nil, err
		}

		dbStates = append(dbStates, dbState)
	}

	sess, err := dsess.NewDoltSession(sql.NewEmptyContext(), sql.NewBaseSession(), pro, mrEnv.Config(), dbStates...)
	if err != nil {
		return nil, err
	}

	// this is overwritten only for server sessions
	for _, db := range dbs {
		db.DbData().Ddb.SetCommitHookLogger(ctx, cli.CliOut)
	}

	// TODO: this should just be the session default like it is with MySQL
	err = sess.SetSessionVariable(sql.NewContext(ctx), sql.AutoCommitSessionVar, true)
	if err != nil {
		return nil, err
	}

	return &SqlEngine{
		dbs:            nameToDB,
		sess:           sess,
		contextFactory: newSqlContext(sess, initialDb),
		engine:         engine,
		resultFormat:   format,
	}, nil
}

// NewRebasedEngine returns a smalled rebased engine primarily used in filterbranch.
func NewRebasedSqlEngine(engine *sqle.Engine, dbs map[string]dsqle.SqlDatabase) *SqlEngine {
	return &SqlEngine{
		dbs:    dbs,
		engine: engine,
	}
}

// IterDBs iterates over the set of databases the engine wraps.
func (se *SqlEngine) IterDBs(cb func(name string, db dsqle.SqlDatabase) (stop bool, err error)) error {
	for name, db := range se.dbs {
		stop, err := cb(name, db)

		if err != nil {
			return err
		}

		if stop {
			break
		}
	}

	return nil
}

// GetRoots returns the underlying roots values the engine read/writes to.
func (se *SqlEngine) GetRoots(sqlCtx *sql.Context) (map[string]*doltdb.RootValue, error) {
	newRoots := make(map[string]*doltdb.RootValue)
	for name, db := range se.dbs {
		var err error
		newRoots[name], err = db.GetRoot(sqlCtx)

		if err != nil {
			return nil, err
		}
	}

	return newRoots, nil
}

// NewContext converts a context.Context to a sql.Context.
func (se *SqlEngine) NewContext(ctx context.Context) (*sql.Context, error) {
	return se.contextFactory(ctx)
}

// GetReturnFormat() returns the printing format the engine is associated with.
func (se *SqlEngine) GetReturnFormat() PrintResultFormat {
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

func (se *SqlEngine) GetAnalyzer() *analyzer.Analyzer {
	return se.engine.Analyzer
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
		if dbddl.DBName == information_schema.InformationSchemaDatabaseName {
			return nil, nil, fmt.Errorf("DROP DATABASE isn't supported for database %s", information_schema.InformationSchemaDatabaseName)
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

func (se *SqlEngine) SetBatchMode() {
	se.sess.EnableBatchedMode()
}

func dsqleDBsAsSqlDBs(dbs []dsqle.SqlDatabase) []sql.Database {
	sqlDbs := make([]sql.Database, 0, len(dbs))
	for _, db := range dbs {
		sqlDbs = append(sqlDbs, db)
	}
	return sqlDbs
}

func newSqlContext(sess *dsess.DoltSession, initialDb string) func(ctx context.Context) (*sql.Context, error) {
	return func(ctx context.Context) (*sql.Context, error) {
		sqlCtx := sql.NewContext(ctx,
			sql.WithSession(sess),
			sql.WithTracer(tracing.Tracer(ctx)))

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
