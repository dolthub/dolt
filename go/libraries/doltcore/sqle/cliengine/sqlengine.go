package cliengine

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/vitess/go/vt/sqlparser"
	"os"
	"runtime"
	"strings"
)

type SqlEngine struct {
	dbs            map[string]dsqle.SqlDatabase
	sess           *dsess.DoltSession
	contextFactory func(ctx context.Context) (*sql.Context, error)
	engine         *sqle.Engine
	resultFormat   PrintResultFormat
}

// sqlEngine packages up the context necessary to run sql queries against dsqle.
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

func NewRebasedSqlEngine(engine *sqle.Engine, dbs map[string]dsqle.SqlDatabase) *SqlEngine {
	return &SqlEngine{
		dbs: dbs,
		engine: engine,
	}
}

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

func (se *SqlEngine) NewContext(ctx context.Context) (*sql.Context, error) {
	return se.contextFactory(ctx)
}

func (se *SqlEngine) GetReturnFormat() PrintResultFormat {
	return se.resultFormat
}

// Execute a SQL statement and return values for printing.
func (se *SqlEngine) Query(ctx *sql.Context, query string) (sql.Schema, sql.RowIter, error) {
	return se.engine.Query(ctx, query)
}

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
