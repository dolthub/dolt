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

package sqlserver

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/auth"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/analyzer"
	"github.com/dolthub/go-mysql-server/sql/information_schema"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/utils/tracing"
)

// Serve starts a MySQL-compatible server. Returns any errors that were encountered.
func Serve(ctx context.Context, version string, serverConfig ServerConfig, serverController *ServerController, dEnv *env.DoltEnv) (startError error, closeError error) {
	if serverConfig == nil {
		cli.Println("No configuration given, using defaults")
		serverConfig = DefaultServerConfig()
	}

	// Code is easier to work through if we assume that serverController is never nil
	if serverController == nil {
		serverController = CreateServerController()
	}

	var mySQLServer *server.Server
	// This guarantees unblocking on any routines with a waiting `ServerController`
	defer func() {
		if mySQLServer != nil {
			serverController.registerCloseFunction(startError, mySQLServer.Close)
		} else {
			serverController.registerCloseFunction(startError, func() error { return nil })
		}
		serverController.StopServer()
		serverController.serverStopped(closeError)
	}()

	if startError = ValidateConfig(serverConfig); startError != nil {
		return startError, nil
	}

	if serverConfig.LogLevel() != LogLevel_Info {
		var level logrus.Level
		level, startError = logrus.ParseLevel(serverConfig.LogLevel().String())
		if startError != nil {
			cli.PrintErr(startError)
			return
		}
		logrus.SetLevel(level)
	}

	permissions := auth.AllPermissions
	if serverConfig.ReadOnly() {
		permissions = auth.ReadPerm
	}

	userAuth := auth.NewAudit(auth.NewNativeSingle(serverConfig.User(), serverConfig.Password(), permissions), auth.NewAuditLog(logrus.StandardLogger()))

	c := sql.NewCatalog()
	a := analyzer.NewBuilder(c).WithParallelism(serverConfig.QueryParallelism()).Build()
	sqlEngine := sqle.New(c, a, nil)

	err := sqlEngine.Catalog.Register(dfunctions.DoltFunctions...)

	if err != nil {
		return nil, err
	}

	var username string
	var email string
	var mrEnv env.MultiRepoEnv
	dbNamesAndPaths := serverConfig.DatabaseNamesAndPaths()
	if len(dbNamesAndPaths) == 0 {
		var err error
		mrEnv, err = env.DoltEnvAsMultiEnv(dEnv)
		if err != nil {
			return err, nil
		}

		username = *dEnv.Config.GetStringOrDefault(env.UserNameKey, "")
		email = *dEnv.Config.GetStringOrDefault(env.UserEmailKey, "")
	} else {
		var err error
		mrEnv, err = env.LoadMultiEnv(ctx, env.GetCurrentUserHomeDir, dEnv.FS, version, dbNamesAndPaths...)

		if err != nil {
			return err, nil
		}
	}

	dbs := commands.CollectDBs(mrEnv)

	for _, db := range dbs {
		sqlEngine.AddDatabase(db)
	}

	sqlEngine.AddDatabase(information_schema.NewInformationSchemaDatabase(sqlEngine.Catalog))

	portAsString := strconv.Itoa(serverConfig.Port())
	hostPort := net.JoinHostPort(serverConfig.Host(), portAsString)

	if portInUse(hostPort) {
		portInUseError := fmt.Errorf("Port %s already in use.", portAsString)
		return portInUseError, nil
	}

	readTimeout := time.Duration(serverConfig.ReadTimeout()) * time.Millisecond
	writeTimeout := time.Duration(serverConfig.WriteTimeout()) * time.Millisecond
	mySQLServer, startError = server.NewServer(
		server.Config{
			Protocol:         "tcp",
			Address:          hostPort,
			Auth:             userAuth,
			ConnReadTimeout:  readTimeout,
			ConnWriteTimeout: writeTimeout,
			MaxConnections:   serverConfig.MaxConnections(),
			// Do not set the value of Version.  Let it default to what go-mysql-server uses.  This should be equivalent
			// to the value of mysql that we support.
		},
		sqlEngine,
		newSessionBuilder(sqlEngine, username, email, mrEnv, serverConfig.AutoCommit()),
	)

	if startError != nil {
		cli.PrintErr(startError)
		return
	}

	serverController.registerCloseFunction(startError, mySQLServer.Close)
	closeError = mySQLServer.Start()
	if closeError != nil {
		cli.PrintErr(closeError)
		return
	}
	return
}

func portInUse(hostPort string) bool {
	timeout := time.Second
	conn, _ := net.DialTimeout("tcp", hostPort, timeout)
	if conn != nil {
		defer conn.Close()
		return true
	}
	return false
}

func newSessionBuilder(sqlEngine *sqle.Engine, username, email string, mrEnv env.MultiRepoEnv, autocommit bool) server.SessionBuilder {
	return func(ctx context.Context, conn *mysql.Conn, host string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error) {
		tmpSqlCtx := sql.NewEmptyContext()
		mysqlSess := sql.NewSession(host, conn.RemoteAddr().String(), conn.User, conn.ConnectionID)

		doltDbs := dbsAsDSQLDBs(sqlEngine.Catalog.AllDatabases())
		dbStates, err := getDbStates(ctx, mrEnv, doltDbs)
		if err != nil {
			return nil, nil, nil, err
		}

		doltSess, err := dsqle.NewDoltSession(tmpSqlCtx, mysqlSess, username, email, dbStates...)

		if err != nil {
			return nil, nil, nil, err
		}

		err = doltSess.SetSessionVariable(tmpSqlCtx, sql.AutoCommitSessionVar, autocommit)

		if err != nil {
			return nil, nil, nil, err
		}

		ir := sql.NewIndexRegistry()
		vr := sql.NewViewRegistry()
		sqlCtx := sql.NewContext(
			ctx,
			sql.WithIndexRegistry(ir),
			sql.WithViewRegistry(vr),
			sql.WithSession(doltSess),
			sql.WithTracer(tracing.Tracer(ctx)))

		dbs := dbsAsDSQLDBs(sqlEngine.Catalog.AllDatabases())
		for _, db := range dbs {
			root, err := db.GetRoot(sqlCtx)
			if err != err {
				cli.PrintErrln(err)
				return nil, nil, nil, err
			}

			err = dsqle.RegisterSchemaFragments(sqlCtx, db, root)
			if err != nil {
				cli.PrintErr(err)
				return nil, nil, nil, err
			}
		}

		return doltSess, ir, vr, nil
	}
}

func dbsAsDSQLDBs(dbs []sql.Database) []dsqle.Database {
	dsqlDBs := make([]dsqle.Database, 0, len(dbs))

	for _, db := range dbs {
		dsqlDB, ok := db.(dsqle.Database)

		if ok {
			dsqlDBs = append(dsqlDBs, dsqlDB)
		}
	}

	return dsqlDBs
}

func getDbStates(ctx context.Context, mrEnv env.MultiRepoEnv, dbs []dsqle.Database) ([]dsqle.InitialDbState, error) {
	var dbStates []dsqle.InitialDbState

	for _, db := range dbs {
		var dEnv *env.DoltEnv
		mrEnv.Iter(func(name string, de *env.DoltEnv) (stop bool, err error) {
			if name == db.Name() {
				dEnv = de
				return true, nil
			}
			return false, nil
		})

		if dEnv == nil {
			return nil, fmt.Errorf("Couldn't find environment for database %s", db.Name())
		}

		roots, err := dEnv.Roots(ctx)
		if err != nil {
			return nil, err
		}

		head := dEnv.RepoStateReader().CWBHeadSpec()
		headCommit, err := dEnv.DoltDB.Resolve(ctx, head, nil)
		if err != nil {
			return nil, err
		}

		headRef := dEnv.RepoStateReader().CWBHeadRef()
		wsRef, err := ref.WorkingSetRefForHead(headRef)
		if err != nil {
			return nil, err
		}

		dbStates = append(dbStates, dsqle.InitialDbState{
			Db:         db,
			Roots:      roots,
			HeadCommit: headCommit,
			WorkingSet: wsRef,
		})
	}

	return dbStates, nil
}
