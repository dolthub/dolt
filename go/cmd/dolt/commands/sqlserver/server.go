// Copyright 2019-2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"net"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	dsqle "github.com/liquidata-inc/dolt/go/libraries/doltcore/sqle"
)



// Serve starts a MySQL-compatible server. Returns any errors that were encountered.
func Serve(ctx context.Context, serverConfig *ServerConfig, root *doltdb.RootValue, serverController *ServerController, dEnv *env.DoltEnv) (startError error, closeError error) {
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

	if startError = serverConfig.Validate(); startError != nil {
		cli.PrintErr(startError)
		return
	}
	if serverConfig.LogLevel != LogLevel_Info {
		var level logrus.Level
		level, startError = logrus.ParseLevel(serverConfig.LogLevel.String())
		if startError != nil {
			cli.PrintErr(startError)
			return
		}
		logrus.SetLevel(level)
	}

	permissions := auth.AllPermissions
	if serverConfig.ReadOnly {
		permissions = auth.ReadPerm
	}

	userAuth := auth.NewAudit(auth.NewNativeSingle(serverConfig.User, serverConfig.Password, permissions), auth.NewAuditLog(logrus.StandardLogger()))
	sqlEngine := sqle.NewDefault()
	db := dsqle.NewDatabase("dolt", root, dEnv.DoltDB, dEnv.RepoState)
	sqlEngine.AddDatabase(db)
	sqlEngine.AddDatabase(sql.NewInformationSchemaDatabase(sqlEngine.Catalog))

	idxDriver := dsqle.NewDoltIndexDriver(db)

	hostPort := net.JoinHostPort(serverConfig.Host, strconv.Itoa(serverConfig.Port))
	timeout := time.Second * time.Duration(serverConfig.Timeout)
	mySQLServer, startError = server.NewServer(
		server.Config{
			Protocol:         "tcp",
			Address:          hostPort,
			Auth:             userAuth,
			ConnReadTimeout:  timeout,
			ConnWriteTimeout: timeout,
		},
		sqlEngine,
		func(ctx context.Context, conn *mysql.Conn, host string) (sql.Session, *sql.IndexRegistry, *sql.ViewRegistry, error) {
			mysqlSess := sql.NewSession(host, conn.RemoteAddr().String(), conn.User, conn.ConnectionID)
			doltSess, err := dsqle.NewSessionWithDefaultRoots(mysqlSess, dbsAsDSQLDBs(sqlEngine.Catalog.AllDatabases())...)

			if err != nil {
				return nil, nil, nil, err
			}

			ir := sql.NewIndexRegistry()
			vr := sql.NewViewRegistry()
			sqlCtx := sql.NewContext(
				ctx,
				sql.WithIndexRegistry(ir),
				sql.WithViewRegistry(vr),
				sql.WithSession(doltSess))

			ir.RegisterIndexDriver(idxDriver)
			err = ir.LoadIndexes(sqlCtx, sqlEngine.Catalog.AllDatabases())

			if err != nil {
				return nil, nil, nil, err
			}

			err = dsqle.RegisterSchemaFragments(sqlCtx, db, db.GetDefaultRoot())
			if startError != nil {
				cli.PrintErr(startError)
				return nil, nil, nil, err
			}

			return doltSess, ir, vr, nil
		},
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

func dbsAsDSQLDBs(dbs []sql.Database) []dsqle.Database {
	dsqlDBs := make([]dsqle.Database, len(dbs))

	for i, db := range dbs {
		dsqlDBs[i] = db.(dsqle.Database)
	}

	return dsqlDBs
}