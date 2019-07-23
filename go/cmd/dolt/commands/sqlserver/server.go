// Copyright 2019 Liquidata, Inc.
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
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	dsqle "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sqle"
	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"net"
	"strconv"
	"time"
	"vitess.io/vitess/go/mysql"
)

// serve starts a MySQL-compatible server. Returns any errors that were encountered.
func serve(serverConfig *ServerConfig, rootValue *doltdb.RootValue, serverController *ServerController) (startError error, closeError error) {
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
	defer func(){
		if mySQLServer != nil {
			serverController.registerCloseFunction(startError, mySQLServer.Close)
		} else {
			serverController.registerCloseFunction(startError, func()error{return nil})
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
	sqlEngine.AddDatabase(dsqle.NewDatabase("dolt", rootValue))

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
		func(conn *mysql.Conn, host string) sql.Session {
			return sql.NewSession(host, conn.RemoteAddr().String(), conn.User, conn.ConnectionID)
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
