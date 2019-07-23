package sqlserver

import (
	"net"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"
	sqle "github.com/src-d/go-mysql-server"
	"github.com/src-d/go-mysql-server/auth"
	"github.com/src-d/go-mysql-server/server"
	"github.com/src-d/go-mysql-server/sql"
	"vitess.io/vitess/go/mysql"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	dsqle "github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sqle"
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
