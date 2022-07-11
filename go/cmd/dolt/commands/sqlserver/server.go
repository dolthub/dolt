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
	"net/http"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
)

// Serve starts a MySQL-compatible server. Returns any errors that were encountered.
func Serve(
	ctx context.Context,
	version string,
	serverConfig ServerConfig,
	serverController *ServerController,
	dEnv *env.DoltEnv,
) (startError error, closeError error) {
	// Code is easier to work through if we assume that serverController is never nil
	if serverController == nil {
		serverController = NewServerController()
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
		sqlserver.SetRunningServer(nil)
	}()

	if startError = ValidateConfig(serverConfig); startError != nil {
		return startError, nil
	}

	lgr := logrus.StandardLogger()
	lgr.SetOutput(cli.CliErr)

	if serverConfig.LogLevel() != LogLevel_Info {
		var level logrus.Level
		level, startError = logrus.ParseLevel(serverConfig.LogLevel().String())
		if startError != nil {
			cli.PrintErr(startError)
			return
		}
		logrus.SetLevel(level)
	}
	logrus.SetFormatter(LogFormat{})

	isReadOnly := false
	if serverConfig.ReadOnly() {
		isReadOnly = true
	}

	var mrEnv *env.MultiRepoEnv
	dbNamesAndPaths := serverConfig.DatabaseNamesAndPaths()

	if len(dbNamesAndPaths) == 0 {
		if len(serverConfig.DataDir()) > 0 && serverConfig.DataDir() != "." {
			fs, err := dEnv.FS.WithWorkingDir(serverConfig.DataDir())
			if err != nil {
				return err, nil
			}

			// TODO: this should be the global config, probably?
			mrEnv, err = env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), fs, dEnv.Version)
			if err != nil {
				return err, nil
			}
		} else {
			var err error
			mrEnv, err = env.DoltEnvAsMultiEnv(ctx, dEnv)
			if err != nil {
				return err, nil
			}
		}
	} else {
		var err error
		fs := dEnv.FS
		if len(serverConfig.DataDir()) > 0 {
			fs, err = fs.WithWorkingDir(serverConfig.DataDir())
			if err != nil {
				return err, nil
			}
		}

		// TODO: this should be the global config, probably?
		mrEnv, err = env.MultiEnvForPaths(ctx, env.GetCurrentUserHomeDir, dEnv.Config.WriteableConfig(), fs, version, dbNamesAndPaths...)

		if err != nil {
			return err, nil
		}
	}

	serverConf, sErr, cErr := getConfigFromServerConfig(serverConfig)
	if cErr != nil {
		return nil, cErr
	} else if sErr != nil {
		return sErr, nil
	}

	// Create SQL Engine with users
	config := &engine.SqlEngineConfig{
		InitialDb:    "",
		IsReadOnly:   isReadOnly,
		PrivFilePath: serverConfig.PrivilegeFilePath(),
		ServerUser:   serverConfig.User(),
		ServerPass:   serverConfig.Password(),
		Autocommit:   serverConfig.AutoCommit(),
	}
	sqlEngine, err := engine.NewSqlEngine(
		ctx,
		mrEnv,
		engine.FormatTabular,
		config,
	)
	if err != nil {
		return err, nil
	}
	defer sqlEngine.Close()

	labels := serverConfig.MetricsLabels()
	listener := newMetricsListener(labels)
	defer listener.Close()

	mySQLServer, startError = server.NewServer(
		serverConf,
		sqlEngine.GetUnderlyingEngine(),
		newSessionBuilder(sqlEngine),
		listener,
	)

	if startError != nil {
		cli.PrintErr(startError)
		return
	} else {
		sqlserver.SetRunningServer(mySQLServer)
	}

	var metSrv *http.Server
	if serverConfig.MetricsHost() != "" && serverConfig.MetricsPort() > 0 {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.Handler())

		metSrv = &http.Server{
			Addr:    fmt.Sprintf("%s:%d", serverConfig.MetricsHost(), serverConfig.MetricsPort()),
			Handler: mux,
		}

		go func() {
			_ = metSrv.ListenAndServe()
		}()
	}

	if ok, f := mrEnv.IsLocked(); ok {
		startError = env.ErrActiveServerLock.New(f)
		return
	}
	if err = mrEnv.Lock(); err != nil {
		startError = err
		return
	}

	serverController.registerCloseFunction(startError, func() error {
		if metSrv != nil {
			metSrv.Close()
		}

		return mySQLServer.Close()
	})

	closeError = mySQLServer.Start()
	if closeError != nil {
		cli.PrintErr(closeError)
	}
	if err := mrEnv.Unlock(); err != nil {
		cli.PrintErr(err)
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

func newSessionBuilder(se *engine.SqlEngine) server.SessionBuilder {
	return func(ctx context.Context, conn *mysql.Conn, host string) (sql.Session, error) {
		mysqlSess, err := server.DefaultSessionBuilder(ctx, conn, host)
		if err != nil {
			return nil, err
		}
		mysqlBaseSess, ok := mysqlSess.(*sql.BaseSession)
		if !ok {
			return nil, fmt.Errorf("unknown GMS base session type")
		}

		return se.NewDoltSession(ctx, mysqlBaseSess)
	}
}

// getConfigFromServerConfig processes ServerConfig and returns server.Config for sql-server.
func getConfigFromServerConfig(serverConfig ServerConfig) (server.Config, error, error) {
	serverConf := server.Config{Protocol: "tcp"}
	serverConf.DisableClientMultiStatements = serverConfig.DisableClientMultiStatements()

	readTimeout := time.Duration(serverConfig.ReadTimeout()) * time.Millisecond
	writeTimeout := time.Duration(serverConfig.WriteTimeout()) * time.Millisecond

	tlsConfig, err := LoadTLSConfig(serverConfig)
	if err != nil {
		return server.Config{}, nil, err
	}

	portAsString := strconv.Itoa(serverConfig.Port())
	hostPort := net.JoinHostPort(serverConfig.Host(), portAsString)

	if portInUse(hostPort) {
		portInUseError := fmt.Errorf("Port %s already in use.", portAsString)
		return server.Config{}, portInUseError, nil
	}

	// if persist is 'load' we use currently set persisted global variable,
	// else if 'ignore' we set persisted global variable to current value from serverConfig
	if serverConfig.PersistenceBehavior() == loadPerisistentGlobals {
		serverConf, err = serverConf.NewConfig()
		if err != nil {
			return server.Config{}, err, nil
		}
	} else {
		err = sql.SystemVariables.SetGlobal("max_connections", serverConfig.MaxConnections())
		if err != nil {
			return server.Config{}, err, nil
		}
	}

	// Do not set the value of Version.  Let it default to what go-mysql-server uses.  This should be equivalent
	// to the value of mysql that we support.
	serverConf.Address = hostPort
	serverConf.ConnReadTimeout = readTimeout
	serverConf.ConnWriteTimeout = writeTimeout
	serverConf.MaxConnections = serverConfig.MaxConnections()
	serverConf.TLSConfig = tlsConfig
	serverConf.RequireSecureTransport = serverConfig.RequireSecureTransport()

	return serverConf, nil, nil
}
