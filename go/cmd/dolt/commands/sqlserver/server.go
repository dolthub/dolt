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
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime"
	"strconv"
	"time"

	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
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

	var mrEnv *env.MultiRepoEnv
	var err error
	fs := dEnv.FS

	dbNamesAndPaths := serverConfig.DatabaseNamesAndPaths()
	if len(dbNamesAndPaths) == 0 {
		if len(serverConfig.DataDir()) > 0 && serverConfig.DataDir() != "." {
			fs, err = dEnv.FS.WithWorkingDir(serverConfig.DataDir())
			if err != nil {
				return err, nil
			}
		}

		mrEnv, err = env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), fs, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
		if err != nil {
			return err, nil
		}
	} else {
		if len(serverConfig.DataDir()) > 0 {
			fs, err = fs.WithWorkingDir(serverConfig.DataDir())
			if err != nil {
				return err, nil
			}
		}

		mrEnv, err = env.MultiEnvForPaths(
			ctx,
			env.GetCurrentUserHomeDir,
			dEnv.Config.WriteableConfig(),
			fs,
			version,
			dEnv.IgnoreLockFile,
			dbNamesAndPaths...,
		)

		if err != nil {
			return err, nil
		}
	}

	clusterController, err := cluster.NewController(lgr, serverConfig.ClusterConfig(), mrEnv.Config())
	if err != nil {
		return err, nil
	}

	serverConf, sErr, cErr := getConfigFromServerConfig(serverConfig)
	if cErr != nil {
		return nil, cErr
	} else if sErr != nil {
		return sErr, nil
	}

	// Create SQL Engine with users
	config := &engine.SqlEngineConfig{
		IsReadOnly:              serverConfig.ReadOnly(),
		PrivFilePath:            serverConfig.PrivilegeFilePath(),
		BranchCtrlFilePath:      serverConfig.BranchControlFilePath(),
		DoltCfgDirPath:          serverConfig.CfgDir(),
		ServerUser:              serverConfig.User(),
		ServerPass:              serverConfig.Password(),
		ServerHost:              serverConfig.Host(),
		Autocommit:              serverConfig.AutoCommit(),
		JwksConfig:              serverConfig.JwksConfig(),
		ClusterController:       clusterController,
		BinlogReplicaController: binlogreplication.DoltBinlogReplicaController,
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

	// Add superuser if specified user exists; add root superuser if no user specified and no existing privileges
	userSpecified := config.ServerUser != ""
	privsExist := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.UserTable().Data().Count() != 0
	if userSpecified {
		superuser := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.GetUser(config.ServerUser, "%", false)
		if userSpecified && superuser == nil {
			sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(config.ServerUser, "%", config.ServerPass)
		}
	} else if !privsExist {
		sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb.AddSuperUser(defaultUser, "%", defaultPass)
	}

	labels := serverConfig.MetricsLabels()

	var listener *metricsListener
	listener, startError = newMetricsListener(labels, version)
	if startError != nil {
		cli.Println(startError)
		return
	}
	defer listener.Close()

	v, ok := serverConfig.(validatingServerConfig)
	if ok && v.goldenMysqlConnectionString() != "" {
		mySQLServer, startError = server.NewValidatingServer(
			serverConf,
			sqlEngine.GetUnderlyingEngine(),
			newSessionBuilder(sqlEngine, serverConfig),
			listener,
			v.goldenMysqlConnectionString(),
		)
	} else {
		mySQLServer, startError = server.NewServer(
			serverConf,
			sqlEngine.GetUnderlyingEngine(),
			newSessionBuilder(sqlEngine, serverConfig),
			listener,
		)
	}

	if errors.Is(startError, server.UnixSocketInUseError) {
		lgr.Warn("unix socket set up failed: file already in use: ", serverConf.Socket)
		startError = nil
	} else if startError != nil {
		cli.PrintErr(startError)
		return
	}
	sqlserver.SetRunningServer(mySQLServer)

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

	var remoteSrv *remotesrv.Server
	if serverConfig.RemotesapiPort() != nil {
		port := *serverConfig.RemotesapiPort()
		if remoteSrvSqlCtx, err := sqlEngine.NewDefaultContext(ctx); err == nil {
			listenaddr := fmt.Sprintf(":%d", port)
			args := sqle.RemoteSrvServerArgs(remoteSrvSqlCtx, remotesrv.ServerArgs{
				Logger:         logrus.NewEntry(lgr),
				ReadOnly:       true,
				HttpListenAddr: listenaddr,
				GrpcListenAddr: listenaddr,
			})
			args.TLSConfig = serverConf.TLSConfig
			remoteSrv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", port, err)
				startError = err
				return
			}
			listeners, err := remoteSrv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners on port %d: %v", port, err)
				startError = err
				return
			} else {
				go remoteSrv.Serve(listeners)
			}
		} else {
			lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
			startError = err
			return
		}
	}

	var clusterRemoteSrv *remotesrv.Server
	if clusterController != nil {
		if remoteSrvSqlCtx, err := sqlEngine.NewDefaultContext(ctx); err == nil {
			args := clusterController.RemoteSrvServerArgs(remoteSrvSqlCtx, remotesrv.ServerArgs{
				Logger: logrus.NewEntry(lgr),
			})

			clusterRemoteSrvTLSConfig, err := LoadClusterTLSConfig(serverConfig.ClusterConfig())
			if err != nil {
				lgr.Errorf("error starting remotesapi server for cluster config, could not load tls config: %v", err)
				startError = err
				return
			}
			args.TLSConfig = clusterRemoteSrvTLSConfig

			clusterRemoteSrv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", *serverConfig.RemotesapiPort(), err)
				startError = err
				return
			}

			listeners, err := clusterRemoteSrv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners for cluster config on %s: %v", clusterController.RemoteSrvListenAddr(), err)
				startError = err
				return
			}

			go clusterRemoteSrv.Serve(listeners)
			go clusterController.Run()

			clusterController.ManageQueryConnections(
				mySQLServer.SessionManager().Iter,
				sqlEngine.GetUnderlyingEngine().ProcessList.Kill,
				mySQLServer.SessionManager().KillConnection,
			)
		} else {
			lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
			startError = err
			return
		}

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
		if remoteSrv != nil {
			remoteSrv.GracefulStop()
		}
		if clusterRemoteSrv != nil {
			clusterRemoteSrv.GracefulStop()
		}
		if clusterController != nil {
			clusterController.GracefulStop()
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

func LoadClusterTLSConfig(cfg cluster.Config) (*tls.Config, error) {
	rcfg := cfg.RemotesAPIConfig()
	if rcfg.TLSKey() == "" && rcfg.TLSCert() == "" {
		return nil, nil
	}
	c, err := tls.LoadX509KeyPair(rcfg.TLSCert(), rcfg.TLSKey())
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{
			c,
		},
	}, nil
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

func newSessionBuilder(se *engine.SqlEngine, config ServerConfig) server.SessionBuilder {
	userToSessionVars := make(map[string]map[string]string)
	userVars := config.UserVars()
	for _, curr := range userVars {
		userToSessionVars[curr.Name] = curr.Vars
	}

	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		mysqlSess, err := server.DefaultSessionBuilder(ctx, conn, addr)
		if err != nil {
			return nil, err
		}
		mysqlBaseSess, ok := mysqlSess.(*sql.BaseSession)
		if !ok {
			return nil, fmt.Errorf("unknown GMS base session type")
		}

		dsess, err := se.NewDoltSession(ctx, mysqlBaseSess)
		if err != nil {
			if goerrors.Is(err, env.ErrFailedToAccessDB) {
				if server := sqlserver.GetRunningServer(); server != nil {
					_ = server.Close()
				}
			}
			return nil, err
		}

		varsForUser := userToSessionVars[conn.User]
		if len(varsForUser) > 0 {
			sqlCtx, err := se.NewContext(ctx, dsess)
			if err != nil {
				return nil, err
			}

			for key, val := range varsForUser {
				err = dsess.InitSessionVariable(sqlCtx, key, val)
				if err != nil {
					return nil, err
				}
			}
		}

		return dsess, nil
	}
}

// getConfigFromServerConfig processes ServerConfig and returns server.Config for sql-server.
func getConfigFromServerConfig(serverConfig ServerConfig) (server.Config, error, error) {
	serverConf, err := handleProtocolAndAddress(serverConfig)
	if err != nil {
		return server.Config{}, err, nil
	}

	serverConf.DisableClientMultiStatements = serverConfig.DisableClientMultiStatements()

	readTimeout := time.Duration(serverConfig.ReadTimeout()) * time.Millisecond
	writeTimeout := time.Duration(serverConfig.WriteTimeout()) * time.Millisecond

	tlsConfig, err := LoadTLSConfig(serverConfig)
	if err != nil {
		return server.Config{}, nil, err
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
	serverConf.ConnReadTimeout = readTimeout
	serverConf.ConnWriteTimeout = writeTimeout
	serverConf.MaxConnections = serverConfig.MaxConnections()
	serverConf.TLSConfig = tlsConfig
	serverConf.RequireSecureTransport = serverConfig.RequireSecureTransport()
	serverConf.MaxLoggedQueryLen = serverConfig.MaxLoggedQueryLen()

	return serverConf, nil, nil
}

// handleProtocolAndAddress returns new server.Config object with only Protocol and Address defined.
func handleProtocolAndAddress(serverConfig ServerConfig) (server.Config, error) {
	serverConf := server.Config{Protocol: "tcp"}

	portAsString := strconv.Itoa(serverConfig.Port())
	hostPort := net.JoinHostPort(serverConfig.Host(), portAsString)
	if portInUse(hostPort) {
		portInUseError := fmt.Errorf("Port %s already in use.", portAsString)
		return server.Config{}, portInUseError
	}
	serverConf.Address = hostPort

	sock, useSock, err := checkForUnixSocket(serverConfig)
	if err != nil {
		return server.Config{}, err
	}
	if useSock {
		serverConf.Socket = sock
	}

	return serverConf, nil
}

// checkForUnixSocket evaluates ServerConfig for whether the unix socket is to be used or not.
// If user defined socket flag or host is 'localhost', it returns the unix socket file location
// either user-defined or the default if it was not defined.
func checkForUnixSocket(config ServerConfig) (string, bool, error) {
	if config.Socket() != "" {
		if runtime.GOOS == "windows" {
			return "", false, fmt.Errorf("cannot define unix socket file on Windows")
		}
		return config.Socket(), true, nil
	} else {
		// if host is undefined or defined as "localhost" -> unix
		if runtime.GOOS != "windows" && config.Host() == "localhost" {
			return defaultUnixSocketFilePath, true, nil
		}
	}

	return "", false, nil
}
