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
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
)

const (
	LocalConnectionUser = "__dolt_local_user__"
)

// ExternalDisableUsers is called by implementing applications to disable users. This is not used by Dolt itself,
// but will break compatibility with implementing applications that do not yet support users.
var ExternalDisableUsers bool = false

// Serve starts a MySQL-compatible server. Returns any errors that were encountered.
func Serve(
	ctx context.Context,
	version string,
	serverConfig ServerConfig,
	controller *svcs.Controller,
	dEnv *env.DoltEnv,
) (startError error, closeError error) {
	// Code is easier to work through if we assume that serverController is never nil
	if controller == nil {
		controller = svcs.NewController()
	}

	ValidateConfigStep := &svcs.Service{
		Init: func(context.Context) error {
			return ValidateConfig(serverConfig)
		},
	}
	controller.Register(ValidateConfigStep)

	lgr := logrus.StandardLogger()
	lgr.SetOutput(cli.CliErr)
	InitLogging := &svcs.Service{
		Init: func(context.Context) error {
			level, err := logrus.ParseLevel(serverConfig.LogLevel().String())
			if err != nil {
				return err
			}
			logrus.SetLevel(level)

			sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
				{
					Name:              dsess.DoltLogLevel,
					Scope:             sql.SystemVariableScope_Global,
					Dynamic:           true,
					SetVarHintApplies: false,
					Type: types.NewSystemEnumType(dsess.DoltLogLevel,
						logrus.PanicLevel.String(),
						logrus.FatalLevel.String(),
						logrus.ErrorLevel.String(),
						logrus.WarnLevel.String(),
						logrus.InfoLevel.String(),
						logrus.DebugLevel.String(),
						logrus.TraceLevel.String(),
					),
					Default: logrus.GetLevel().String(),
					NotifyChanged: func(scope sql.SystemVariableScope, v sql.SystemVarValue) error {
						level, err := logrus.ParseLevel(v.Val.(string))
						if err != nil {
							return fmt.Errorf("could not parse requested log level %s as a log level. dolt_log_level variable value and logging behavior will diverge.", v.Val.(string))
						}

						logrus.SetLevel(level)
						return nil
					},
				},
			})
			return nil
		},
	}
	controller.Register(InitLogging)

	fs := dEnv.FS
	InitDataDir := &svcs.Service{
		Init: func(context.Context) (err error) {
			if len(serverConfig.DataDir()) > 0 && serverConfig.DataDir() != "." {
				fs, err = dEnv.FS.WithWorkingDir(serverConfig.DataDir())
				if err != nil {
					return err
				}
				dEnv.FS = fs
			}
			return nil
		},
	}
	controller.Register(InitDataDir)

	var serverLock *env.DBLock
	InitGlobalServerLock := &svcs.Service{
		Init: func(context.Context) (err error) {
			serverLock, err = acquireGlobalSqlServerLock(serverConfig.Port(), dEnv)
			return err
		},
		Stop: func() error {
			dEnv.FS.Delete(dEnv.LockFile(), false)
			return nil
		},
	}
	controller.Register(InitGlobalServerLock)

	var mrEnv *env.MultiRepoEnv
	InitMultiEnv := &svcs.Service{
		Init: func(ctx context.Context) (err error) {
			mrEnv, err = env.MultiEnvForDirectory(ctx, dEnv.Config.WriteableConfig(), fs, dEnv.Version, dEnv.IgnoreLockFile, dEnv)
			return err
		},
	}
	controller.Register(InitMultiEnv)

	var clusterController *cluster.Controller
	InitClusterController := &svcs.Service{
		Init: func(context.Context) (err error) {
			clusterController, err = cluster.NewController(lgr, serverConfig.ClusterConfig(), mrEnv.Config())
			return err
		},
	}
	controller.Register(InitClusterController)

	var serverConf server.Config
	LoadServerConfig := &svcs.Service{
		Init: func(context.Context) (err error) {
			serverConf, err = getConfigFromServerConfig(serverConfig)
			return err
		},
	}
	controller.Register(LoadServerConfig)

	// Create SQL Engine with users

	var config *engine.SqlEngineConfig
	InitSqlEngineConfig := &svcs.Service{
		Init: func(context.Context) error {
			config = &engine.SqlEngineConfig{
				IsReadOnly:              serverConfig.ReadOnly(),
				PrivFilePath:            serverConfig.PrivilegeFilePath(),
				BranchCtrlFilePath:      serverConfig.BranchControlFilePath(),
				DoltCfgDirPath:          serverConfig.CfgDir(),
				ServerUser:              serverConfig.User(),
				ServerPass:              serverConfig.Password(),
				ServerHost:              serverConfig.Host(),
				Autocommit:              serverConfig.AutoCommit(),
				DoltTransactionCommit:   serverConfig.DoltTransactionCommit(),
				JwksConfig:              serverConfig.JwksConfig(),
				SystemVariables:         serverConfig.SystemVars(),
				ClusterController:       clusterController,
				BinlogReplicaController: binlogreplication.DoltBinlogReplicaController,
			}
			return nil
		},
	}
	controller.Register(InitSqlEngineConfig)

	var esStatus eventscheduler.SchedulerStatus
	InitEventSchedulerStatus := &svcs.Service{
		Init: func(context.Context) (err error) {
			esStatus, err = getEventSchedulerStatus(serverConfig.EventSchedulerStatus())
			if err != nil {
				return err
			}
			config.EventSchedulerStatus = esStatus
			return nil
		},
	}
	controller.Register(InitEventSchedulerStatus)

	var sqlEngine *engine.SqlEngine
	InitSqlEngine := &svcs.Service{
		Init: func(ctx context.Context) (err error) {
			sqlEngine, err = engine.NewSqlEngine(
				ctx,
				mrEnv,
				config,
			)
			return err
		},
		Stop: func() error {
			sqlEngine.Close()
			return nil
		},
	}
	controller.Register(InitSqlEngine)

	// Add superuser if specified user exists; add root superuser if no user specified and no existing privileges
	var userSpecified bool

	InitSuperUser := &svcs.Service{
		Init: func(context.Context) error {
			userSpecified = config.ServerUser != ""

			mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
			ed := mysqlDb.Editor()
			var numUsers int
			ed.VisitUsers(func(*mysql_db.User) { numUsers += 1 })
			privsExist := numUsers != 0
			if userSpecified {
				superuser := mysqlDb.GetUser(ed, config.ServerUser, "%", false)
				if userSpecified && superuser == nil {
					mysqlDb.AddSuperUser(ed, config.ServerUser, "%", config.ServerPass)
				}
			} else if !privsExist {
				mysqlDb.AddSuperUser(ed, defaultUser, "%", defaultPass)
			}
			ed.Close()

			return nil
		},
	}
	controller.Register(InitSuperUser)

	var metListener *metricsListener
	InitMetricsListener := &svcs.Service{
		Init: func(context.Context) (err error) {
			labels := serverConfig.MetricsLabels()
			metListener, err = newMetricsListener(labels, version, clusterController)
			return err
		},
		Stop: func() error {
			metListener.Close()
			return nil
		},
	}
	controller.Register(InitMetricsListener)

	LockMultiRepoEnv := &svcs.Service{
		Init: func(context.Context) error {
			if ok, f := mrEnv.IsLocked(); ok {
				return env.ErrActiveServerLock.New(f)
			}
			if err := mrEnv.Lock(serverLock); err != nil {
				return err
			}
			return nil
		},
		Stop: func() error {
			if err := mrEnv.Unlock(); err != nil {
				cli.PrintErr(err)
			}
			return nil
		},
	}
	controller.Register(LockMultiRepoEnv)

	var mySQLServer *server.Server
	InitSQLServer := &svcs.Service{
		Init: func(context.Context) (err error) {
			v, ok := serverConfig.(validatingServerConfig)
			if ok && v.goldenMysqlConnectionString() != "" {
				mySQLServer, err = server.NewValidatingServer(
					serverConf,
					sqlEngine.GetUnderlyingEngine(),
					newSessionBuilder(sqlEngine, serverConfig),
					metListener,
					v.goldenMysqlConnectionString(),
				)
			} else {
				mySQLServer, err = server.NewServer(
					serverConf,
					sqlEngine.GetUnderlyingEngine(),
					newSessionBuilder(sqlEngine, serverConfig),
					metListener,
				)
			}
			if errors.Is(err, server.UnixSocketInUseError) {
				lgr.Warn("unix socket set up failed: file already in use: ", serverConf.Socket)
				err = nil
			}
			return err
		},
	}
	controller.Register(InitSQLServer)

	InitLockSuperUser := &svcs.Service{
		Init: func(context.Context) error {
			mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
			ed := mysqlDb.Editor()
			mysqlDb.AddSuperUser(ed, LocalConnectionUser, "localhost", serverLock.Secret)
			ed.Close()
			return nil
		},
	}
	controller.Register(InitLockSuperUser)

	DisableMySQLDbIfRequired := &svcs.Service{
		Init: func(context.Context) error {
			if ExternalDisableUsers {
				mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
				mysqlDb.SetEnabled(false)
			}
			return nil
		},
	}
	controller.Register(DisableMySQLDbIfRequired)

	var metSrv *http.Server
	RunMetricsServer := &svcs.Service{
		Run: func(context.Context) {
			if serverConfig.MetricsHost() != "" && serverConfig.MetricsPort() > 0 {
				mux := http.NewServeMux()
				mux.Handle("/metrics", promhttp.Handler())

				metSrv = &http.Server{
					Addr:    fmt.Sprintf("%s:%d", serverConfig.MetricsHost(), serverConfig.MetricsPort()),
					Handler: mux,
				}

				_ = metSrv.ListenAndServe()
			}
		},
		Stop: func() error {
			if metSrv != nil {
				metSrv.Close()
			}
			return nil
		},
	}
	controller.Register(RunMetricsServer)

	var remoteSrv *remotesrv.Server
	var remoteSrvListeners remotesrv.Listeners
	RunRemoteSrv := &svcs.Service{
		Init: func(ctx context.Context) error {
			if serverConfig.RemotesapiPort() == nil {
				return nil
			}

			port := *serverConfig.RemotesapiPort()
			listenaddr := fmt.Sprintf(":%d", port)
			args, err := sqle.RemoteSrvServerArgs(sqlEngine.NewDefaultContext, remotesrv.ServerArgs{
				Logger:         logrus.NewEntry(lgr),
				ReadOnly:       true,
				HttpListenAddr: listenaddr,
				GrpcListenAddr: listenaddr,
			})
			if err != nil {
				lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
				return err
			}

			ctxFactory := func() (*sql.Context, error) { return sqlEngine.NewDefaultContext(ctx) }
			authenticator := newAuthenticator(ctxFactory, sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb)
			args = sqle.WithUserPasswordAuth(args, authenticator)
			args.TLSConfig = serverConf.TLSConfig

			remoteSrv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", port, err)
				return err
			}
			remoteSrvListeners, err = remoteSrv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners on port %d: %v", port, err)
				return err
			}
			return nil
		},
		Run: func(ctx context.Context) {
			if remoteSrv == nil {
				return
			}
			remoteSrv.Serve(remoteSrvListeners)
		},
		Stop: func() error {
			if remoteSrv == nil {
				return nil
			}
			remoteSrv.GracefulStop()
			return nil
		},
	}
	controller.Register(RunRemoteSrv)

	var clusterRemoteSrv *remotesrv.Server
	var clusterRemoteSrvListeners remotesrv.Listeners
	RunClusterRemoteSrv := &svcs.Service{
		Init: func(context.Context) error {
			if clusterController == nil {
				return nil
			}

			args, err := clusterController.RemoteSrvServerArgs(sqlEngine.NewDefaultContext, remotesrv.ServerArgs{
				Logger: logrus.NewEntry(lgr),
			})
			if err != nil {
				lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
				return err
			}

			clusterRemoteSrvTLSConfig, err := LoadClusterTLSConfig(serverConfig.ClusterConfig())
			if err != nil {
				lgr.Errorf("error starting remotesapi server for cluster config, could not load tls config: %v", err)
				return err
			}
			args.TLSConfig = clusterRemoteSrvTLSConfig

			clusterRemoteSrv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", *serverConfig.RemotesapiPort(), err)
				return err
			}
			clusterController.RegisterGrpcServices(sqlEngine.NewDefaultContext, clusterRemoteSrv.GrpcServer())

			clusterRemoteSrvListeners, err = clusterRemoteSrv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners for cluster config on %s: %v", clusterController.RemoteSrvListenAddr(), err)
				return err
			}
			return nil
		},
		Run: func(context.Context) {
			if clusterRemoteSrv == nil {
				return
			}
			clusterRemoteSrv.Serve(clusterRemoteSrvListeners)
		},
		Stop: func() error {
			if clusterRemoteSrv == nil {
				return nil
			}
			clusterRemoteSrv.GracefulStop()
			return nil
		},
	}
	controller.Register(RunClusterRemoteSrv)

	RunClusterController := &svcs.Service{
		Init: func(context.Context) error {
			if clusterController == nil {
				return nil
			}
			clusterController.ManageQueryConnections(
				mySQLServer.SessionManager().Iter,
				sqlEngine.GetUnderlyingEngine().ProcessList.Kill,
				mySQLServer.SessionManager().KillConnection,
			)
			return nil
		},
		Run: func(context.Context) {
			if clusterController == nil {
				return
			}
			clusterController.Run()
		},
		Stop: func() error {
			if clusterController == nil {
				return nil
			}
			clusterController.GracefulStop()
			return nil
		},
	}
	controller.Register(RunClusterController)

	RunSQLServer := &svcs.Service{
		Run: func(context.Context) {
			sqlserver.SetRunningServer(mySQLServer, serverLock)
			defer sqlserver.UnsetRunningServer()
			mySQLServer.Start()
		},
		Stop: func() error {
			return mySQLServer.Close()
		},
	}
	controller.Register(RunSQLServer)

	go controller.Start(ctx)
	err := controller.WaitForStart()
	if err != nil {
		return err, nil
	}
	return nil, controller.WaitForStop()
}

// acquireGlobalSqlServerLock attempts to acquire a global lock on the SQL server. If no error is returned, then the lock was acquired.
func acquireGlobalSqlServerLock(port int, dEnv *env.DoltEnv) (*env.DBLock, error) {
	locked, _, err := dEnv.GetLock()
	if err != nil {
		return nil, err
	}
	if locked {
		lockPath := dEnv.LockFile()
		err = fmt.Errorf("Database locked by another sql-server; Lock file: %s", lockPath)
		return nil, err
	}

	lck := env.NewDBLock(port)
	err = dEnv.Lock(&lck)
	if err != nil {
		err = fmt.Errorf("Server can not start. Failed to acquire lock: %s", err.Error())
		return nil, err
	}

	return &lck, nil
}

type remotesapiAuth struct {
	ctxFactory func() (*sql.Context, error)
	rawDb      *mysql_db.MySQLDb
}

func newAuthenticator(ctxFactory func() (*sql.Context, error), rawDb *mysql_db.MySQLDb) remotesrv.Authenticator {
	return &remotesapiAuth{ctxFactory, rawDb}
}

func (r *remotesapiAuth) Authenticate(creds *remotesrv.RequestCredentials) bool {
	err := commands.ValidatePasswordWithAuthResponse(r.rawDb, creds.Username, creds.Password)
	if err != nil {
		return false
	}

	ctx, err := r.ctxFactory()
	if err != nil {
		return false
	}
	ctx.Session.SetClient(sql.Client{User: creds.Username, Address: creds.Address, Capabilities: 0})

	privOp := sql.NewDynamicPrivilegedOperation(plan.DynamicPrivilege_CloneAdmin)
	return r.rawDb.UserHasPrivileges(ctx, privOp)
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
				if server, _ := sqlserver.GetRunningServer(); server != nil {
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
func getConfigFromServerConfig(serverConfig ServerConfig) (server.Config, error) {
	serverConf, err := handleProtocolAndAddress(serverConfig)
	if err != nil {
		return server.Config{}, err
	}

	serverConf.DisableClientMultiStatements = serverConfig.DisableClientMultiStatements()

	readTimeout := time.Duration(serverConfig.ReadTimeout()) * time.Millisecond
	writeTimeout := time.Duration(serverConfig.WriteTimeout()) * time.Millisecond

	tlsConfig, err := LoadTLSConfig(serverConfig)
	if err != nil {
		return server.Config{}, err
	}

	// if persist is 'load' we use currently set persisted global variable,
	// else if 'ignore' we set persisted global variable to current value from serverConfig
	if serverConfig.PersistenceBehavior() == loadPerisistentGlobals {
		serverConf, err = serverConf.NewConfig()
		if err != nil {
			return server.Config{}, err
		}
	} else {
		err = sql.SystemVariables.SetGlobal("max_connections", serverConfig.MaxConnections())
		if err != nil {
			return server.Config{}, err
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
	serverConf.EncodeLoggedQuery = serverConfig.ShouldEncodeLoggedQuery()

	return serverConf, nil
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

func getEventSchedulerStatus(status string) (eventscheduler.SchedulerStatus, error) {
	switch strings.ToLower(status) {
	case "on", "1":
		return eventscheduler.SchedulerOn, nil
	case "off", "0":
		return eventscheduler.SchedulerOff, nil
	case "disabled":
		return eventscheduler.SchedulerDisabled, nil
	default:
		return eventscheduler.SchedulerDisabled, fmt.Errorf("Error while setting value '%s' to 'event_scheduler'.", status)
	}
}
