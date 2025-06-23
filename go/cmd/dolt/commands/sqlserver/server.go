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
	"github.com/dolthub/vitess/go/vt/log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/eventscheduler"
	"github.com/dolthub/go-mysql-server/server"
	"github.com/dolthub/go-mysql-server/server/golden"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/plan"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	goerrors "gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	remotesapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/remotesapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotesrv"
	"github.com/dolthub/dolt/go/libraries/doltcore/servercfg"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/binlogreplication"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/cluster"
	_ "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dfunctions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/svcs"
	"github.com/dolthub/dolt/go/store/chunks"
)

const (
	LocalConnectionUser = "__dolt_local_user__"
	ApiSqleContextKey   = "__sqle_context__"
)

// sqlServerHeartbeatIntervalEnvVar is the duration between heartbeats sent to the remote server, used for testing
const sqlServerHeartbeatIntervalEnvVar = "DOLT_SQL_SERVER_HEARTBEAT_INTERVAL"

// ExternalDisableUsers is called by implementing applications to disable users. This is not used by Dolt itself,
// but will break compatibility with implementing applications that do not yet support users.
var ExternalDisableUsers bool = false

var ErrCouldNotLockDatabase = goerrors.NewKind("database \"%s\" is locked by another dolt process; either clone the database to run a second server, or stop the dolt process which currently holds an exclusive write lock on the database")

type Config struct {
	ServerConfig            servercfg.ServerConfig
	DoltEnv                 *env.DoltEnv
	SkipRootUserInit        bool
	Version                 string
	Controller              *svcs.Controller
	ProtocolListenerFactory server.ProtocolListenerFunc
}

// Serve starts a MySQL-compatible server. Returns any errors that were encountered.
func Serve(
	ctx context.Context,
	cfg *Config,
) (startError error, closeError error) {
	// Code is easier to work through if we assume that serverController is never nil
	if cfg.Controller == nil {
		cfg.Controller = svcs.NewController()
	}

	ConfigureServices(cfg)

	go cfg.Controller.Start(ctx)
	err := cfg.Controller.WaitForStart()
	if err != nil {
		return err, nil
	}
	return nil, cfg.Controller.WaitForStop()
}

func ConfigureServices(
	cfg *Config,
) {
	controller := cfg.Controller
	ValidateConfigStep := &svcs.AnonService{
		InitF: func(context.Context) error {
			return servercfg.ValidateConfig(cfg.ServerConfig)
		},
	}
	controller.Register(ValidateConfigStep)

	lgr := logrus.StandardLogger()
	lgr.SetOutput(cli.CliErr)
	InitLogging := &svcs.AnonService{
		InitF: func(context.Context) error {
			level, err := logrus.ParseLevel(cfg.ServerConfig.LogLevel().String())
			if err != nil {
				return err
			}
			logrus.SetLevel(level)
			switch strings.ToLower(string(cfg.ServerConfig.LogFormat())) {
			case string(servercfg.LogFormat_JSON):
				logrus.SetFormatter(&logrus.JSONFormatter{})
			case string(servercfg.LogFormat_Text):
				logrus.SetFormatter(&logrus.TextFormatter{})
			default:
				return fmt.Errorf("unknown log format: %s", cfg.ServerConfig.LogFormat())
			}

			sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
				&sql.MysqlSystemVariable{
					Name:              dsess.DoltLogLevel,
					Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
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
					NotifyChanged: func(ctx *sql.Context, _ sql.SystemVariableScope, v sql.SystemVarValue) error {
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

	controller.Register(newHeartbeatService(cfg.Version, cfg.DoltEnv))

	fs := cfg.DoltEnv.FS
	InitFailsafes := &svcs.AnonService{
		InitF: func(ctx context.Context) (err error) {
			cfg.DoltEnv.Config.SetFailsafes(env.DefaultFailsafeConfig)
			return nil
		},
	}
	controller.Register(InitFailsafes)

	var mrEnv *env.MultiRepoEnv
	InitMultiEnv := &svcs.AnonService{
		InitF: func(ctx context.Context) (err error) {
			mrEnv, err = env.MultiEnvForDirectory(ctx, cfg.DoltEnv.Config.WriteableConfig(), fs, cfg.DoltEnv.Version, cfg.DoltEnv)
			return err
		},
	}
	controller.Register(InitMultiEnv)

	AssertNoDatabasesInAccessModeReadOnly := &svcs.AnonService{
		InitF: func(ctx context.Context) (err error) {
			return mrEnv.Iter(func(name string, dEnv *env.DoltEnv) (stop bool, err error) {
				if dEnv.IsAccessModeReadOnly(ctx) {
					return true, ErrCouldNotLockDatabase.New(name)
				}
				return false, nil
			})
		},
	}
	controller.Register(AssertNoDatabasesInAccessModeReadOnly)

	var localCreds *LocalCreds
	InitServerLocalCreds := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			localCreds, err = persistServerLocalCreds(cfg.ServerConfig.Port(), cfg.DoltEnv)
			return err
		},
		StopF: func() error {
			RemoveLocalCreds(cfg.DoltEnv.FS)
			return nil
		},
	}
	controller.Register(InitServerLocalCreds)

	var clusterController *cluster.Controller
	InitClusterController := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			clusterController, err = cluster.NewController(lgr, cfg.ServerConfig.ClusterConfig(), mrEnv.Config())
			return err
		},
	}
	controller.Register(InitClusterController)

	var serverConf server.Config
	LoadServerConfig := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			serverConf, err = getConfigFromServerConfig(cfg.ServerConfig, cfg.ProtocolListenerFactory)
			return err
		},
	}
	controller.Register(LoadServerConfig)

	// Create SQL Engine with users
	var config *engine.SqlEngineConfig
	InitSqlEngineConfig := &svcs.AnonService{
		InitF: func(context.Context) error {
			config = &engine.SqlEngineConfig{
				IsReadOnly:                 cfg.ServerConfig.ReadOnly(),
				PrivFilePath:               cfg.ServerConfig.PrivilegeFilePath(),
				BranchCtrlFilePath:         cfg.ServerConfig.BranchControlFilePath(),
				DoltCfgDirPath:             cfg.ServerConfig.CfgDir(),
				ServerUser:                 cfg.ServerConfig.User(),
				ServerPass:                 cfg.ServerConfig.Password(),
				ServerHost:                 cfg.ServerConfig.Host(),
				Autocommit:                 cfg.ServerConfig.AutoCommit(),
				DoltTransactionCommit:      cfg.ServerConfig.DoltTransactionCommit(),
				JwksConfig:                 cfg.ServerConfig.JwksConfig(),
				SystemVariables:            cfg.ServerConfig.SystemVars(),
				ClusterController:          clusterController,
				BinlogReplicaController:    binlogreplication.DoltBinlogReplicaController,
				SkipRootUserInitialization: cfg.SkipRootUserInit,
			}
			return nil
		},
	}
	controller.Register(InitSqlEngineConfig)

	var esStatus eventscheduler.SchedulerStatus
	InitEventSchedulerStatus := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			esStatus, err = getEventSchedulerStatus(cfg.ServerConfig.EventSchedulerStatus())
			if err != nil {
				return err
			}
			config.EventSchedulerStatus = esStatus
			return nil
		},
	}
	controller.Register(InitEventSchedulerStatus)

	InitAutoGCController := &svcs.AnonService{
		InitF: func(context.Context) error {
			if cfg.ServerConfig.AutoGCBehavior() != nil && cfg.ServerConfig.AutoGCBehavior().Enable() {
				cmp := chunks.GCArchiveLevel(cfg.ServerConfig.AutoGCBehavior().ArchiveLevel())
				if cmp < chunks.NoArchive || cmp > chunks.MaxArchiveLevel {
					return fmt.Errorf("invalid value for %s: %d", cli.ArchiveLevelParam, cmp)
				}

				config.AutoGCController = sqle.NewAutoGCController(cmp, lgr)
			}
			return nil
		},
	}
	controller.Register(InitAutoGCController)

	// mySQLServer is going to be populated down below once further services
	// are initialized. However, we want to block Controller shutdown on all
	// connections being fully drained from the Server. Stopping the
	// SQL server itself only stops the connection listener, and inflight
	// work is shutdown by other services, which can stop things like
	// replication threads, listeners for other services, etc.
	//
	// On the shutdown path, we block for connection draining
	// right after we have stopped the sql engine itself, which
	// was responsible for canceling the contexts associated with
	// all inflight queries.
	var mySQLServer *server.Server
	DrainClientConnectionsOnShutdown := &svcs.AnonService{
		StopF: func() error {
			if mySQLServer != nil {
				mySQLServer.SessionManager().WaitForClosedConnections()
			}
			return nil
		},
	}
	controller.Register(DrainClientConnectionsOnShutdown)

	var sqlEngine *engine.SqlEngine
	InitSqlEngine := &svcs.AnonService{
		InitF: func(ctx context.Context) (err error) {
			if _, err := mrEnv.Config().GetString(env.SqlServerGlobalsPrefix + "." + dsess.DoltStatsPaused); err != nil {
				// unless otherwise specified, run stats writer alongside server
				sqlCtx := sql.NewEmptyContext()
				sql.SystemVariables.SetGlobal(sqlCtx, dsess.DoltStatsPaused, 0)
			}
			sqlEngine, err = engine.NewSqlEngine(
				ctx,
				mrEnv,
				config,
			)
			return err
		},
		StopF: func() error {
			sqlEngine.Close()
			return nil
		},
	}
	controller.Register(InitSqlEngine)

	// Closing the connections on shutdown attempts to prevent
	// them from creating new work after we cancel their running
	// queries up above. GMS should ideally avoid creating new
	// process list queries or operations for these connections
	// after they are killed, but it is not currently set up that
	// way.
	CloseClientConnectionsOnShutdown := &svcs.AnonService{
		StopF: func() error {
			if mySQLServer != nil {
				sm := mySQLServer.SessionManager()
				return sm.Iter(func(s sql.Session) (bool, error) {
					sm.KillConnection(s.ID())
					return false, nil
				})
			}
			return nil
		},
	}
	controller.Register(CloseClientConnectionsOnShutdown)

	// Persist any system variables that have a non-deterministic default value (i.e. @@server_uuid)
	// We only do this on sql-server startup initially since we want to keep the persisted server_uuid
	// in the configuration files for a sql-server, and not global for the whole host.
	PersistNondeterministicSystemVarDefaults := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			err := dsess.PersistSystemVarDefaults(cfg.DoltEnv)
			if err != nil {
				logrus.Errorf("unable to persist system variable defaults: %v", err)
			}
			// Always return nil, because we don't want an invalid config value to prevent
			// the server from starting up.
			return nil
		},
	}
	controller.Register(PersistNondeterministicSystemVarDefaults)

	InitStatsController := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			return sqlEngine.InitStats(ctx)
		},
	}
	controller.Register(InitStatsController)

	InitBinlogging := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			sqlCtx := sql.NewContext(ctx)
			primaryController := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.BinlogPrimaryController
			doltBinlogPrimaryController, ok := primaryController.(*binlogreplication.DoltBinlogPrimaryController)
			if !ok {
				return fmt.Errorf("unexpected type of binlog controller: %T", primaryController)
			}

			_, logBinValue, ok := sql.SystemVariables.GetGlobal("log_bin")
			if !ok {
				return fmt.Errorf("unable to load @@log_bin system variable")
			}
			logBin, ok := logBinValue.(int8)
			if !ok {
				return fmt.Errorf("unexpected type for @@log_bin system variable: %T", logBinValue)
			}

			_, logBinBranchValue, ok := sql.SystemVariables.GetGlobal("log_bin_branch")
			if !ok {
				return fmt.Errorf("unable to load @@log_bin_branch system variable")
			}
			logBinBranch, ok := logBinBranchValue.(string)
			if !ok {
				return fmt.Errorf("unexpected type for @@log_bin_branch system variable: %T", logBinBranchValue)
			}
			if logBinBranch != "" {
				// If an invalid branch has been configured, let the server start up so that it's
				// easier for customers to correct the value, but log a warning and don't enable
				// binlog replication.
				if strings.Contains(logBinBranch, "/") {
					logrus.Warnf("branch names containing '/' are not supported "+
						"for binlog replication. Not enabling binlog replication; fix "+
						"@@log_bin_branch value and restart Dolt (current value: %s)", logBinBranch)
					return nil
				}

				binlogreplication.BinlogBranch = logBinBranch
			}

			if logBin == 1 {
				logrus.Infof("Enabling binary logging for branch %s", logBinBranch)
				binlogProducer, err := binlogreplication.NewBinlogProducer(sqlCtx, cfg.DoltEnv.FS)
				if err != nil {
					return err
				}

				logManager, err := binlogreplication.NewLogManager(sqlCtx, fs)
				if err != nil {
					return err
				}
				binlogProducer.LogManager(logManager)
				doltdb.RegisterDatabaseUpdateListener(binlogProducer)
				doltBinlogPrimaryController.BinlogProducer(binlogProducer)

				// Register binlog hooks for database creation/deletion
				provider := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.DbProvider
				if doltProvider, ok := provider.(*sqle.DoltDatabaseProvider); ok {
					doltProvider.AddInitDatabaseHook(binlogreplication.NewBinlogInitDatabaseHook(nil, doltdb.DatabaseUpdateListeners))
					doltProvider.AddDropDatabaseHook(binlogreplication.NewBinlogDropDatabaseHook(nil, doltdb.DatabaseUpdateListeners))
				}
			}

			return nil
		},
	}
	controller.Register(InitBinlogging)

	// MySQL creates a root superuser when the mysql install is first initialized. Depending on the options
	// specified, the root superuser is created without a password, or with a random password. This varies
	// slightly in some OS-specific installers. Dolt initializes the root superuser the first time a
	// sql-server is started and initializes its privileges database. We do this on sql-server initialization,
	// instead of dolt db initialization, because we only want to create the privileges database when it's
	// used for a server, and because we want the same root initialization logic when a sql-server is started
	// for a clone. More details: https://dev.mysql.com/doc/mysql-security-excerpt/8.0/en/default-privileges.html
	InitImplicitRootSuperUser := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			// If privileges.db has already been initialized, indicating that this is NOT the
			// first time sql-server has been launched, then don't initialize the root superuser.
			if permissionDbExists, err := doesPrivilegesDbExist(cfg.DoltEnv, cfg.ServerConfig.PrivilegeFilePath()); err != nil {
				return err
			} else if permissionDbExists {
				logrus.Debug("privileges.db already exists, not creating root superuser")
				return nil
			}

			// We always persist the privileges.db file, to signal that the privileges system has been initialized
			mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
			ed := mysqlDb.Editor()
			defer ed.Close()

			// Create the root@localhost superuser, unless --skip-root-user-initialization was specified
			if !config.SkipRootUserInitialization {
				// Allow the user to override the default root host (localhost) and password ("").
				// This is particularly useful in a Docker container, where you need to connect
				// to the sql-server from outside the container and can't rely on localhost.
				rootHost := "localhost"
				doltRootHost := os.Getenv(dconfig.EnvDoltRootHost)
				if doltRootHost != "" {
					logrus.Infof("Overriding root user host with value from DOLT_ROOT_HOST: %s", doltRootHost)
					rootHost = doltRootHost
				}

				rootPassword := servercfg.DefaultPass
				doltRootPassword := os.Getenv(dconfig.EnvDoltRootPassword)
				if doltRootPassword != "" {
					logrus.Info("Overriding root user password with value from DOLT_ROOT_PASSWORD")
					rootPassword = doltRootPassword
				}

				logrus.Infof("Creating root@%s superuser", rootHost)
				mysqlDb.AddSuperUser(ed, servercfg.DefaultUser, rootHost, rootPassword)
			}

			// TODO: The in-memory filesystem doesn't work with the GMS API
			//       for persisting the privileges database. The filesys API
			//       is in the Dolt layer, so when the file path is passed to
			//       GMS, it expects it to be a path on disk, and errors out.
			if _, isInMemFs := cfg.DoltEnv.FS.(*filesys.InMemFS); isInMemFs {
				return nil
			} else {
				sqlCtx, err := sqlEngine.NewDefaultContext(context.Background())
				if err != nil {
					return err
				}
				return mysqlDb.Persist(sqlCtx, ed)
			}
		},
	}
	controller.Register(InitImplicitRootSuperUser)

	var metListener *metricsListener
	InitMetricsListener := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			labels := cfg.ServerConfig.MetricsLabels()
			metListener, err = newMetricsListener(labels, cfg.Version, clusterController)
			return err
		},
		StopF: func() error {
			metListener.Close()
			return nil
		},
	}
	controller.Register(InitMetricsListener)

	InitLockSuperUser := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb

			host := "localhost"
			// LocalConnectionUser is reserved for `dolt sql` otherwise the ephemeral user can't be created
			reserved := [...]string{LocalConnectionUser} // users necessary at startup to enter dolt sql-server

			// check for reserved user conflicts
			rd := mysqlDb.Reader()
			var warn strings.Builder
			conflicts := make([]mysql_db.UserPrimaryKey, 0, len(reserved))
			for _, user := range reserved {
				conflict, _ := rd.GetUser(mysql_db.UserPrimaryKey{Host: host, User: user})
				if conflict != nil && !conflict.IsEphemeral {
					warn.WriteString(fmt.Sprintf("Dropped persistent '%s@%s' as it conflicts with dolt reserved '%s@%s'", conflict.User, conflict.Host, user, host))
					conflicts = append(conflicts, mysql_db.UserPrimaryKey{Host: conflict.Host, User: conflict.User})
				}
			}
			rd.Close()

			ed := mysqlDb.Editor()
			defer ed.Close()

			for _, conflict := range conflicts {
				ed.RemoveUser(conflict)

				ed.RemoveRoleEdgesFromKey(mysql_db.RoleEdgesFromKey{
					FromHost: conflict.Host,
					FromUser: conflict.User,
				})

				ed.RemoveRoleEdgesToKey(mysql_db.RoleEdgesToKey{
					ToHost: conflict.Host,
					ToUser: conflict.User,
				})
			}

			if len(conflicts) > 0 {
				log.Warning(warn.String())
				sqlCtx, err := sqlEngine.NewDefaultContext(ctx)
				if err != nil {
					return fmt.Errorf("failed to create SQL context: %v", err)
				}

				if err := mysqlDb.Persist(sqlCtx, ed); err != nil {
					return fmt.Errorf("failed to persist changes to privileges database: %v", err)
				}
			}

			mysqlDb.AddEphemeralSuperUser(ed, LocalConnectionUser, "localhost", localCreds.Secret)

			return nil
		},
	}
	controller.Register(InitLockSuperUser)

	DisableMySQLDbIfRequired := &svcs.AnonService{
		InitF: func(context.Context) error {
			if ExternalDisableUsers {
				mysqlDb := sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb
				mysqlDb.SetEnabled(false)
			}
			return nil
		},
	}
	controller.Register(DisableMySQLDbIfRequired)

	type SQLMetricsService struct {
		state svcs.ServiceState
		lis   net.Listener
		srv   *http.Server
	}

	var metSrv SQLMetricsService

	RunMetricsServer := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			if cfg.ServerConfig.MetricsHost() != "" && cfg.ServerConfig.MetricsPort() > 0 {
				metSrv.state.Swap(svcs.ServiceState_Init)

				addr := fmt.Sprintf("%s:%d", cfg.ServerConfig.MetricsHost(), cfg.ServerConfig.MetricsPort())
				metSrv.lis, err = net.Listen("tcp", addr)
				if err != nil {
					return err
				}

				mux := http.NewServeMux()
				mux.Handle("/metrics", promhttp.Handler())
				metSrv.srv = &http.Server{
					Addr:    addr,
					Handler: mux,
				}

			}
			return nil
		},
		RunF: func(context.Context) {
			if metSrv.state.CompareAndSwap(svcs.ServiceState_Init, svcs.ServiceState_Run) {
				_ = metSrv.srv.Serve(metSrv.lis)
			}
		},
		StopF: func() error {
			state := metSrv.state.Swap(svcs.ServiceState_Stopped)
			if state == svcs.ServiceState_Run {
				metSrv.srv.Close()
			} else if state == svcs.ServiceState_Init {
				metSrv.lis.Close()
			}
			return nil
		},
	}
	controller.Register(RunMetricsServer)

	type RemoteSrvService struct {
		state svcs.ServiceState
		lis   remotesrv.Listeners
		srv   *remotesrv.Server
	}
	var remoteSrv RemoteSrvService
	RunRemoteSrv := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			if cfg.ServerConfig.RemotesapiPort() == nil {
				return nil
			}
			remoteSrv.state.Swap(svcs.ServiceState_Init)

			port := *cfg.ServerConfig.RemotesapiPort()

			apiReadOnly := false
			if cfg.ServerConfig.RemotesapiReadOnly() != nil {
				apiReadOnly = *cfg.ServerConfig.RemotesapiReadOnly()
			}

			listenaddr := fmt.Sprintf(":%d", port)
			sqlContextInterceptor := sqle.SqlContextServerInterceptor{
				Factory: sqlEngine.NewDefaultContext,
			}
			args := remotesrv.ServerArgs{
				Logger:             logrus.NewEntry(lgr),
				ReadOnly:           apiReadOnly || cfg.ServerConfig.ReadOnly(),
				HttpListenAddr:     listenaddr,
				GrpcListenAddr:     listenaddr,
				ConcurrencyControl: remotesapi.PushConcurrencyControl_PUSH_CONCURRENCY_CONTROL_ASSERT_WORKING_SET,
				Options:            sqlContextInterceptor.Options(),
				HttpInterceptor:    sqlContextInterceptor.HTTP(nil),
			}
			var err error
			args.FS = sqlEngine.FileSystem()
			args.DBCache, err = sqle.RemoteSrvDBCache(sqle.GetInterceptorSqlContext, sqle.DoNotCreateUnknownDatabases)
			if err != nil {
				lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
				return err
			}

			authenticator := newAccessController(sqle.GetInterceptorSqlContext, sqlEngine.GetUnderlyingEngine().Analyzer.Catalog.MySQLDb)
			args = sqle.WithUserPasswordAuth(args, authenticator)
			args.TLSConfig = serverConf.TLSConfig

			remoteSrv.srv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", port, err)
				return err
			}
			remoteSrv.lis, err = remoteSrv.srv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners on port %d: %v", port, err)
				return err
			}
			return nil
		},
		RunF: func(ctx context.Context) {
			if remoteSrv.state.CompareAndSwap(svcs.ServiceState_Init, svcs.ServiceState_Run) {
				remoteSrv.srv.Serve(remoteSrv.lis)
			}
		},
		StopF: func() error {
			state := remoteSrv.state.Swap(svcs.ServiceState_Stopped)
			if state == svcs.ServiceState_Run {
				remoteSrv.srv.GracefulStop()
			} else if state == svcs.ServiceState_Init {
				remoteSrv.lis.Close()
			}
			return nil
		},
	}
	controller.Register(RunRemoteSrv)

	var clusterRemoteSrv RemoteSrvService
	RunClusterRemoteSrv := &svcs.AnonService{
		InitF: func(context.Context) error {
			if clusterController == nil {
				return nil
			}
			clusterRemoteSrv.state.Swap(svcs.ServiceState_Init)

			args, err := clusterController.RemoteSrvServerArgs(sqlEngine.NewDefaultContext, remotesrv.ServerArgs{
				Logger: logrus.NewEntry(lgr),
			})
			if err != nil {
				lgr.Errorf("error creating SQL engine context for remotesapi server: %v", err)
				return err
			}
			args.FS = sqlEngine.FileSystem()

			clusterRemoteSrvTLSConfig, err := LoadClusterTLSConfig(cfg.ServerConfig.ClusterConfig())
			if err != nil {
				lgr.Errorf("error starting remotesapi server for cluster config, could not load tls config: %v", err)
				return err
			}
			args.TLSConfig = clusterRemoteSrvTLSConfig

			clusterRemoteSrv.srv, err = remotesrv.NewServer(args)
			if err != nil {
				lgr.Errorf("error creating remotesapi server on port %d: %v", *cfg.ServerConfig.RemotesapiPort(), err)
				return err
			}
			clusterController.RegisterGrpcServices(sqle.GetInterceptorSqlContext, clusterRemoteSrv.srv.GrpcServer())

			clusterRemoteSrv.lis, err = clusterRemoteSrv.srv.Listeners()
			if err != nil {
				lgr.Errorf("error starting remotesapi server listeners for cluster config on %s: %v", clusterController.RemoteSrvListenAddr(), err)
				return err
			}
			return nil
		},
		RunF: func(context.Context) {
			if clusterRemoteSrv.state.CompareAndSwap(svcs.ServiceState_Init, svcs.ServiceState_Run) {
				clusterRemoteSrv.srv.Serve(clusterRemoteSrv.lis)
			}
		},
		StopF: func() error {
			state := clusterRemoteSrv.state.Swap(svcs.ServiceState_Stopped)
			if state == svcs.ServiceState_Run {
				clusterRemoteSrv.srv.GracefulStop()
			} else if state == svcs.ServiceState_Init {
				clusterRemoteSrv.lis.Close()
			}
			return nil
		},
	}
	controller.Register(RunClusterRemoteSrv)

	// We still have some startup to do from this point, and we do not run
	// the SQL server until we are fully booted. We also want to stop the
	// SQL server as the first thing we stop. However, if startup fails
	// during initialization, we want to shutdown the SQL server cleanly.
	// So we track whether the server has been shutdown by either service
	// which is responsible for it and we only do it here if it hasn't
	// already been Closed.

	var sqlServerClosed bool
	InitSQLServer := &svcs.AnonService{
		InitF: func(context.Context) (err error) {
			v, ok := cfg.ServerConfig.(servercfg.ValidatingServerConfig)
			if ok && v.GoldenMysqlConnectionString() != "" {
				mySQLServer, err = server.NewServerWithHandler(
					serverConf,
					sqlEngine.GetUnderlyingEngine(),
					sqlEngine.ContextFactory,
					newSessionBuilder(sqlEngine, cfg.ServerConfig),
					metListener,
					func(h mysql.Handler) (mysql.Handler, error) {
						return golden.NewValidatingHandler(h, v.GoldenMysqlConnectionString(), logrus.StandardLogger())
					},
				)
			} else {
				mySQLServer, err = server.NewServer(
					serverConf,
					sqlEngine.GetUnderlyingEngine(),
					sqlEngine.ContextFactory,
					newSessionBuilder(sqlEngine, cfg.ServerConfig),
					metListener,
				)
			}
			if errors.Is(err, server.UnixSocketInUseError) {
				lgr.Warn("unix socket set up failed: file already in use: ", serverConf.Socket)
				err = nil
			}
			return err
		},
		StopF: func() (err error) {
			if !sqlServerClosed {
				sqlServerClosed = true
				return mySQLServer.Close()
			}
			return nil
		},
	}
	controller.Register(InitSQLServer)

	// Automatically restart binlog replication if replication was enabled when the server was last shut down
	AutoStartBinlogReplica := &svcs.AnonService{
		InitF: func(ctx context.Context) error {
			// If we're unable to restart replication, log an error, but don't prevent the server from starting up
			sqlCtx, err := sqlEngine.NewDefaultContext(ctx)
			if err != nil {
				logrus.Errorf("unable to restart replication, could not create session: %s", err.Error())
				return nil
			}
			defer sql.SessionEnd(sqlCtx.Session)
			if err := binlogreplication.DoltBinlogReplicaController.AutoStart(sqlCtx); err != nil {
				logrus.Errorf("unable to restart replication: %s", err.Error())
			}
			return nil
		},
	}
	controller.Register(AutoStartBinlogReplica)

	RunClusterController := &svcs.AnonService{
		InitF: func(context.Context) error {
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
		RunF: func(context.Context) {
			if clusterController == nil {
				return
			}
			clusterController.Run()
		},
		StopF: func() error {
			if clusterController == nil {
				return nil
			}
			clusterController.GracefulStop()
			return nil
		},
	}
	controller.Register(RunClusterController)

	RunSQLServer := &svcs.AnonService{
		RunF: func(context.Context) {
			sqlserver.SetRunningServer(mySQLServer)
			defer sqlserver.UnsetRunningServer()
			mySQLServer.Start()
		},
		StopF: func() error {
			sqlServerClosed = true
			return mySQLServer.Close()
		},
	}
	controller.Register(RunSQLServer)
}

// heartbeatService is a service that sends a heartbeat event to the metrics server once a day
type heartbeatService struct {
	version      string
	eventEmitter events.Emitter
	interval     time.Duration
	closer       func() error
}

func newHeartbeatService(version string, dEnv *env.DoltEnv) *heartbeatService {
	metricsDisabled := dEnv.Config.GetStringOrDefault(config.MetricsDisabled, "false")
	disabled, err := strconv.ParseBool(metricsDisabled)
	if err != nil || disabled {
		return &heartbeatService{} // will be defunct on Run()
	}

	emitterType, ok := os.LookupEnv(events.EmitterTypeEnvVar)
	if !ok {
		emitterType = events.EmitterTypeGrpc
	}

	interval, ok := os.LookupEnv(sqlServerHeartbeatIntervalEnvVar)
	if !ok {
		interval = "24h"
	}

	duration, err := time.ParseDuration(interval)
	if err != nil {
		return &heartbeatService{} // will be defunct on Run()
	}

	emitter, closer, err := commands.NewEmitter(emitterType, dEnv)
	if err != nil {
		return &heartbeatService{} // will be defunct on Run()
	}

	events.SetGlobalCollector(events.NewCollector(version, emitter))

	return &heartbeatService{
		version:      version,
		eventEmitter: emitter,
		interval:     duration,
		closer:       closer,
	}
}

func (h *heartbeatService) Init(ctx context.Context) error { return nil }

func (h *heartbeatService) Stop() error {
	if h.closer != nil {
		return h.closer()
	}
	return nil
}

func (h *heartbeatService) Run(ctx context.Context) {
	// Faulty config settings or disabled metrics can cause us to not have a valid event emitter
	if h.eventEmitter == nil {
		return
	}

	ticker := time.NewTicker(h.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t := events.NowTimestamp()
			logrus.Debugf("sending heartbeat event to %s:%s", events.DefaultMetricsHost, events.DefaultMetricsPort)
			err := h.eventEmitter.LogEvents(ctx, h.version, []*eventsapi.ClientEvent{
				{
					Id:        uuid.New().String(),
					StartTime: t,
					EndTime:   t,
					Type:      eventsapi.ClientEventType_SQL_SERVER_HEARTBEAT,
				},
			})

			if err != nil {
				logrus.Debugf("failed to send heartbeat event: %v", err)
			}
		}
	}
}

var _ svcs.Service = &heartbeatService{}

func persistServerLocalCreds(port int, dEnv *env.DoltEnv) (*LocalCreds, error) {
	creds := NewLocalCreds(port)
	err := WriteLocalCreds(dEnv.FS, creds)
	if err != nil {
		return nil, err
	}
	return creds, err
}

// remotesapiAuth facilitates the implementation remotesrv.AccessControl for the remotesapi server.
type remotesapiAuth struct {
	// ctxFactory is a function that returns a new sql.Context. This will create a new context every time it is called,
	// so it should be called once per API request.
	ctxFactory func(context.Context) (*sql.Context, error)
	rawDb      *mysql_db.MySQLDb
}

func newAccessController(ctxFactory func(context.Context) (*sql.Context, error), rawDb *mysql_db.MySQLDb) remotesrv.AccessControl {
	return &remotesapiAuth{ctxFactory, rawDb}
}

// ApiAuthenticate checks the provided credentials against the database and return a SQL context if the credentials are
// valid. If the credentials are invalid, then a nil context is returned. Failures to authenticate are logged.
func (r *remotesapiAuth) ApiAuthenticate(ctx context.Context) (context.Context, error) {
	creds, err := remotesrv.ExtractBasicAuthCreds(ctx)
	if err != nil {
		return nil, err
	}

	err = commands.ValidatePasswordWithAuthResponse(r.rawDb, creds.Username, creds.Password)
	if err != nil {
		return nil, fmt.Errorf("API Authentication Failure: %v", err)
	}

	address := creds.Address
	if strings.Index(address, ":") > 0 {
		address, _, err = net.SplitHostPort(creds.Address)
		if err != nil {
			return nil, fmt.Errorf("Invalid Host string for authentication: %s", creds.Address)
		}
	}

	sqlCtx, err := r.ctxFactory(ctx)
	if err != nil {
		return nil, fmt.Errorf("API Runtime error: %v", err)
	}

	sqlCtx.Session.SetClient(sql.Client{User: creds.Username, Address: address, Capabilities: 0})

	updatedCtx := context.WithValue(ctx, ApiSqleContextKey, sqlCtx)

	return updatedCtx, nil
}

func (r *remotesapiAuth) ApiAuthorize(ctx context.Context, superUserRequired bool) (bool, error) {
	sqlCtx, ok := ctx.Value(ApiSqleContextKey).(*sql.Context)
	if !ok {
		return false, fmt.Errorf("Runtime error: could not get SQL context from context")
	}

	privOp := sql.NewDynamicPrivilegedOperation(plan.DynamicPrivilege_CloneAdmin)
	if superUserRequired {
		database := sqlCtx.GetCurrentDatabase()
		subject := sql.PrivilegeCheckSubject{Database: database}
		privOp = sql.NewPrivilegedOperation(subject, sql.PrivilegeType_Super)
	}

	authorized := r.rawDb.UserHasPrivileges(sqlCtx, privOp)

	if !authorized {
		if superUserRequired {
			return false, fmt.Errorf("API Authorization Failure: %s has not been granted SuperUser access", sqlCtx.Session.Client().User)
		}
		return false, fmt.Errorf("API Authorization Failure: %s has not been granted CLONE_ADMIN access", sqlCtx.Session.Client().User)
	}
	return true, nil
}

// doesPrivilegesDbExist looks for an existing privileges database as the specified |privilegeFilePath|. If
// |privilegeFilePath| is an absolute path, it is used directly. If it is a relative path, then it is resolved
// relative to the root of the specified |dEnv|.
func doesPrivilegesDbExist(dEnv *env.DoltEnv, privilegeFilePath string) (exists bool, err error) {
	if !filepath.IsAbs(privilegeFilePath) {
		privilegeFilePath, err = dEnv.FS.Abs(privilegeFilePath)
		if err != nil {
			return false, err
		}
	}

	_, err = os.Stat(privilegeFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	}

	return true, nil
}

func LoadClusterTLSConfig(cfg servercfg.ClusterConfig) (*tls.Config, error) {
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

func newSessionBuilder(se *engine.SqlEngine, config servercfg.ServerConfig) server.SessionBuilder {
	userToSessionVars := make(map[string]map[string]interface{})
	userVars := config.UserVars()
	for _, curr := range userVars {
		userToSessionVars[curr.Name] = curr.Vars
	}

	return func(ctx context.Context, conn *mysql.Conn, addr string) (sql.Session, error) {
		baseSession, err := sql.BaseSessionFromConnection(ctx, conn, addr)
		if err != nil {
			return nil, err
		}

		dsess, err := se.NewDoltSession(ctx, baseSession)
		if err != nil {
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
func getConfigFromServerConfig(serverConfig servercfg.ServerConfig, plf server.ProtocolListenerFunc) (server.Config, error) {
	serverConf, err := handleProtocolAndAddress(serverConfig)
	if err != nil {
		return server.Config{}, err
	}

	serverConf.DisableClientMultiStatements = serverConfig.DisableClientMultiStatements()

	readTimeout := time.Duration(serverConfig.ReadTimeout()) * time.Millisecond
	writeTimeout := time.Duration(serverConfig.WriteTimeout()) * time.Millisecond

	tlsConfig, err := servercfg.LoadTLSConfig(serverConfig)
	if err != nil {
		return server.Config{}, err
	}

	serverConf, err = serverConf.NewConfig()
	if err != nil {
		return server.Config{}, err
	}

	// Do not set the value of Version.  Let it default to what go-mysql-server uses.  This should be equivalent
	// to the value of mysql that we support.
	serverConf.ConnReadTimeout = readTimeout
	serverConf.ConnWriteTimeout = writeTimeout
	serverConf.MaxConnections = serverConfig.MaxConnections()
	serverConf.MaxWaitConnections = serverConfig.MaxWaitConnections()
	serverConf.MaxWaitConnectionsTimeout = serverConfig.MaxWaitConnectionsTimeout()
	serverConf.TLSConfig = tlsConfig
	serverConf.RequireSecureTransport = serverConfig.RequireSecureTransport()
	serverConf.MaxLoggedQueryLen = serverConfig.MaxLoggedQueryLen()
	serverConf.EncodeLoggedQuery = serverConfig.ShouldEncodeLoggedQuery()
	serverConf.ProtocolListenerFactory = plf

	return serverConf, nil
}

// handleProtocolAndAddress returns new server.Config object with only Protocol and Address defined.
func handleProtocolAndAddress(serverConfig servercfg.ServerConfig) (server.Config, error) {
	serverConf := server.Config{Protocol: "tcp"}

	portAsString := strconv.Itoa(serverConfig.Port())
	hostPort := net.JoinHostPort(serverConfig.Host(), portAsString)
	if portInUse(hostPort) {
		portInUseError := fmt.Errorf("Port %s already in use.", portAsString)
		return server.Config{}, portInUseError
	}
	serverConf.Address = hostPort

	sock, useSock, err := servercfg.CheckForUnixSocket(serverConfig)
	if err != nil {
		return server.Config{}, err
	}
	if useSock {
		serverConf.Socket = sock
	}

	return serverConf, nil
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
