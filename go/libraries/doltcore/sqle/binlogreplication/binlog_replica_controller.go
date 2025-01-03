// Copyright 2022 Dolthub, Inc.
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

package binlogreplication

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	sqle "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

var DoltBinlogReplicaController = newDoltBinlogReplicaController()

// binlogApplierUser is the locked, super user account that is used to execute replicated SQL statements.
// We cannot always assume the root account will exist, so we automatically create this account that is
// specific to binlog replication and lock it so that it cannot be used to login.
const binlogApplierUser = "dolt-binlog-applier"

// ErrServerNotConfiguredAsReplica is returned when replication is started without enough configuration provided.
var ErrServerNotConfiguredAsReplica = fmt.Errorf(
	"server is not configured as a replica; fix with CHANGE REPLICATION SOURCE TO")

// ErrEmptyHostname is returned when replication is started without a hostname configured.
var ErrEmptyHostname = fmt.Errorf("fatal error: Invalid (empty) hostname when attempting to connect " +
	"to the source server. Connection attempt terminated")

// ErrEmptyUsername is returned when replication is started without a username configured.
var ErrEmptyUsername = fmt.Errorf("fatal error: Invalid (empty) username when attempting to connect " +
	"to the source server. Connection attempt terminated")

// ErrReplicationStopped is an internal error that is not returned to users, and signals that STOP REPLICA was called.
var ErrReplicationStopped = fmt.Errorf("replication stop requested")

// doltBinlogReplicaController implements the BinlogReplicaController interface for a Dolt database in order to
// provide support for a Dolt server to be a replica of a MySQL primary.
//
// This type is used concurrently – multiple sessions on the DB can call this interface concurrently,
// so all state that the controller tracks MUST be protected with a mutex.
type doltBinlogReplicaController struct {
	status  binlogreplication.ReplicaStatus
	filters *filterConfiguration
	applier *binlogReplicaApplier
	ctx     *sql.Context

	// statusMutex blocks concurrent access to the ReplicaStatus struct
	statusMutex *sync.Mutex

	// operationMutex blocks concurrent access to the START/STOP/RESET REPLICA operations
	operationMutex *sync.Mutex
	engine         *sqle.Engine
}

var _ binlogreplication.BinlogReplicaController = (*doltBinlogReplicaController)(nil)

// newDoltBinlogReplicaController creates a new doltBinlogReplicaController instance.
func newDoltBinlogReplicaController() *doltBinlogReplicaController {
	controller := doltBinlogReplicaController{
		filters:        newFilterConfiguration(),
		statusMutex:    &sync.Mutex{},
		operationMutex: &sync.Mutex{},
	}
	controller.status.ConnectRetry = 60
	controller.status.SourceRetryCount = 86400
	controller.status.AutoPosition = true
	controller.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	controller.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning
	controller.applier = newBinlogReplicaApplier(controller.filters)
	return &controller
}

// StartReplica implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) StartReplica(ctx *sql.Context) error {
	d.operationMutex.Lock()
	defer d.operationMutex.Unlock()

	// START REPLICA may be called multiple times, but if replication is already running,
	// it will log a warning and not start up new threads.
	if d.applier.IsRunning() {
		ctx.Warn(3083, "Replication thread(s) for channel '' are already running.")
		return nil
	}

	if false {
		// TODO: If the database is already configured for Dolt replication/clustering, then error out.
		//       Add a (BATS?) test to cover this case
		return fmt.Errorf("dolt replication already enabled; unable to use binlog replication with other replication modes. " +
			"Disable Dolt replication first before starting binlog replication")
	}

	// If we aren't running in a sql-server context, it would be nice to return a helpful, Dolt-specific
	// error message. Currently, this case would trigger an error from the GMS layer, so we can't give
	// a specific error message about needing to run Dolt in sql-server mode yet.

	_, err := loadReplicaServerId()
	if err != nil {
		return fmt.Errorf("unable to start replication: %s", err.Error())
	}

	configuration, err := loadReplicationConfiguration(ctx, d.engine.Analyzer.Catalog.MySQLDb)
	if err != nil {
		return err
	} else if configuration == nil {
		return ErrServerNotConfiguredAsReplica
	} else if configuration.Host == "" {
		DoltBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyHostname.Error())
		return ErrEmptyHostname
	} else if configuration.User == "" {
		DoltBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyUsername.Error())
		return ErrEmptyUsername
	}

	if d.ctx == nil {
		return fmt.Errorf("no execution context set for the replica controller")
	}

	d.configureReplicationUser(ctx)

	// Set execution context's user to the binlog replication user
	d.ctx.SetClient(sql.Client{
		User:    binlogApplierUser,
		Address: "localhost",
	})

	ctx.GetLogger().Info("starting binlog replication...")
	d.applier.Go(d.ctx)

	// Attempt to record that the replica has started replication so that it will
	// start automatically the next time the replica server is started.
	if err := persistReplicaRunningState(ctx, running); err != nil {
		ctx.GetLogger().Errorf("unable to persist replica running state: %s", err.Error())
	}

	return nil
}

// configureReplicationUser creates or configures the superuser account needed to apply replication
// changes and execute DDL statements on the running server. If the account doesn't exist, it will be
// created and locked to disable log ins, and if it does exist, but is missing super privs or is not
// locked, it will be given superuser privs and locked.
func (d *doltBinlogReplicaController) configureReplicationUser(_ *sql.Context) {
	mySQLDb := d.engine.Analyzer.Catalog.MySQLDb
	ed := mySQLDb.Editor()
	defer ed.Close()
	mySQLDb.AddLockedSuperUser(ed, binlogApplierUser, "localhost", "")
}

// SetExecutionContext sets the unique |ctx| for the replica's applier to use when applying changes from binlog events
// to a database. The applier cannot reuse any existing context, because it executes in a separate routine and would
// cause race conditions.
func (d *doltBinlogReplicaController) SetExecutionContext(ctx *sql.Context) {
	d.ctx = ctx
}

// SetEngine sets the SQL engine this replica will use when running replicated statements and
// when loading the Catalog to find the "mysql" database.
func (d *doltBinlogReplicaController) SetEngine(engine *sqle.Engine) {
	d.engine = engine
	d.applier.engine = engine
}

// StopReplica implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) StopReplica(ctx *sql.Context) error {
	if d.applier.IsRunning() == false {
		ctx.Warn(3084, "Replication thread(s) for channel '' are already stopped.")
		return nil
	}

	d.applier.stopReplicationChan <- struct{}{}

	d.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
		status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning
	})

	// Attempt to record that the replica has stopped replication so that it will not
	// start automatically the next time the replica server is started.
	if err := persistReplicaRunningState(ctx, notRunning); err != nil {
		ctx.GetLogger().Errorf("unable to persist replica running state: %s", err.Error())
	}

	return nil
}

// SetReplicationSourceOptions implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) SetReplicationSourceOptions(ctx *sql.Context, options []binlogreplication.ReplicationOption) error {
	replicaSourceInfo, err := loadReplicationConfiguration(ctx, d.engine.Analyzer.Catalog.MySQLDb)
	if err != nil {
		return err
	}

	if replicaSourceInfo == nil {
		replicaSourceInfo = mysql_db.NewReplicaSourceInfo()
	}

	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "SOURCE_HOST":
			value, err := getOptionValueAsString(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.Host = value
		case "SOURCE_USER":
			value, err := getOptionValueAsString(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.User = value
		case "SOURCE_PASSWORD":
			value, err := getOptionValueAsString(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.Password = value
		case "SOURCE_PORT":
			intValue, err := getOptionValueAsInt(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.Port = uint16(intValue)
		case "SOURCE_CONNECT_RETRY":
			intValue, err := getOptionValueAsInt(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.ConnectRetryInterval = uint32(intValue)
		case "SOURCE_RETRY_COUNT":
			intValue, err := getOptionValueAsInt(option)
			if err != nil {
				return err
			}
			replicaSourceInfo.ConnectRetryCount = uint64(intValue)
		case "SOURCE_AUTO_POSITION":
			intValue, err := getOptionValueAsInt(option)
			if err != nil {
				return err
			}
			if intValue < 1 {
				return fmt.Errorf("SOURCE_AUTO_POSITION cannot be disabled")
			}
		default:
			return fmt.Errorf("unknown replication source option: %s", option.Name)
		}
	}

	// Persist the updated replica source configuration to disk
	return persistReplicationConfiguration(ctx, replicaSourceInfo, d.engine.Analyzer.Catalog.MySQLDb)
}

// SetReplicationFilterOptions implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) SetReplicationFilterOptions(_ *sql.Context, options []binlogreplication.ReplicationOption) error {
	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "REPLICATE_DO_TABLE":
			value, err := getOptionValueAsTableNames(option)
			if err != nil {
				return err
			}
			err = d.filters.setDoTables(value)
			if err != nil {
				return err
			}
		case "REPLICATE_IGNORE_TABLE":
			value, err := getOptionValueAsTableNames(option)
			if err != nil {
				return err
			}
			err = d.filters.setIgnoreTables(value)
			if err != nil {
				return err
			}
		default:
			return fmt.Errorf("unsupported replication filter option: %s", option.Name)
		}
	}

	// TODO: Consider persisting filter settings. MySQL doesn't actually do this... unlike CHANGE REPLICATION SOURCE,
	//       CHANGE REPLICATION FILTER requires users to re-apply the filter options every time a server is restarted,
	//       or to pass them to mysqld on the command line or in configuration. Since we don't want to force users
	//       to specify these on the command line, we should consider diverging from MySQL behavior here slightly and
	//       persisting the filter configuration options if customers want this.

	return nil
}

// GetReplicaStatus implements the BinlogReplicaController interface
func (d *doltBinlogReplicaController) GetReplicaStatus(ctx *sql.Context) (*binlogreplication.ReplicaStatus, error) {
	replicaSourceInfo, err := loadReplicationConfiguration(ctx, d.engine.Analyzer.Catalog.MySQLDb)
	if err != nil {
		return nil, err
	}

	// Lock to read status consistently
	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()
	var copy = d.status

	if replicaSourceInfo == nil {
		return &copy, nil
	}

	copy.SourceUser = replicaSourceInfo.User
	copy.SourceHost = replicaSourceInfo.Host
	copy.SourcePort = uint(replicaSourceInfo.Port)
	copy.SourceServerUuid = replicaSourceInfo.Uuid
	copy.ConnectRetry = replicaSourceInfo.ConnectRetryInterval
	copy.SourceRetryCount = replicaSourceInfo.ConnectRetryCount
	copy.ReplicateDoTables = d.filters.getDoTables()
	copy.ReplicateIgnoreTables = d.filters.getIgnoreTables()

	if d.applier.currentPosition != nil {
		copy.ExecutedGtidSet = d.applier.currentPosition.GTIDSet.String()
		copy.RetrievedGtidSet = copy.ExecutedGtidSet
	}

	return &copy, nil
}

// ResetReplica implements the BinlogReplicaController interface
func (d *doltBinlogReplicaController) ResetReplica(ctx *sql.Context, resetAll bool) error {
	d.operationMutex.Lock()
	defer d.operationMutex.Unlock()

	if d.applier.IsRunning() {
		return fmt.Errorf("unable to reset replica while replication is running; stop replication and try again")
	}

	// Reset error status
	d.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.LastIoErrNumber = 0
		status.LastSqlErrNumber = 0
		status.LastIoErrorTimestamp = nil
		status.LastSqlErrorTimestamp = nil
		status.LastSqlError = ""
		status.LastIoError = ""
	})

	if resetAll {
		err := deleteReplicationConfiguration(ctx, d.engine.Analyzer.Catalog.MySQLDb)
		if err != nil {
			return err
		}

		d.filters = newFilterConfiguration()
	}

	return nil
}

// updateStatus allows the caller to safely update the replica controller's status. The controller locks it's mutex
// before the specified function |f| is called, and unlocks it after |f| is finished running. The current status is
// passed into the callback function |f| and the caller can safely update or copy any fields they need.
func (d *doltBinlogReplicaController) updateStatus(f func(status *binlogreplication.ReplicaStatus)) {
	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()
	f(&d.status)
}

// setIoError updates the current replication status with the specific |errno| and |message| to describe an IO error.
func (d *doltBinlogReplicaController) setIoError(errno uint, message string) {
	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()

	// truncate the message to avoid errors when reporting replica status
	if len(message) > 256 {
		message = message[:256]
	}

	currentTime := time.Now()
	d.status.LastIoErrorTimestamp = &currentTime
	d.status.LastIoErrNumber = errno
	d.status.LastIoError = message
}

// setSqlError updates the current replication status with the specific |errno| and |message| to describe an SQL error.
func (d *doltBinlogReplicaController) setSqlError(errno uint, message string) {
	d.statusMutex.Lock()
	defer d.statusMutex.Unlock()

	// truncate the message to avoid errors when reporting replica status
	if len(message) > 256 {
		message = message[:256]
	}

	currentTime := time.Now()
	d.status.LastSqlErrorTimestamp = &currentTime
	d.status.LastSqlErrNumber = errno
	d.status.LastSqlError = message
}

// AutoStart starts up replication if replication was running before the server was shutdown. If
// replication is not configured, hasn't been started, or has been stopped before the server was
// shutdown, then this method will not start replication. This method should only be called during
// the server startup process and should not be invoked after that.
func (d *doltBinlogReplicaController) AutoStart(_ context.Context) error {
	runningState, err := loadReplicationRunningState(d.ctx)
	if err != nil {
		logrus.Errorf("Unable to load replication running state: %s", err.Error())
		return err
	}

	if runningState == notRunning {
		logrus.Trace("no previous replication running state; not auto starting replication")
		return nil
	}

	logrus.Info("auto-starting binlog replication from source...")
	return d.StartReplica(d.ctx)
}

//
// Helper functions
//

func getOptionValueAsString(option binlogreplication.ReplicationOption) (string, error) {
	stringOptionValue, ok := option.Value.(binlogreplication.StringReplicationOptionValue)
	if ok {
		return stringOptionValue.GetValueAsString(), nil
	}

	return "", fmt.Errorf("unsupported value type for option %q; found %T, "+
		"but expected a string", option.Name, option.Value.GetValue())
}

func getOptionValueAsInt(option binlogreplication.ReplicationOption) (int, error) {
	integerOptionValue, ok := option.Value.(binlogreplication.IntegerReplicationOptionValue)
	if ok {
		return integerOptionValue.GetValueAsInt(), nil
	}

	return 0, fmt.Errorf("unsupported value type for option %q; found %T, "+
		"but expected an integer", option.Name, option.Value.GetValue())
}

func getOptionValueAsTableNames(option binlogreplication.ReplicationOption) ([]sql.UnresolvedTable, error) {
	tableNamesOptionValue, ok := option.Value.(binlogreplication.TableNamesReplicationOptionValue)
	if ok {
		return tableNamesOptionValue.GetValueAsTableList(), nil
	}

	return nil, fmt.Errorf("unsupported value type for option %q; found %T, "+
		"but expected a list of tables", option.Name, option.Value.GetValue())
}

func verifyAllTablesAreQualified(urts []sql.UnresolvedTable) error {
	for _, urt := range urts {
		if urt.Database().Name() == "" {
			return fmt.Errorf("no database specified for table '%s'; "+
				"all filter table names must be qualified with a database name", urt.Name())
		}
	}
	return nil
}
