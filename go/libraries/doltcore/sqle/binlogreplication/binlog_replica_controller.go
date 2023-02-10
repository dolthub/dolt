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
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
)

var DoltBinlogReplicaController = newDoltBinlogReplicaController()

// ErrServerNotConfiguredAsReplica is returned when replication is started without enough configuration provided.
var ErrServerNotConfiguredAsReplica = fmt.Errorf(
	"server is not configured as a replica; fix with CHANGE REPLICATION SOURCE TO")

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
	mu      *sync.Mutex
}

var _ binlogreplication.BinlogReplicaController = (*doltBinlogReplicaController)(nil)

// newDoltBinlogReplicaController creates a new doltBinlogReplicaController instance.
func newDoltBinlogReplicaController() *doltBinlogReplicaController {
	controller := doltBinlogReplicaController{
		mu:      &sync.Mutex{},
		filters: newFilterConfiguration(),
	}
	controller.status.AutoPosition = true
	controller.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	controller.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning
	controller.applier = newBinlogReplicaApplier(controller.filters)
	return &controller
}

// StartReplica implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) StartReplica(ctx *sql.Context) error {
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

	configuration, err := loadReplicationConfiguration(ctx)
	if err != nil {
		return err
	} else if configuration == nil {
		return ErrServerNotConfiguredAsReplica
	}

	if d.ctx == nil {
		return fmt.Errorf("no execution context set for the replica controller")
	}

	ctx.GetLogger().Info("starting binlog replication...")
	d.applier.Go(d.ctx)
	return nil
}

// SetExecutionContext sets the unique |ctx| for the replica's applier to use when applying changes from binlog events
// to a database. The applier cannot reuse any existing context, because it executes in a separate routine and would
// cause race conditions.
func (d *doltBinlogReplicaController) SetExecutionContext(ctx *sql.Context) {
	d.ctx = ctx
}

// StopReplica implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) StopReplica(_ *sql.Context) error {
	d.applier.stopReplicationChan <- struct{}{}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	d.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning

	return nil
}

// SetReplicationSourceOptions implements the BinlogReplicaController interface.
func (d *doltBinlogReplicaController) SetReplicationSourceOptions(ctx *sql.Context, options []binlogreplication.ReplicationOption) error {
	replicaSourceInfo, err := loadReplicationConfiguration(ctx)
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
		default:
			return fmt.Errorf("unknown replication source option: %s", option.Name)
		}
	}

	// Persist the updated replica source configuration to disk
	return persistReplicationConfiguration(ctx, replicaSourceInfo)
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
	replicaSourceInfo, err := loadReplicationConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	if replicaSourceInfo == nil {
		return nil, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	var copy = d.status

	copy.SourceUser = replicaSourceInfo.User
	copy.SourceHost = replicaSourceInfo.Host
	copy.SourcePort = uint(replicaSourceInfo.Port)
	copy.SourceServerUuid = replicaSourceInfo.Uuid
	copy.ConnectRetry = replicaSourceInfo.ConnectRetryInterval
	copy.SourceRetryCount = replicaSourceInfo.ConnectRetryCount
	copy.ReplicateDoTables = d.filters.getDoTables()
	copy.ReplicateIgnoreTables = d.filters.getIgnoreTables()

	return &copy, nil
}

// ResetReplica implements the BinlogReplicaController interface
func (d *doltBinlogReplicaController) ResetReplica(ctx *sql.Context, resetAll bool) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.status.ReplicaIoRunning != binlogreplication.ReplicaIoNotRunning ||
		d.status.ReplicaSqlRunning != binlogreplication.ReplicaSqlNotRunning {
		return fmt.Errorf("unable to reset replica while replication is running; stop replication and try again")
	}

	// Reset error status
	d.status.LastIoErrNumber = 0
	d.status.LastSqlErrNumber = 0
	d.status.LastIoErrorTimestamp = nil
	d.status.LastSqlErrorTimestamp = nil
	d.status.LastSqlError = ""
	d.status.LastIoError = ""

	if resetAll {
		err := deleteReplicationConfiguration(ctx)
		if err != nil {
			return err
		}

		d.filters = nil
	}

	return nil
}

// updateStatus allows the caller to safely update the replica controller's status. The controller locks it's mutex
// before the specified function |f| is called, and unlocks it after |f| is finished running. The current status is
// passed into the callback function |f| and the caller can safely update or copy any fields they need.
func (d *doltBinlogReplicaController) updateStatus(f func(status *binlogreplication.ReplicaStatus)) {
	d.mu.Lock()
	defer d.mu.Unlock()
	f(&d.status)
}

// setIoError updates the current replication status with the specific |errno| and |message| to describe an IO error.
func (d *doltBinlogReplicaController) setIoError(errno uint, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()

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
	d.mu.Lock()
	defer d.mu.Unlock()

	// truncate the message to avoid errors when reporting replica status
	if len(message) > 256 {
		message = message[:256]
	}

	currentTime := time.Now()
	d.status.LastSqlErrorTimestamp = &currentTime
	d.status.LastSqlErrNumber = errno
	d.status.LastSqlError = message
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
		if urt.Database() == "" {
			return fmt.Errorf("no database specified for table '%s'; "+
				"all filter table names must be qualified with a database name", urt.Name())
		}
	}
	return nil
}
