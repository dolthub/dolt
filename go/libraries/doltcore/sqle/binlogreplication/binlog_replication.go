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
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/store/datas"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

var DoltBinlogReplicaController = newDoltBinlogReplicaController()

var logger *logrus.Logger

func initializeLogger() {
	logger = logrus.StandardLogger()
	logger.SetLevel(logrus.TraceLevel)
}

// doltBinlogReplicaController implements the BinlogReplicaController interface for a Dolt database in order to
// provide support for a Dolt server to be a replica of a MySQL primary.
type doltBinlogReplicaController struct {
	status  binlogreplication.ReplicaStatus
	filters *filterConfiguration
	mu      *sync.Mutex
}

// filterConfiguration defines the binlog filtering rules applied on the replica.
type filterConfiguration struct {
	// doTables holds a map of database name to map of table names, indicating tables that SHOULD be replicated.
	doTables map[string]map[string]struct{}
	// ignoreTables holds a map of database name to map of table names, indicating tables that should NOT be replicated.
	ignoreTables map[string]map[string]struct{}
}

// newFilterConfiguration creates a new filterConfiguration instance and initializes members.
func newFilterConfiguration() *filterConfiguration {
	return &filterConfiguration{
		doTables:     make(map[string]map[string]struct{}),
		ignoreTables: make(map[string]map[string]struct{}),
	}
}

// TODO: Move these into the doltBinlogReplicaController struct
var format mysql.BinlogFormat
var tableMapsById = make(map[uint64]*mysql.TableMap)
var stopReplicationChan = make(chan struct{})

// modifiedDatabases holds the set of databases that have had data changes and are pending commit
var modifiedDatabases = make(map[string]struct{})

// currentGtid is the current GTID being processed, but not yet committed
var currentGtid mysql.GTID

var replicationSourceUuid string

// currentPosition records which GTIDs have been successfully executed
var currentPosition *mysql.Position

// positionStore is the singleton instance for loading/saving binlog position state to disk for durable storage
var positionStore = &binlogPositionStore{}

// ErrServerNotConfiguredAsReplica is returned when replication is started without enough configuration provided.
var ErrServerNotConfiguredAsReplica = fmt.Errorf(
	"server is not configured as a replica; fix with CHANGE REPLICATION SOURCE TO")

func newDoltBinlogReplicaController() *doltBinlogReplicaController {
	controller := doltBinlogReplicaController{
		mu: &sync.Mutex{},
	}
	controller.status.AutoPosition = true
	controller.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	controller.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning

	initializeLogger()

	return &controller
}

var _ binlogreplication.BinlogReplicaController = (*doltBinlogReplicaController)(nil)

func (d *doltBinlogReplicaController) StartReplica(ctx *sql.Context) error {
	if false {
		// TODO: If the database is already configured for Dolt replication/clustering, then error out
		//       Add a BATS test to cover this case
		return fmt.Errorf("dolt replication already enabled; unable to use binlog replication with other replication modes. " +
			"Disable Dolt replication first before starting binlog replication")
	}

	// TODO: If we aren't running in a sql-server context, return an error
	//       Add a BATS test for this; today, an error would come from the GMS layer, so we can't give
	//       a specific error message about needing to run Dolt as a sql-server yet.

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

	logger.Info("starting binlog replication...")

	// Create a new context to use, because otherwise the engine will cancel the original
	// context after the 'start replica' statement has finished executing.
	ctx = ctx.WithContext(context.Background())
	go func() {
		err := d.replicaBinlogEventHandler(ctx)
		if err != nil {
			logger.Errorf("unexpected error of type %T: '%v'", err, err.Error())
		}
	}()
	return nil
}

func (d *doltBinlogReplicaController) StopReplica(_ *sql.Context) error {
	stopReplicationChan <- struct{}{}

	d.mu.Lock()
	defer d.mu.Unlock()

	d.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	d.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning

	return nil
}

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

func getOptionValueAsInteger(option binlogreplication.ReplicationOption) (int, error) {
	integerOptionValue, ok := option.Value.(binlogreplication.IntegerReplicationOptionValue)
	if ok {
		return integerOptionValue.GetValueAsInt(), nil
	}

	return -1, fmt.Errorf("unsupported value type for option %q; found %T, "+
		"but expected an int", option.Name, option.Value.GetValue())
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

func (d *doltBinlogReplicaController) updateDoTablesFilter(urts []sql.UnresolvedTable) error {
	err := verifyAllTablesAreQualified(urts)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Setting new replication filters clears out any existing filters
	d.filters.doTables = make(map[string]map[string]struct{})

	for _, urt := range urts {
		if d.filters.doTables[urt.Database()] == nil {
			d.filters.doTables[urt.Database()] = make(map[string]struct{})
		}
		tableMap := d.filters.doTables[urt.Database()]
		tableMap[urt.Name()] = struct{}{}
	}
	return nil
}

func (d *doltBinlogReplicaController) updateIgnoreTablesFilter(urts []sql.UnresolvedTable) error {
	err := verifyAllTablesAreQualified(urts)
	if err != nil {
		return err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Setting new replication filters clears out any existing filters
	d.filters.ignoreTables = make(map[string]map[string]struct{})

	for _, urt := range urts {
		if d.filters.ignoreTables[urt.Database()] == nil {
			d.filters.ignoreTables[urt.Database()] = make(map[string]struct{})
		}
		tableMap := d.filters.ignoreTables[urt.Database()]
		tableMap[urt.Name()] = struct{}{}
	}
	return nil
}

// initializeFilterConfiguration instantiates this binlog controller's filter configuration.
func (d *doltBinlogReplicaController) initializeFilterConfiguration() {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.filters == nil {
		d.filters = newFilterConfiguration()
	}
}

// SetReplicationFilterOptions implements the BinlogReplicaController interface
func (d *doltBinlogReplicaController) SetReplicationFilterOptions(_ *sql.Context, options []binlogreplication.ReplicationOption) error {
	d.initializeFilterConfiguration()

	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "REPLICATE_DO_TABLE":
			value, err := getOptionValueAsTableNames(option)
			if err != nil {
				return err
			}
			err = d.updateDoTablesFilter(value)
			if err != nil {
				return err
			}
		case "REPLICATE_IGNORE_TABLE":
			value, err := getOptionValueAsTableNames(option)
			if err != nil {
				return err
			}
			err = d.updateIgnoreTablesFilter(value)
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

	if d.filters != nil {
		copy.ReplicateDoTables = convertFilterMapToStringSlice(d.filters.doTables)
		copy.ReplicateIgnoreTables = convertFilterMapToStringSlice(d.filters.ignoreTables)
	}

	return &copy, nil
}

// convertFilterMapToStringSlice converts the specified |filterMap| into a string slice, by iterating over every
// key in the top level map, which stores a database name, and for each of those keys, iterating over every key
// in the inner map, which stores a table name. Each table name is qualified with the matching database name and the
// results are returned as a slice of qualified table names.
func convertFilterMapToStringSlice(filterMap map[string]map[string]struct{}) []string {
	if filterMap == nil {
		return nil
	}

	tableNames := make([]string, 0, len(filterMap))
	for dbName, tableMap := range filterMap {
		for tableName, _ := range tableMap {
			tableNames = append(tableNames, fmt.Sprintf("%s.%s", dbName, tableName))
		}
	}
	return tableNames
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

// setIoError updates the current replication status with the specific |errno| and |message| to describe an IO error.
func (d *doltBinlogReplicaController) setIoError(errno uint, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	currentTime := time.Now()
	d.status.LastIoErrorTimestamp = &currentTime
	d.status.LastIoErrNumber = errno
	d.status.LastIoError = message
}

// setSqlError updates the current replication status with the specific |errno| and |message| to describe an SQL error.
func (d *doltBinlogReplicaController) setSqlError(errno uint, message string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	currentTime := time.Now()
	d.status.LastSqlErrorTimestamp = &currentTime
	d.status.LastSqlErrNumber = errno
	d.status.LastSqlError = message
}

// Row Flags – https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/

// rowFlag_endOfStatement indicates that a row event with this flag set is the last event in a statement.
const rowFlag_endOfStatement = 0x0001
const rowFlag_noForeignKeyChecks = 0x0002
const rowFlag_noUniqueKeyChecks = 0x0004
const rowFlag_noCheckConstraints = 0x0010

// rowFlag_rowsAreComplete indicates that rows in this event are complete, and contain values for all columns of the table.
const rowFlag_rowsAreComplete = 0x0008

// connectAndStartReplicationEventStream connects to the configured MySQL replication source, including pausing
// and retrying if errors are encountered.
//
// NOTE: Our fork of Vitess currently only supports mysql_native_password auth. The latest code in the main
//
//	Vitess repo supports the current MySQL default auth plugin, caching_sha2_password.
//	https://dev.mysql.com/blog-archive/upgrading-to-mysql-8-0-default-authentication-plugin-considerations/
//	To work around this limitation, add the following to your /etc/my.cnf:
//	    [mysqld]
//	    default-authentication-plugin=mysql_native_password
//	or start mysqld with:
//	    --default-authentication-plugin=mysql_native_password
func (d *doltBinlogReplicaController) connectAndStartReplicationEventStream(ctx *sql.Context) (*mysql.Conn, error) {
	d.mu.Lock()
	d.status.ReplicaIoRunning = binlogreplication.ReplicaIoConnecting
	d.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlRunning
	maxConnectionAttempts := d.status.SourceRetryCount
	connectRetryDelay := d.status.ConnectRetry
	d.mu.Unlock()

	var conn *mysql.Conn
	var err error
	for connectionAttempts := uint64(0); ; connectionAttempts++ {
		replicaSourceInfo, err := loadReplicationConfiguration(ctx)

		if replicaSourceInfo == nil {
			err = ErrServerNotConfiguredAsReplica
			d.setIoError(13117, err.Error())
			return nil, err
		} else if replicaSourceInfo.Uuid != "" {
			replicationSourceUuid = replicaSourceInfo.Uuid
		}

		if replicaSourceInfo.Host == "" {
			err = fmt.Errorf("fatal error: Invalid (empty) hostname when attempting to connect " +
				"to the source server. Connection attempt terminated")
			d.setIoError(13117, err.Error())
			return nil, err
		} else if replicaSourceInfo.User == "" {
			err = fmt.Errorf("fatal error: Invalid (empty) username when attempting to connect " +
				"to the source server. Connection attempt terminated")
			d.setIoError(13117, err.Error())
			return nil, err
		}

		connParams := mysql.ConnParams{
			Host:  replicaSourceInfo.Host,
			Port:  int(replicaSourceInfo.Port),
			Uname: replicaSourceInfo.User,
			Pass:  replicaSourceInfo.Password,
			// ConnectTimeoutMs: 0, // TODO: Set a non-zero timeout
		}

		conn, err = mysql.Connect(ctx, &connParams)
		if err != nil && connectionAttempts >= maxConnectionAttempts {
			return nil, err
		} else if err != nil {
			time.Sleep(time.Duration(connectRetryDelay) * time.Second)
		} else {
			break
		}
	}

	// Request binlog events to start
	// TODO: This also needs retry logic
	err = d.startReplicationEventStream(ctx, conn)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	d.status.ReplicaIoRunning = binlogreplication.ReplicaIoRunning
	d.mu.Unlock()

	return conn, nil
}

func (d *doltBinlogReplicaController) replicaBinlogEventHandler(ctx *sql.Context) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("unable to access a running SQL server")
	}
	engine := server.Engine

	conn, err := d.connectAndStartReplicationEventStream(ctx)
	if err != nil {
		return err
	}

	// Process binlog events
	for {
		select {
		case <-stopReplicationChan:
			return nil
		default:
			// TODO: What are the default network timeouts in vitess?
			event, err := conn.ReadBinlogEvent()
			if err != nil {
				if sqlError, isSqlError := err.(*mysql.SQLError); isSqlError {
					if sqlError.Message == io.EOF.Error() {
						logger.Trace("No more binlog messages; retrying in 1s...")
						time.Sleep(1 * time.Second)
						continue
					} else if strings.HasPrefix(sqlError.Message, io.ErrUnexpectedEOF.Error()) {
						// TODO: Do we have these errors defined in GMS anywhere yet?
						//       https://dev.mysql.com/doc/mysql-errors/8.0/en/server-error-reference.html
						const ER_NET_READ_ERROR = 1158
						d.mu.Lock()
						d.status.LastIoError = io.ErrUnexpectedEOF.Error()
						d.status.LastIoErrNumber = ER_NET_READ_ERROR
						currentTime := time.Now()
						d.status.LastIoErrorTimestamp = &currentTime
						d.mu.Unlock()
						conn, err = d.connectAndStartReplicationEventStream(ctx)
						if err != nil {
							return err
						}
						continue
					} else if strings.Contains(sqlError.Message, "can not handle replication events with the checksum") {
						// For now, just ignore any errors about checksums
						logger.Debug("received binlog checksum error message")
						continue
					}
				}

				// otherwise, log the error if it's something we don't expect and continue
				logger.Errorf("unexpected error of type %T: '%v'", err, err.Error())
				continue
			}

			err = d.processBinlogEvent(ctx, engine, event)
			if err != nil {
				logger.Errorf("unexpected error of type %T: '%v'", err, err.Error())
				err = nil
				// TODO: We don't currently return an error from this function yet.
				//       Need to refine error handling so that all errors are always logged, but not all errors
				//       stop the replication processor and ensure proper retry is in place for various operations.
			}
		}
	}
}

func (d *doltBinlogReplicaController) processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	var err error
	createCommit := false
	commitToAllDatabases := false

	switch {
	case event.IsRand():
		// A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are
		// used to compute the random number. For more details, see: https://mariadb.com/kb/en/rand_event/
		// Note: it is written only before a QUERY_EVENT and is NOT used with row-based logging.
		logger.Debug("Received binlog event: Rand")

	case event.IsXID():
		// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an
		// XA-capable storage engine. For more details, see: https://mariadb.com/kb/en/xid_event/
		logger.Debug("Received binlog event: XID")
		createCommit = true

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		query, err := event.Query(format)
		if err != nil {
			return err
		}
		logger.WithFields(logrus.Fields{
			"database": query.Database,
			"charset":  query.Charset,
			"query":    query.SQL,
		}).Debug("Received binlog event: Query")

		// When executing SQL statements sent from the primary, we can't be sure what database was modified unless we
		// look closely at the statement. For example, we could be connected to db01, but executed
		// "create table db02.t (...);" – i.e., looking at query.Database is NOT enough to always determine the correct
		// database that was modified, so instead, we commit to all databases when we see a Query binlog event to
		// avoid issues with correctness, at the cost of being slightly less efficient
		commitToAllDatabases = true

		ctx.SetCurrentDatabase(query.Database)
		executeQueryWithEngine(ctx, engine, query.SQL)
		createCommit = strings.ToLower(query.SQL) != "begin"

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		logger.Debug("Received binlog event: Rotate")

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		format, err = event.Format()
		if err != nil {
			return err
		}
		logger.WithFields(logrus.Fields{
			"format": format,
		}).Debug("Received binlog event: FormatDescription")

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		position, err := event.PreviousGTIDs(format)
		if err != nil {
			return err
		}
		logger.WithFields(logrus.Fields{
			"previousGtids": position.GTIDSet.String(),
		}).Debug("Received binlog event: PreviousGTIDs")

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		gtid, isBegin, err := event.GTID(format)
		if err != nil {
			return err
		}
		if isBegin {
			logger.Errorf("unsupported binlog protocol message: GTID event with 'isBegin' set to true")
		}
		logger.WithFields(logrus.Fields{
			"gtid":    gtid,
			"isBegin": isBegin,
		}).Debug("Received binlog event: GTID")
		currentGtid = gtid
		err = persistSourceUuid(ctx, gtid.SourceServer())
		if err != nil {
			return err
		}

	case event.IsTableMap():
		// Used for row-based binary logging beginning (binlog_format=ROW or MIXED). This event precedes each row
		// operation event and maps a table definition to a number, where the table definition consists of database
		// and table names. For more details, see: https://mariadb.com/kb/en/table_map_event/
		// Note: TableMap events are sent before each row event, so there is no need to persist them between restarts.
		tableId := event.TableID(format)
		tableMap, err := event.TableMap(format)
		if err != nil {
			return err
		}
		logger.WithFields(logrus.Fields{
			"id":        tableId,
			"tableName": tableMap.Name,
			"database":  tableMap.Database,
			"flags":     convertToHexString(tableMap.Flags),
			"metadata":  tableMap.Metadata,
			"types":     tableMap.Types,
		}).Debug("Received binlog event: TableMap")

		if tableId == 0xFFFFFF {
			// TODO: Handle special Table ID value 0xFFFFFF. We have yet to see this specific message.
			//		Table id refers to a table defined by TABLE_MAP_EVENT. The special value 0xFFFFFF should have
			//	 	"end of statement flag" (0x0001) set and indicates that table maps can be freed.
			logger.Errorf("unsupported binlog protocol message: TableMap event with table ID '0xFFFFFF'")
		}
		flags := tableMap.Flags
		if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
			// nothing to be done for end of statement; just clear the flag
			flags = flags ^ rowFlag_endOfStatement
		}
		if flags != 0 {
			logger.Errorf("unsupported binlog protocol message: TableMap event with flags '%x'", tableMap.Flags)
		}
		tableMapsById[tableId] = tableMap

	case event.IsDeleteRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		logger.Debug("Received binlog event: DeleteRows")
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}

		if d.isTableFilteredOut(tableMap) {
			return nil
		}
		modifiedDatabases[tableMap.Database] = struct{}{}

		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}

		flags := rows.Flags
		if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
			// nothing to be done for end of statement; just clear the flag and move on
			flags = flags ^ rowFlag_endOfStatement
		}
		if flags != 0 {
			logger.Errorf("unsupported binlog protocol message: DeleteRows event with flags '%x'", tableMap.Flags)
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		logger.Debugf(" - Deleted Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			deletedRow, err := parseRow(ctx, tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			logger.Debugf("     - Identify: %v ", sql.FormatRow(deletedRow))

			writeSession, tableWriter, err := getTableWriter(ctx, engine, tableMap.Name, tableMap.Database)
			if err != nil {
				return err
			}

			err = tableWriter.Delete(ctx, deletedRow)
			if err != nil {
				return err
			}

			err = closeWriteSession(ctx, engine, tableMap.Database, writeSession)
			if err != nil {
				return err
			}
		}

	case event.IsWriteRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		logger.Debug("Received binlog event: WriteRows")
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}

		if d.isTableFilteredOut(tableMap) {
			return nil
		}
		modifiedDatabases[tableMap.Database] = struct{}{}

		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}

		flags := rows.Flags
		if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
			// nothing to be done for end of statement; just clear the flag and move on
			flags = flags ^ rowFlag_endOfStatement
		}
		if flags != 0 {
			logger.Errorf("unsupported binlog protocol message: WriteRows event with flags '%x'", tableMap.Flags)
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		logger.Debugf(" - New Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			newRow, err := parseRow(ctx, tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			logger.Debugf("     - Data: %v ", sql.FormatRow(newRow))

			retryCount := 0
			for {
				writeSession, tableWriter, err := getTableWriter(ctx, engine, tableMap.Name, tableMap.Database)
				if err != nil {
					return err
				}

				err = tableWriter.Insert(ctx, newRow)
				if err != nil {
					return err
				}

				err = closeWriteSession(ctx, engine, tableMap.Database, writeSession)
				if err != nil {
					if errors.Is(datas.ErrOptimisticLockFailed, err) && retryCount < 3 {
						logger.Errorf("Retrying after error writing table updates: %s", err)
						retryCount++
						continue
					} else {
						return err
					}
				} else {
					break
				}
			}
		}

	case event.IsUpdateRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		logger.Debug("Received binlog event: UpdateRows")
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}

		if d.isTableFilteredOut(tableMap) {
			return nil
		}
		modifiedDatabases[tableMap.Database] = struct{}{}

		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}

		flags := rows.Flags
		if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
			// nothing to be done for end of statement; just clear the flag and move on
			flags = flags ^ rowFlag_endOfStatement
		}
		if flags != 0 {
			logger.Errorf("unsupported binlog protocol message: UpdateRows event with flags '%x'", tableMap.Flags)
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		logger.Debugf(" - Updated Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			identifyRow, err := parseRow(ctx, tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			updatedRow, err := parseRow(ctx, tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			logger.Debugf("     - Identify: %v Data: %v ", sql.FormatRow(identifyRow), sql.FormatRow(updatedRow))

			writeSession, tableWriter, err := getTableWriter(ctx, engine, tableMap.Name, tableMap.Database)
			if err != nil {
				return err
			}

			err = tableWriter.Update(ctx, identifyRow, updatedRow)
			if err != nil {
				return err
			}

			err = closeWriteSession(ctx, engine, tableMap.Database, writeSession)
			if err != nil {
				return err
			}
		}

	default:
		// TODO: we can't access the bytes directly because the non-interface types are not exposed
		//       having a Bytes() or Type() method on the interface would let us clean this up.
		byteString := fmt.Sprintf("%v", event)
		if strings.HasPrefix(byteString, "{[0 0 0 0 27 ") {
			// Type 27 is a Heartbeat event. This event does not appear in the binary log. It's only sent over the
			// network by a primary to a replica to let it know that the primary is still alive, and is only sent
			// when the primary has no binlog events to send to replica servers.
			// For more details, see: https://mariadb.com/kb/en/heartbeat_log_event/
			logger.Debug("Received binlog event: Heartbeat")
		} else {
			return fmt.Errorf("received unknown event: %v", event)
		}
	}

	if createCommit {
		databasesToCommit := keys(modifiedDatabases)
		if commitToAllDatabases {
			databasesToCommit = getAllUserDatabaseNames(ctx, engine)
		}
		for _, database := range databasesToCommit {
			executeQueryWithEngine(ctx, engine, "use `"+database+"`;")
			executeQueryWithEngine(ctx, engine, "commit;")
		}

		// Record the last GTID processed after the commit
		currentPosition.GTIDSet = currentPosition.GTIDSet.AddGTID(currentGtid)
		err := sql.SystemVariables.SetGlobal("gtid_executed", currentPosition.GTIDSet.String())
		if err != nil {
			logger.Errorf("unable to set @@GLOBAL.gtid_executed: %s", err.Error())
		}
		err = positionStore.Save(ctx, currentPosition)
		if err != nil {
			return fmt.Errorf("unable to store GTID executed metadata to disk: %s", err.Error())
		}

		// For now, create a Dolt commit from every data update. Eventually, we'll want to make this configurable.
		logger.Trace("Creating Dolt commit(s)")
		for _, database := range databasesToCommit {
			executeQueryWithEngine(ctx, engine, "use `"+database+"`;")
			executeQueryWithEngine(ctx, engine,
				fmt.Sprintf("call dolt_commit('-Am', 'Dolt binlog replica commit: GTID %s');", currentGtid))
		}

		// Clear the modified database metadata for the next commit
		modifiedDatabases = make(map[string]struct{})
		commitToAllDatabases = false
	}

	return nil
}

// closeWriteSession flushes and closes the specified |writeSession| and returns an error if anything failed.
func closeWriteSession(ctx *sql.Context, engine *gms.Engine, databaseName string, writeSession writer.WriteSession) error {
	newWorkingSet, err := writeSession.Flush(ctx)
	if err != nil {
		return err
	}

	database, err := engine.Analyzer.Catalog.Database(ctx, databaseName)
	if err != nil {
		return err
	}
	if privDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privDatabase.Unwrap()
	}
	sqlDatabase, ok := database.(sqle.Database)
	if !ok {
		return fmt.Errorf("unexpected database type: %T", database)
	}

	hash, err := newWorkingSet.HashOf()
	if err != nil {
		return err
	}

	return sqlDatabase.DbData().Ddb.UpdateWorkingSet(ctx, newWorkingSet.Ref(), newWorkingSet, hash, newWorkingSet.Meta())
}

// isTableFilteredOut returns true if the table identified by |tableMap| has been filtered out on this replica and
// should not have any updates applied from binlog messages.
func (d *doltBinlogReplicaController) isTableFilteredOut(tableMap *mysql.TableMap) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	if d.filters == nil {
		return false
	}

	// If any filter doTable options are specified, then a table MUST be listed in the set
	// for it to be replicated. doTables options are processed BEFORE ignoreTables options.
	// If a table appears in both doTable and ignoreTables, it is ignored.
	// https://dev.mysql.com/doc/refman/8.0/en/replication-rules-table-options.html
	if len(d.filters.doTables) > 0 {
		if doTables, ok := d.filters.doTables[tableMap.Database]; ok {
			if _, ok := doTables[tableMap.Name]; !ok {
				logger.Tracef("skipping table %s.%s (not in doTables) ", tableMap.Database, tableMap.Name)
				return true
			}
		}
	}

	if len(d.filters.ignoreTables) > 0 {
		if ignoredTables, ok := d.filters.ignoreTables[tableMap.Database]; ok {
			if _, ok := ignoredTables[tableMap.Name]; ok {
				// If this table is being ignored, don't process any further
				logger.Tracef("skipping table %s.%s (in ignoreTables)", tableMap.Database, tableMap.Name)
				return true
			}
		}
	}

	return false
}

// startReplicationEventStream sends a request over |conn|, the connection to the MySQL source server, to begin
// sending binlog events.
func (d *doltBinlogReplicaController) startReplicationEventStream(ctx *sql.Context, conn *mysql.Conn) error {
	serverId, err := loadReplicaServerId()
	if err != nil {
		return err
	}

	position, err := positionStore.Load(ctx)
	if err != nil {
		return err
	}

	if position == nil {
		// When there is no existing record of executed GTIDs, we create a GTIDSet with just one transaction ID
		// for the 0000 server ID. There doesn't seem to be a cleaner way of saying "start at the very beginning".
		//
		// Also... "starting position" is a bit of a misnomer – it's actually the processed GTIDs, which
		// indicate the NEXT GTID where replication should start, but it's not as direct as specifying
		// a starting position, like the function signature seems to suggest.
		gtid := mysql.Mysql56GTID{
			Sequence: 1,
		}
		position = &mysql.Position{GTIDSet: gtid.GTIDSet()}
	}

	currentPosition = position

	return conn.SendBinlogDumpCommand(serverId, *position)
}

// getTableSchema returns a sql.Schema for the specified table in the specified database.
func getTableSchema(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string) (sql.Schema, error) {
	database, err := engine.Analyzer.Catalog.Database(ctx, databaseName)
	if err != nil {
		return nil, err
	}
	table, ok, err := database.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("unable to find table %q", tableName)
	}

	return table.Schema(), nil
}

// getTableWriter returns a WriteSession and a TableWriter for writing to the specified |table| in the specified |database|.
func getTableWriter(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string) (writer.WriteSession, writer.TableWriter, error) {
	database, err := engine.Analyzer.Catalog.Database(ctx, databaseName)
	if err != nil {
		return nil, nil, err
	}
	if privDatabase, ok := database.(mysql_db.PrivilegedDatabase); ok {
		database = privDatabase.Unwrap()
	}
	sqlDatabase, ok := database.(sqle.Database)
	if !ok {
		return nil, nil, fmt.Errorf("unexpected database type: %T", database)
	}

	binFormat := sqlDatabase.DbData().Ddb.Format()

	ws, err := env.WorkingSet(ctx, sqlDatabase.GetDoltDB(), sqlDatabase.DbData().Rsr)
	if err != nil {
		return nil, nil, err
	}

	tracker, err := globalstate.NewAutoIncrementTracker(ctx, ws)
	if err != nil {
		return nil, nil, err
	}

	writeSession := writer.NewWriteSession(binFormat, ws, tracker, editor.Options{})

	ds := dsess.DSessFromSess(ctx.Session)
	setter := ds.SetRoot
	tableWriter, err := writeSession.GetTableWriter(ctx, tableName, databaseName, setter, false)
	if err != nil {
		return nil, nil, err
	}

	return writeSession, tableWriter, nil
}

// parseRow parses the binary row data from a MySQL binlog event and converts it into a go-mysql-server Row using the
// |schema| information provided. |columnsPresentBitmap| indicates which column values are present in |data| and
// |nullValuesBitmap| indicates which columns have null values and are NOT present in |data|.
func parseRow(ctx *sql.Context, tableMap *mysql.TableMap, schema sql.Schema, columnsPresentBitmap, nullValuesBitmap mysql.Bitmap, data []byte) (sql.Row, error) {
	var parsedRow sql.Row
	pos := 0

	for i, typ := range tableMap.Types {
		column := schema[i]

		if columnsPresentBitmap.Bit(i) == false {
			parsedRow = append(parsedRow, nil)
			continue
		}

		var value sqltypes.Value
		var err error
		if nullValuesBitmap.Bit(i) {
			value, err = sqltypes.NewValue(query.Type_NULL_TYPE, nil)
			if err != nil {
				return nil, err
			}
		} else {
			var length int
			value, length, err = mysql.CellValue(data, pos, typ, tableMap.Metadata[i], getSignedType(column))
			if err != nil {
				return nil, err
			}
			pos += length
		}

		convertedValue, err := convertSqlTypesValue(ctx, value, column)
		if err != nil {
			return nil, err
		}
		parsedRow = append(parsedRow, convertedValue)
	}

	return parsedRow, nil
}

// getSignedType returns a Vitess query.Type that can be used with the Vitess mysql.CellValue function to correctly
// parse the value of a signed or unsigned integer value. The mysql.TableMap structure provides information about the
// type, but it doesn't indicate if an integer type is signed or unsigned, so we have to look at the column type in the
// replica's schema and then choose any signed/unsigned query.Type to pass into mysql.CellValue to instruct it whether
// to treat a value as signed or unsigned – the actual type does not matter, only the signed/unsigned property.
func getSignedType(column *sql.Column) query.Type {
	switch column.Type.Type() {
	case query.Type_UINT8, query.Type_UINT16, query.Type_UINT24, query.Type_UINT32, query.Type_UINT64:
		// For any unsigned integer value, we just need to return any unsigned numeric type to signal to Vitess to treat
		// the value as unsigned. The actual type returned doesn't matter – only the signed/unsigned property is used.
		return query.Type_UINT64
	default:
		return query.Type_INT64
	}
}

// convertSqlTypesValues converts a sqltypes.Value instance (from vitess) into a sql.Type value (for go-mysql-server).
func convertSqlTypesValue(ctx *sql.Context, value sqltypes.Value, column *sql.Column) (interface{}, error) {
	if value.IsNull() {
		return nil, nil
	}

	var convertedValue interface{}
	var err error
	switch {
	case types.IsEnum(column.Type), types.IsSet(column.Type):
		atoi, err := strconv.Atoi(value.ToString())
		if err != nil {
			return nil, err
		}
		convertedValue, err = column.Type.Convert(atoi)
	case types.IsDecimal(column.Type):
		// Decimal values need to have any leading/trailing whitespace trimmed off
		// TODO: Consider moving this into DecimalType_.Convert; if DecimalType_.Convert handled trimming
		//       leading/trailing whitespace, this special case for Decimal types wouldn't be needed.
		convertedValue, err = column.Type.Convert(strings.TrimSpace(value.ToString()))
	case types.IsJSON(column.Type):
		convertedValue, err = convertVitessJsonExpressionString(ctx, value)
	default:
		convertedValue, err = column.Type.Convert(value.ToString())
	}
	if err != nil {
		return nil, fmt.Errorf("unable to convert value %q, for column of type %T: %v", value.ToString(), column.Type, err.Error())
	}

	return convertedValue, nil
}

// convertVitessJsonExpressionString extracts a JSON value from the specified |value| instance, which Vitess has
// encoded as a SQL expression string. Vitess parses the binary JSON representation from an incoming binlog event,
// and converts it into an expression string containing JSON_OBJECT and JSON_ARRAY function calls. Because we don't
// have access to the raw JSON string or JSON bytes, we have to do extra work to translate from Vitess' SQL
// expression syntax into a raw JSON string value that we can pass to the storage layer. If Vitess kept around the
// raw string representation and returned it from value.ToString, this logic would not be necessary.
func convertVitessJsonExpressionString(ctx *sql.Context, value sqltypes.Value) (interface{}, error) {
	if value.Type() != query.Type_EXPRESSION {
		return nil, fmt.Errorf("invalid sqltypes.Value specified; expected a Value instance with an Expression type")
	}

	strValue := value.String()
	if strings.HasPrefix(strValue, "EXPRESSION(") {
		strValue = strValue[len("EXPRESSION(") : len(strValue)-1]
	}

	node, err := parse.Parse(ctx, "SELECT "+strValue)
	if err != nil {
		return nil, err
	}

	server := sqlserver.GetRunningServer()
	if server == nil {
		return nil, fmt.Errorf("unable to access running SQL server")
	}

	analyze, err := server.Engine.Analyzer.Analyze(ctx, node, nil)
	if err != nil {
		return nil, err
	}
	rowIter, err := analyze.RowIter(ctx, nil)
	if err != nil {
		return nil, err
	}
	row, err := rowIter.Next(ctx)
	if err != nil {
		return nil, err
	}

	return row[0], nil
}

func getAllUserDatabaseNames(ctx *sql.Context, engine *gms.Engine) []string {
	allDatabases := engine.Analyzer.Catalog.AllDatabases(ctx)
	userDatabaseNames := make([]string, 0, len(allDatabases))
	for _, database := range allDatabases {
		switch database.Name() {
		case "information_schema", "mysql":
		default:
			userDatabaseNames = append(userDatabaseNames, database.Name())
		}
	}
	return userDatabaseNames
}

// loadReplicaServerId loads the @@GLOBAL.server_id system variable needed to register the replica with the source,
// and returns an error specific to replication configuration if the variable is not set to a valid value.
func loadReplicaServerId() (uint32, error) {
	_, value, ok := sql.SystemVariables.GetGlobal("server_id")
	if !ok {
		return 0, fmt.Errorf("no server_id global system variable set")
	}

	serverId, ok := value.(uint32)
	if !ok {
		return 0, fmt.Errorf("unexpected type for @@GLOBAL.server_id value: %T", value)
	}

	if serverId == 0 {
		return 0, fmt.Errorf("invalid server ID configured for @@GLOBAL.server_id (%d); "+
			"must be an integer greater than zero and less than 4,294,967,296", serverId)
	}

	return serverId, nil
}

func executeQueryWithEngine(ctx *sql.Context, engine *gms.Engine, query string) {
	if ctx.GetCurrentDatabase() == "" {
		logger.Error("No current database selected, aborting query")
		return
	}

	_, iter, err := engine.Query(ctx, query)
	if err != nil {
		// Log any errors, except for commits with "nothing to commit"
		if err.Error() != "nothing to commit" {
			logger.Errorf("ERROR executing query: %v ", err.Error())
		}
		return
	}
	for {
		row, err := iter.Next(ctx)
		if err != nil {
			if err != io.EOF {
				logger.Errorf("ERROR reading query results: %v ", err.Error())
			}
			return
		}
		logger.Debugf("   row: %s ", sql.FormatRow(row))
	}
}

//
// Generic util functions...
//

// convertToHexString returns a lower-case hex string representation of the specified uint16 value |v|.
func convertToHexString(v uint16) string {
	return fmt.Sprintf("%x", v)
}

// keys returns a slice containing the keys in the specified map |m|.
func keys[K comparable, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
