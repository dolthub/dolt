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

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
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
	status binlogreplication.ReplicaStatus
	mu     *sync.Mutex
}

// TODO: Move these into the doltBinlogReplicaController struct
var format mysql.BinlogFormat
var tableMapsById = make(map[uint64]*mysql.TableMap)
var stopReplicationChan = make(chan struct{})

func newDoltBinlogReplicaController() *doltBinlogReplicaController {
	controller := doltBinlogReplicaController{
		mu: &sync.Mutex{},
	}
	controller.status.AutoPosition = true
	controller.status.ReplicaIoRunning = binlogreplication.ReplicaIoNotRunning
	controller.status.ReplicaSqlRunning = binlogreplication.ReplicaSqlNotRunning

	controller.status.SourceRetryCount = 86400
	// TODO: The ConnectRetry default *should* be 60s to match MySQL's behavior, but for easier testing,
	//        we're starting with a very quick retry
	controller.status.ConnectRetry = 5

	initializeLogger()

	return &controller
}

var _ binlogreplication.BinlogReplicaController = (*doltBinlogReplicaController)(nil)

func (d *doltBinlogReplicaController) StartReplica(ctx *sql.Context) error {
	// TODO: If the database is already configured for Dolt replication/clustering, then error out
	if false {
		return fmt.Errorf("dolt replication already enabled; unable to use binlog replication with other replication modes. Disable Dolt replication first before starting binlog replication")
	}

	logger.Info("starting binlog replication...")

	// TODO: If we aren't running in a sql-server context, return an error

	// Create a new context to use, because otherwise the engine will cancel the original
	// context after the 'start replica' statement has finished executing.
	ctx = ctx.WithContext(context.Background())
	go func() {
		err := d.replicaBinlogEventHandler(ctx)
		if err != nil {
			// TODO: Need better error handling here... we shouldn't be crashing the server obviously
			//       Instead, we should log the errors and expose them through the replica status API
			//       Do we need another log just for replication? (not the binlog relay log... just a debugging log)
			//       For now though... this panic is convenient since it fails the tests hard for any unexpected error.
			panic(fmt.Sprintf("unexpected error of type %T: '%v'", err, err.Error()))
		}
	}()
	return nil
}

func (d *doltBinlogReplicaController) StopReplica(_ *sql.Context) error {
	stopReplicationChan <- struct{}{}
	return nil
}

func (d *doltBinlogReplicaController) loadReplicationConfiguration(ctx *sql.Context) (*mysql_db.ReplicaSourceInfo, error) {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return nil, fmt.Errorf("no SQL server running; " +
			"replication commands may only be used when running from dolt sql-server, and not from dolt sql")
	}
	engine := server.Engine
	return engine.Analyzer.Catalog.MySQLDb.GetReplicaSourceInfo(), nil
}

func (d *doltBinlogReplicaController) persistReplicationConfiguration(ctx *sql.Context, replicaSourceInfo *mysql_db.ReplicaSourceInfo) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("no SQL server running; " +
			"replication commands may only be used when running from dolt sql-server, and not from dolt sql")
	}
	engine := server.Engine

	replicaSourceInfoTableData := engine.Analyzer.Catalog.MySQLDb.ReplicaSourceInfoTable().Data()
	err := replicaSourceInfoTableData.Put(ctx, replicaSourceInfo)
	if err != nil {
		return err
	}
	return engine.Analyzer.Catalog.MySQLDb.Persist(ctx)
}

func (d *doltBinlogReplicaController) SetReplicationOptions(ctx *sql.Context, options []binlogreplication.ReplicationOption) error {
	replicaSourceInfo, err := d.loadReplicationConfiguration(ctx)
	if err != nil {
		return err
	}

	for _, option := range options {
		switch strings.ToUpper(option.Name) {
		case "SOURCE_HOST":
			replicaSourceInfo.Host = option.Value
		case "SOURCE_USER":
			replicaSourceInfo.User = option.Value
		case "SOURCE_PASSWORD":
			replicaSourceInfo.Password = option.Value
		case "SOURCE_PORT":
			intValue, err := strconv.Atoi(option.Value)
			if err != nil {
				return fmt.Errorf("Unable to parse SOURCE_PORT value (%q) as an integer: %s", option.Value, err.Error())
			}
			replicaSourceInfo.Port = uint16(intValue)

		default:
			return fmt.Errorf("Unknown replication option: %s", option.Name)
		}
	}

	// Persist the updated replica source configuration to disk
	err = d.persistReplicationConfiguration(ctx, replicaSourceInfo)
	if err != nil {
		return err
	}

	return nil
}

func (d *doltBinlogReplicaController) GetReplicaStatus(ctx *sql.Context) (*binlogreplication.ReplicaStatus, error) {
	// TODO: If there is NO ReplicaSourceInfo, then we should return a nil reference, but we can't distinguish that yet
	replicaSourceInfo, err := d.loadReplicationConfiguration(ctx)
	if err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	var copy = d.status

	copy.SourceUser = replicaSourceInfo.User
	copy.SourceHost = replicaSourceInfo.Host
	copy.SourcePort = uint(replicaSourceInfo.Port)
	copy.SourceServerUuid = replicaSourceInfo.Uuid

	return &copy, nil
}

// Row Flags – https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
const endOfStatementRowFlag = 0x0001
const noForeignKeyChecksRowFlag = 0x0002
const noUniqueKeyChecksRowFlag = 0x0004
const rowsAreCompleteRowFlag = 0x0008
const noCheckConstraintsRowFlag = 0x0010

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
	for connectionAttempts := uint(0); ; connectionAttempts++ {
		replicaSourceInfo, err := d.loadReplicationConfiguration(ctx)

		// TODO: Validate that all required settings are present
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
	err = startReplicationEventStream(conn)
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
				// TODO: We don't currently return an error from this function yet, because the
				//       calling function currently panics and crashes the process.
			}
		}
	}
}

func (d *doltBinlogReplicaController) processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	var err error
	createDoltCommit := false

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
		// TODO: parse XID transaction number and record it durably
		//       gtid, b, err := event.GTID(format)
		executeQueryWithEngine(ctx, engine, "commit;")
		createDoltCommit = true

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		logger.Debug("Received binlog event: Query")
		query, err := event.Query(format)
		if err != nil {
			return err
		}
		logger.Debugf(" - Query: %s", query)
		ctx.SetCurrentDatabase(query.Database)
		executeQueryWithEngine(ctx, engine, query.SQL)
		createDoltCommit = true

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		logger.Debug("Received binlog event: Rotate")

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		logger.Debug("Received binlog event: FormatDescription")
		format, err = event.Format()
		if err != nil {
			return err
		}

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		logger.Debug("Received binlog event: PreviousGTIDs")
		position, err := event.PreviousGTIDs(format)
		if err != nil {
			return err
		}
		logger.Debugf("  - previous GTIDs: %s ", position.GTIDSet.String())
		// TODO: record the last GTIDs seen
		//       insert into gtid_executed for now if that's easy/quick, but that can't last long //position.GTIDSet.

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		logger.Debug("Received binlog event: GTID")
		// TODO: Does this mean we should perform a commit?
		// TODO: Read MariaDB KB docs on GTID: https://mariadb.com/kb/en/gtid/
		// TODO: Warnings for unsupported flags in event
		// Seems like we don't have access to other fields for GTID?
		// Does isBegin mean not FL_STANDALONE?
		gtid, isBegin, err := event.GTID(format)
		logger.Debugf(" - %v (isBegin: %t)", gtid, isBegin)
		if err != nil {
			return err
		}

		replicaSourceInfo, err := d.loadReplicationConfiguration(ctx)
		if err != nil {
			return err
		}
		replicaSourceInfo.Uuid = fmt.Sprintf("%v", gtid.SourceServer())
		err = d.persistReplicationConfiguration(ctx, replicaSourceInfo)
		if err != nil {
			return err
		}

		// TODO: Our server restart test is failing, because it is getting duplicate primary key errors –
		//       it isn't restarting the replication stream from teh right GTID sequence! We need to persist
		//       what GTIDs we have processed and then restart replication from the correct event with auto-positioning.

	case event.IsTableMap():
		// Used for row-based binary logging beginning (binlog_format=ROW or MIXED). This event precedes each row
		// operation event and maps a table definition to a number, where the table definition consists of database
		// and table names. For more details, see: https://mariadb.com/kb/en/table_map_event/
		logger.Debug("Received binlog event: TableMap")
		tableId := event.TableID(format)
		tableMap, err := event.TableMap(format)
		if err != nil {
			return err
		}

		// TODO: Handle special Table ID value 0xFFFFFF:
		//		Table id refers to a table defined by TABLE_MAP_EVENT. The special value 0xFFFFFF should have
		//	 	"end of statement flag" (0x0001) set and indicates that table maps can be freed.
		if tableId == 0xFFFFFF {
			logger.Debug("  - received signal to clear cached table maps")
		}
		tableMapsById[tableId] = tableMap
		logger.Debugf(" - tableMap: %v ", formatTableMapAsString(tableId, tableMap))
		// TODO: Will these be resent before each row event, like the documentation seems to indicate? If so, that
		//       seems to remove the requirement to make this metadata durable between server restarts.

	case event.IsDeleteRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		logger.Debug("Received binlog event: DeleteRows")
		createDoltCommit = true
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}
		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		logger.Debugf(" - Deleted Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			deletedRow, err := parseRow(tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
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
		createDoltCommit = true
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}
		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		logger.Debugf(" - New Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			newRow, err := parseRow(tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			logger.Debugf("     - Data: %v ", sql.FormatRow(newRow))

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
				return err
			}
		}

	case event.IsUpdateRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		logger.Debug("Received binlog event: UpdateRows")
		createDoltCommit = true
		tableId := event.TableID(format)
		tableMap, ok := tableMapsById[tableId]
		if !ok {
			return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
		}
		rows, err := event.Rows(format, tableMap)
		if err != nil {
			return err
		}
		schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
		if err != nil {
			return err
		}

		// TODO: process rows.Flags

		logger.Debugf(" - Updated Rows (table: %s)", tableMap.Name)
		for _, row := range rows.Rows {
			identifyRow, err := parseRow(tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			updatedRow, err := parseRow(tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
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

	//case event.IsStop():
	// The primary server writes a STOP event to the binary log when it shuts down or when resuming after a mysqld
	// process crash. A new binary log file is always created but there is no ROTATE_EVENT. STOP_EVENT is then the
	// last written event after clean shutdown or resuming a crash.
	// NOTE: this event is NEVER sent to replica servers!

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

	// For now, create a Dolt commit from every data update. Eventually, we'll want to make this configurable.
	if createDoltCommit {
		executeQueryWithEngine(ctx, engine, "call dolt_commit('-Am', 'automatic Dolt replication commit');")
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

	// TODO: Does this work correctly?
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
func parseRow(tableMap *mysql.TableMap, schema sql.Schema, columnsPresentBitmap, nullValuesBitmap mysql.Bitmap, data []byte) (sql.Row, error) {
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

		convertedValue, err := convertSqlTypesValue(value, column)
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
		// For any unsigned numeric value, we just need to return any unsigned numeric type to signal to Vitess to treat
		// the value as unsigned. The actual type returned doesn't matter – only the signed/unsigned property is used.
		return query.Type_UINT64
	default:
		return query.Type_INT64
	}
}

// convertSqlTypesValues converts a sqltypes.Value instance (vitess) into a sql.Type value (go-mysql-server).
func convertSqlTypesValue(value sqltypes.Value, column *sql.Column) (interface{}, error) {
	// TODO: This is currently a pretty hacky way to convert values and needs to be cleaned up so that it's more
	//	     efficient and handles all types.
	if value.IsNull() {
		return nil, nil
	}

	var convertedValue interface{}
	var err error
	switch {
	case sql.IsEnum(column.Type), sql.IsSet(column.Type):
		atoi, err := strconv.Atoi(value.ToString())
		if err != nil {
			return nil, err
		}
		convertedValue, err = column.Type.Convert(atoi)
	default:
		convertedValue, err = column.Type.Convert(value.ToString())
	}
	if err != nil {
		return nil, fmt.Errorf("unable to convert value %q: %v", value, err.Error())
	}

	return convertedValue, nil
}

// startReplicationEventStream sends a request over |conn|, the connection to the MySQL source server, to begin
// sending binlog events.
func startReplicationEventStream(conn *mysql.Conn) error {
	// TODO: unhardcode GTID starting ID 1 and use the last executed GTID if available
	gtid := mysql.Mysql56GTID{
		Sequence: 1,
	}
	startPosition := mysql.Position{GTIDSet: gtid.GTIDSet()}

	// TODO: unhardcode 1 as the replica's server_id
	return conn.SendBinlogDumpCommand(1, startPosition)
}

func formatTableMapAsString(tableId uint64, tableMap *mysql.TableMap) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("ID: %v, ", tableId))
	sb.WriteString(fmt.Sprintf("Name: %s, ", tableMap.Name))
	sb.WriteString(fmt.Sprintf("Database: %s, ", tableMap.Database))
	sb.WriteString(fmt.Sprintf("Flags: %v, ", tableMap.Flags))
	sb.WriteString(fmt.Sprintf("Metadata: %v, ", tableMap.Metadata))
	sb.WriteString(fmt.Sprintf("Types: %v, ", tableMap.Types))

	return sb.String()
}

func executeQueryWithEngine(ctx *sql.Context, engine *gms.Engine, query string) {
	if ctx.GetCurrentDatabase() == "" {
		logger.Debug("No current database selected, aborting query")
		return
	}

	_, iter, err := engine.Query(ctx, query)
	if err != nil {
		logger.Errorf("ERROR executing query: %v ", err.Error())
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
