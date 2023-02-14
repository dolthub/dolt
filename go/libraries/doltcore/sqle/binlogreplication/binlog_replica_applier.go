// Copyright 2023 Dolthub, Inc.
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
	"io"
	"strconv"
	"strings"
	"time"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/parse"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	vquery "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
)

// positionStore is a singleton instance for loading/saving binlog position state to disk for durable storage.
var positionStore = &binlogPositionStore{}

const (
	ERNetReadError      = 1158
	ERFatalReplicaError = 13117
)

// binlogReplicaApplier represents the process that applies updates from a binlog connection.
//
// This type is NOT used concurrently – there is currently only one single applier process running to process binlog
// events, so the state in this type is NOT protected with a mutex.
type binlogReplicaApplier struct {
	format              mysql.BinlogFormat
	tableMapsById       map[uint64]*mysql.TableMap
	stopReplicationChan chan struct{}
	// currentGtid is the current GTID being processed, but not yet committed
	currentGtid mysql.GTID
	// replicationSourceUuid holds the UUID of the source server
	replicationSourceUuid string
	// currentPosition records which GTIDs have been successfully executed
	currentPosition *mysql.Position
	filters         *filterConfiguration
}

func newBinlogReplicaApplier(filters *filterConfiguration) *binlogReplicaApplier {
	return &binlogReplicaApplier{
		tableMapsById:       make(map[uint64]*mysql.TableMap),
		stopReplicationChan: make(chan struct{}),
		filters:             filters,
	}
}

// Row Flags – https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/

// rowFlag_endOfStatement indicates that a row event with this flag set is the last event in a statement.
const rowFlag_endOfStatement = 0x0001
const rowFlag_noForeignKeyChecks = 0x0002
const rowFlag_noUniqueKeyChecks = 0x0004
const rowFlag_noCheckConstraints = 0x0010

// rowFlag_rowsAreComplete indicates that rows in this event are complete, and contain values for all columns of the table.
const rowFlag_rowsAreComplete = 0x0008

// Go spawns a new goroutine to run the applier's binlog event handler.
func (a *binlogReplicaApplier) Go(ctx *sql.Context) {
	go func() {
		err := a.replicaBinlogEventHandler(ctx)
		if err != nil {
			ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
			DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
		}
	}()
}

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
func (a *binlogReplicaApplier) connectAndStartReplicationEventStream(ctx *sql.Context) (*mysql.Conn, error) {
	var maxConnectionAttempts uint64
	var connectRetryDelay uint32
	DoltBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoConnecting
		status.ReplicaSqlRunning = binlogreplication.ReplicaSqlRunning
		maxConnectionAttempts = status.SourceRetryCount
		connectRetryDelay = status.ConnectRetry
	})

	var conn *mysql.Conn
	var err error
	for connectionAttempts := uint64(0); ; connectionAttempts++ {
		replicaSourceInfo, err := loadReplicationConfiguration(ctx)

		if replicaSourceInfo == nil {
			err = ErrServerNotConfiguredAsReplica
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, err.Error())
			return nil, err
		} else if replicaSourceInfo.Uuid != "" {
			a.replicationSourceUuid = replicaSourceInfo.Uuid
		}

		if replicaSourceInfo.Host == "" {
			err = fmt.Errorf("fatal error: Invalid (empty) hostname when attempting to connect " +
				"to the source server. Connection attempt terminated")
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, err.Error())
			return nil, err
		} else if replicaSourceInfo.User == "" {
			err = fmt.Errorf("fatal error: Invalid (empty) username when attempting to connect " +
				"to the source server. Connection attempt terminated")
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, err.Error())
			return nil, err
		}

		connParams := mysql.ConnParams{
			Host:             replicaSourceInfo.Host,
			Port:             int(replicaSourceInfo.Port),
			Uname:            replicaSourceInfo.User,
			Pass:             replicaSourceInfo.Password,
			ConnectTimeoutMs: 4_000,
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
	// TODO: This should also have retry logic
	err = a.startReplicationEventStream(ctx, conn)
	if err != nil {
		return nil, err
	}

	DoltBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
		status.ReplicaIoRunning = binlogreplication.ReplicaIoRunning
	})

	return conn, nil
}

// startReplicationEventStream sends a request over |conn|, the connection to the MySQL source server, to begin
// sending binlog events.
func (a *binlogReplicaApplier) startReplicationEventStream(ctx *sql.Context, conn *mysql.Conn) error {
	serverId, err := loadReplicaServerId()
	if err != nil {
		return err
	}

	position, err := positionStore.Load(ctx)
	if err != nil {
		return err
	}

	if position == nil {
		// If the positionStore doesn't have a record of executed GTIDs, check to see if the gtid_purged system
		// variable is set. If it holds a GTIDSet, then we use that as our starting position. As part of loading
		// a mysqldump onto a replica, gtid_purged will be set to indicate where to start replication.
		_, value, ok := sql.SystemVariables.GetGlobal("gtid_purged")
		gtidPurged, isString := value.(string)
		if ok && value != nil && isString {
			// Starting in MySQL 8.0, when setting the GTID_PURGED sys variable, if the new value starts with '+', then
			// the specified GTID Set value is added to the current GTID Set value to get a new GTID Set that contains
			// all the previous GTIDs, plus the new ones from the current assignment. Dolt doesn't support this
			// special behavior for appending to GTID Sets yet, so in this case the GTID_PURGED sys var will end up
			// with a "+" prefix. For now, just ignore the "+" prefix if we see it.
			// https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_purged
			if strings.HasPrefix(gtidPurged, "+") {
				ctx.GetLogger().Warnf("Ignoring unsupported '+' prefix on @@GTID_PURGED value")
				gtidPurged = gtidPurged[1:]
			}

			purged, err := mysql.ParsePosition(mysqlFlavor, gtidPurged)
			if err != nil {
				return err
			}
			position = &purged
		}
	}

	if position == nil {
		// If we still don't have any record of executed GTIDs, we create a GTIDSet with just one transaction ID
		// for the 0000 server ID. There doesn't seem to be a cleaner way of saying "start at the very beginning".
		//
		// Also... "starting position" is a bit of a misnomer – it's actually the processed GTIDs, which
		// indicate the NEXT GTID where replication should start, but it's not as direct as specifying
		// a starting position, like the Vitess function signature seems to suggest.
		gtid := mysql.Mysql56GTID{
			Sequence: 1,
		}
		position = &mysql.Position{GTIDSet: gtid.GTIDSet()}
	}

	a.currentPosition = position

	return conn.SendBinlogDumpCommand(serverId, *position)
}

// replicaBinlogEventHandler runs a loop, processing binlog events until the applier's stop replication channel
// receives a signal to stop.
func (a *binlogReplicaApplier) replicaBinlogEventHandler(ctx *sql.Context) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("unable to access a running SQL server")
	}
	engine := server.Engine

	conn, err := a.connectAndStartReplicationEventStream(ctx)
	if err != nil {
		return err
	}

	// Process binlog events
	for {
		select {
		case <-a.stopReplicationChan:
			ctx.GetLogger().Trace("received signal to stop replication routine")
			return nil
		default:
			event, err := conn.ReadBinlogEvent()
			if err != nil {
				if sqlError, isSqlError := err.(*mysql.SQLError); isSqlError {
					if sqlError.Message == io.EOF.Error() {
						ctx.GetLogger().Trace("No more binlog messages; retrying in 1s...")
						time.Sleep(1 * time.Second)
						continue
					} else if strings.HasPrefix(sqlError.Message, io.ErrUnexpectedEOF.Error()) {
						DoltBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
							status.LastIoError = io.ErrUnexpectedEOF.Error()
							status.LastIoErrNumber = ERNetReadError
							currentTime := time.Now()
							status.LastIoErrorTimestamp = &currentTime
						})
						conn, err = a.connectAndStartReplicationEventStream(ctx)
						if err != nil {
							return err
						}
						continue
					} else if strings.Contains(sqlError.Message, "can not handle replication events with the checksum") {
						// Ignore any errors about checksums
						ctx.GetLogger().Debug("ignoring binlog checksum error message")
						continue
					}
				}

				// otherwise, log the error if it's something we don't expect and continue
				ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
				DoltBinlogReplicaController.setIoError(mysql.ERUnknownError, err.Error())

				continue
			}

			err = a.processBinlogEvent(ctx, engine, event)
			if err != nil {
				ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
				DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
			}
		}
	}
}

// processBinlogEvent processes a single binlog event message and returns an error if there were any problems
// processing it.
func (a *binlogReplicaApplier) processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	var err error
	createCommit := false
	commitToAllDatabases := false

	switch {
	case event.IsRand():
		// A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are
		// used to compute the random number. For more details, see: https://mariadb.com/kb/en/rand_event/
		// Note: it is written only before a QUERY_EVENT and is NOT used with row-based logging.
		ctx.GetLogger().Debug("Received binlog event: Rand")

	case event.IsXID():
		// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an
		// XA-capable storage engine. For more details, see: https://mariadb.com/kb/en/xid_event/
		ctx.GetLogger().Debug("Received binlog event: XID")
		createCommit = true
		commitToAllDatabases = true

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		query, err := event.Query(a.format)
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
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

		executeQueryWithEngine(ctx, engine, query.SQL)
		createCommit = strings.ToLower(query.SQL) != "begin"

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		ctx.GetLogger().Debug("Received binlog event: Rotate")

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		a.format, err = event.Format()
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"format": a.format,
		}).Debug("Received binlog event: FormatDescription")

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		position, err := event.PreviousGTIDs(a.format)
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"previousGtids": position.GTIDSet.String(),
		}).Debug("Received binlog event: PreviousGTIDs")

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		gtid, isBegin, err := event.GTID(a.format)
		if err != nil {
			return err
		}
		if isBegin {
			ctx.GetLogger().Errorf("unsupported binlog protocol message: GTID event with 'isBegin' set to true")
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"gtid":    gtid,
			"isBegin": isBegin,
		}).Debug("Received binlog event: GTID")
		a.currentGtid = gtid
		// if the source's UUID hasn't been set yet, set it and persist it
		if a.replicationSourceUuid == "" {
			uuid := fmt.Sprintf("%v", gtid.SourceServer())
			err = persistSourceUuid(ctx, uuid)
			if err != nil {
				return err
			}
			a.replicationSourceUuid = uuid
		}

	case event.IsTableMap():
		// Used for row-based binary logging beginning (binlog_format=ROW or MIXED). This event precedes each row
		// operation event and maps a table definition to a number, where the table definition consists of database
		// and table names. For more details, see: https://mariadb.com/kb/en/table_map_event/
		// Note: TableMap events are sent before each row event, so there is no need to persist them between restarts.
		tableId := event.TableID(a.format)
		tableMap, err := event.TableMap(a.format)
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"id":        tableId,
			"tableName": tableMap.Name,
			"database":  tableMap.Database,
			"flags":     convertToHexString(tableMap.Flags),
			"metadata":  tableMap.Metadata,
			"types":     tableMap.Types,
		}).Debug("Received binlog event: TableMap")

		if tableId == 0xFFFFFF {
			// Table ID 0xFFFFFF is a special value that indicates table maps can be freed.
			ctx.GetLogger().Infof("binlog protocol message: table ID '0xFFFFFF'; clearing table maps")
			a.tableMapsById = make(map[uint64]*mysql.TableMap)
		} else {
			flags := tableMap.Flags
			if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
				// nothing to be done for end of statement; just clear the flag
				flags = flags &^ rowFlag_endOfStatement
			}
			if flags&rowFlag_noForeignKeyChecks == rowFlag_noForeignKeyChecks {
				flags = flags &^ rowFlag_noForeignKeyChecks
			}
			if flags != 0 {
				msg := fmt.Sprintf("unsupported binlog protocol message: TableMap event with unsupported flags '%x'", flags)
				ctx.GetLogger().Errorf(msg)
				DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
			}
			a.tableMapsById[tableId] = tableMap
		}

	case event.IsDeleteRows(), event.IsWriteRows(), event.IsUpdateRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		err = a.processRowEvent(ctx, event, engine)
		if err != nil {
			return err
		}

	default:
		// We can't access the bytes directly because these non-interface types in Vitess are not exposed.
		// Having a Bytes() or Type() method on the Vitess interface would let us clean this up.
		byteString := fmt.Sprintf("%v", event)
		if strings.HasPrefix(byteString, "{[0 0 0 0 27 ") {
			// Type 27 is a Heartbeat event. This event does not appear in the binary log. It's only sent over the
			// network by a primary to a replica to let it know that the primary is still alive, and is only sent
			// when the primary has no binlog events to send to replica servers.
			// For more details, see: https://mariadb.com/kb/en/heartbeat_log_event/
			ctx.GetLogger().Debug("Received binlog event: Heartbeat")
		} else {
			return fmt.Errorf("received unknown event: %v", event)
		}
	}

	if createCommit {
		var databasesToCommit []string
		if commitToAllDatabases {
			databasesToCommit = getAllUserDatabaseNames(ctx, engine)
			for _, database := range databasesToCommit {
				executeQueryWithEngine(ctx, engine, "use `"+database+"`;")
				executeQueryWithEngine(ctx, engine, "commit;")
			}
		}

		// Record the last GTID processed after the commit
		a.currentPosition.GTIDSet = a.currentPosition.GTIDSet.AddGTID(a.currentGtid)
		err := sql.SystemVariables.AssignValues(map[string]interface{}{"gtid_executed": a.currentPosition.GTIDSet.String()})
		if err != nil {
			ctx.GetLogger().Errorf("unable to set @@GLOBAL.gtid_executed: %s", err.Error())
		}
		err = positionStore.Save(ctx, a.currentPosition)
		if err != nil {
			return fmt.Errorf("unable to store GTID executed metadata to disk: %s", err.Error())
		}

		// For now, create a Dolt commit from every data update. Eventually, we'll want to make this configurable.
		ctx.GetLogger().Trace("Creating Dolt commit(s)")
		for _, database := range databasesToCommit {
			executeQueryWithEngine(ctx, engine, "use `"+database+"`;")
			executeQueryWithEngine(ctx, engine,
				fmt.Sprintf("call dolt_commit('-Am', 'Dolt binlog replica commit: GTID %s');", a.currentGtid))
		}
	}

	return nil
}

// processRowEvent processes a WriteRows, DeleteRows, or UpdateRows binlog event and returns an error if any problems
// were encountered.
func (a *binlogReplicaApplier) processRowEvent(ctx *sql.Context, event mysql.BinlogEvent, engine *gms.Engine) error {
	switch {
	case event.IsDeleteRows():
		ctx.GetLogger().Debug("Received binlog event: DeleteRows")
	case event.IsWriteRows():
		ctx.GetLogger().Debug("Received binlog event: WriteRows")
	case event.IsUpdateRows():
		ctx.GetLogger().Debug("Received binlog event: UpdateRows")
	default:
		return fmt.Errorf("unsupported event type: %v", event)
	}

	tableId := event.TableID(a.format)
	tableMap, ok := a.tableMapsById[tableId]
	if !ok {
		return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
	}

	if a.filters.isTableFilteredOut(ctx, tableMap) {
		return nil
	}

	rows, err := event.Rows(a.format, tableMap)
	if err != nil {
		return err
	}

	flags := rows.Flags
	if flags&rowFlag_endOfStatement == rowFlag_endOfStatement {
		// nothing to be done for end of statement; just clear the flag and move on
		flags = flags &^ rowFlag_endOfStatement
	}
	if flags&rowFlag_noForeignKeyChecks == rowFlag_noForeignKeyChecks {
		flags = flags &^ rowFlag_noForeignKeyChecks
	}
	if flags != 0 {
		msg := fmt.Sprintf("unsupported binlog protocol message: DeleteRows event with unsupported flags '%x'", flags)
		ctx.GetLogger().Errorf(msg)
		DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
	}
	schema, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
	if err != nil {
		return err
	}

	switch {
	case event.IsDeleteRows():
		ctx.GetLogger().Debugf(" - Deleted Rows (table: %s)", tableMap.Name)
	case event.IsUpdateRows():
		ctx.GetLogger().Debugf(" - Updated Rows (table: %s)", tableMap.Name)
	case event.IsWriteRows():
		ctx.GetLogger().Debugf(" - New Rows (table: %s)", tableMap.Name)
	}

	foreignKeyChecksDisabled := tableMap.Flags&rowFlag_noForeignKeyChecks > 0
	writeSession, tableWriter, err := getTableWriter(ctx, engine, tableMap.Name, tableMap.Database, foreignKeyChecksDisabled)
	if err != nil {
		return err
	}

	for _, row := range rows.Rows {
		var identityRow, dataRow sql.Row
		if len(row.Identify) > 0 {
			identityRow, err = parseRow(ctx, tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			ctx.GetLogger().Debugf("     - Identity: %v ", sql.FormatRow(identityRow))
		}

		if len(row.Data) > 0 {
			dataRow, err = parseRow(ctx, tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			ctx.GetLogger().Debugf("     - Data: %v ", sql.FormatRow(dataRow))
		}

		switch {
		case event.IsDeleteRows():
			err = tableWriter.Delete(ctx, identityRow)
		case event.IsWriteRows():
			err = tableWriter.Insert(ctx, dataRow)
		case event.IsUpdateRows():
			err = tableWriter.Update(ctx, identityRow, dataRow)
		}
		if err != nil {
			return err
		}

	}

	err = closeWriteSession(ctx, engine, tableMap.Database, writeSession)
	if err != nil {
		return err
	}

	return nil
}

//
// Helper functions
//

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
func getTableWriter(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string, foreignKeyChecksDisabled bool) (writer.WriteSession, writer.TableWriter, error) {
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

	options := sqlDatabase.EditOptions()
	options.ForeignKeyChecksDisabled = foreignKeyChecksDisabled
	writeSession := writer.NewWriteSession(binFormat, ws, tracker, options)

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
			value, err = sqltypes.NewValue(vquery.Type_NULL_TYPE, nil)
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
func getSignedType(column *sql.Column) vquery.Type {
	switch column.Type.Type() {
	case vquery.Type_UINT8, vquery.Type_UINT16, vquery.Type_UINT24, vquery.Type_UINT32, vquery.Type_UINT64:
		// For any unsigned integer value, we just need to return any unsigned numeric type to signal to Vitess to treat
		// the value as unsigned. The actual type returned doesn't matter – only the signed/unsigned property is used.
		return vquery.Type_UINT64
	default:
		return vquery.Type_INT64
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
	if value.Type() != vquery.Type_EXPRESSION {
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
	if !ok || serverId == 0 {
		return 0, fmt.Errorf("invalid server ID configured for @@GLOBAL.server_id (%v); "+
			"must be an integer greater than zero and less than 4,294,967,296", serverId)
	}

	return serverId, nil
}

func executeQueryWithEngine(ctx *sql.Context, engine *gms.Engine, query string) {
	if ctx.GetCurrentDatabase() == "" {
		ctx.GetLogger().Warn("No current database selected")
	}

	_, iter, err := engine.Query(ctx, query)
	if err != nil {
		// Log any errors, except for commits with "nothing to commit"
		if err.Error() != "nothing to commit" {
			msg := fmt.Sprintf("ERROR executing query: %v ", err.Error())
			ctx.GetLogger().Errorf(msg)
			DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
		}
		return
	}
	for {
		_, err := iter.Next(ctx)
		if err != nil {
			if err != io.EOF {
				ctx.GetLogger().Errorf("ERROR reading query results: %v ", err.Error())
			}
			return
		}
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
