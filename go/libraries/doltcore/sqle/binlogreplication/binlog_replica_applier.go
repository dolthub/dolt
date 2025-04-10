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
	"sync"
	"sync/atomic"
	"time"

	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/go-mysql-server/sql/planbuilder"
	"github.com/dolthub/go-mysql-server/sql/rowexec"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	vquery "github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/dolthub/vitess/go/vt/vttls"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
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
	format                    *mysql.BinlogFormat
	tableMapsById             map[uint64]*mysql.TableMap
	stopReplicationChan       chan struct{}
	currentGtid               mysql.GTID
	replicationSourceUuid     string
	currentPosition           *mysql.Position // successfully executed GTIDs
	filters                   *filterConfiguration
	running                   atomic.Bool
	handlerWg                 sync.WaitGroup
	engine                    *gms.Engine
	dbsWithUncommittedChanges map[string]struct{}
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
	if !a.running.CompareAndSwap(false, true) {
		panic("attempt to start binlogReplicaApplier while it is already running")
	}
	a.handlerWg.Add(1)
	go func() {
		defer a.handlerWg.Done()
		defer a.running.Store(false)
		err := a.replicaBinlogEventHandler(ctx)
		if err != nil {
			ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
			DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
		}
	}()
}

// IsRunning returns true if this binlog applier is running and has not been stopped, otherwise returns false.
func (a *binlogReplicaApplier) IsRunning() bool {
	return a.running.Load()
}

// Stop will shutdown the replication thread if it is running. This is not safe to call concurrently |Go|.
// This is used by the controller when implementing STOP REPLICA, but it is also used on shutdown when the
// replication thread should be shutdown cleanly in the event that it is still running.
func (a *binlogReplicaApplier) Stop() {
	if a.IsRunning() {
		// We jump through some hoops here. It is not the case that the replication thread
		// is guaranteed to read from |stopReplicationChan|. Instead, it can exit on its
		// own with an error, for example, after exceeding connection retry attempts.
		done := make(chan struct{})
		go func() {
			defer close(done)
			a.handlerWg.Wait()
		}()
		select {
		case a.stopReplicationChan <- struct{}{}:
		case <-done:
		}
		a.handlerWg.Wait()
	}
}

// connectAndStartReplicationEventStream connects to the configured MySQL replication source, including pausing
// and retrying if errors are encountered.
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
		sql.SessionCommandBegin(ctx.Session)
		replicaSourceInfo, err := loadReplicationConfiguration(ctx, a.engine.Analyzer.Catalog.MySQLDb)
		sql.SessionCommandEnd(ctx.Session)
		if replicaSourceInfo == nil {
			err = ErrServerNotConfiguredAsReplica
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, err.Error())
			return nil, err
		} else if replicaSourceInfo.Uuid != "" {
			a.replicationSourceUuid = replicaSourceInfo.Uuid
		}

		if replicaSourceInfo.Host == "" {
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyHostname.Error())
			return nil, ErrEmptyHostname
		} else if replicaSourceInfo.User == "" {
			DoltBinlogReplicaController.setIoError(ERFatalReplicaError, ErrEmptyUsername.Error())
			return nil, ErrEmptyUsername
		}

		sslMode := vttls.Disabled
		if replicaSourceInfo.Ssl {
			sslMode = vttls.Required
		}

		connParams := mysql.ConnParams{
			Host:             replicaSourceInfo.Host,
			Port:             int(replicaSourceInfo.Port),
			SslMode:          sslMode,
			Uname:            replicaSourceInfo.User,
			Pass:             replicaSourceInfo.Password,
			ConnectTimeoutMs: 4_000,
		}

		conn, err = mysql.Connect(ctx, &connParams)
		if err != nil {
			logrus.Warnf("failed connection attempt to source (%s): %s",
				replicaSourceInfo.Host, err.Error())

			if connectionAttempts >= maxConnectionAttempts {
				ctx.GetLogger().Errorf("Exceeded max connection attempts (%d) to source (%s)",
					maxConnectionAttempts, replicaSourceInfo.Host)
				return nil, err
			}
			// If there was an error connecting (and we haven't used up all our retry attempts), listen for a
			// STOP REPLICA signal or for the retry delay timer to fire. We need to use select here so that we don't
			// block on our retry backoff and ignore the STOP REPLICA signal for a long time.
			select {
			case <-a.stopReplicationChan:
				ctx.GetLogger().Debugf("Received stop replication signal while trying to connect")
				return nil, ErrReplicationStopped
			case <-time.After(time.Duration(connectRetryDelay) * time.Second):
				// Nothing to do here if our timer completes; just fall through
			}
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
	serverId, err := loadReplicaServerId(ctx)
	if err != nil {
		return err
	}

	doltSession := dsess.DSessFromSess(ctx.Session)
	filesys := doltSession.Provider().FileSystem()

	position, err := positionStore.Load(filesys)
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

	// Clear out the format description in case we're reconnecting, so that we don't use the old format description
	// to interpret any event messages before we receive the new format description from the new stream.
	a.format = nil

	// If the source server has binlog checksums enabled (@@global.binlog_checksum), then the replica MUST
	// set @master_binlog_checksum to handshake with the server to acknowledge that it knows that checksums
	// are in use. Without this step, the server will just send back error messages saying that the replica
	// does not support the binlog checksum algorithm in use on the primary.
	// For more details, see: https://dev.mysql.com/worklog/task/?id=2540
	_, err = conn.ExecuteFetch("set @master_binlog_checksum=@@global.binlog_checksum;", 0, false)
	if err != nil {
		return err
	}

	return conn.SendBinlogDumpCommand(serverId, *position)
}

// replicaBinlogEventHandler runs a loop, processing binlog events until the applier's stop replication channel
// receives a signal to stop.
func (a *binlogReplicaApplier) replicaBinlogEventHandler(ctx *sql.Context) error {
	engine := a.engine

	var eventProducer *binlogEventProducer

	// Process binlog events
	for {
		if eventProducer == nil {
			ctx.GetLogger().Debug("no binlog connection to source, attempting to establish one")

			if conn, err := a.connectAndStartReplicationEventStream(ctx); err == ErrReplicationStopped {
				return nil
			} else if err != nil {
				return err
			} else {
				eventProducer = newBinlogEventProducer(conn)
				eventProducer.Go(ctx)
			}
		}

		select {
		case event := <-eventProducer.EventChan():
			err := a.processBinlogEvent(ctx, engine, event)
			if err != nil {
				ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
				DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, err.Error())
			}

		case err := <-eventProducer.ErrorChan():
			if sqlError, isSqlError := err.(*mysql.SQLError); isSqlError {
				badConnection := sqlError.Message == io.EOF.Error() ||
					strings.HasPrefix(sqlError.Message, io.ErrUnexpectedEOF.Error())
				if badConnection {
					DoltBinlogReplicaController.updateStatus(func(status *binlogreplication.ReplicaStatus) {
						status.LastIoError = sqlError.Message
						status.LastIoErrNumber = ERNetReadError
						currentTime := time.Now()
						status.LastIoErrorTimestamp = &currentTime
					})
					eventProducer.Stop()
					eventProducer = nil
				}
			} else {
				// otherwise, log the error if it's something we don't expect and continue
				ctx.GetLogger().Errorf("unexpected error of type %T: '%v'", err, err.Error())
				DoltBinlogReplicaController.setIoError(mysql.ERUnknownError, err.Error())
			}

		case <-a.stopReplicationChan:
			ctx.GetLogger().Trace("received stop replication signal")
			eventProducer.Stop()
			eventProducer = nil
			return nil
		}
	}
}

// processBinlogEvent processes a single binlog event message and returns an error if there were any problems
// processing it.
func (a *binlogReplicaApplier) processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	sql.SessionCommandBegin(ctx.Session)
	defer sql.SessionCommandEnd(ctx.Session)
	var err error
	createCommit := false

	// We don't support checksum validation, so we MUST strip off any checksum bytes if present, otherwise it gets
	// interpreted as part of the payload and corrupts the data. Future checksum sizes, are not guaranteed to be the
	// same size, so we can't strip the checksum until we've seen a valid Format binlog event that definitively
	// tells us if checksums are enabled and what algorithm they use. We can NOT strip the checksum off of
	// FormatDescription events, because FormatDescription always includes a CRC32 checksum, and Vitess depends on
	// those bytes always being present when we parse the event into a FormatDescription type.
	if a.format != nil && event.IsFormatDescription() == false {
		var err error
		event, _, err = event.StripChecksum(*a.format)
		if err != nil {
			msg := fmt.Sprintf("unable to strip checksum from binlog event: '%v'", err.Error())
			ctx.GetLogger().Error(msg)
			DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
		}
	}

	switch {
	case event.IsRand():
		// A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are
		// used to compute the random number. For more details, see: https://mariadb.com/kb/en/rand_event/
		// Note: it is written only before a QUERY_EVENT and is NOT used with row-based logging.
		ctx.GetLogger().Trace("Received binlog event: Rand")

	case event.IsXID():
		// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an
		// XA-capable storage engine. For more details, see: https://mariadb.com/kb/en/xid_event/
		ctx.GetLogger().Trace("Received binlog event: XID")
		createCommit = true

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		query, err := event.Query(*a.format)
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"database": query.Database,
			"charset":  query.Charset,
			"query":    query.SQL,
			"options":  fmt.Sprintf("0x%x", query.Options),
			"sql_mode": fmt.Sprintf("0x%x", query.SqlMode),
		}).Trace("Received binlog event: Query")

		if query.Options&mysql.QFlagOptionAutoIsNull > 0 {
			ctx.GetLogger().Tracef("Setting sql_auto_is_null ON")
			ctx.SetSessionVariable(ctx, "sql_auto_is_null", 1)
		} else {
			ctx.GetLogger().Tracef("Setting sql_auto_is_null OFF")
			ctx.SetSessionVariable(ctx, "sql_auto_is_null", 0)
		}

		if query.Options&mysql.QFlagOptionNotAutocommit > 0 {
			ctx.GetLogger().Tracef("Setting autocommit=0")
			ctx.SetSessionVariable(ctx, "autocommit", 0)
		} else {
			ctx.GetLogger().Tracef("Setting autocommit=1")
			ctx.SetSessionVariable(ctx, "autocommit", 1)
		}

		if query.Options&mysql.QFlagOptionNoForeignKeyChecks > 0 {
			ctx.GetLogger().Tracef("Setting foreign_key_checks=0")
			ctx.SetSessionVariable(ctx, "foreign_key_checks", 0)
		} else {
			ctx.GetLogger().Tracef("Setting foreign_key_checks=1")
			ctx.SetSessionVariable(ctx, "foreign_key_checks", 1)
		}

		// NOTE: unique_checks is not currently honored by Dolt
		if query.Options&mysql.QFlagOptionRelaxedUniqueChecks > 0 {
			ctx.GetLogger().Tracef("Setting unique_checks=0")
			ctx.SetSessionVariable(ctx, "unique_checks", 0)
		} else {
			ctx.GetLogger().Tracef("Setting unique_checks=1")
			ctx.SetSessionVariable(ctx, "unique_checks", 1)
		}

		ctx.SetCurrentDatabase(query.Database)
		executeQueryWithEngine(ctx, engine, query.SQL)
		createCommit = !strings.EqualFold(query.SQL, "begin")

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		ctx.GetLogger().Trace("Received binlog event: Rotate")

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		format, err := event.Format()
		if err != nil {
			return err
		}
		a.format = &format
		ctx.GetLogger().WithFields(logrus.Fields{
			"format":        a.format,
			"formatVersion": a.format.FormatVersion,
			"serverVersion": a.format.ServerVersion,
			"checksum":      a.format.ChecksumAlgorithm,
		}).Trace("Received binlog event: FormatDescription")

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		position, err := event.PreviousGTIDs(*a.format)
		if err != nil {
			return err
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"previousGtids": position.GTIDSet.String(),
		}).Trace("Received binlog event: PreviousGTIDs")

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		gtid, isBegin, err := event.GTID(*a.format)
		if err != nil {
			return err
		}
		if isBegin {
			ctx.GetLogger().Errorf("unsupported binlog protocol message: GTID event with 'isBegin' set to true")
		}
		ctx.GetLogger().WithFields(logrus.Fields{
			"gtid":    gtid,
			"isBegin": isBegin,
		}).Trace("Received binlog event: GTID")
		a.currentGtid = gtid
		// if the source's UUID hasn't been set yet, set it and persist it
		if a.replicationSourceUuid == "" {
			uuid := fmt.Sprintf("%v", gtid.SourceServer())
			err = persistSourceUuid(ctx, uuid, a.engine.Analyzer.Catalog.MySQLDb)
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
		tableId := event.TableID(*a.format)
		tableMap, err := event.TableMap(*a.format)
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
		}).Trace("Received binlog event: TableMap")

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
				ctx.GetLogger().Error(msg)
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
			ctx.GetLogger().Trace("Received binlog event: Heartbeat")
		} else {
			return fmt.Errorf("received unknown event: %v", event)
		}
	}

	if createCommit {
		doltSession := dsess.DSessFromSess(ctx.Session)
		databasesToCommit := doltSession.DirtyDatabases()
		if err = doltSession.CommitTransaction(ctx, doltSession.GetTransaction()); err != nil {
			return err
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
		// We commit to every database that we saw had a dirty session – these identify the databases where we have
		// run DML commands through the engine. We also commit to every database that was modified through a RowEvent,
		// which is all tracked through the applier's databasesWithUncommitedChanges property – these don't show up
		// as dirty in our session, since we used TableWriter to update them.
		a.addDatabasesWithUncommittedChanges(databasesToCommit...)
		for _, database := range a.databasesWithUncommittedChanges() {
			executeQueryWithEngine(ctx, engine, "use `"+database+"`;")
			executeQueryWithEngine(ctx, engine,
				fmt.Sprintf("call dolt_commit('-Am', 'Dolt binlog replica commit: GTID %s');", a.currentGtid))
		}
		a.dbsWithUncommittedChanges = nil
	}

	return nil
}

// addDatabasesWithUncommittedChanges marks the specifeid |dbNames| as databases with uncommitted changes so that
// the replica applier knows which databases need to have Dolt commits created.
func (a *binlogReplicaApplier) addDatabasesWithUncommittedChanges(dbNames ...string) {
	if a.dbsWithUncommittedChanges == nil {
		a.dbsWithUncommittedChanges = make(map[string]struct{})
	}
	for _, dbName := range dbNames {
		a.dbsWithUncommittedChanges[dbName] = struct{}{}
	}
}

// databasesWithUncommittedChanges returns a slice of database names indicating which databases have uncommitted
// changes and need a Dolt commit created.
func (a *binlogReplicaApplier) databasesWithUncommittedChanges() []string {
	if a.dbsWithUncommittedChanges == nil {
		return nil
	}
	dbNames := make([]string, 0, len(a.dbsWithUncommittedChanges))
	for dbName, _ := range a.dbsWithUncommittedChanges {
		dbNames = append(dbNames, dbName)
	}
	return dbNames
}

// processRowEvent processes a WriteRows, DeleteRows, or UpdateRows binlog event and returns an error if any problems
// were encountered.
func (a *binlogReplicaApplier) processRowEvent(ctx *sql.Context, event mysql.BinlogEvent, engine *gms.Engine) error {
	var eventType string
	switch {
	case event.IsDeleteRows():
		eventType = "DeleteRows"
	case event.IsWriteRows():
		eventType = "WriteRows"
	case event.IsUpdateRows():
		eventType = "UpdateRows"
	default:
		return fmt.Errorf("unsupported event type: %v", event)
	}
	ctx.GetLogger().Tracef("Received binlog event: %s", eventType)

	tableId := event.TableID(*a.format)
	tableMap, ok := a.tableMapsById[tableId]
	if !ok {
		return fmt.Errorf("unable to find replication metadata for table ID: %d", tableId)
	}

	if a.filters.isTableFilteredOut(ctx, tableMap) {
		return nil
	}

	a.addDatabasesWithUncommittedChanges(tableMap.Database)
	rows, err := event.Rows(*a.format, tableMap)
	if err != nil {
		return err
	}

	ctx.GetLogger().WithFields(logrus.Fields{
		"flags": fmt.Sprintf("%x", rows.Flags),
	}).Tracef("Processing rows from %s event", eventType)

	flags := rows.Flags
	foreignKeyChecksDisabled := false
	if flags&rowFlag_endOfStatement > 0 {
		// nothing to be done for end of statement; just clear the flag and move on
		flags = flags &^ rowFlag_endOfStatement
	}
	if flags&rowFlag_noForeignKeyChecks > 0 {
		foreignKeyChecksDisabled = true
		flags = flags &^ rowFlag_noForeignKeyChecks
	}
	if flags != 0 {
		msg := fmt.Sprintf("unsupported binlog protocol message: row event with unsupported flags '%x'", flags)
		ctx.GetLogger().Error(msg)
		DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
	}
	schema, tableName, err := getTableSchema(ctx, engine, tableMap.Name, tableMap.Database)
	if err != nil {
		return err
	}

	switch {
	case event.IsDeleteRows():
		ctx.GetLogger().Tracef(" - Deleted Rows (table: %s)", tableMap.Name)
	case event.IsUpdateRows():
		ctx.GetLogger().Tracef(" - Updated Rows (table: %s)", tableMap.Name)
	case event.IsWriteRows():
		ctx.GetLogger().Tracef(" - Inserted Rows (table: %s)", tableMap.Name)
	}

	writeSession, tableWriter, err := getTableWriter(ctx, engine, tableName, tableMap.Database, foreignKeyChecksDisabled)
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
			ctx.GetLogger().Tracef("     - Identity: %v ", sql.FormatRow(identityRow))
		}

		if len(row.Data) > 0 {
			dataRow, err = parseRow(ctx, tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			ctx.GetLogger().Tracef("     - Data: %v ", sql.FormatRow(dataRow))
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
func closeWriteSession(ctx *sql.Context, engine *gms.Engine, databaseName string, writeSession dsess.WriteSession) error {
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

	return sqlDatabase.DbData().Ddb.UpdateWorkingSet(ctx, newWorkingSet.Ref(), newWorkingSet, hash, newWorkingSet.Meta(), nil)
}

// getTableSchema returns a sql.Schema for the case-insensitive |tableName| in the database named
// |databaseName|, along with the exact, case-sensitive table name.
func getTableSchema(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string) (sql.Schema, string, error) {
	database, err := engine.Analyzer.Catalog.Database(ctx, databaseName)
	if err != nil {
		return nil, "", err
	}
	table, ok, err := database.GetTableInsensitive(ctx, tableName)
	if err != nil {
		return nil, "", err
	}
	if !ok {
		return nil, "", fmt.Errorf("unable to find table %q", tableName)
	}

	return table.Schema(), table.Name(), nil
}

// getTableWriter returns a WriteSession and a TableWriter for writing to the specified |table| in the specified |database|.
func getTableWriter(ctx *sql.Context, engine *gms.Engine, tableName, databaseName string, foreignKeyChecksDisabled bool) (dsess.WriteSession, dsess.TableWriter, error) {
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

	tracker, err := dsess.NewAutoIncrementTracker(ctx, sqlDatabase.Name(), ws)
	if err != nil {
		return nil, nil, err
	}

	options := sqlDatabase.EditOptions()
	options.ForeignKeyChecksDisabled = foreignKeyChecksDisabled
	writeSession := writer.NewWriteSession(binFormat, ws, tracker, options)

	ds := dsess.DSessFromSess(ctx.Session)
	setter := ds.SetWorkingRoot

	tableWriter, err := writeSession.GetTableWriter(ctx, doltdb.TableName{Name: tableName}, databaseName, setter, false)
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
		convertedValue, _, err = column.Type.Convert(ctx, atoi)
	case types.IsDecimal(column.Type):
		// Decimal values need to have any leading/trailing whitespace trimmed off
		// TODO: Consider moving this into DecimalType_.Convert; if DecimalType_.Convert handled trimming
		//       leading/trailing whitespace, this special case for Decimal types wouldn't be needed.
		convertedValue, _, err = column.Type.Convert(ctx, strings.TrimSpace(value.ToString()))
	case types.IsJSON(column.Type):
		convertedValue, err = convertVitessJsonExpressionString(ctx, value)
	default:
		convertedValue, _, err = column.Type.Convert(ctx, value.ToString())
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

	server := sqlserver.GetRunningServer()
	if server == nil {
		return nil, fmt.Errorf("unable to access running SQL server")
	}

	binder := planbuilder.New(ctx, server.Engine.Analyzer.Catalog, server.Engine.EventScheduler, server.Engine.Parser)
	node, _, _, qFlags, err := binder.Parse("SELECT "+strValue, nil, false)
	if err != nil {
		return nil, err
	}

	analyze, err := server.Engine.Analyzer.Analyze(ctx, node, nil, qFlags)
	if err != nil {
		return nil, err
	}

	rowIter, err := rowexec.DefaultBuilder.Build(ctx, analyze, nil)
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
func loadReplicaServerId(ctx *sql.Context) (uint32, error) {
	serverIdVar, value, ok := sql.SystemVariables.GetGlobal("server_id")
	if !ok {
		return 0, fmt.Errorf("no server_id global system variable set")
	}

	// Persisted values stored in .dolt/config.json can cause string values to be stored in
	// system variables, so attempt to convert the value if we can't directly cast it to a uint32.
	serverId, ok := value.(uint32)
	if !ok {
		var err error
		value, _, err = serverIdVar.GetType().Convert(ctx, value)
		if err != nil {
			return 0, err
		}
	}

	serverId, ok = value.(uint32)
	if !ok || serverId == 0 {
		return 0, fmt.Errorf("invalid server ID configured for @@GLOBAL.server_id (%v); "+
			"must be an integer greater than zero and less than 4,294,967,296", serverId)
	}

	return serverId, nil
}

func executeQueryWithEngine(ctx *sql.Context, engine *gms.Engine, query string) {
	// Create a sub-context when running queries against the engine, so that we get an accurate query start time.
	queryCtx := sql.NewContext(ctx, sql.WithSession(ctx.Session))

	if queryCtx.GetCurrentDatabase() == "" {
		ctx.GetLogger().WithFields(logrus.Fields{
			"query": query,
		}).Warn("No current database selected")
	}

	_, iter, _, err := engine.Query(queryCtx, query)
	if err != nil {
		// Log any errors, except for commits with "nothing to commit"
		if err.Error() != "nothing to commit" {
			queryCtx.GetLogger().WithFields(logrus.Fields{
				"error": err.Error(),
				"query": query,
			}).Errorf("Error executing query")
			msg := fmt.Sprintf("Error executing query: %v", err.Error())
			DoltBinlogReplicaController.setSqlError(mysql.ERUnknownError, msg)
		}
		return
	}
	for {
		_, err := iter.Next(queryCtx)
		if err != nil {
			if err != io.EOF {
				queryCtx.GetLogger().Errorf("ERROR reading query results: %v ", err.Error())
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
