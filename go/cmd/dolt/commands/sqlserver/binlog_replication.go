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

package sqlserver

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/globalstate"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/writer"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqlserver"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	gms "github.com/dolthub/go-mysql-server"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/mysql_db"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/sqltypes"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// TODO: Move these into a struct to track?
var format mysql.BinlogFormat
var tableMapsById = make(map[uint64]*mysql.TableMap)

var stopReplicationChan = make(chan struct{})

// Row Flags – https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
const endOfStatementRowFlag = 0x0001
const noForeignKeyChecksRowFlag = 0x0002
const noUniqueKeyChecksRowFlag = 0x0004
const rowsAreCompleteRowFlag = 0x0008
const noCheckConstraintsRowFlag = 0x0010

// TODO: Look at configuration interfaces for other replication options and naming patterns
type replicaConfiguration struct {
	sourceServerUuid string
	connectionParams *mysql.ConnParams
	startingGtid     int64
}

// NewReplicaConfiguration creates a new replica configuration for the server with a UUID of |sourceServerUuid|
// (found from the @@server_uuid variable on the source server) and |connectionParams| indicating how to connect
// to the source server.
func NewReplicaConfiguration(sourceServerUuid string, connectionParams *mysql.ConnParams) *replicaConfiguration {
	return &replicaConfiguration{
		sourceServerUuid: sourceServerUuid,
		connectionParams: connectionParams,
		startingGtid:     1,
	}
}

// StopReplication stops any current replication process.
func StopReplication() {
	stopReplicationChan <- struct{}{}
}

// TODO: Turn this into a struct with an API that can be called
//	"replicationController" or something similar to match "clusterController"?

func replicaBinlogEventHandler(ctx *sql.Context, replicaConfiguration *replicaConfiguration) error {
	server := sqlserver.GetRunningServer()
	if server == nil {
		return fmt.Errorf("unable to access a running SQL server")
	}
	engine := server.Engine

	// Connect to the MySQL Replication Source
	// NOTE: Our fork of Vitess currently only supports mysql_native_password auth. The latest code in the main
	//       Vitess repo supports the current MySQL default auth plugin, caching_sha2_password.
	//       https://dev.mysql.com/blog-archive/upgrading-to-mysql-8-0-default-authentication-plugin-considerations/
	//       To work around this limitation, add the following to your /etc/my.cnf:
	//           [mysqld]
	//           default-authentication-plugin=mysql_native_password
	//       or start mysqld with:
	//           --default-authentication-plugin=mysql_native_password
	conn, err := mysql.Connect(ctx, replicaConfiguration.connectionParams)
	if err != nil {
		return err
	}

	// Request binlog events to start
	err = startReplicationEventStream(replicaConfiguration, conn)
	if err != nil {
		return err
	}

	// Process binlog events
	for {
		select {
		case <-stopReplicationChan:
			return nil
		default:
			// TODO: How do we configure network timeouts?
			event, err := conn.ReadBinlogEvent()
			if err != nil {
				if sqlError, isSqlError := err.(*mysql.SQLError); isSqlError {
					if sqlError.Message == io.EOF.Error() {
						fmt.Printf("No more binlog messages; retrying in 1s...\n")
						time.Sleep(1 * time.Second)
						continue
					} else if strings.Contains(sqlError.Message, "can not handle replication events with the checksum") {
						// For now, just ignore any errors about checksums
						fmt.Printf("!!! received checksum error message !!!\n")
						continue
					}
				}

				// otherwise, return the error if it's something we don't expect
				return err
			}

			err = processBinlogEvent(ctx, engine, event)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func processBinlogEvent(ctx *sql.Context, engine *gms.Engine, event mysql.BinlogEvent) error {
	var err error
	createDoltCommit := false

	switch {
	case event.IsRand():
		// A RAND_EVENT contains two seed values that set the rand_seed1 and rand_seed2 system variables that are
		// used to compute the random number. For more details, see: https://mariadb.com/kb/en/rand_event/
		// Note: it is written only before a QUERY_EVENT and is NOT used with row-based logging.
		fmt.Printf("Received: Rand event\n")

	case event.IsXID():
		// An XID event is generated for a COMMIT of a transaction that modifies one or more tables of an
		// XA-capable storage engine. For more details, see: https://mariadb.com/kb/en/xid_event/
		fmt.Printf("Received: XID event\n")
		// TODO: parse XID transaction number and record it durably
		//       gtid, b, err := event.GTID(format)
		executeQueryWithEngine(ctx, engine, "commit;")
		createDoltCommit = true

	case event.IsQuery():
		// A Query event represents a statement executed on the source server that should be executed on the
		// replica. Used for all statements with statement-based replication, DDL statements with row-based replication
		// as well as COMMITs for non-transactional engines such as MyISAM.
		// For more details, see: https://mariadb.com/kb/en/query_event/
		fmt.Printf("Received: Query event\n")
		query, err := event.Query(format)
		if err != nil {
			return err
		}
		fmt.Printf(" - %s \n", query.String())
		ctx.SetCurrentDatabase(query.Database)
		executeQueryWithEngine(ctx, engine, query.SQL)
		createDoltCommit = true

	case event.IsRotate():
		// When a binary log file exceeds the configured size limit, a ROTATE_EVENT is written at the end of the file,
		// pointing to the next file in the sequence. ROTATE_EVENT is generated locally and written to the binary log
		// on the source server and it's also written when a FLUSH LOGS statement occurs on the source server.
		// For more details, see: https://mariadb.com/kb/en/rotate_event/
		fmt.Printf("Received: Rotate event\n")
		// TODO: What action do we take at rotate?

	case event.IsPreviousGTIDs():
		// Logged in every binlog to record the current replication state. Consists of the last GTID seen for each
		// replication domain. For more details, see: https://mariadb.com/kb/en/gtid_list_event/
		fmt.Printf("Received: PreviousGTIDs event\n")
		// TODO: Is there an action we should take here?

	case event.IsFormatDescription():
		// This is a descriptor event that is written to the beginning of a binary log file, at position 4 (after
		// the 4 magic number bytes). For more details, see: https://mariadb.com/kb/en/format_description_event/
		fmt.Printf("Received: FormatDescription event\n")
		format, err = event.Format()
		if err != nil {
			return err
		}

	case event.IsGTID():
		// For global transaction ID, used to start a new transaction event group, instead of the old BEGIN query event,
		// and also to mark stand-alone (ddl). For more details, see: https://mariadb.com/kb/en/gtid_event/
		fmt.Printf("Received: GTID event\n")
		// TODO: Does this mean we should perform a commit?
		// TODO: Read MariaDB KB docs on GTID: https://mariadb.com/kb/en/gtid/
		gtid, isBegin, err := event.GTID(format)
		fmt.Printf(" - %v (isBegin: %t) \n", gtid, isBegin)
		if err != nil {
			return err
		}

	case event.IsTableMap():
		// Used for row-based binary logging beginning (binlog_format=ROW or MIXED). This event precedes each row
		// operation event and maps a table definition to a number, where the table definition consists of database
		// and table names. For more details, see: https://mariadb.com/kb/en/table_map_event/
		// TODO: Handle special Table ID value 0xFFFFFF:
		//		Table id refers to a table defined by TABLE_MAP_EVENT. The special value 0xFFFFFF should have
		//	 	"end of statement flag" (0x0001) set and indicates that table maps can be freed.
		fmt.Printf("Received: TableMap event\n")
		tableId := event.TableID(format)
		tableMap, err := event.TableMap(format)
		if err != nil {
			return err
		}
		tableMapsById[tableId] = tableMap
		fmt.Printf(" - tableMap: %v \n", formatTableMapAsString(tableId, tableMap))
		// TODO: Will these be resent before each row event, like the documentation seems to indicate? If so, that
		//       seems to remove the requirement to make this metadata durable between server restarts.

	case event.IsDeleteRows():
		// A ROWS_EVENT is written for row based replication if data is inserted, deleted or updated.
		// For more details, see: https://mariadb.com/kb/en/rows_event_v1v2-rows_compressed_event_v1/
		fmt.Printf("Received: DeleteRows event")
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

		fmt.Printf(" - Deleted Rows (table: %s)\n", tableMap.Name)
		for _, row := range rows.Rows {
			deletedRow, err := parseRow(tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			fmt.Printf("     - Identify: %v \n", sql.FormatRow(deletedRow))

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
		fmt.Printf("Received: WriteRows event\n")
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

		fmt.Printf(" - New Rows (table: %s)\n", tableMap.Name)
		for _, row := range rows.Rows {
			newRow, err := parseRow(tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			fmt.Printf("     - Data: %v \n", sql.FormatRow(newRow))

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
		fmt.Printf("Received: UpdateRows event\n")
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

		fmt.Printf(" - Updated Rows (table: %s)\n", tableMap.Name)
		for _, row := range rows.Rows {
			identifyRow, err := parseRow(tableMap, schema, rows.IdentifyColumns, row.NullIdentifyColumns, row.Identify)
			if err != nil {
				return err
			}
			updatedRow, err := parseRow(tableMap, schema, rows.DataColumns, row.NullColumns, row.Data)
			if err != nil {
				return err
			}
			fmt.Printf("     - Identify: %v Data: %v \n", sql.FormatRow(identifyRow), sql.FormatRow(updatedRow))

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
			fmt.Printf("Received: Heartbeat event\n")
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
			// TODO: Plug in correct type (just needs to show signed/unsigned; why doesn't typ show that?)
			var length int
			value, length, err = mysql.CellValue(data, pos, typ, tableMap.Metadata[i], query.Type_INT8)
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

// convertSqlTypesValues converts a sqltypes.Value instance (vitess) into a sql.Type value (go-mysql-server).
//
// TODO: This is currently a pretty hacky way to convert values and needs to be cleaned up so that it's more
//
//	efficient and handles all types.
func convertSqlTypesValue(value sqltypes.Value, column *sql.Column) (interface{}, error) {
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
func startReplicationEventStream(replicaConfiguration *replicaConfiguration, conn *mysql.Conn) error {
	sid, err := mysql.ParseSID(replicaConfiguration.sourceServerUuid)
	if err != nil {
		return err
	}
	gtid := mysql.Mysql56GTID{
		Server:   sid,
		Sequence: replicaConfiguration.startingGtid,
	}
	startPosition := mysql.Position{GTIDSet: gtid.GTIDSet()}
	// TODO: unhardcode 1 as the replica's server id
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
		fmt.Printf("!!!No current database selected, aborting query...\n")
		return
	}

	_, iter, err := engine.Query(ctx, query)
	if err != nil {
		fmt.Printf("!!! ERROR executing query: %v \n", err.Error())
		return
	}
	for {
		row, err := iter.Next(ctx)
		if err != nil {
			if err != io.EOF {
				fmt.Printf("!!! ERROR reading query results: %v \n", err.Error())
			}
			return
		}
		fmt.Printf("   row: %s \n", sql.FormatRow(row))
	}
}
