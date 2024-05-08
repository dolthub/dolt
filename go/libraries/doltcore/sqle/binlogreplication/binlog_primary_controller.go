// Copyright 2024 Dolthub, Inc.
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
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

const replicationBranch = "main"

// BinlogEnabled indicates whether binary logging is enabled or not. Similar to Dolt's other replication features,
// changes to binary logging are only applied at server startup.
//
// NOTE: By default, binary logging for Dolt is not enabled, which differs from MySQL's @@log_bin default. Dolt's
//
//	binary logging is initially an opt-in feature, but we may change that after measuring and tuning the
//	performance hit that binary logging adds.
var BinlogEnabled = false

type binlogStreamer struct {
	quitChan  chan struct{}
	eventChan chan []mysql.BinlogEvent
	ticker    *time.Ticker
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan:  make(chan struct{}),
		eventChan: make(chan []mysql.BinlogEvent),
		ticker:    time.NewTicker(30 * time.Second),
	}
}

// RemoteReplicaStreamer?
// RemoteReplicaManager?
type binlogStreamerManager struct {
	streamers    []*binlogStreamer
	quitChan     chan struct{}
	binlogStream *mysql.BinlogStream
	binlogFormat mysql.BinlogFormat
}

var _ doltdb.DatabaseUpdateListener = (*binlogStreamerManager)(nil)

// DatabaseCreated implements the doltdb.DatabaseUpdateListener interface
func (m *binlogStreamerManager) DatabaseCreated(ctx *sql.Context, databaseName string) error {
	// no-op if binary logging isn't turned on
	if !BinlogEnabled {
		return nil
	}

	var binlogEvents []mysql.BinlogEvent
	binlogEvent, err := m.createGtidEvent(ctx)
	if err != nil {
		return err
	}
	binlogEvents = append(binlogEvents, binlogEvent)

	createDatabaseStatement := fmt.Sprintf("create database `%s`;", databaseName)
	binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
		Database: databaseName,
		Charset:  nil, // TODO:
		SQL:      createDatabaseStatement,
		Options:  0,
		SqlMode:  0, // TODO:
	})
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()

	for _, streamer := range m.streamers {
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// DatabaseDropped implements the doltdb.DatabaseUpdateListener interface.
func (m *binlogStreamerManager) DatabaseDropped(ctx *sql.Context, databaseName string) error {
	// no-op if binary logging isn't turned on
	if !BinlogEnabled {
		return nil
	}

	var binlogEvents []mysql.BinlogEvent
	binlogEvent, err := m.createGtidEvent(ctx)
	if err != nil {
		return err
	}
	binlogEvents = append(binlogEvents, binlogEvent)

	dropDatabaseStatement := fmt.Sprintf("drop database `%s`;", databaseName)
	binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
		Database: databaseName,
		Charset:  nil, // TODO:
		SQL:      dropDatabaseStatement,
		Options:  0,
		SqlMode:  0, // TODO:
	})
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()

	for _, streamer := range m.streamers {
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// WorkingRootUpdated implements the DatabaseUpdateListener interface. When a transaction is committed, this function
// generates events for the binary log and sends them to all connected replicas.
//
// For a data update, the following events are generated
//
//	– GTID event
//	- Query event (BEGIN)
//	– TableMap event for each table updated
//	- DELETE_ROWS or WRITE_ROWS or UPDATE_ROWS event with the data changes
//	- XID event (ends the transaction; needed when Query BEGIN is present)
//
// For each schema update, the following events are generated
//   - GTID event
//   - Query event (with SQL statement for schema change)
//
// TODO: This function currently does all its work synchronously, in the same user thread as the transaction commit.
// We should split this out to a background routine to process, in order of the commits.
//
// TODO: This function currently sends the events to all connected replicas (through a channel). Eventually we need
// to change this so that it writes to a binary log file as the intermediate, and then the readers are watching
// that log to stream events back to the connected replicas.
func (m *binlogStreamerManager) WorkingRootUpdated(ctx *sql.Context, databaseName string, before *doltdb.RootValue, after *doltdb.RootValue) error {
	// no-op if binary logging isn't turned on
	if !BinlogEnabled {
		return nil
	}

	var binlogEvents []mysql.BinlogEvent
	tableDeltas, err := diff.GetTableDeltas(ctx, before, after)
	if err != nil {
		return err
	}

	// Process schema changes first
	binlogEvents, hasDataChanges, err := m.createSchemaChangeQueryEvents(ctx, databaseName, tableDeltas, after)
	if err != nil {
		return err
	}

	// Process data changes...
	if hasDataChanges {
		// GTID
		binlogEvent, err := m.createGtidEvent(ctx)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, binlogEvent)

		// Send a Query BEGIN event to start the new transaction
		binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
			Database: databaseName,
			Charset:  nil,
			SQL:      "BEGIN",
			Options:  0,
			SqlMode:  0, // TODO:
		})
		binlogEvents = append(binlogEvents, binlogEvent)
		m.binlogStream.LogPosition += binlogEvent.Length()

		// Create TableMap events describing the schemas of the tables being updated
		tableMapEvents, tablesToId, err := m.createTableMapEvents(ctx, databaseName, tableDeltas)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, tableMapEvents...)

		// Loop over the tableDeltas to pull out their diff contents
		rowEvents, err := m.createRowEvents(ctx, tableDeltas, tablesToId)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, rowEvents...)

		// Add an XID event to mark the transaction as completed
		binlogEvent = mysql.NewXIDEvent(m.binlogFormat, m.binlogStream)
		binlogEvents = append(binlogEvents, binlogEvent)
		m.binlogStream.LogPosition += binlogEvent.Length()
	}

	for _, streamer := range m.streamers {
		logrus.StandardLogger().Warnf("sending %d binlog events\n", len(binlogEvents))
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// createSchemaChangeQueryEvents processes the specified |tableDeltas| for the database |databaseName| at |newRoot| and returns
// a slice of binlog events that replicate any schema changes in the TableDeltas, as well as a boolean indicating if
// any TableDeltas were seen that contain data changes that need to be replicated.
func (m *binlogStreamerManager) createSchemaChangeQueryEvents(
	ctx *sql.Context, databaseName string, tableDeltas []diff.TableDelta, newRoot *doltdb.RootValue) (
	events []mysql.BinlogEvent, hasDataChanges bool, err error) {
	for _, tableDelta := range tableDeltas {
		isRename := tableDelta.IsRename()
		schemaChanged, err := tableDelta.HasSchemaChanged(ctx)
		if err != nil {
			return nil, false, err
		}

		b, err := tableDelta.HasDataChanged(ctx)
		if err != nil {
			return nil, false, err
		}
		if b && !tableDelta.IsDrop() {
			hasDataChanges = true
		}

		if !schemaChanged && !isRename {
			continue
		}

		schemaPatchStatements, err := sqle.GenerateSqlPatchSchemaStatements(ctx, newRoot, tableDelta)
		if err != nil {
			return nil, false, err
		}

		for _, schemaPatchStatement := range schemaPatchStatements {
			// Schema changes in MySQL are always in an implicit transaction, so before each one, we
			// send a new GTID event for the next transaction start
			binlogEvent, err := m.createGtidEvent(ctx)
			if err != nil {
				return nil, false, err
			}
			events = append(events, binlogEvent)

			binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
				Database: databaseName,
				Charset:  nil, // TODO:
				SQL:      schemaPatchStatement,
				Options:  0,
				SqlMode:  0, // TODO:
			})
			events = append(events, binlogEvent)
			m.binlogStream.LogPosition += binlogEvent.Length()
		}
	}

	return events, hasDataChanges, nil
}

// createTableMapEvents returns a slice of TableMap binlog events that describe the tables with data changes from
// |tableDeltas| in the database named |databaseName|. In addition to the binlog events, it also returns a map of
// table name to the table IDs used for that table in the generated TableMap events, so that callers can look up
// the correct table ID to use in Row events.
func (m *binlogStreamerManager) createTableMapEvents(ctx *sql.Context, databaseName string, tableDeltas []diff.TableDelta) (events []mysql.BinlogEvent, tablesToId map[string]uint64, err error) {
	tableId := uint64(02)
	tablesToId = make(map[string]uint64)
	for _, tableDelta := range tableDeltas {
		dataChanged, err := tableDelta.HasDataChanged(ctx)
		if err != nil {
			return nil, nil, err
		}

		// TODO: Make sure to not replicate ignored tables? Or do we want to replicate them and
		//       just exclude them from Dolt commits?

		if !dataChanged || tableDelta.IsDrop() {
			continue
		}

		// For every table with data changes, we need to send a TableMap event over the stream.
		tableId++
		tableName := tableDelta.ToName
		if tableName == "" {
			tableName = tableDelta.FromName
		}
		tablesToId[tableName] = tableId
		tableMap, err := createTableMapFromDoltTable(ctx, databaseName, tableName, tableDelta.ToTable)
		if err != nil {
			return nil, nil, err
		}

		binlogEvent := mysql.NewTableMapEvent(m.binlogFormat, m.binlogStream, tableId, tableMap)
		events = append(events, binlogEvent)
		m.binlogStream.LogPosition += binlogEvent.Length()
	}

	return events, tablesToId, nil
}

// createRowEvents returns a slice of binlog Insert/Update/Delete row events that represent the data changes
// present in the specified |tableDeltas|. The |tablesToId| map contains the mapping of table names to IDs used
// in the associated TableMap events describing the table schemas.
func (m *binlogStreamerManager) createRowEvents(ctx *sql.Context, tableDeltas []diff.TableDelta, tablesToId map[string]uint64) (events []mysql.BinlogEvent, err error) {
	for _, tableDelta := range tableDeltas {
		// If a table has been dropped, we don't need to process its data updates, since the DROP TABLE
		// DDL statement we send will automatically delete all the data.
		if tableDelta.IsDrop() {
			continue
		}

		fromRowData, toRowData, err := tableDelta.GetRowData(ctx)
		if err != nil {
			return nil, err
		}

		tableName := tableDelta.ToName
		if tableName == "" {
			tableName = tableDelta.FromName
		}

		fromMap := durable.ProllyMapFromIndex(fromRowData)
		toMap := durable.ProllyMapFromIndex(toRowData)

		sch, err := tableDelta.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		columns := sch.GetAllCols().GetColumns()
		tableId := tablesToId[tableName]

		var tableRowsToWrite []mysql.Row
		var tableRowsToDelete []mysql.Row
		var tableRowsToUpdate []mysql.Row

		err = prolly.DiffMaps(ctx, fromMap, toMap, false, func(_ context.Context, diff tree.Diff) error {
			// Keyless tables encode their fields differently than tables with primary keys, notably, they
			// include an extra field indicating how many duplicate rows they represent, so we need to
			// extract that information here before we can serialize these changes to the binlog.
			rowCount, diffType, err := extractRowCountAndDiffType(sch, diff)
			if err != nil {
				return err
			}

			switch diffType {
			case tree.AddedDiff:
				data, nullBitmap, err := serializeRowToBinlogBytes(ctx,
					sch, diff.Key, diff.To, tableDelta.ToTable.NodeStore())
				if err != nil {
					return err
				}
				for range rowCount {
					tableRowsToWrite = append(tableRowsToWrite, mysql.Row{
						NullColumns: nullBitmap,
						Data:        data,
					})
				}

			case tree.ModifiedDiff:
				data, nullColumns, err := serializeRowToBinlogBytes(ctx,
					sch, diff.Key, diff.To, tableDelta.ToTable.NodeStore())
				if err != nil {
					return err
				}
				identify, nullIdentifyColumns, err := serializeRowToBinlogBytes(ctx,
					sch, diff.Key, diff.From, tableDelta.FromTable.NodeStore())
				if err != nil {
					return err
				}
				for range rowCount {
					tableRowsToUpdate = append(tableRowsToUpdate, mysql.Row{
						NullColumns:         nullColumns,
						Data:                data,
						NullIdentifyColumns: nullIdentifyColumns,
						Identify:            identify,
					})
				}

			case tree.RemovedDiff:
				// TODO: If the schema of the table has changed between FromTable and ToTable, then this probably breaks
				identifyData, nullBitmap, err := serializeRowToBinlogBytes(ctx,
					sch, diff.Key, diff.From, tableDelta.FromTable.NodeStore())
				if err != nil {
					return err
				}
				for range rowCount {
					tableRowsToDelete = append(tableRowsToDelete, mysql.Row{
						NullIdentifyColumns: nullBitmap,
						Identify:            identifyData,
					})
				}

			default:
				return fmt.Errorf("unexpected diff type: %v", diff.Type)
			}

			return nil
		})
		if err != nil && err != io.EOF {
			return nil, err
		}

		if tableRowsToWrite != nil {
			rows := mysql.Rows{
				DataColumns: mysql.NewServerBitmap(len(columns)),
				Rows:        tableRowsToWrite,
			}
			// All columns are included
			for i := 0; i < len(columns); i++ {
				rows.DataColumns.Set(i, true)
			}

			if tableRowsToDelete == nil && tableRowsToUpdate == nil {
				rows.Flags |= 0x0001 // End of Statement
			}

			binlogEvent := mysql.NewWriteRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
			events = append(events, binlogEvent)
			m.binlogStream.LogPosition += binlogEvent.Length()
		}

		// TODO: Ordering – Should we execute all deletes first? Or updates, deletes, then inserts?
		//       A delete would never delete a row inserted or updated in the same transaction, so it seems like processing those first makes sense
		if tableRowsToDelete != nil {
			rows := mysql.Rows{
				IdentifyColumns: mysql.NewServerBitmap(len(columns)),
				Rows:            tableRowsToDelete,
			}
			// All identity columns are included
			for i := 0; i < len(columns); i++ {
				rows.IdentifyColumns.Set(i, true)
			}

			if tableRowsToUpdate == nil {
				rows.Flags |= 0x0001 // End of Statement
			}

			binlogEvent := mysql.NewDeleteRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
			events = append(events, binlogEvent)
			m.binlogStream.LogPosition += binlogEvent.Length()
		}

		if tableRowsToUpdate != nil {
			rows := mysql.Rows{
				Flags:           0x0001,
				DataColumns:     mysql.NewServerBitmap(len(columns)),
				IdentifyColumns: mysql.NewServerBitmap(len(columns)),
				Rows:            tableRowsToUpdate,
			}
			// All columns are included for data and identify fields
			for i := 0; i < len(columns); i++ {
				rows.DataColumns.Set(i, true)
			}
			for i := 0; i < len(columns); i++ {
				rows.IdentifyColumns.Set(i, true)
			}

			binlogEvent := mysql.NewUpdateRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
			events = append(events, binlogEvent)
			m.binlogStream.LogPosition += binlogEvent.Length()
		}
	}

	return events, nil
}

// createGtidEvent creates a new GTID event for the current transaction and updates the stream's
// current log position.
func (m *binlogStreamerManager) createGtidEvent(ctx *sql.Context) (mysql.BinlogEvent, error) {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return nil, err
	}
	sid, err := mysql.ParseSID(serverUuid)
	if err != nil {
		return nil, err
	}
	gtid := mysql.Mysql56GTID{Server: sid, Sequence: gtidSequence}
	binlogEvent := mysql.NewMySQLGTIDEvent(m.binlogFormat, m.binlogStream, gtid, false)
	m.binlogStream.LogPosition += binlogEvent.Length()
	gtidSequence++

	return binlogEvent, nil
}

// extractRowCountAndDiffType uses |sch| and |diff| to determine how many changed rows this
// diff represents (returned as the |rowCount| param) and what type of change it represents
// (returned as the |diffType| param). For tables with primary keys, this function will always
// return a |rowCount| of 1 and a |diffType| directly from |diff.Type|, however, for keyless
// tables, there is a translation needed due to how keyless tables encode the number of
// duplicate rows in the table that they represent. For example, a |diff| may show that a
// row was updated, but if the rowCount for the keyless row in diff.To is 4 and the rowCount
// for the keyless row in diff.From is 2, then it is translated to a deletion of 2 rows, by
// returning a |rowCount| of 2 and a |diffType| of tree.RemovedDiff.
func extractRowCountAndDiffType(sch schema.Schema, diff tree.Diff) (rowCount uint64, diffType tree.DiffType, err error) {
	// For tables with primary keys, we don't have to worry about duplicate rows or
	// translating the diff type, so just return immediately
	if schema.IsKeyless(sch) == false {
		return 1, diff.Type, nil
	}

	switch diff.Type {
	case tree.AddedDiff:
		toRowCount, notNull := sch.GetValueDescriptor().GetUint64(0, val.Tuple(diff.To))
		if !notNull {
			return 0, 0, fmt.Errorf(
				"row count for a keyless table row cannot be null")
		}
		return toRowCount, diff.Type, nil

	case tree.RemovedDiff:
		fromRowCount, notNull := sch.GetValueDescriptor().GetUint64(0, val.Tuple(diff.From))
		if !notNull {
			return 0, 0, fmt.Errorf(
				"row count for a keyless table row cannot be null")
		}
		return fromRowCount, diff.Type, nil

	case tree.ModifiedDiff:
		toRowCount, notNull := sch.GetValueDescriptor().GetUint64(0, val.Tuple(diff.To))
		if !notNull {
			return 0, 0, fmt.Errorf(
				"row count for a keyless table row cannot be null")
		}

		fromRowCount, notNull := sch.GetValueDescriptor().GetUint64(0, val.Tuple(diff.From))
		if !notNull {
			return 0, 0, fmt.Errorf(
				"row count for a keyless table row cannot be null")
		}

		if toRowCount > fromRowCount {
			return toRowCount - fromRowCount, tree.AddedDiff, nil
		} else if toRowCount < fromRowCount {
			return fromRowCount - toRowCount, tree.RemovedDiff, nil
		} else {
			// For keyless tables, we will never see a diff where the rowcount is 1 on both the from and
			// to tuples, because there is no primary key to identify the before and after row as having
			// the same identity, so this case is always represented as one row added, one row removed.
			return 0, 0, fmt.Errorf(
				"row count for a modified row diff cannot be the same on both sides of the diff")
		}

	default:
		return 0, 0, fmt.Errorf("unexpected DiffType: %v", diff.Type)
	}
}

// createTableMapFromDoltTable creates a binlog TableMap for the given Dolt table.
func createTableMapFromDoltTable(ctx *sql.Context, databaseName, tableName string, table *doltdb.Table) (*mysql.TableMap, error) {
	schema, err := table.GetSchema(ctx)
	if err != nil {
		return nil, err
	}

	columns := schema.GetAllCols().GetColumns()
	types := make([]byte, len(columns))
	metadata := make([]uint16, len(columns))
	canBeNullMap := mysql.NewServerBitmap(len(columns))

	for i, col := range columns {
		metadata[i] = 0
		typ := col.TypeInfo.ToSqlType()

		serializer, ok := typeSerializersMap[typ.Type()]
		if !ok {
			return nil, fmt.Errorf(
				"unsupported type for binlog replication: %v \n", typ.String())
		}
		types[i], metadata[i] = serializer.metadata(ctx, typ)

		if col.IsNullable() {
			canBeNullMap.Set(i, true)
		}
	}

	return &mysql.TableMap{
		Flags:     0x0001, // TODO: hardcoding to end of statement
		Database:  databaseName,
		Name:      tableName,
		Types:     types,
		CanBeNull: canBeNullMap,
		Metadata:  metadata,
	}, nil
}

// gtidSequence represents the current sequence number for the global transaction identifier (GTID).
// TODO: This needs locking obviously, and needs to be moved to a different package, and needs to be encapsulated.
var gtidSequence int64 = 1

var doltBinlogStreamerManager = newBinlogStreamerManager()

// NewBinlogStreamerManager creates a new binlogStreamerManager instance.
func newBinlogStreamerManager() *binlogStreamerManager {
	binlogFormat := createBinlogFormat()
	binlogStream, err := createBinlogStream()
	if err != nil {
		// TODO: Change this to log an error message, and say that we weren't able to start replication
		//       because of this error. Make the error message say that the value needs to be set persistently,
		//       and then the server needs to be restarted before replication will work. We can always
		//       make that better later, but at least it'll work and will be consistent with dolt replication.
		panic(err.Error())
	}

	manager := &binlogStreamerManager{
		streamers:    make([]*binlogStreamer, 0),
		quitChan:     make(chan struct{}),
		binlogFormat: binlogFormat,
		binlogStream: binlogStream,
	}

	doltdb.RegisterDatabaseUpdateListener(manager)

	go func() {
		for {
			select {
			case <-manager.quitChan:
				// TODO: Since we just have one channel now... might be easier to just use an atomic var
				for _, streamer := range manager.streamers {
					streamer.quitChan <- struct{}{}
				}
				return
			}
		}
	}()

	return manager
}

func (m *binlogStreamerManager) StartNewStreamer(ctx *sql.Context, conn *mysql.Conn) error {
	streamer := newBinlogStreamer()
	m.streamers = append(m.streamers, streamer)

	if err := sendInitialEvents(ctx, conn, m.binlogFormat, m.binlogStream); err != nil {
		return err
	}

	for {
		logrus.StandardLogger().Warn("streamer is listening for messages")

		select {
		case <-streamer.quitChan:
			logrus.StandardLogger().Warn("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			if conn.IsClosed() {
				logrus.StandardLogger().Warn("connection is closed! can't send heartbeat")
			} else {
				logrus.StandardLogger().Warn("sending heartbeat")
				if err := sendHeartbeat(conn, m.binlogFormat, m.binlogStream); err != nil {
					return err
				}
				if err := conn.FlushBuffer(); err != nil {
					return fmt.Errorf("unable to flush connection: %s", err.Error())
				}
			}

		case events := <-streamer.eventChan:
			logrus.StandardLogger().Warn("received message from streamer's event channel")
			logrus.StandardLogger().Warnf("sending %d binlog events", len(events))

			// TODO: need to gracefully handle connection closed errors
			for _, event := range events {
				if err := conn.WriteBinlogEvent(event, false); err != nil {
					return err
				}
			}

			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush connection: %s", err.Error())
			}
		}
	}
}

var DoltBinlogPrimaryController = newDoltBinlogPrimaryController()

type registeredReplica struct {
	connectionId uint32
	host         string
	port         uint16
}

// newDoltBinlogPrimaryController creates a new doltBinlogPrimaryController instance.
func newDoltBinlogPrimaryController() *doltBinlogPrimaryController {
	controller := doltBinlogPrimaryController{
		registeredReplicas: make([]*registeredReplica, 0),
	}
	return &controller
}

type doltBinlogPrimaryController struct {
	registeredReplicas []*registeredReplica
}

var _ binlogreplication.BinlogPrimaryController = (*doltBinlogPrimaryController)(nil)

// RegisterReplica implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) RegisterReplica(ctx *sql.Context, c *mysql.Conn, replicaHost string, replicaPort uint16) error {
	// TODO: Do we actually need the connection here? Doesn't seem like it...
	// TODO: Obviously need locking on the datastructure, but just getting something stubbed out
	d.registeredReplicas = append(d.registeredReplicas, &registeredReplica{
		connectionId: c.ConnectionID,
		host:         replicaHost,
		port:         replicaPort,
	})

	return nil
}

// BinlogDumpGtid implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) BinlogDumpGtid(ctx *sql.Context, conn *mysql.Conn, gtidSet mysql.GTIDSet) error {
	return doltBinlogStreamerManager.StartNewStreamer(ctx, conn)
}

// ListReplicas implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) ListReplicas(ctx *sql.Context) error {
	return fmt.Errorf("DOLT: ListReplicas not implemented yet")
}

// ListBinaryLogs implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) ListBinaryLogs(ctx *sql.Context) error {
	return fmt.Errorf("DOLT: ListBinaryLogs not implemented yet")
}

// GetBinaryLogStatus implements the BinlogPrimaryController interface.
func (d doltBinlogPrimaryController) GetBinaryLogStatus(ctx *sql.Context) ([]binlogreplication.BinaryLogStatus, error) {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return nil, err
	}

	// TODO: This data is just stubbed out; need to fill in the correct GTID info
	return []binlogreplication.BinaryLogStatus{{
		File:          binlogFilename,
		Position:      uint(doltBinlogStreamerManager.binlogStream.LogPosition),
		DoDbs:         "",
		IgnoreDbs:     "",
		ExecutedGtids: serverUuid + ":1-3",
	}}, nil
}

// createBinlogFormat returns a new BinlogFormat that describes the format of this binlog stream, which will always
// be a MySQL 5.6+ compatible binlog format.
func createBinlogFormat() mysql.BinlogFormat {
	binlogFormat := mysql.NewMySQL56BinlogFormat()

	_, value, ok := sql.SystemVariables.GetGlobal("binlog_checksum")
	if !ok {
		panic("unable to read binlog_checksum system variable")
	}

	switch value {
	case "NONE":
		binlogFormat.ChecksumAlgorithm = mysql.BinlogChecksumAlgOff
	case "CRC32":
		binlogFormat.ChecksumAlgorithm = mysql.BinlogChecksumAlgCRC32
	default:
		panic(fmt.Sprintf("unsupported binlog_checksum value: %v", value))
	}

	return binlogFormat
}

// createBinlogStream returns a new BinlogStream instance, configured with this server's @@server_id, a zero value for
// the log position, and the current time for the timestamp. If any errors are encountered while loading @@server_id,
// this function will return an error.
func createBinlogStream() (*mysql.BinlogStream, error) {
	serverId, err := getServerId()
	if err != nil {
		return nil, err
	}

	return &mysql.BinlogStream{
		ServerID:    serverId,
		LogPosition: 0,
		Timestamp:   uint32(time.Now().Unix()),
	}, nil
}

// sendInitialEvents sends the initial binlog events (i.e. Rotate, FormatDescription) over a newly established binlog
// streaming connection.
func sendInitialEvents(_ *sql.Context, conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	err := sendRotateEvent(conn, binlogFormat, binlogStream)
	if err != nil {
		return err
	}

	err = sendFormatDescription(conn, binlogFormat, binlogStream)
	if err != nil {
		return err
	}

	return conn.FlushBuffer()
}

const binlogFilename = "binlog.000001"

func sendRotateEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogFilePosition := uint64(0)
	// TODO: why does vitess define binlogStream.LotPosition as a uint32? We should probably just change that.
	binlogStream.LogPosition = uint32(binlogFilePosition)

	binlogEvent := mysql.NewRotateEvent(binlogFormat, binlogStream, binlogFilePosition, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendFormatDescription(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogEvent := mysql.NewFormatDescriptionEvent(binlogFormat, binlogStream)
	binlogStream.LogPosition += binlogEvent.Length()
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogStream.Timestamp = uint32(0) // Timestamp needs to be zero for a heartbeat event
	logrus.StandardLogger().Warnf("sending heartbeat with log position: %v", binlogStream.LogPosition)

	binlogEvent := mysql.NewHeartbeatEventWithLogFile(binlogFormat, binlogStream, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}
