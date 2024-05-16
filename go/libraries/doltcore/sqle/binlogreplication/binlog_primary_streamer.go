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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
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

// gtidSequence represents the current sequence number for the global transaction identifier (GTID).
// TODO: This needs locking obviously, and needs to be moved to a different package, and needs to be encapsulated.
// TODO: If we broke out the binlog event factory, then we could have it maintain this?
var gtidSequence int64 = 1

// TODO: We could avoid this being a global var by grabbing it from
//
//	the DoltBinlogPrimaryController var
var doltBinlogStreamerManager = newBinlogStreamerManager()

// binlogStreamer is responsible for receiving binlog events over its eventChan
// channel, and streaming those out to a connected replica over a MySQL connection.
// It also sends heartbeat events to the replica over the same connection at
// regular intervals. There is one streamer per connected replica.
type binlogStreamer struct {
	quitChan  chan struct{}
	eventChan chan []mysql.BinlogEvent
	ticker    *time.Ticker
}

// NewBinlogStreamer creates a new binlogStreamer instance.
func newBinlogStreamer() *binlogStreamer {
	return &binlogStreamer{
		quitChan:  make(chan struct{}),
		eventChan: make(chan []mysql.BinlogEvent, 5),
		ticker:    time.NewTicker(30 * time.Second),
	}
}

// streamEvents listens for new binlog events sent to this streamer over its binlog even
// channel and sends them over |conn|. It also listens for ticker ticks to send hearbeats
// over |conn|. The specified |binlogFormat| is used to define the format of binlog events
// and |binlogStream| records the position of the stream. This method blocks until an error
// is received over the stream (e.g. the connection closing) or the streamer is closed,
// through it's quit channel.
func (streamer *binlogStreamer) streamEvents(ctx *sql.Context, conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	if err := sendInitialEvents(ctx, conn, binlogFormat, binlogStream); err != nil {
		return err
	}

	for {
		logrus.StandardLogger().Trace("streamer is listening for messages")

		select {
		case <-streamer.quitChan:
			logrus.StandardLogger().Trace("received message from streamer's quit channel")
			streamer.ticker.Stop()
			return nil

		case <-streamer.ticker.C:
			logrus.StandardLogger().Trace("sending heartbeat")
			if err := sendHeartbeat(conn, binlogFormat, binlogStream); err != nil {
				return err
			}
			if err := conn.FlushBuffer(); err != nil {
				return fmt.Errorf("unable to flush connection: %s", err.Error())
			}

		case events := <-streamer.eventChan:
			logrus.StandardLogger().Tracef("streaming %d binlog events", len(events))
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

// binlogStreamerManager manages a collection of binlogStreamers, one for reach connected replica,
// and implements the doltdb.DatabaseUpdateListener interface to receive notifications of database
// changes that need to be turned into binlog events and then sent to connected replicas.
type binlogStreamerManager struct {
	streamers      []*binlogStreamer
	streamersMutex sync.Mutex
	quitChan       chan struct{}
	binlogStream   *mysql.BinlogStream
	binlogFormat   mysql.BinlogFormat
	gtidPosition   *mysql.Position
}

var _ doltdb.DatabaseUpdateListener = (*binlogStreamerManager)(nil)

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
		streamers:      make([]*binlogStreamer, 0),
		streamersMutex: sync.Mutex{},
		quitChan:       make(chan struct{}),
		binlogFormat:   binlogFormat,
		binlogStream:   binlogStream,
	}

	doltdb.RegisterDatabaseUpdateListener(manager)

	go func() {
		for {
			select {
			case <-manager.quitChan:
				// TODO: Since we just have one channel now... might be easier to just use an atomic var
				streamers := manager.copyStreamers()
				for _, streamer := range streamers {
					streamer.quitChan <- struct{}{}
				}
				return
			}
		}
	}()

	return manager
}

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
	// TODO: Charset and SQL_MODE support
	binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
		Database: databaseName,
		Charset:  nil,
		SQL:      createDatabaseStatement,
		Options:  0,
		SqlMode:  0,
	})
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()

	streamers := m.copyStreamers()
	for _, streamer := range streamers {
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
	// TODO: Charset and SQL_MODE support
	binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
		Database: databaseName,
		Charset:  nil,
		SQL:      dropDatabaseStatement,
		Options:  0,
		SqlMode:  0,
	})
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()

	for _, streamer := range m.copyStreamers() {
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// WorkingRootUpdated implements the DatabaseUpdateListener interface. When a working root changes, this function
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
func (m *binlogStreamerManager) WorkingRootUpdated(ctx *sql.Context, databaseName string, before doltdb.RootValue, after doltdb.RootValue) error {
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
		// TODO: Charset and SQL_MODE support
		binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
			Database: databaseName,
			Charset:  nil,
			SQL:      "BEGIN",
			Options:  0,
			SqlMode:  0,
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

	streamers := m.copyStreamers()
	for _, streamer := range streamers {
		logrus.StandardLogger().Tracef("queuing %d binlog events\n", len(binlogEvents))
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// copyStreamers returns a copy of the streamers owned by this streamer manager.
func (m *binlogStreamerManager) copyStreamers() []*binlogStreamer {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	results := make([]*binlogStreamer, len(m.streamers))
	copy(results, m.streamers)
	return results
}

// createSchemaChangeQueryEvents processes the specified |tableDeltas| for the database |databaseName| at |newRoot| and returns
// a slice of binlog events that replicate any schema changes in the TableDeltas, as well as a boolean indicating if
// any TableDeltas were seen that contain data changes that need to be replicated.
func (m *binlogStreamerManager) createSchemaChangeQueryEvents(
	ctx *sql.Context, databaseName string, tableDeltas []diff.TableDelta, newRoot doltdb.RootValue) (
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

			// TODO: Charset and SQL_MODE support
			binlogEvent = mysql.NewQueryEvent(m.binlogFormat, m.binlogStream, mysql.Query{
				Database: databaseName,
				Charset:  nil,
				SQL:      schemaPatchStatement,
				Options:  0,
				SqlMode:  0,
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

// initializeGtidPosition loads the persisted GTID position from disk and initializes it
// in this binlogStreamerManager instance. If the gtidPosition has already been loaded
// from disk and initialized, this method simply returns. If any problems were encountered
// loading the GTID position from disk, or parsing its value, an error is returned.
func (m *binlogStreamerManager) initializeGtidPosition(ctx *sql.Context) error {
	if m.gtidPosition != nil {
		return nil
	}

	position, err := positionStore.Load(ctx)
	if err != nil {
		return err
	}

	// If there is no position stored on disk, then initialize to an empty GTID set
	if position == nil || position.IsZero() || position.GTIDSet.String() == "" {
		m.gtidPosition = &mysql.Position{
			GTIDSet: mysql.Mysql56GTIDSet{},
		}
		return nil
	}

	// Otherwise, interpret the value loaded from disk and set the GTID position
	// Unfortunately, the GTIDSet API from Vitess doesn't provide a good way to directly
	// access the GTID value, so we have to resort to string parsing.
	m.gtidPosition = position
	gtidString := position.GTIDSet.String()
	if strings.Contains(gtidString, ",") {
		return fmt.Errorf("unexpected GTID format: %s", gtidString)
	}
	gtidComponents := strings.Split(gtidString, ":")
	if len(gtidComponents) != 2 {
		return fmt.Errorf("unexpected GTID format: %s", gtidString)
	}
	sequenceComponents := strings.Split(gtidComponents[1], "-")
	var gtidSequenceString string
	switch len(sequenceComponents) {
	case 1:
		gtidSequenceString = sequenceComponents[0]
	case 2:
		gtidSequenceString = sequenceComponents[1]
	default:
		return fmt.Errorf("unexpected GTID format: %s", gtidString)
	}

	i, err := strconv.Atoi(gtidSequenceString)
	if err != nil {
		return fmt.Errorf("unable to parse GTID position (%s): %s", gtidString, err.Error())
	}
	gtidSequence = int64(i + 1)
	return nil
}

// createGtidEvent creates a new GTID event for the current transaction and updates the stream's
// current log position.
func (m *binlogStreamerManager) createGtidEvent(ctx *sql.Context) (mysql.BinlogEvent, error) {
	err := m.initializeGtidPosition(ctx)
	if err != nil {
		return nil, err
	}

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

	// Store the latest executed GTID to disk
	m.gtidPosition.GTIDSet = m.gtidPosition.GTIDSet.AddGTID(gtid)
	err = positionStore.Save(ctx, m.gtidPosition)
	if err != nil {
		return nil, fmt.Errorf("unable to store GTID executed metadata to disk: %s", err.Error())
	}

	return binlogEvent, nil
}

// StreamEvents starts a new binlogStreamer and streams events over |conn| until the connection
// is closed, the streamer is sent a quit signal over its quit channel, or the streamer receives
// errors while sending events over the connection. Note that this method blocks until the
// streamer exits.
func (m *binlogStreamerManager) StreamEvents(ctx *sql.Context, conn *mysql.Conn) error {
	streamer := newBinlogStreamer()
	m.addStreamer(streamer)
	defer m.removeStreamer(streamer)

	return streamer.streamEvents(ctx, conn, m.binlogFormat, m.binlogStream)
}

// addStreamer adds |streamer| to the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) addStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = append(m.streamers, streamer)
}

// removeStreamer removes |streamer| from the slice of streamers managed by this binlogStreamerManager.
func (m *binlogStreamerManager) removeStreamer(streamer *binlogStreamer) {
	m.streamersMutex.Lock()
	defer m.streamersMutex.Unlock()

	m.streamers = make([]*binlogStreamer, len(m.streamers)-1, 0)
	for _, element := range m.streamers {
		if element != streamer {
			m.streamers = append(m.streamers, element)
		}
	}
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
		Flags:     0x0000,
		Database:  databaseName,
		Name:      tableName,
		Types:     types,
		CanBeNull: canBeNullMap,
		Metadata:  metadata,
	}, nil
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogStream.Timestamp = uint32(0) // Timestamp needs to be zero for a heartbeat event
	logrus.WithField("log_position", binlogStream.LogPosition).Tracef("sending heartbeat")

	binlogEvent := mysql.NewHeartbeatEventWithLogFile(binlogFormat, binlogStream, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
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

func sendRotateEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogFilePosition := uint64(0)
	// TODO: why does vitess define binlogStream.LogPosition as a uint32? We should probably just change that.
	binlogStream.LogPosition = uint32(binlogFilePosition)

	binlogEvent := mysql.NewRotateEvent(binlogFormat, binlogStream, binlogFilePosition, binlogFilename)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendFormatDescription(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogEvent := mysql.NewFormatDescriptionEvent(binlogFormat, binlogStream)
	binlogStream.LogPosition += binlogEvent.Length()
	return conn.WriteBinlogEvent(binlogEvent, false)
}