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
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlfmt"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// BinlogBranch specifies the branch used for generating binlog events.
var BinlogBranch = "main"

// binlogProducer implements the doltdb.DatabaseUpdateListener interface so that it can listen for updates to Dolt
// databases and generate binlog events describing them. Those binlog events are sent to the binlogStreamerManager,
// which is responsible for delivering them to each connected replica.
//
// Note that the initial version of binlogProducer currently delivers the generated binlog events directly to the
// connected replicas, without actually writing them to a real binlog file on disk.
type binlogProducer struct {
	binlogFormat    *mysql.BinlogFormat
	binlogEventMeta mysql.BinlogEventMetadata

	mu *sync.Mutex

	gtidPosition *mysql.Position
	gtidSequence int64

	logManager *logManager
}

var _ doltdb.DatabaseUpdateListener = (*binlogProducer)(nil)

// NewBinlogProducer creates and returns a new instance of BinlogProducer. Note that callers must register the
// returned binlogProducer as a DatabaseUpdateListener before it will start receiving database updates and start
// producing binlog events.
func NewBinlogProducer(fs filesys.Filesys) (*binlogProducer, error) {
	binlogFormat := createBinlogFormat()
	binlogEventMeta, err := createBinlogEventMetadata()
	if err != nil {
		return nil, err
	}

	b := &binlogProducer{
		binlogEventMeta: *binlogEventMeta,
		binlogFormat:    binlogFormat,
		mu:              &sync.Mutex{},
	}

	if err = b.initializeGtidPosition(fs); err != nil {
		return nil, err
	}

	return b, nil
}

// LogManager sets the |logManager| this producer will send events to.
func (b *binlogProducer) LogManager(logManager *logManager) {
	b.logManager = logManager
}

// WorkingRootUpdated implements the doltdb.DatabaseUpdateListener interface. When a working root changes,
// this function generates events for the binary log and sends them to all connected replicas.
//
// This function currently sends the events to all connected replicas as the events are produced. Eventually we
// need to change this so that it writes to a binary log file as the intermediate, and the readers watch that
// log to stream events back to the connected replicas.
func (b *binlogProducer) WorkingRootUpdated(ctx *sql.Context, databaseName string, branchName string, before doltdb.RootValue, after doltdb.RootValue) error {
	// We only support updates to a single branch for binlog events, so ignore all other updates
	if branchName != BinlogBranch {
		return nil
	}

	var binlogEvents []mysql.BinlogEvent
	tableDeltas, err := diff.GetTableDeltas(ctx, before, after)
	if err != nil {
		return err
	}

	// Process schema changes first
	binlogEvents, hasDataChanges, err := b.createSchemaChangeQueryEvents(ctx, databaseName, tableDeltas, after)
	if err != nil {
		return err
	}

	// Process data changes...
	if hasDataChanges {
		// GTID
		binlogEvent, err := b.createGtidEvent(ctx)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, binlogEvent)

		// Send a Query BEGIN event to start the new transaction
		binlogEvents = append(binlogEvents, b.newQueryEvent(databaseName, "BEGIN"))

		// Create TableMap events describing the schemas of the tables being updated
		tableMapEvents, tablesToId, err := b.createTableMapEvents(ctx, databaseName, tableDeltas)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, tableMapEvents...)

		// Loop over the tableDeltas to pull out their diff contents
		rowEvents, err := b.createRowEvents(ctx, tableDeltas, tablesToId)
		if err != nil {
			return err
		}
		binlogEvents = append(binlogEvents, rowEvents...)

		// Add an XID event to mark the transaction as completed
		binlogEvents = append(binlogEvents, b.newXIDEvent())
	}

	return b.logManager.WriteEvents(binlogEvents...)
}

// DatabaseCreated implements the doltdb.DatabaseUpdateListener interface.
func (b *binlogProducer) DatabaseCreated(ctx *sql.Context, databaseName string) error {
	// TODO: All of these need to be sequentially processed by a single goroutine, so that we can ensure the GTID
	//       assignment happens sequentially and safely. Also... if a database is created, we need to process that
	//       update before any data updates to the database itself. Seems like that race could happen otherwise?

	var binlogEvents []mysql.BinlogEvent
	binlogEvent, err := b.createGtidEvent(ctx)
	if err != nil {
		return err
	}
	binlogEvents = append(binlogEvents, binlogEvent)

	createDatabaseStatement := fmt.Sprintf("create database `%s`;", databaseName)
	binlogEvents = append(binlogEvents, b.newQueryEvent(databaseName, createDatabaseStatement))

	return b.logManager.WriteEvents(binlogEvents...)
}

// DatabaseDropped implements the doltdb.DatabaseUpdateListener interface.
func (b *binlogProducer) DatabaseDropped(ctx *sql.Context, databaseName string) error {
	var binlogEvents []mysql.BinlogEvent
	binlogEvent, err := b.createGtidEvent(ctx)
	if err != nil {
		return err
	}
	binlogEvents = append(binlogEvents, binlogEvent)

	dropDatabaseStatement := fmt.Sprintf("drop database `%s`;", databaseName)
	binlogEvents = append(binlogEvents, b.newQueryEvent(databaseName, dropDatabaseStatement))

	return b.logManager.WriteEvents(binlogEvents...)
}

// initializeGtidPosition loads the persisted GTID position from disk and initializes it
// in this binlogStreamerManager instance. If the gtidPosition has already been loaded
// from disk and initialized, this method simply returns. If any problems were encountered
// loading the GTID position from disk, or parsing its value, an error is returned.
func (b *binlogProducer) initializeGtidPosition(fs filesys.Filesys) error {
	if b.gtidPosition != nil {
		return nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	position, err := positionStore.Load(fs)
	if err != nil {
		return err
	}

	// If there is no position stored on disk, then initialize to an empty GTID set
	if position == nil || position.IsZero() || position.GTIDSet.String() == "" {
		b.gtidPosition = &mysql.Position{
			GTIDSet: mysql.Mysql56GTIDSet{},
		}
		b.gtidSequence = int64(1)
		return nil
	}

	// Otherwise, interpret the value loaded from disk and set the GTID position
	// Unfortunately, the GTIDSet API from Vitess doesn't provide a good way to directly
	// access the GTID value, so we have to resort to string parsing.
	b.gtidPosition = position
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
	b.gtidSequence = int64(i + 1)

	logrus.Tracef("setting @@gtid_executed to %s", b.gtidPosition.GTIDSet.String())
	return sql.SystemVariables.AssignValues(map[string]any{
		"gtid_executed": b.gtidPosition.GTIDSet.String()})
}

// createGtidEvent creates a new GTID event for the current transaction and updates the stream's
// current log position.
func (b *binlogProducer) createGtidEvent(ctx *sql.Context) (mysql.BinlogEvent, error) {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return nil, err
	}
	sid, err := mysql.ParseSID(serverUuid)
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	gtid := mysql.Mysql56GTID{Server: sid, Sequence: b.gtidSequence}
	binlogEvent := mysql.NewMySQLGTIDEvent(*b.binlogFormat, b.binlogEventMeta, gtid, false)
	b.gtidSequence++

	// Store the latest executed GTID to disk
	b.gtidPosition.GTIDSet = b.gtidPosition.GTIDSet.AddGTID(gtid)
	err = positionStore.Save(ctx, b.gtidPosition)
	if err != nil {
		return nil, fmt.Errorf("unable to store GTID executed metadata to disk: %s", err.Error())
	}

	err = sql.SystemVariables.AssignValues(map[string]any{
		"gtid_executed": b.gtidPosition.GTIDSet.String()})
	if err != nil {
		return nil, err
	}

	return binlogEvent, nil
}

// createSchemaChangeQueryEvents processes the specified |tableDeltas| for the database |databaseName| at |newRoot| and returns
// a slice of binlog events that replicate any schema changes in the TableDeltas, as well as a boolean indicating if
// any TableDeltas were seen that contain data changes that need to be replicated.
func (b *binlogProducer) createSchemaChangeQueryEvents(
	ctx *sql.Context, databaseName string, tableDeltas []diff.TableDelta, newRoot doltdb.RootValue) (
	events []mysql.BinlogEvent, hasDataChanges bool, err error) {
	for _, tableDelta := range tableDeltas {
		isRename := tableDelta.IsRename()
		schemaChanged, err := tableDelta.HasSchemaChanged(ctx)
		if err != nil {
			return nil, false, err
		}

		dataChanged, err := tableDelta.HasDataChanged(ctx)
		if err != nil {
			return nil, false, err
		}
		if dataChanged && !tableDelta.IsDrop() {
			hasDataChanges = true
		}

		if !schemaChanged && !isRename {
			continue
		}

		schemaPatchStatements, err := sqlfmt.GenerateSqlPatchSchemaStatements(ctx, newRoot, tableDelta)
		if err != nil {
			return nil, false, err
		}

		for _, schemaPatchStatement := range schemaPatchStatements {
			// Schema changes in MySQL are always in an implicit transaction, so before each one, we
			// send a new GTID event for the next transaction start
			binlogEvent, err := b.createGtidEvent(ctx)
			if err != nil {
				return nil, false, err
			}
			events = append(events, binlogEvent)
			events = append(events, b.newQueryEvent(databaseName, schemaPatchStatement))
		}
	}

	return events, hasDataChanges, nil
}

// createTableMapEvents returns a slice of TableMap binlog events that describe the tables with data changes from
// |tableDeltas| in the database named |databaseName|. In addition to the binlog events, it also returns a map of
// table name to the table IDs used for that table in the generated TableMap events, so that callers can look up
// the correct table ID to use in Row events.
func (b *binlogProducer) createTableMapEvents(ctx *sql.Context, databaseName string, tableDeltas []diff.TableDelta) (events []mysql.BinlogEvent, tablesToId map[string]uint64, err error) {
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
		if tableName.Name == "" {
			tableName = tableDelta.FromName
		}
		tablesToId[tableName.Name] = tableId
		tableMap, err := createTableMapFromDoltTable(ctx, databaseName, tableName.Name, tableDelta.ToTable)
		if err != nil {
			return nil, nil, err
		}
		events = append(events, b.newTableMapEvent(tableId, tableMap))
	}

	return events, tablesToId, nil
}

// createRowEvents returns a slice of binlog Insert/Update/Delete row events that represent the data changes
// present in the specified |tableDeltas|. The |tablesToId| map contains the mapping of table names to IDs used
// in the associated TableMap events describing the table schemas.
func (b *binlogProducer) createRowEvents(ctx *sql.Context, tableDeltas []diff.TableDelta, tablesToId map[string]uint64) (events []mysql.BinlogEvent, err error) {
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
		if tableName.Name == "" {
			tableName = tableDelta.FromName
		}

		var fromMap, toMap prolly.Map
		if fromRowData != nil {
			fromMap = durable.ProllyMapFromIndex(fromRowData)
		}
		if toRowData != nil {
			toMap = durable.ProllyMapFromIndex(toRowData)
		}

		sch, err := tableDelta.ToTable.GetSchema(ctx)
		if err != nil {
			return nil, err
		}

		columns := sch.GetAllCols().GetColumns()
		tableId := tablesToId[tableName.Name]

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
			events = append(events, b.newWriteRowsEvent(tableId, rows))
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
			events = append(events, b.newDeleteRowsEvent(tableId, rows))
		}

		if tableRowsToUpdate != nil {
			rows := mysql.Rows{
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
			events = append(events, b.newUpdateRowsEvent(tableId, rows))
		}
	}

	return events, nil
}

// currentGtidPosition returns the current GTID position of the binlog events.
func (b *binlogProducer) currentGtidPosition() string {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.gtidPosition == nil {
		return ""
	}

	return b.gtidPosition.String()
}

// newQueryEvent creates a new Query BinlogEvent for the specified |databaseName| and |query|, and updates the
// stream's log position.
func (b *binlogProducer) newQueryEvent(databaseName, query string) mysql.BinlogEvent {
	// TODO: Charset and SQL_MODE support
	return mysql.NewQueryEvent(*b.binlogFormat, b.binlogEventMeta, mysql.Query{
		Database: databaseName,
		Charset:  nil,
		SQL:      query,
		Options:  0,
		SqlMode:  0,
	})
}

// newXIDEvent returns a new XID BinlogEvent and updates the stream's log position.
func (b *binlogProducer) newXIDEvent() mysql.BinlogEvent {
	return mysql.NewXIDEvent(*b.binlogFormat, b.binlogEventMeta)
}

// newTableMapEvent returns a new TableMap BinlogEvent for the specified |tableId| and |tableMap|, and updates the
// stream's log position.
func (b *binlogProducer) newTableMapEvent(tableId uint64, tableMap *mysql.TableMap) mysql.BinlogEvent {
	return mysql.NewTableMapEvent(*b.binlogFormat, b.binlogEventMeta, tableId, tableMap)
}

// newWriteRowsEvent returns a new WriteRows BinlogEvent for the specified |tableId| and |rows|, and updates the
// stream's log position.
func (b *binlogProducer) newWriteRowsEvent(tableId uint64, rows mysql.Rows) mysql.BinlogEvent {
	return mysql.NewWriteRowsEvent(*b.binlogFormat, b.binlogEventMeta, tableId, rows)
}

// newDeleteRowsEvent returns a new DeleteRows BinlogEvent for the specified |tableId| and |rows|, and updates the
// stream's log position.
func (b *binlogProducer) newDeleteRowsEvent(tableId uint64, rows mysql.Rows) mysql.BinlogEvent {
	return mysql.NewDeleteRowsEvent(*b.binlogFormat, b.binlogEventMeta, tableId, rows)
}

// newUpdateRowsEvent returns a new UpdateRows BinlogEvent for the specified |tableId| and |rows|, and updates the
// stream's log position.
func (b *binlogProducer) newUpdateRowsEvent(tableId uint64, rows mysql.Rows) mysql.BinlogEvent {
	return mysql.NewUpdateRowsEvent(*b.binlogFormat, b.binlogEventMeta, tableId, rows)
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

// createBinlogFormat returns a new BinlogFormat that describes the format of this binlog stream, which will always
// be a MySQL 5.6+ compatible binlog format.
func createBinlogFormat() *mysql.BinlogFormat {
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

	return &binlogFormat
}

// createBinlogEventMetadata returns a new BinlogStream instance, configured with this server's @@server_id, a zero value for
// the log position, and the current time for the timestamp. If any errors are encountered while loading @@server_id,
// this function will return an error.
func createBinlogEventMetadata() (*mysql.BinlogEventMetadata, error) {
	serverId, err := getServerId()
	if err != nil {
		return nil, err
	}

	return &mysql.BinlogEventMetadata{
		ServerID:  serverId,
		Timestamp: uint32(time.Now().Unix()),
	}, nil
}
