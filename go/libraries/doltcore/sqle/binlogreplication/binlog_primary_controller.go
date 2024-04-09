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
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"
	"github.com/sirupsen/logrus"

	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb/durable"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dsess"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

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

var _ dsess.TransactionListener = (*binlogStreamerManager)(nil)

// TransactionCommit implements the TransactionListener interface. When a transaction is committed, this function
// generates events for the binary log and sends them to all connected replicas.
//
// For a data update, the following events are generated:
//
//	– GTID event (declares the new transaction)
//	– TableMap event for each table updated
//	- DELETE_ROWS or WRITE_ROWS or UPDATE_ROWS event with the data changes
//	- XID event (ends the transaction)
//
// TODO: This function currently does all its work synchronously, in the same user thread as the transaction commit.
// We should split this out to a background routine to process, in order of the commits.
//
// TODO: This function currently sends the events to all connected replicas (through a channel). Eventually we need
// to change this so that it writes to a binary log file as the intermediate, and then the readers are watching
// that log to stream events back to the connected replicas.
func (m *binlogStreamerManager) TransactionCommit(ctx *sql.Context, databaseName string, before *doltdb.RootValue, after *doltdb.RootValue) error {
	tableDeltas, err := diff.GetTableDeltas(ctx, before, after)
	if err != nil {
		// TODO: Clean up err handling; Probably just log the error, don't bring down the server or stop replication
		panic(err.Error())
	}

	tableId := uint64(42)
	tablesToId := make(map[string]uint64)
	binlogEvents := make([]mysql.BinlogEvent, 0)

	// GTID
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return err
	}
	sid, err := mysql.ParseSID(serverUuid)
	if err != nil {
		return err
	}
	gtid := mysql.Mysql56GTID{Server: sid, Sequence: gtidSequence}
	binlogEvent := mysql.NewMySQLGTIDEvent(m.binlogFormat, m.binlogStream, gtid, false)
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()
	gtidSequence++

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

	for _, tableDelta := range tableDeltas {
		dataChanged, err := tableDelta.HasDataChanged(ctx)
		if err != nil {
			panic(err.Error()) // TODO:
		}
		if !dataChanged {
			// TODO: For now, we are only processing data changes. Eventually, we'll need to figure
			//       out how to process schema changes, too. Seems like we'll have to capture the exact
			//       DDL statements though – trying to reconstruct them is going to be error prone.
			continue
		}

		// TODO: Make sure to not replicate ignored tables? Or do we want to replicate them and
		//       just exclude them from Dolt commits?

		tableId++
		tableName := tableDelta.ToName
		if tableName == "" {
			tableName = tableDelta.FromName
		}
		tablesToId[tableName] = tableId
		tableMap, err := createTableMapFromDoltTable(ctx, databaseName, tableName, tableDelta.ToTable)
		if err != nil {
			panic(err.Error())
		}

		binlogEvent := mysql.NewTableMapEvent(m.binlogFormat, m.binlogStream, tableId, tableMap)
		binlogEvents = append(binlogEvents, binlogEvent)
		m.binlogStream.LogPosition += binlogEvent.Length()
	}

	// Now loop over the tableDeltas to pull out their diff contents
	for _, tableDelta := range tableDeltas {
		fromRowData, toRowData, err := tableDelta.GetRowData(ctx)
		if err != nil {
			panic(err.Error()) // TODO:
		}
		// TODO: Considering limiting to at most one replica supported at a time? Does that actually help at all?
		// TODO: If tableDelta.IsDrop(), then we can skip the data transfer and just send the drop table DDL statement

		tableName := tableDelta.ToName
		if tableName == "" {
			tableName = tableDelta.FromName
		}

		fromMap := durable.ProllyMapFromIndex(fromRowData)
		toMap := durable.ProllyMapFromIndex(toRowData)

		schema, err := tableDelta.ToTable.GetSchema(ctx)
		if err != nil {
			return err
		}

		columns := schema.GetAllCols().GetColumns()
		tableId := tablesToId[tableName]

		var tableRowsToWrite []mysql.Row
		var tableRowsToDelete []mysql.Row
		var tableRowsToUpdate []mysql.Row

		err = prolly.DiffMaps(ctx, fromMap, toMap, false, func(_ context.Context, diff tree.Diff) error {
			switch diff.Type {
			case tree.AddedDiff:
				data, nullBitmap, err := serializeRowToBinlogBytes(schema, diff.Key, diff.To)
				if err != nil {
					return err
				}
				tableRowsToWrite = append(tableRowsToWrite, mysql.Row{
					NullColumns: nullBitmap,
					Data:        data,
				})

			case tree.ModifiedDiff:
				data, nullColumns, err := serializeRowToBinlogBytes(schema, diff.Key, diff.To)
				if err != nil {
					return err
				}
				identify, nullIdentifyColumns, err := serializeRowToBinlogBytes(schema, diff.Key, diff.From)
				if err != nil {
					return err
				}
				tableRowsToUpdate = append(tableRowsToUpdate, mysql.Row{
					NullColumns:         nullColumns,
					Data:                data,
					NullIdentifyColumns: nullIdentifyColumns,
					Identify:            identify,
				})

			case tree.RemovedDiff:
				// TODO: If the schema of the talbe has changed between FromTable and ToTable, then this probably breaks
				identifyData, nullBitmap, err := serializeRowToBinlogBytes(schema, diff.Key, diff.From)
				if err != nil {
					return err
				}
				tableRowsToDelete = append(tableRowsToDelete, mysql.Row{
					NullIdentifyColumns: nullBitmap,
					Identify:            identifyData,
				})

			default:
				return fmt.Errorf("unexpected diff type: %v", diff.Type)
			}

			return nil
		})
		if err != nil && err != io.EOF {
			panic(err.Error()) // TODO:
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
			binlogEvents = append(binlogEvents, binlogEvent)
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
			binlogEvents = append(binlogEvents, binlogEvent)
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
			binlogEvents = append(binlogEvents, binlogEvent)
			m.binlogStream.LogPosition += binlogEvent.Length()
		}
	}

	binlogEvent = mysql.NewXIDEvent(m.binlogFormat, m.binlogStream)
	binlogEvents = append(binlogEvents, binlogEvent)
	m.binlogStream.LogPosition += binlogEvent.Length()

	for _, streamer := range m.streamers {
		logrus.StandardLogger().Warnf("sending %d binlog events\n", len(binlogEvents))
		streamer.eventChan <- binlogEvents
	}

	return nil
}

// TODO: godocs
func serializeRowToBinlogBytes(schema schema.Schema, key, value tree.Item) (data []byte, nullBitmap mysql.Bitmap, err error) {
	columns := schema.GetAllCols().GetColumns()
	nullBitmap = mysql.NewServerBitmap(len(columns))

	keyTuple := val.Tuple(key)
	valueTuple := val.Tuple(value)

	dataLength := 0
	keyIdx := -1
	valueIdx := -1
	keyDesc, valueDesc := schema.GetMapDescriptors()
	var descriptor val.TupleDesc
	var idx int
	var tuple val.Tuple
	for rowIdx, col := range columns {
		if col.IsPartOfPK {
			descriptor = keyDesc
			keyIdx++
			idx = keyIdx
			tuple = keyTuple
		} else {
			descriptor = valueDesc
			valueIdx++
			idx = valueIdx
			tuple = valueTuple
		}

		currentPos := dataLength

		typ := col.TypeInfo.ToSqlType()
		switch typ.Type() {
		case query.Type_VARCHAR:
			stringVal, notNull := descriptor.GetString(idx, tuple)
			if notNull {
				// TODO: when the field size is 255 or less, we use one byte to encode the length of the data,
				//       otherwise we use 2 bytes.
				numBytesForLength := 1
				dataLength += numBytesForLength + len(stringVal)
				data = append(data, make([]byte, numBytesForLength+len(stringVal))...)
				if numBytesForLength == 1 {
					data[currentPos] = byte(int8(len(stringVal)))
				} else if numBytesForLength == 2 {
					binary.LittleEndian.PutUint16(data[currentPos:], uint16(len(stringVal)))
				} else {
					panic("this shouldn't happen!") // TODO:
				}
				copy(data[currentPos+numBytesForLength:], stringVal)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_FLOAT32: // FLOAT
			floatValue, notNull := descriptor.GetFloat32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				dataLength += 4
				bits := math.Float32bits(floatValue)
				binary.LittleEndian.PutUint32(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_FLOAT64: // DOUBLE
			floatValue, notNull := descriptor.GetFloat64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				dataLength += 8
				bits := math.Float64bits(floatValue)
				binary.LittleEndian.PutUint64(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_YEAR: // YEAR
			intValue, notNull := descriptor.GetYear(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				dataLength += 1
				data[currentPos] = byte(intValue - 1900)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_DATETIME: // DATETIME
			timeValue, notNull := descriptor.GetDatetime(idx, tuple)
			if notNull {
				year, month, day := timeValue.Date()
				hour, minute, second := timeValue.Clock()
				// TODO: fractional second support

				// Calculate year-month (ym), year-month-day (ymd), and hour-minute-second (hms) components
				ym := uint64((year * 13) + int(month))
				ymd := (ym << 5) | uint64(day)
				hms := (uint64(hour) << 12) | (uint64(minute) << 6) | uint64(second)

				// Combine ymd and hms into a single uint64, adjusting with the offset used in the decoding
				ymdhms := ((ymd << 17) | hms) + uint64(0x8000000000)

				// Grab the last 5 bytes of the uint64 we just packed, and put them into the data buffer. Note that
				// we do NOT use LittleEndian here, because we are manually packing the bytes in the right format.
				temp := make([]byte, 8)
				binary.BigEndian.PutUint64(temp, ymdhms)
				data = append(data, temp[3:]...)
				dataLength += 5
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TIMESTAMP: // TIMESTAMP
			timeValue, notNull := descriptor.GetDatetime(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				dataLength += 4
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(timeValue.Unix()))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_DATE: // DATE
			dateValue, notNull := descriptor.GetDate(idx, tuple)
			if notNull {
				buffer := uint32(dateValue.Year())<<9 | uint32(dateValue.Month())<<5 | uint32(dateValue.Day())
				temp := make([]byte, 4)
				binary.LittleEndian.PutUint32(temp, buffer)
				data = append(data, make([]byte, 3)...)
				dataLength += 3
				copy(data[currentPos:], temp[:3])
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TIME: // TIME
			// NOTE: MySQL documents two different formats for serializing time data types. This format seems to be
			//       the "older" format, but the newer format doesn't seem to work when we tried it.
			epochNanos, notNull := descriptor.GetSqlTime(idx, tuple)
			timeValue := time.Unix(epochNanos/1_000_000, 0) // TODO: support fractional seconds
			if notNull {
				// TODO: Support negative time periods
				temp := uint32(timeValue.Hour()*10000 + timeValue.Minute()*100 + timeValue.Second())
				buffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(buffer, temp)
				data = append(data, buffer[:3]...)
				dataLength += 3

			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT8: // TINYINT
			intValue, notNull := descriptor.GetInt8(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				dataLength += 1
				data[currentPos] = byte(intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT8: // TINYINT UNSIGNED
			intValue, notNull := descriptor.GetUint8(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				dataLength += 1
				data[currentPos] = intValue
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT16: // SMALLINT
			intValue, notNull := descriptor.GetInt16(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				dataLength += 2
				binary.LittleEndian.PutUint16(data[currentPos:], uint16(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT16: // SMALLINT UNSIGNED
			intValue, notNull := descriptor.GetUint16(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				dataLength += 2
				binary.LittleEndian.PutUint16(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT24: // MEDIUMINT
			intValue, notNull := descriptor.GetInt32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 3)...)
				dataLength += 3
				tempBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(tempBuffer, uint32(intValue))
				copy(data[currentPos:], tempBuffer[0:3])
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT24: // MEDIUMINT UNSIGNED
			intValue, notNull := descriptor.GetUint32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 3)...)
				dataLength += 3
				tempBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(tempBuffer, intValue)
				copy(data[currentPos:], tempBuffer[0:3])
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		// TODO: These could probably be broken out into separate structs per datatype, as a cleaner
		//       way to organize these and then throw them into a separate file
		case query.Type_INT32: // INT
			intValue, notNull := descriptor.GetInt32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				dataLength += 4
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT32: // INT UNSIGNED
			intValue, notNull := descriptor.GetUint32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				dataLength += 4
				binary.LittleEndian.PutUint32(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT64: // BIGINT
			intValue, notNull := descriptor.GetInt64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				dataLength += 8
				binary.LittleEndian.PutUint64(data[currentPos:], uint64(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT64: // BIGINT UNSIGNED
			intValue, notNull := descriptor.GetUint64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				dataLength += 8
				binary.LittleEndian.PutUint64(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		default:
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}
	}

	return data, nullBitmap, nil
}

// TODO: godocs
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
		typ := col.TypeInfo.ToSqlType()
		metadata[i] = 0

		switch typ.Type() {
		case query.Type_CHAR:
			types[i] = mysql.TypeString
		case query.Type_VARCHAR:
			types[i] = mysql.TypeVarchar
			sTyp := typ.(sql.StringType)
			metadata[i] = uint16(4 * sTyp.Length())

		case query.Type_YEAR:
			types[i] = mysql.TypeYear
		case query.Type_DATE:
			types[i] = mysql.TypeDate
		case query.Type_DATETIME:
			// TypeDateTime2 means use the new DateTime format, which was introduced after MySQL 5.6.4,
			// and has a more efficient binary representation.
			types[i] = mysql.TypeDateTime2
			// TODO: length of microseconds in metadata
		case query.Type_TIMESTAMP:
			types[i] = mysql.TypeTimestamp
			// TODO: length of microseconds in metadata
		case query.Type_TIME:
			types[i] = mysql.TypeTime
			// TODO: length of microseconds in metadata

		case query.Type_INT8: // TINYINT
			types[i] = mysql.TypeTiny
		case query.Type_INT16: // SMALLINT
			types[i] = mysql.TypeShort
		case query.Type_INT24: // MEDIUMINT
			types[i] = mysql.TypeInt24
		case query.Type_INT32: // INT
			types[i] = mysql.TypeLong
		case query.Type_INT64: // BIGINT
			types[i] = mysql.TypeLongLong

		case query.Type_UINT8: // TINYINT UNSIGNED
			types[i] = mysql.TypeTiny
		case query.Type_UINT16: // SMALLINT UNSIGNED
			types[i] = mysql.TypeShort
		case query.Type_UINT24: // MEDIUMINT UNSIGNED
			types[i] = mysql.TypeInt24
		case query.Type_UINT32: // INT UNSIGNED
			types[i] = mysql.TypeLong
		case query.Type_UINT64: // BIGINT UNSIGNED
			types[i] = mysql.TypeLongLong

		default:
			panic(fmt.Sprintf("unsupported type: %v \n", typ.String()))
		}

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
		Metadata:  metadata, // https://mariadb.com/kb/en/table_map_event/#optional-metadata-block
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

	dsess.RegisterTransactionListener(manager)

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
					panic("unable to flush: " + err.Error())
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
				panic("unable to flush: " + err.Error())
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
func (d doltBinlogPrimaryController) GetBinaryLogStatus(ctx *sql.Context) error {
	return fmt.Errorf("DOLT: GetBinaryLogStatus not implemented yet")
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

//func sendQueryEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, query string) error {
//	binlogEvent := mysql.NewQueryEvent(binlogFormat, binlogStream, mysql.Query{
//		Database: "db01",
//		Charset:  nil,
//		SQL:      query,
//		Options:  0,
//		SqlMode:  0,
//	})
//
//	return conn.WriteBinlogEvent(binlogEvent, false)
//}
//
//func sendGtidEvent(ctx *sql.Context, conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, sequenceNumber int64) error {
//	serverUuid, err := getServerUuid(ctx)
//	if err != nil {
//		return err
//	}
//
//	sid, err := mysql.ParseSID(serverUuid)
//	if err != nil {
//		return err
//	}
//	gtid := mysql.Mysql56GTID{Server: sid, Sequence: sequenceNumber}
//	binlogEvent := mysql.NewMySQLGTIDEvent(binlogFormat, binlogStream, gtid, false)
//	return conn.WriteBinlogEvent(binlogEvent, false)
//}
//
//func sendTableMapEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, tableId uint64) error {
//	tableMap := &mysql.TableMap{
//		// TODO: What do these flags mean?
//		Flags:    0x8090,
//		Database: "db01",
//		Name:     "t",
//		Types: []byte{
//			mysql.TypeLong,
//			mysql.TypeVarchar,
//		},
//		CanBeNull: mysql.NewServerBitmap(2),
//		// https://mariadb.com/kb/en/table_map_event/#optional-metadata-block
//		Metadata: []uint16{
//			0,
//			400, // varchar size 4*100=400
//		},
//	}
//	tableMap.CanBeNull.Set(1, true)
//	binlogEvent := mysql.NewTableMapEvent(binlogFormat, binlogStream, tableId, tableMap)
//	return conn.WriteBinlogEvent(binlogEvent, false)
//}
//
//func sendXidEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
//	binlogEvent := mysql.NewXIDEvent(binlogFormat, binlogStream)
//	return conn.WriteBinlogEvent(binlogEvent, false)
//}
//
//// createInsertRows creates the Rows for a WriteRows binlog event, using |key| and |value| for the data.
//func createInsertRows(key int, value string) (*mysql.Rows, error) {
//	data := make([]byte, 4+2+len(value))
//	binary.LittleEndian.PutUint32(data, uint32(key))
//	binary.LittleEndian.PutUint16(data[4:], uint16(len(value)))
//	copy(data[6:], value)
//
//	rows := mysql.Rows{
//		Flags: 0x1234, // TODO: ???
//		//IdentifyColumns: mysql.NewServerBitmap(2),
//		DataColumns: mysql.NewServerBitmap(2),
//		Rows: []mysql.Row{
//			{
//				// NOTE: We don't send identify information for inserts
//				//NullIdentifyColumns: mysql.NewServerBitmap(2),
//				//Identify: []byte{
//				//	0x10, 0x20, 0x30, 0x40, // long
//				//	0x03, 0x00, // len('abc')
//				//	'a', 'b', 'c', // 'abc'
//				//},
//
//				NullColumns: mysql.NewServerBitmap(2),
//				Data:        data,
//			},
//		},
//	}
//	// All rows are included, none are NULL.
//	//rows.IdentifyColumns.Set(0, true)
//	//rows.IdentifyColumns.Set(1, true)
//	rows.DataColumns.Set(0, true)
//	rows.DataColumns.Set(1, true)
//
//	return &rows, nil
//}
//
//func sendWriteRowsEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, tableId uint64, rows *mysql.Rows) error {
//	binlogEvent := mysql.NewWriteRowsEvent(binlogFormat, binlogStream, tableId, *rows)
//	return conn.WriteBinlogEvent(binlogEvent, false)
//}
