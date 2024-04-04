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
// TODO: This function currently does all its work synchronously, in the same user thread as the transaction commit. We
//
//	should split this out to a background routine to process, in order of the commits.
//
// TODO: This function currently sends the events to all connected replicas (through a channel). Eventually we need
//
//	to change this so that it writes to a binary log file as the intermediate, and then the readers are watching
//	that log to stream events back to the connected replicas.
func (m *binlogStreamerManager) TransactionCommit(ctx *sql.Context, before *doltdb.RootValue, after *doltdb.RootValue) error {
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
		tableMap, err := createTableMapFromDoltTable(ctx, tableName, tableDelta.ToTable)
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
		err = prolly.DiffMaps(ctx, fromMap, toMap, false, func(_ context.Context, diff tree.Diff) error {
			switch diff.Type {
			case tree.AddedDiff:
				schema, err := tableDelta.ToTable.GetSchema(ctx)
				if err != nil {
					return err
				}

				columns := schema.GetAllCols().GetColumns()
				data, err := serializeRowToBinlogBytes(schema, diff.Key, diff.To)
				if err != nil {
					return err
				}

				// Send a binlog event for the added row
				rows := mysql.Rows{
					Flags:       0x1234, // TODO: What are these flags?
					DataColumns: mysql.NewServerBitmap(len(columns)),
					// TODO: We should be batching all rows for the same table into the same Rows instance, not one row-per-Rows
					Rows: []mysql.Row{
						{
							NullColumns: mysql.NewServerBitmap(len(columns)),
							Data:        data,
						},
					},
				}
				// All rows are included, none are NULL (TODO: We don't support null values yet)
				for i := 0; i < len(columns); i++ {
					rows.DataColumns.Set(i, true)
				}

				tableId := tablesToId[tableName]
				binlogEvent := mysql.NewWriteRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
				binlogEvents = append(binlogEvents, binlogEvent)
				m.binlogStream.LogPosition += binlogEvent.Length()

			case tree.ModifiedDiff:
				schema, err := tableDelta.ToTable.GetSchema(ctx)
				if err != nil {
					return err
				}
				columns := schema.GetAllCols().GetColumns()

				data, err := serializeRowToBinlogBytes(schema, diff.Key, diff.To)
				if err != nil {
					return err
				}
				identifyData, err := serializeRowToBinlogBytes(schema, diff.Key, diff.From)
				if err != nil {
					return err
				}

				// Send a binlog event for the modified row
				rows := mysql.Rows{
					Flags:       0x1234, // TODO: What are these flags?
					DataColumns: mysql.NewServerBitmap(len(columns)),
					// TODO: We should be batching all rows for the same table into the same Rows instance, not one row-per-Rows
					IdentifyColumns: mysql.NewServerBitmap(len(columns)),
					Rows: []mysql.Row{
						{
							// TODO: Support for NULL values
							NullColumns:         mysql.NewServerBitmap(len(columns)),
							Data:                data,
							NullIdentifyColumns: mysql.NewServerBitmap(len(columns)),
							Identify:            identifyData,
						},
					},
				}
				// All rows are included, none are NULL (TODO: We don't support null values yet)
				for i := 0; i < len(columns); i++ {
					rows.DataColumns.Set(i, true)
				}
				for i := 0; i < len(columns); i++ {
					rows.IdentifyColumns.Set(i, true)
				}

				tableId := tablesToId[tableName]
				binlogEvent := mysql.NewUpdateRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
				binlogEvents = append(binlogEvents, binlogEvent)
				m.binlogStream.LogPosition += binlogEvent.Length()

			case tree.RemovedDiff:
				// TODO: If the schema of the talbe has changed between FromTable and ToTable, then this probably breaks
				schema, err := tableDelta.ToTable.GetSchema(ctx)
				if err != nil {
					return err
				}
				columns := schema.GetAllCols().GetColumns()

				identifyData, err := serializeRowToBinlogBytes(schema, diff.Key, diff.From)
				if err != nil {
					return err
				}

				// Send a binlog event for the added row
				rows := mysql.Rows{
					Flags: 0x1234, // TODO: What are these flags?
					// TODO: We should be batching all rows for the same table into the same Rows instance, not one row-per-Rows
					IdentifyColumns: mysql.NewServerBitmap(len(columns)),
					Rows: []mysql.Row{
						{
							// TODO: Support for NULL values
							NullIdentifyColumns: mysql.NewServerBitmap(len(columns)),
							Identify:            identifyData,
						},
					},
				}
				// All columns are included in the identifyData, none are NULL yet
				for i := 0; i < len(columns); i++ {
					rows.IdentifyColumns.Set(i, true)
				}

				tableId := tablesToId[tableName]
				binlogEvent := mysql.NewDeleteRowsEvent(m.binlogFormat, m.binlogStream, tableId, rows)
				binlogEvents = append(binlogEvents, binlogEvent)
				m.binlogStream.LogPosition += binlogEvent.Length()

			default:
				return fmt.Errorf("unexpected diff type: %v", diff.Type)
			}

			return nil
		})
		if err != nil && err != io.EOF {
			panic(err.Error()) // TODO:
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

func serializeRowToBinlogBytes(schema schema.Schema, key, value tree.Item) (data []byte, err error) {
	columns := schema.GetAllCols().GetColumns()

	dataLength := 0
	keyIdx := -1
	valueIdx := -1
	keyDesc, valueDesc := schema.GetMapDescriptors()
	for _, col := range columns {
		if col.IsPartOfPK {
			keyIdx++
		} else {
			valueIdx++
		}

		currentPos := dataLength

		typ := col.TypeInfo.ToSqlType()
		switch typ.Type() {
		case query.Type_VARCHAR:
			var value string
			if col.IsPartOfPK {
				keyTuple := val.Tuple(key)
				value, _ = keyDesc.GetString(keyIdx, keyTuple)
			} else {
				valueTuple := val.Tuple(value)
				value, _ = valueDesc.GetString(valueIdx, valueTuple)
			}
			dataLength += 2 + len(value)
			data = append(data, make([]byte, 2+len(value))...)
			binary.LittleEndian.PutUint16(data[currentPos:], uint16(len(value)))
			copy(data[currentPos+2:], value)

		case query.Type_INT32:
			data = append(data, make([]byte, 4)...)
			if col.IsPartOfPK {
				keyTuple := val.Tuple(key)
				value, _ := keyDesc.GetInt32(keyIdx, keyTuple)
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(value))
			} else {
				valueTuple := val.Tuple(value)
				value, _ := valueDesc.GetInt32(valueIdx, valueTuple)
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(value))
			}
			dataLength += 4

		default:
			return nil, fmt.Errorf("unsupported type: %v \n", typ.String())
		}
	}

	return data, nil
}

func createTableMapFromDoltTable(ctx *sql.Context, name string, table *doltdb.Table) (*mysql.TableMap, error) {
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
		switch typ.Type() {
		case query.Type_VARCHAR:
			types[i] = mysql.TypeVarchar
			sTyp := typ.(sql.StringType)
			metadata[i] = uint16(4 * sTyp.Length())
		case query.Type_INT32:
			types[i] = mysql.TypeLong
			metadata[i] = 0
		default:
			panic(fmt.Sprintf("unsupported type: %v \n", typ.String()))
		}

		if col.IsNullable() {
			canBeNullMap.Set(i, true)
		}
	}

	return &mysql.TableMap{
		Flags:     0x8090, // TODO: What do these flags mean?
		Database:  "db01", // TODO: db is still hardcoded to db01
		Name:      name,
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
		// TODO: error handling!
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
