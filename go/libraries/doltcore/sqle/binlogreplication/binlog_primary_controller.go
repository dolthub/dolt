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
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/binlogreplication"
	"github.com/dolthub/vitess/go/mysql"
)

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
func (d doltBinlogPrimaryController) BinlogDumpGtid(ctx *sql.Context, c *mysql.Conn, gtidSet mysql.GTIDSet) error {
	if err := sendTestBinlogEvents(ctx, c); err != nil {
		return err
	}

	return nil
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

func getServerUuid(ctx *sql.Context) (string, error) {
	variable, err := ctx.GetSessionVariable(ctx, "server_uuid")
	if err != nil {
		return "", err
	}

	if s, ok := variable.(string); ok {
		if len(s) == 0 {
			return "", fmt.Errorf("@@server_uuid is empty – must be set to a valid UUID")
		}
		return s, nil
	}

	return "", fmt.Errorf("@@server_uuid is not a string – must be set to a valid UUID")
}

func sendTestBinlogEvents(ctx *sql.Context, conn *mysql.Conn) error {
	binlogFormat := mysql.NewMySQL56BinlogFormat()
	// TODO: We should be able to turn checksums back on
	binlogFormat.ChecksumAlgorithm = mysql.BinlogChecksumAlgOff
	binlogStream := &mysql.BinlogStream{
		ServerID:    42, // TODO: Hardcoded – read from @@server_id instead
		LogPosition: 0,
		Timestamp:   uint32(time.Now().Unix()),
	}

	// Rotate
	err := sendRotateEvent(conn, binlogFormat, binlogStream)
	if err != nil {
		return err
	}

	// Format Description
	err = sendFormatDescription(conn, binlogFormat, binlogStream)
	if err != nil {
		return err
	}

	err = conn.FlushBuffer()
	if err != nil {
		return err
	}

	// TODO: Send Previous GTIDs event? Is this always needed?
	//binlogEvent = mysql.NewPreviousGTIDsEvent(binlogFormat, binlogStream)
	//err = conn.WriteBinlogEvent(binlogEvent, false)
	//if err != nil {
	//	return err
	//}

	// GTID: sequence 1
	err = sendGtidEvent(ctx, conn, binlogFormat, binlogStream, 1)
	if err != nil {
		return err
	}

	// Query: CREATE TABLE...
	err = sendQueryEvent(conn, binlogFormat, binlogStream,
		"CREATE TABLE t (pk int primary key, c1 varchar(100));")
	if err != nil {
		return err
	}

	err = conn.FlushBuffer()
	if err != nil {
		return err
	}

	// GTID: sequence 2
	err = sendGtidEvent(ctx, conn, binlogFormat, binlogStream, 2)
	if err != nil {
		return err
	}

	// TableMap: db01.t
	// TODO: Right now, hardcoded to a single table: db01.t, with the schema (int, varchar)
	tableId := uint64(49)
	err = sendTableMapEvent(conn, binlogFormat, binlogStream, tableId)
	if err != nil {
		return err
	}

	// WriteRows
	rows, err := createInsertRows()
	if err != nil {
		return err
	}
	err = sendWriteRowsEvent(conn, binlogFormat, binlogStream, tableId, rows)
	if err != nil {
		return err
	}

	// Send the XID event to commit the transaction
	err = sendXidEvent(conn, binlogFormat, binlogStream)
	if err != nil {
		return err
	}

	err = conn.FlushBuffer()
	if err != nil {
		return err
	}

	// Sleep for 10 minutes to avoid immediately closing the connection
	for i := 0; i < 20; i++ {
		time.Sleep(30 * time.Second)

		err = sendHeartbeat(conn, binlogFormat, binlogStream)
		if err != nil {
			return err
		}

		err = conn.FlushBuffer()
		if err != nil {
			return err
		}
	}

	return nil
}

func sendRotateEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	// TODO: position and filename should be tracked in a struct somewhere
	binlogEvent := mysql.NewRotateEvent(binlogFormat, binlogStream, 0, "binlog.000028")
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendFormatDescription(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogEvent := mysql.NewFormatDescriptionEvent(binlogFormat, binlogStream)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendHeartbeat(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogEvent := mysql.NewHeartbeatEvent(binlogFormat, binlogStream)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendQueryEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, query string) error {
	binlogEvent := mysql.NewQueryEvent(binlogFormat, binlogStream, mysql.Query{
		Database: "db01",
		Charset:  nil,
		SQL:      query,
		Options:  0,
		SqlMode:  0,
	})
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendGtidEvent(ctx *sql.Context, conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, sequenceNumber int64) error {
	serverUuid, err := getServerUuid(ctx)
	if err != nil {
		return err
	}

	sid, err := mysql.ParseSID(serverUuid)
	if err != nil {
		return err
	}
	gtid := mysql.Mysql56GTID{Server: sid, Sequence: sequenceNumber}
	binlogEvent := mysql.NewMySQLGTIDEvent(binlogFormat, binlogStream, gtid, false)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendTableMapEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, tableId uint64) error {
	tableMap := &mysql.TableMap{
		// TODO: What do these flags mean?
		Flags:    0x8090,
		Database: "db01",
		Name:     "t",
		Types: []byte{
			mysql.TypeLong,
			mysql.TypeVarchar,
		},
		CanBeNull: mysql.NewServerBitmap(2),
		// https://mariadb.com/kb/en/table_map_event/#optional-metadata-block
		Metadata: []uint16{
			0,
			400, // varchar size 4*100=400
		},
	}
	tableMap.CanBeNull.Set(1, true)
	binlogEvent := mysql.NewTableMapEvent(binlogFormat, binlogStream, tableId, tableMap)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func sendXidEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream) error {
	binlogEvent := mysql.NewXIDEvent(binlogFormat, binlogStream)
	return conn.WriteBinlogEvent(binlogEvent, false)
}

func createInsertRows() (*mysql.Rows, error) {
	rows := mysql.Rows{
		Flags: 0x1234, // TODO: ???
		//IdentifyColumns: mysql.NewServerBitmap(2),
		DataColumns: mysql.NewServerBitmap(2),
		Rows: []mysql.Row{
			{
				// NOTE: We don't send identify information for inserts
				//NullIdentifyColumns: mysql.NewServerBitmap(2),
				//Identify: []byte{
				//	0x10, 0x20, 0x30, 0x40, // long
				//	0x03, 0x00, // len('abc')
				//	'a', 'b', 'c', // 'abc'
				//},

				NullColumns: mysql.NewServerBitmap(2),
				Data: []byte{
					// | 1076895760 | abcd       |
					//   270544960
					//   1076895760 <-- little endian
					0x10, 0x20, 0x30, 0x40, // long
					0x04, 0x00, // len('abcd')
					'a', 'b', 'c', 'd', // 'abcd'
				},
			},
		},
	}
	// All rows are included, none are NULL.
	//rows.IdentifyColumns.Set(0, true)
	//rows.IdentifyColumns.Set(1, true)
	rows.DataColumns.Set(0, true)
	rows.DataColumns.Set(1, true)

	return &rows, nil
}

func sendWriteRowsEvent(conn *mysql.Conn, binlogFormat mysql.BinlogFormat, binlogStream *mysql.BinlogStream, tableId uint64, rows *mysql.Rows) error {
	binlogEvent := mysql.NewWriteRowsEvent(binlogFormat, binlogStream, tableId, *rows)
	return conn.WriteBinlogEvent(binlogEvent, false)
}
