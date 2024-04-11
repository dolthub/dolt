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
	"strconv"
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
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
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
				data, nullBitmap, err := serializeRowToBinlogBytes(ctx,
					schema, diff.Key, diff.To, tableDelta.ToTable.NodeStore())
				if err != nil {
					return err
				}
				tableRowsToWrite = append(tableRowsToWrite, mysql.Row{
					NullColumns: nullBitmap,
					Data:        data,
				})

			case tree.ModifiedDiff:
				data, nullColumns, err := serializeRowToBinlogBytes(ctx,
					schema, diff.Key, diff.To, tableDelta.ToTable.NodeStore())
				if err != nil {
					return err
				}
				identify, nullIdentifyColumns, err := serializeRowToBinlogBytes(ctx,
					schema, diff.Key, diff.From, tableDelta.FromTable.NodeStore())
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
				// TODO: If the schema of the table has changed between FromTable and ToTable, then this probably breaks
				identifyData, nullBitmap, err := serializeRowToBinlogBytes(ctx,
					schema, diff.Key, diff.From, tableDelta.FromTable.NodeStore())
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
func serializeRowToBinlogBytes(ctx *sql.Context, schema schema.Schema, key, value tree.Item, ns tree.NodeStore) (data []byte, nullBitmap mysql.Bitmap, err error) {
	columns := schema.GetAllCols().GetColumns()
	nullBitmap = mysql.NewServerBitmap(len(columns))

	keyTuple := val.Tuple(key)
	valueTuple := val.Tuple(value)

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

		currentPos := len(data)
		typ := col.TypeInfo.ToSqlType()
		switch typ.Type() {
		case query.Type_VARCHAR, query.Type_CHAR:
			stringVal, notNull := descriptor.GetString(idx, tuple)
			if notNull {
				// When the field size is greater than 255 bytes, the serialization format
				// requires us to use 2 bytes for the length of the field value.
				numBytesForLength := 1
				stringLength := len(stringVal)
				if stringType, ok := col.TypeInfo.ToSqlType().(sql.StringType); ok {
					if stringType.Length()*4 > 255 {
						numBytesForLength = 2
					}
				} else {
					return nil, nullBitmap, fmt.Errorf("expected string type, got %T", col.TypeInfo.ToSqlType())
				}
				data = append(data, make([]byte, numBytesForLength+stringLength)...)
				if numBytesForLength == 1 {
					data[currentPos] = uint8(stringLength)
				} else if numBytesForLength == 2 {
					binary.LittleEndian.PutUint16(data[currentPos:], uint16(stringLength))
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
				bits := math.Float32bits(floatValue)
				binary.LittleEndian.PutUint32(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_FLOAT64: // DOUBLE
			floatValue, notNull := descriptor.GetFloat64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				bits := math.Float64bits(floatValue)
				binary.LittleEndian.PutUint64(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_YEAR: // YEAR
			intValue, notNull := descriptor.GetYear(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
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
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TIMESTAMP: // TIMESTAMP
			timeValue, notNull := descriptor.GetDatetime(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
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
				copy(data[currentPos:], temp[:3])
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TIME: // TIME
			durationInNanoseconds, notNull := descriptor.GetSqlTime(idx, tuple)
			if notNull {
				negative := false
				if durationInNanoseconds < 0 {
					negative = true
					durationInNanoseconds *= -1
				}

				durationInSeconds := durationInNanoseconds / 1_000_000
				hours := durationInSeconds / (60 * 60)
				minutes := durationInSeconds / 60 % 60
				seconds := durationInSeconds % 60
				// TODO: support fractional seconds
				//nanoseconds := durationInNanoseconds % 1_000_000

				buffer := hours<<12 | minutes<<6 | seconds + 0x800000
				if negative {
					buffer *= -1
				}
				temp := make([]byte, 4)
				binary.BigEndian.PutUint32(temp, uint32(buffer))
				data = append(data, temp[1:]...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT8: // TINYINT
			intValue, notNull := descriptor.GetInt8(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				data[currentPos] = byte(intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT8: // TINYINT UNSIGNED
			intValue, notNull := descriptor.GetUint8(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				data[currentPos] = intValue
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT16: // SMALLINT
			intValue, notNull := descriptor.GetInt16(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(data[currentPos:], uint16(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT16: // SMALLINT UNSIGNED
			intValue, notNull := descriptor.GetUint16(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT24: // MEDIUMINT
			intValue, notNull := descriptor.GetInt32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 3)...)
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
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT32: // INT UNSIGNED
			intValue, notNull := descriptor.GetUint32(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				binary.LittleEndian.PutUint32(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT64: // BIGINT
			intValue, notNull := descriptor.GetInt64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				binary.LittleEndian.PutUint64(data[currentPos:], uint64(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT64: // BIGINT UNSIGNED
			intValue, notNull := descriptor.GetUint64(idx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				binary.LittleEndian.PutUint64(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_BIT: // BIT
			// TODO: why doesn't descriptor.GetBit(idx, tuple) work? Seems like we must not be using the
			//       BitEnc encoding, and just writing BIT types with the Uint64 methods. The BitEnc has
			//       the same data encoding, but it would be more descriptive to use it.
			bitValue, notNull := descriptor.GetUint64(idx, tuple)
			if notNull {
				bitType := col.TypeInfo.ToSqlType().(gmstypes.BitType)
				numBytes := int((bitType.NumberOfBits() + 7) / 8)
				temp := make([]byte, 8)
				binary.BigEndian.PutUint64(temp, bitValue)
				data = append(data, temp[len(temp)-numBytes:]...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_ENUM: // ENUM
			enumValue, notNull := descriptor.GetEnum(idx, tuple)
			if notNull {
				enumType := col.TypeInfo.ToSqlType().(gmstypes.EnumType)
				if enumType.NumberOfElements() <= 0xFF {
					data = append(data, byte(enumValue))
				} else {
					data = append(data, make([]byte, 2)...)
					binary.LittleEndian.PutUint16(data[currentPos:], enumValue)
				}
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_SET: // SET
			setValue, notNull := descriptor.GetSet(idx, tuple)
			if notNull {
				setType := col.TypeInfo.ToSqlType().(gmstypes.SetType)
				numElements := setType.NumberOfElements()
				numBytes := int((numElements + 7) / 8)
				temp := make([]byte, 8)
				binary.LittleEndian.PutUint64(temp, setValue)
				data = append(data, temp[:numBytes]...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_DECIMAL: // DECIMAL
			decimalValue, notNull := descriptor.GetDecimal(idx, tuple)
			if notNull {
				decimalType := typ.(sql.DecimalType)

				// Example:
				//   NNNNNNNNNNNN.MMMMMM
				//     12 bytes     6 bytes
				// precision is 18
				// scale is 6
				// storage is done by groups of 9 digits:
				// - 32 bits are used to store groups of 9 digits.
				// - any leftover digit is stored in:
				//   - 1 byte for 1 or 2 digits
				//   - 2 bytes for 3 or 4 digits
				//   - 3 bytes for 5 or 6 digits
				//   - 4 bytes for 7 or 8 digits (would also work for 9)
				// both sides of the dot are stored separately.
				// In this example, we'd have:
				// - 2 bytes to store the first 3 full digits.
				// - 4 bytes to store the next 9 full digits.
				// - 3 bytes to store the 6 fractional digits.
				precision := decimalType.Precision() // total number of fractional and full digits
				scale := decimalType.Scale()         // number of fractional digits
				numFullDigits := precision - scale
				numFullDigitUint32s := numFullDigits / 9
				numFractionalDigitUint32s := decimalType.Scale() / 9
				numLeftoverFullDigits := numFullDigits - numFullDigitUint32s*9
				numLeftoverFractionalDigits := decimalType.Scale() - numFractionalDigitUint32s*9

				length := numFullDigitUint32s*4 + digitsToBytes[numLeftoverFullDigits] +
					numFractionalDigitUint32s*4 + digitsToBytes[numLeftoverFractionalDigits]

				// Ensure the exponent is negative
				if decimalValue.Exponent() > 0 {
					// TODO: Turn this into an error
					panic(fmt.Sprintf("unexpected positive exponent: %d for decimalValue: %s",
						decimalValue.Exponent(), decimalValue.String()))
				}

				absStringVal := decimalValue.Abs().String()
				foo := len(absStringVal) + int(decimalValue.Exponent())
				stringIntegerVal := absStringVal[:foo-1]
				stringFractionalVal := absStringVal[foo:]

				buffer := make([]byte, length)
				bufferPos := 0

				// Fill in leftover digits – these are at the front of the integer component of the decimal
				writtenBytes, err := encodePartialDecimalBits(stringIntegerVal[:numLeftoverFullDigits], buffer[bufferPos:])
				if err != nil {
					panic(err.Error())
				}
				bufferPos += int(writtenBytes)

				// Fill in full digits for the integer component of the decimal
				writtenBytes, remainingString, err := encodeDecimalBits(stringIntegerVal[numLeftoverFullDigits:], buffer[bufferPos:])
				if err != nil {
					// TODO: return an error
					panic(err.Error())
				}
				bufferPos += int(writtenBytes)

				if len(remainingString) > 0 {
					// TODO: Convert to an error
					panic(fmt.Sprintf("unexpected remaining string after encoding full digits for integer component of decimal value: %s",
						remainingString))
				}

				// Fill in full fractional digits
				writtenBytes, remainingString, err = encodeDecimalBits(stringFractionalVal, buffer[bufferPos:])
				if err != nil {
					// TODO: return an error
					panic(err.Error())
				}
				bufferPos += int(writtenBytes)

				// Fill in partial fractional digits – these are at the end of the fractional component
				writtenBytes, err = encodePartialDecimalBits(remainingString, buffer[bufferPos:])
				if err != nil {
					panic(err.Error())
				}
				bufferPos += int(writtenBytes)

				if bufferPos != len(buffer) {
					panic(fmt.Sprintf("unexpected position; bufferPos: %d, len(buffer): %d", bufferPos, len(buffer)))
				}

				// We always xor the first bit in the first byte to indicate a positive value. If the value is
				// negative, we xor every bit with 0xff to invert the value.
				buffer[0] ^= 0x80
				if decimalValue.IsNegative() {
					for i := range buffer {
						buffer[i] ^= 0xff
					}
				}

				data = append(data, buffer...)
				// TODO: Seems like we don't actually need dataLength, right it can be computed by len(data)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_BLOB: // TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
			addr, notNull := descriptor.GetBytesAddr(idx, tuple)
			if notNull {
				bytes, err := encodeBytesFromAddress(ctx, addr, ns, typ)
				if err != nil {
					// TODO: Change to return an error
					panic(err.Error())
				}
				data = append(data, bytes...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TEXT: // TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT
			addr, notNull := descriptor.GetStringAddr(idx, tuple)
			if notNull {
				bytes, err := encodeBytesFromAddress(ctx, addr, ns, typ)
				if err != nil {
					// TODO: Change to return an error
					panic(err.Error())
				}
				data = append(data, bytes...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		default:
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}
	}

	return data, nullBitmap, nil
}

// encodeBytesFromAddress loads the out-of-band content from |addr| in |ns| and serializes it into a binary format
// in the returned |data| slice. The |typ| parameter is used to determine the maximum byte length of the serialized
// type, in order to determine how many bytes to use for the length prefix.
func encodeBytesFromAddress(ctx *sql.Context, addr hash.Hash, ns tree.NodeStore, typ sql.Type) (data []byte, err error) {
	if ns == nil {
		return nil, fmt.Errorf("nil NodeStore used to encode bytes from address")
	}
	bytes, err := tree.NewByteArray(addr, ns).ToBytes(ctx)
	if err != nil {
		// TODO: return error
		panic(err.Error())
	}

	blobType := typ.(sql.StringType)
	if blobType.MaxByteLength() > 0xFFFFFF {
		data = append(data, make([]byte, 4)...)
		binary.LittleEndian.PutUint32(data, uint32(len(bytes)))
	} else if blobType.MaxByteLength() > 0xFFFF {
		temp := make([]byte, 4)
		binary.LittleEndian.PutUint32(temp, uint32(len(bytes)))
		data = append(data, temp[:3]...)
	} else if blobType.MaxByteLength() > 0xFF {
		data = append(data, make([]byte, 2)...)
		binary.LittleEndian.PutUint16(data, uint16(len(bytes)))
	} else {
		data = append(data, uint8(len(bytes)))
	}
	data = append(data, bytes...)

	return data, nil
}

var digitsToBytes = []uint8{0, 1, 1, 2, 2, 3, 3, 4, 4, 4}

// encodePartialDecimalBits encodes the sequence of digits from |stringVal| as decimal encoded bytes in |buffer|. This
// function is intended for encoding a partial sequence of digits – i.e. where there are less than 9 digits to encode.
// For full blocks of 9 digits, the encodeDecimalBits function should be used. The number of bytes written to buffer is
// returned, along with any error encountered.
func encodePartialDecimalBits(stringVal string, buffer []byte) (uint, error) {
	numDigits := len(stringVal)
	if numDigits == 0 {
		return 0, nil
	}

	v, err := strconv.Atoi(stringVal)
	if err != nil {
		panic(err.Error())
	}

	switch digitsToBytes[numDigits] {
	case 1:
		// one byte, up to two digits
		buffer[0] = uint8(v)
		return 1, nil
	case 2:
		// two bytes, up to four digits
		buffer[0] = uint8(v >> 8)
		buffer[1] = byte(v & 0xFF)
		return 2, nil
	case 3:
		// three bytes, up to six digits
		buffer[0] = byte(v >> 16)
		buffer[1] = byte(v >> 8 & 0xFF)
		buffer[2] = byte(v & 0xFF)
		return 3, nil
	case 4:
		// four bytes, up to eight digits
		buffer[0] = byte(v >> 24)
		buffer[1] = byte(v >> 16 & 0xFF)
		buffer[2] = byte(v >> 8 & 0xFF)
		buffer[3] = byte(v & 0xFF)
		return 4, nil
	}

	return 0, fmt.Errorf("unexpected number of digits: %d", numDigits)
}

// encodeDecimalBits encodes full blocks of 9 digits from the sequence of digits in |stringVal| as decimal encoded bytes
// in |buffer|. This function will encode as many full blocks of 9 digits from |stringVal| as possible, returning the
// number of bytes written to |buffer| as well as any remaining substring from |stringVal| that did not fit cleanly into
// a full block of 9 digits. For example, if |stringVal| is "1234567890" the first 9 digits are encoded as 4 bytes in
// |buffer| and the string "0" is returned to indicate the single remaining digit that did not fit cleanly into a 4 byte
// block.
func encodeDecimalBits(stringVal string, buffer []byte) (uint, string, error) {
	bufferPos := uint(0)
	stringValPos := uint(0)
	for len(stringVal[stringValPos:]) >= 9 {
		v, err := strconv.Atoi(stringVal[stringValPos : stringValPos+9])
		if err != nil {
			return 0, "", err
		}
		stringValPos += 9

		binary.BigEndian.PutUint32(buffer[bufferPos:], uint32(v))
		bufferPos += 4
	}

	return bufferPos, stringVal[stringValPos:], nil
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
			sTyp := typ.(sql.StringType)
			maxFieldLength := uint16(4 * sTyp.Length())
			upperBits := (maxFieldLength >> 8) << 12
			lowerBits := maxFieldLength & 0xFF
			// This is one of the less obvious parts of the MySQL serialization protocol... Several types use
			// mysql.TypeString as their serialization type in binlog events (i.e. SET, ENUM, CHAR), so the first
			// metadata byte for this serialization type indicates what field type is using this serialization type
			// (i.e. SET, ENUM, or CHAR), and the second metadata byte indicates the number of bytes needed to serialize
			// a type value. However, for CHAR, that second byte isn't enough, since it can only represent up to 255
			// bytes. For sizes larger than that, we need to find two more bits. MySQL does this by reusing the third
			// and fourth bits from the first metadata byte. By XOR'ing them against the known mysql.TypeString value
			// in that byte, MySQL is able to reuse those two bits and extend the second metadata byte enough to
			// account for the max size of CHAR fields (255 chars).
			metadata[i] = ((mysql.TypeString << 8) ^ upperBits) | lowerBits

		case query.Type_VARCHAR:
			types[i] = mysql.TypeVarchar
			sTyp := typ.(sql.StringType)
			metadata[i] = uint16(4 * sTyp.Length())

		case query.Type_YEAR:
			types[i] = mysql.TypeYear
		case query.Type_DATE:
			// TODO: Do we need to switch to mysql.TypeNewDate ?
			types[i] = mysql.TypeDate
		case query.Type_DATETIME:
			// TypeDateTime2 means use the new DateTime format, which was introduced after MySQL 5.6.4,
			// and has a more efficient binary representation.
			types[i] = mysql.TypeDateTime2
			// TODO: length of microseconds in metadata
		case query.Type_TIMESTAMP:
			// TODO: Do we need to switch to mysql.TypeTimestamp2 ?
			types[i] = mysql.TypeTimestamp
			// TODO: length of microseconds in metadata
		case query.Type_TIME:
			// TypeTime2 is the newer serialization format for TIME values
			types[i] = mysql.TypeTime2
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

		case query.Type_BIT: // BIT
			types[i] = mysql.TypeBit
			bitType := typ.(gmstypes.BitType)
			// bitmap length is in metadata, as:
			// upper 8 bits: bytes length
			// lower 8 bits: bit length
			numBytes := bitType.NumberOfBits() / 8
			numBits := bitType.NumberOfBits() % 8
			metadata[i] = uint16(numBytes)<<8 | uint16(numBits)

		case query.Type_ENUM: // ENUM
			types[i] = mysql.TypeString
			enumType := typ.(gmstypes.EnumType)
			numElements := enumType.NumberOfElements()
			if numElements <= 0xFF {
				metadata[i] = mysql.TypeEnum<<8 | 1
			} else {
				metadata[i] = mysql.TypeEnum<<8 | 2
			}

		case query.Type_SET: // SET
			types[i] = mysql.TypeString
			setType := typ.(gmstypes.SetType)
			numElements := setType.NumberOfElements()
			numBytes := (numElements + 7) / 8
			metadata[i] = mysql.TypeSet<<8 | numBytes

		case query.Type_DECIMAL: // DECIMAL
			types[i] = mysql.TypeNewDecimal
			decimalType := typ.(sql.DecimalType)
			metadata[i] = (uint16(decimalType.Precision()) << 8) | uint16(decimalType.Scale())

		case query.Type_BLOB, // TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
			query.Type_TEXT: // TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT
			types[i] = mysql.TypeBlob
			blobType := typ.(sql.StringType)
			if blobType.MaxByteLength() > 0xFFFFFF {
				metadata[i] = uint16(4)
			} else if blobType.MaxByteLength() > 0xFFFF {
				metadata[i] = uint16(3)
			} else if blobType.MaxByteLength() > 0xFF {
				metadata[i] = uint16(2)
			} else {
				metadata[i] = uint16(1)
			}

		// TODO: Others?
		case query.Type_JSON: // JSON
		case query.Type_GEOMETRY: // GEOMETRY
		case query.Type_BINARY:
		case query.Type_VARBINARY:
		case query.Type_NULL_TYPE: // ???

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
