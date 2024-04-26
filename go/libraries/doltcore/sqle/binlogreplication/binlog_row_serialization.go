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
	"encoding/binary"
	"fmt"
	"math"
	"strconv"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"
)

// rowSerializationIter iterates over the columns in a schema and abstracts access to the key and value tuples storing
// the data for a row, so that callers can ask for the next column information and get the right descriptor, tuple,
// and tuple index to use to load that column's data.
type rowSerializationIter struct {
	sch    schema.Schema // The schema representing the row being serialized
	colIdx int           // The position in the schema for the current column

	key     val.Tuple     // The key tuple for the row being serialized
	keyDesc val.TupleDesc // The descriptor for the key tuple
	keyIdx  int           // The last index in the key tuple used for a column

	value     val.Tuple     // The value tuple for the row being serialized
	valueDesc val.TupleDesc // The descriptor for the value tuple
	valueIdx  int           // The last index in the value tuple used for a column
}

// newRowSerializationIter creates a new rowSerializationIter for the specified |schema| and row data from the
// |key| and |value| tuples.
func newRowSerializationIter(sch schema.Schema, key, value tree.Item) *rowSerializationIter {
	return &rowSerializationIter{
		sch:       sch,
		key:       val.Tuple(key),
		keyDesc:   sch.GetKeyDescriptor(),
		value:     val.Tuple(value),
		valueDesc: sch.GetValueDescriptor(),
		keyIdx:    -1,
		valueIdx:  -1,
		colIdx:    0,
	}
}

// hasNext returns true if this iterator has more columns to provide and the |nextColumn| method can be called.
func (rsi *rowSerializationIter) hasNext() bool {
	return rsi.colIdx < rsi.sch.GetAllCols().Size()
}

// nextColumn provides the data needed to process the next column in a row, including the column itself, the tuple
// holding the data, the tuple descriptor for that tuple, and the position index into that tuple where the column
// is stored. Callers should always call hasNext() before calling nextColumn() to ensure that it is safe to call.
func (rsi *rowSerializationIter) nextColumn() (schema.Column, val.TupleDesc, val.Tuple, int) {
	col := rsi.sch.GetAllCols().GetColumns()[rsi.colIdx]
	rsi.colIdx++

	// For keyless schemas, the key is a single hash column representing the row's unique identity, so we
	// always use the value descriptor for all columns. Additionally, the first field in the value is a
	// count of how many times that row appears in the table, so we increment |idx| by one extra field to
	// skip over that row count field and get to the real data fields.
	if schema.IsKeyless(rsi.sch) {
		rsi.valueIdx++
		return col, rsi.valueDesc, rsi.value, rsi.valueIdx + 1
	}

	// Otherwise, for primary key tables, we need to check if the next column is stored in the key or value.
	if col.IsPartOfPK {
		rsi.keyIdx++
		return col, rsi.keyDesc, rsi.key, rsi.keyIdx
	} else {
		rsi.valueIdx++
		return col, rsi.valueDesc, rsi.value, rsi.valueIdx
	}
}

// serializeRowToBinlogBytes serializes the row formed by |key| and |value| and defined by the |schema| structure, into
// MySQL binlog binary format. For data stored out of band (e.g. BLOB, TEXT, GEOMETRY, JSON), |ns| is used to load the
// out-of-band data. This function returns the binary representation of the row, as well as a bitmap that indicates
// which fields of the row are null (and therefore don't contribute any bytes to the returned binary data).
func serializeRowToBinlogBytes(ctx *sql.Context, sch schema.Schema, key, value tree.Item, ns tree.NodeStore) (data []byte, nullBitmap mysql.Bitmap, err error) {
	columns := sch.GetAllCols().GetColumns()
	nullBitmap = mysql.NewServerBitmap(len(columns))

	iter := newRowSerializationIter(sch, key, value)
	rowIdx := -1
	for iter.hasNext() {
		rowIdx++
		col, descriptor, tuple, tupleIdx := iter.nextColumn()

		currentPos := len(data)
		typ := col.TypeInfo.ToSqlType()

		// TODO: If the schema has changed in a commit... then this code won't work... ?

		switch typ.Type() {
		case query.Type_CHAR, query.Type_VARCHAR: // CHAR, VARCHAR
			stringVal, notNull := descriptor.GetString(tupleIdx, tuple)
			if notNull {
				encodedData, err := encodeBytes([]byte(stringVal), col)
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				data = append(data, encodedData...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_BINARY, query.Type_VARBINARY: // BINARY, VARBINARY
			bytes, notNull := descriptor.GetBytes(tupleIdx, tuple)
			if notNull {
				encodedData, err := encodeBytes(bytes, col)
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				data = append(data, encodedData...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_FLOAT32: // FLOAT
			floatValue, notNull := descriptor.GetFloat32(tupleIdx, tuple)
			if notNull {
				bits := math.Float32bits(floatValue)
				data = append(data, make([]byte, 4)...)
				binary.LittleEndian.PutUint32(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_FLOAT64: // DOUBLE
			floatValue, notNull := descriptor.GetFloat64(tupleIdx, tuple)
			if notNull {
				bits := math.Float64bits(floatValue)
				data = append(data, make([]byte, 8)...)
				binary.LittleEndian.PutUint64(data[currentPos:], bits)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_YEAR: // YEAR
			intValue, notNull := descriptor.GetYear(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				data[currentPos] = byte(intValue - 1900)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_DATETIME: // DATETIME
			timeValue, notNull := descriptor.GetDatetime(tupleIdx, tuple)
			if notNull {
				year, month, day := timeValue.Date()
				hour, minute, second := timeValue.Clock()

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

				// Serialize fractional seconds
				nanos := timeValue.Nanosecond()
				micros := nanos / 1000
				dtType := typ.(sql.DatetimeType)
				switch dtType.Precision() {
				case 1, 2:
					data = append(data, byte(micros/10000))
				case 3, 4:
					data = append(data, byte(micros/100>>8), byte(micros/100))
				case 5, 6:
					data = append(data, byte(micros>>16), byte(micros>>8), byte(micros))
				}
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TIMESTAMP: // TIMESTAMP
			timeValue, notNull := descriptor.GetDatetime(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				binary.BigEndian.PutUint32(data[currentPos:], uint32(timeValue.Unix()))

				// Serialize fractional seconds
				nanos := timeValue.Nanosecond()
				micros := nanos / 1000
				dtType := typ.(sql.DatetimeType)
				switch dtType.Precision() {
				case 1, 2:
					data = append(data, byte(micros/10000))
				case 3, 4:
					data = append(data, byte(micros/100>>8), byte(micros/100))
				case 5, 6:
					data = append(data, byte(micros>>16), byte(micros>>8), byte(micros))
				}
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_DATE: // DATE
			dateValue, notNull := descriptor.GetDate(tupleIdx, tuple)
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
			durationInMicroseconds, notNull := descriptor.GetSqlTime(tupleIdx, tuple)
			if notNull {
				negative := false
				if durationInMicroseconds < 0 {
					negative = true
					durationInMicroseconds *= -1
				}

				durationInSeconds := durationInMicroseconds / 1_000_000
				hours := durationInSeconds / (60 * 60)
				minutes := durationInSeconds / 60 % 60
				seconds := durationInSeconds % 60

				// Prepare the fractional seconds component first
				// NOTE: Dolt always uses 6 digits of precision. When Dolt starts supporting other time precisions,
				//       this code will need to change.
				microseconds := durationInMicroseconds % 1_000_000
				if negative {
					seconds++
					if seconds == 60 {
						seconds = 0
						minutes += 1
					}
					if minutes == 60 {
						minutes = 0
						hours += 1
					}
					microseconds = 0x1000000 - microseconds
				}

				// Prepare the 3 byte hour/minute/second component
				hms := hours<<12 | minutes<<6 | seconds + 0x800000
				if negative {
					hms *= -1
				}

				// Write the components to the data buffer
				temp := make([]byte, 4)
				binary.BigEndian.PutUint32(temp, uint32(hms))
				data = append(data, temp[1:]...)
				data = append(data, uint8(microseconds>>16), uint8(microseconds>>8), uint8(microseconds))

			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT8: // TINYINT
			intValue, notNull := descriptor.GetInt8(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				data[currentPos] = byte(intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT8: // TINYINT UNSIGNED
			intValue, notNull := descriptor.GetUint8(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 1)...)
				data[currentPos] = intValue
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT16: // SMALLINT
			intValue, notNull := descriptor.GetInt16(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(data[currentPos:], uint16(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT16: // SMALLINT UNSIGNED
			intValue, notNull := descriptor.GetUint16(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 2)...)
				binary.LittleEndian.PutUint16(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT24: // MEDIUMINT
			intValue, notNull := descriptor.GetInt32(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 3)...)
				tempBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(tempBuffer, uint32(intValue))
				copy(data[currentPos:], tempBuffer[0:3])
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT24: // MEDIUMINT UNSIGNED
			intValue, notNull := descriptor.GetUint32(tupleIdx, tuple)
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
			intValue, notNull := descriptor.GetInt32(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				binary.LittleEndian.PutUint32(data[currentPos:], uint32(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT32: // INT UNSIGNED
			intValue, notNull := descriptor.GetUint32(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 4)...)
				binary.LittleEndian.PutUint32(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_INT64: // BIGINT
			intValue, notNull := descriptor.GetInt64(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				binary.LittleEndian.PutUint64(data[currentPos:], uint64(intValue))
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_UINT64: // BIGINT UNSIGNED
			intValue, notNull := descriptor.GetUint64(tupleIdx, tuple)
			if notNull {
				data = append(data, make([]byte, 8)...)
				binary.LittleEndian.PutUint64(data[currentPos:], intValue)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_BIT: // BIT
			// NOTE: descriptor.GetBit(tupleIdx, tuple) doesn't work here. BIT datatypes are described with a Uint64
			//       encoding, so trying to use GetBit results in an error. At the data level, both are stored with a
			//       uint64 value, so they are compatible, but we seem to only use Uint64 in the descriptor.
			bitValue, notNull := descriptor.GetUint64(tupleIdx, tuple)
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
			enumValue, notNull := descriptor.GetEnum(tupleIdx, tuple)
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
			setValue, notNull := descriptor.GetSet(tupleIdx, tuple)
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
			decimalValue, notNull := descriptor.GetDecimal(tupleIdx, tuple)
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
					return nil, mysql.Bitmap{}, fmt.Errorf(
						"unexpected positive exponent: %d for decimalValue: %s",
						decimalValue.Exponent(), decimalValue.String())
				}

				absStringVal := decimalValue.Abs().String()
				firstFractionalDigitIdx := len(absStringVal) + int(decimalValue.Exponent())
				stringIntegerVal := absStringVal[:firstFractionalDigitIdx-1]
				stringFractionalVal := absStringVal[firstFractionalDigitIdx:]

				buffer := make([]byte, length)
				bufferPos := 0

				// Fill in leftover digits – these are at the front of the integer component of the decimal
				writtenBytes, err := encodePartialDecimalBits(stringIntegerVal[:numLeftoverFullDigits], buffer[bufferPos:])
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				bufferPos += int(writtenBytes)

				// Fill in full digits for the integer component of the decimal
				writtenBytes, remainingString, err := encodeDecimalBits(stringIntegerVal[numLeftoverFullDigits:], buffer[bufferPos:])
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				bufferPos += int(writtenBytes)

				if len(remainingString) > 0 {
					return nil, mysql.Bitmap{}, fmt.Errorf(
						"unexpected remaining string after encoding full digits for integer component of decimal value: %s",
						remainingString)
				}

				// Fill in full fractional digits
				writtenBytes, remainingString, err = encodeDecimalBits(stringFractionalVal, buffer[bufferPos:])
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				bufferPos += int(writtenBytes)

				// Fill in partial fractional digits – these are at the end of the fractional component
				writtenBytes, err = encodePartialDecimalBits(remainingString, buffer[bufferPos:])
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				bufferPos += int(writtenBytes)

				if bufferPos != len(buffer) {
					return nil, mysql.Bitmap{}, fmt.Errorf(
						"unexpected position; bufferPos: %d, len(buffer): %d", bufferPos, len(buffer))
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
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_BLOB: // TINYBLOB, BLOB, MEDIUMBLOB, LONGBLOB
			addr, notNull := descriptor.GetBytesAddr(tupleIdx, tuple)
			if notNull {
				bytes, err := encodeBytesFromAddress(ctx, addr, ns, typ)
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				data = append(data, bytes...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_TEXT: // TINYTEXT, TEXT, MEDIUMTEXT, LONGTEXT
			addr, notNull := descriptor.GetStringAddr(tupleIdx, tuple)
			if notNull {
				bytes, err := encodeBytesFromAddress(ctx, addr, ns, typ)
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
				data = append(data, bytes...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_GEOMETRY: // GEOMETRY
			// NOTE: Using descriptor.GetGeometry() here will return the stored bytes, but
			//       we need to use tree.GetField() so that they get deserialized into WKB
			//       format bytes for the correct MySQL binlog serialization format.
			geometry, err := tree.GetField(ctx, descriptor, tupleIdx, tuple, ns)
			if err != nil {
				return nil, mysql.Bitmap{}, err
			}
			if geometry != nil {
				geoType := geometry.(gmstypes.GeometryValue)
				bytes := geoType.Serialize()
				bytesLengthBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(bytesLengthBuffer, uint32(len(bytes)))
				data = append(data, bytesLengthBuffer...)
				data = append(data, bytes...)
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		case query.Type_JSON: // JSON
			json, err := tree.GetField(ctx, descriptor, tupleIdx, tuple, ns)
			if err != nil {
				return nil, mysql.Bitmap{}, err
			}
			if json != nil {
				jsonDoc, ok := json.(gmstypes.JSONDocument)
				if !ok {
					return nil, mysql.Bitmap{}, fmt.Errorf("supported JSON type: %T", json)
				}

				jsonBuffer, err := encodeJsonDoc(jsonDoc)
				jsonLengthBuffer := make([]byte, 4)
				binary.LittleEndian.PutUint32(jsonLengthBuffer, uint32(len(jsonBuffer)))
				data = append(data, jsonLengthBuffer...)
				data = append(data, jsonBuffer...)
				if err != nil {
					return nil, mysql.Bitmap{}, err
				}
			} else {
				nullBitmap.Set(rowIdx, true)
			}

		default:
			return nil, nullBitmap, fmt.Errorf("unsupported type: %v (%d)\n", typ.String(), typ.Type())
		}
	}

	return data, nullBitmap, nil
}

// encodeBytes encodes the bytes from a BINARY, VARBINARY, CHAR, or VARCHAR field, passed in |data|,
// into the returned byte slice, by first encoding the length of |data| and then encoding the bytes
// from data. As per MySQL's serialization protocol, the length field is a variable size depending on
// the maximum size of |col|. Fields using 255 or less bytes use a single byte for the length of the
// data, and if a field is declared as larger than 255 bytes, then two bytes are used to encode the
// length of each data value.
func encodeBytes(data []byte, col schema.Column) ([]byte, error) {
	// When the field size is greater than 255 bytes, the serialization format
	// requires us to use 2 bytes for the length of the field value.
	numBytesForLength := 1
	dataLength := len(data)
	if stringType, ok := col.TypeInfo.ToSqlType().(sql.StringType); ok {
		if stringType.MaxByteLength() > 255 {
			numBytesForLength = 2
		}
	} else {
		return nil, fmt.Errorf("expected string type, got %T", col.TypeInfo.ToSqlType())
	}

	buffer := make([]byte, numBytesForLength+dataLength)
	if numBytesForLength == 1 {
		buffer[0] = uint8(dataLength)
	} else if numBytesForLength == 2 {
		binary.LittleEndian.PutUint16(buffer, uint16(dataLength))
	} else {
		return nil, fmt.Errorf("unexpected number of bytes for length: %d", numBytesForLength)
	}
	copy(buffer[numBytesForLength:], data)

	return buffer, nil
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
		return nil, err
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
		return 0, err
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
