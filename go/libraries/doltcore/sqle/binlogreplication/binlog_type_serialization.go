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
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	gmstypes "github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/mysql"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

// typeSerializer defines the serialization interface for serializing a value in Dolt's
// storage system to the binary encoded used in MySQL's binlog.
type typeSerializer interface {
	// serialize extracts the value of type |typ| from the |tupleIdx| position of the
	// specified |tuple|, described by |descriptor|, and serializes it to MySQL's binary
	// encoding used in binlog events. For values stored out of band, the |ns| parameter
	// provides the node storage location.
	serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error)

	// metadata returns the MySQL binlog protocol type metadata for the specified |typ|.
	// The first return parameter identifies the data type, and the second return
	// parameter is two bytes of optional metadata that is specific to each type. For
	// example, some types use this optional metadata to indicate how many bytes are
	// used in the encoding for size fields.
	metadata(_ *sql.Context, typ sql.Type) (byte, uint16)
}

// typeSerializersMap maps a SQL type ID to the typeSerializer needed to encode
// values of that type into MySQL's binary encoding.
var typeSerializersMap = map[query.Type]typeSerializer{
	query.Type_FLOAT32: &floatSerializer{},
	query.Type_FLOAT64: &floatSerializer{},

	query.Type_VARCHAR:   &stringSerializer{},
	query.Type_CHAR:      &stringSerializer{},
	query.Type_VARBINARY: &stringSerializer{},
	query.Type_BINARY:    &stringSerializer{},

	query.Type_YEAR:      &yearSerializer{},
	query.Type_DATETIME:  &datetimeSerializer{},
	query.Type_TIMESTAMP: &timestampSerializer{},
	query.Type_DATE:      &dateSerializer{},
	query.Type_TIME:      &timeSerializer{},

	query.Type_INT8:   &integerSerializer{},
	query.Type_INT16:  &integerSerializer{},
	query.Type_INT24:  &integerSerializer{},
	query.Type_INT32:  &integerSerializer{},
	query.Type_INT64:  &integerSerializer{},
	query.Type_UINT8:  &integerSerializer{},
	query.Type_UINT16: &integerSerializer{},
	query.Type_UINT24: &integerSerializer{},
	query.Type_UINT32: &integerSerializer{},
	query.Type_UINT64: &integerSerializer{},

	query.Type_DECIMAL: &decimalSerializer{},

	query.Type_BIT:  &bitSerializer{},
	query.Type_ENUM: &enumSerializer{},
	query.Type_SET:  &setSerializer{},

	query.Type_BLOB:     &blobSerializer{},
	query.Type_TEXT:     &textSerializer{},
	query.Type_JSON:     &jsonSerializer{},
	query.Type_GEOMETRY: &geometrySerializer{},
}

// integerSerializer loads a signed or unsigned INT8/16/24/32/64 type value from Dolt's
// storage and encodes it into MySQL's binary encoding.
type integerSerializer struct{}

var _ typeSerializer = (*integerSerializer)(nil)

func (i integerSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	switch typ.Type() {
	case query.Type_INT8: // TINYINT
		intValue, notNull := descriptor.GetInt8(tupleIdx, tuple)
		if notNull {
			data = append(data, byte(intValue))
		}

	case query.Type_UINT8: // TINYINT UNSIGNED
		intValue, notNull := descriptor.GetUint8(tupleIdx, tuple)
		if notNull {
			data = append(data, intValue)
		}

	case query.Type_INT16: // SMALLINT
		intValue, notNull := descriptor.GetInt16(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 2)
			binary.LittleEndian.PutUint16(data, uint16(intValue))
		}

	case query.Type_UINT16: // SMALLINT UNSIGNED
		intValue, notNull := descriptor.GetUint16(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 2)
			binary.LittleEndian.PutUint16(data, intValue)
		}

	case query.Type_INT24: // MEDIUMINT
		intValue, notNull := descriptor.GetInt32(tupleIdx, tuple)
		if notNull {
			buffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(buffer, uint32(intValue))
			data = buffer[0:3]
		}

	case query.Type_UINT24: // MEDIUMINT UNSIGNED
		intValue, notNull := descriptor.GetUint32(tupleIdx, tuple)
		if notNull {
			tempBuffer := make([]byte, 4)
			binary.LittleEndian.PutUint32(tempBuffer, intValue)
			data = tempBuffer[0:3]
		}

	case query.Type_INT32: // INT
		intValue, notNull := descriptor.GetInt32(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 4)
			binary.LittleEndian.PutUint32(data, uint32(intValue))
		}

	case query.Type_UINT32: // INT UNSIGNED
		intValue, notNull := descriptor.GetUint32(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 4)
			binary.LittleEndian.PutUint32(data, intValue)
		}

	case query.Type_INT64: // BIGINT
		intValue, notNull := descriptor.GetInt64(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, uint64(intValue))
		}

	case query.Type_UINT64: // BIGINT UNSIGNED
		intValue, notNull := descriptor.GetUint64(tupleIdx, tuple)
		if notNull {
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, intValue)
		}

	default:
		return nil, fmt.Errorf("unsupported type %s", typ)
	}

	return data, nil
}

func (i integerSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	switch typ.Type() {
	case query.Type_INT8: // TINYINT
		return mysql.TypeTiny, 0
	case query.Type_INT16: // SMALLINT
		return mysql.TypeShort, 0
	case query.Type_INT24: // MEDIUMINT
		return mysql.TypeInt24, 0
	case query.Type_INT32: // INT
		return mysql.TypeLong, 0
	case query.Type_INT64: // BIGINT
		return mysql.TypeLongLong, 0

	case query.Type_UINT8: // TINYINT UNSIGNED
		return mysql.TypeTiny, 0
	case query.Type_UINT16: // SMALLINT UNSIGNED
		return mysql.TypeShort, 0
	case query.Type_UINT24: // MEDIUMINT UNSIGNED
		return mysql.TypeInt24, 0
	case query.Type_UINT32: // INT UNSIGNED
		return mysql.TypeLong, 0
	case query.Type_UINT64: // BIGINT UNSIGNED
		return mysql.TypeLongLong, 0

	default:
		return 0, 0
	}
}

// floatSerializer loads a FLOAT or DOUBLE type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type floatSerializer struct{}

var _ typeSerializer = (*floatSerializer)(nil)

func (f floatSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	switch typ.Type() {
	case query.Type_FLOAT32:
		floatValue, notNull := descriptor.GetFloat32(tupleIdx, tuple)
		if notNull {
			bits := math.Float32bits(floatValue)
			data = make([]byte, 4)
			binary.LittleEndian.PutUint32(data, bits)
		} else {
			return nil, nil
		}

	case query.Type_FLOAT64:
		floatValue, notNull := descriptor.GetFloat64(tupleIdx, tuple)
		if notNull {
			bits := math.Float64bits(floatValue)
			data = make([]byte, 8)
			binary.LittleEndian.PutUint64(data, bits)
		}

	default:
		return nil, fmt.Errorf("unsupported type %v", typ)
	}

	return data, nil
}

func (f floatSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	switch typ.Type() {
	case query.Type_FLOAT32: // FLOAT
		return mysql.TypeFloat, uint16(4)
	case query.Type_FLOAT64: // DOUBLE
		return mysql.TypeDouble, uint16(8)
	default:
		return 0, 0
	}
}

// decimalSerializer loads an DECIMAL type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type decimalSerializer struct{}

var _ typeSerializer = (*decimalSerializer)(nil)

func (d decimalSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
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
		numFractionalDigitUint32s := scale / 9
		numLeftoverFullDigits := numFullDigits - numFullDigitUint32s*9
		numLeftoverFractionalDigits := scale - numFractionalDigitUint32s*9

		length := numFullDigitUint32s*4 + digitsToBytes[numLeftoverFullDigits] +
			numFractionalDigitUint32s*4 + digitsToBytes[numLeftoverFractionalDigits]

		// Ensure the exponent is negative
		if decimalValue.Exponent() > 0 {
			return nil, fmt.Errorf(
				"unexpected positive exponent: %d for decimalValue: %s",
				decimalValue.Exponent(), decimalValue.String())
		}

		// Load the value into a fully padded (to precision and scale) string format,
		// so that we can process the digit groups for the binary encoding.
		absStringVal := decimalValue.Abs().StringFixed(int32(scale))
		stringIntegerVal := absStringVal
		stringFractionalVal := ""
		if scale > 0 {
			firstFractionalDigitIdx := strings.Index(absStringVal, ".") + 1
			stringIntegerVal = absStringVal[:firstFractionalDigitIdx-1]
			stringFractionalVal = absStringVal[firstFractionalDigitIdx:]
		}
		for len(stringIntegerVal) < int(numFullDigits) {
			stringIntegerVal = "0" + stringIntegerVal
		}

		buffer := make([]byte, length)
		bufferPos := 0

		// Fill in leftover digits – these are at the front of the integer component of the decimal
		writtenBytes, err := encodePartialDecimalBits(stringIntegerVal[:numLeftoverFullDigits], buffer[bufferPos:])
		if err != nil {
			return nil, err
		}
		bufferPos += int(writtenBytes)

		// Fill in full digits for the integer component of the decimal
		writtenBytes, remainingString, err := encodeDecimalBits(stringIntegerVal[numLeftoverFullDigits:], buffer[bufferPos:])
		if err != nil {
			return nil, err
		}
		bufferPos += int(writtenBytes)

		if len(remainingString) > 0 {
			return nil, fmt.Errorf(
				"unexpected remaining string after encoding full digits for integer component of decimal value: %s",
				remainingString)
		}

		// If there is a scale, then encode the fractional digits of the number
		if scale > 0 {
			// Fill in full fractional digits
			writtenBytes, remainingString, err = encodeDecimalBits(stringFractionalVal, buffer[bufferPos:])
			if err != nil {
				return nil, err
			}
			bufferPos += int(writtenBytes)

			// Fill in partial fractional digits – these are at the end of the fractional component
			writtenBytes, err = encodePartialDecimalBits(remainingString, buffer[bufferPos:])
			if err != nil {
				return nil, err
			}
			bufferPos += int(writtenBytes)

			if bufferPos != len(buffer) {
				return nil, fmt.Errorf(
					"unexpected position; bufferPos: %d, len(buffer): %d", bufferPos, len(buffer))
			}
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
	}

	return data, nil
}

func (d decimalSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	decimalType := typ.(sql.DecimalType)
	return mysql.TypeNewDecimal,
		(uint16(decimalType.Precision()) << 8) | uint16(decimalType.Scale())
}

// timeSerializer loads a TIME type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type timeSerializer struct{}

var _ typeSerializer = (*timeSerializer)(nil)

func (t timeSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
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
		if negative && microseconds > 0 {
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
	}

	return data, nil
}

func (t timeSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	// TypeTime2 is the newer serialization format for TIME values
	// NOTE: Dolt currently always uses a TIME precision of 6
	return mysql.TypeTime2, uint16(6)
}

// dateSerializer loads a DATE type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type dateSerializer struct{}

var _ typeSerializer = (*dateSerializer)(nil)

func (d dateSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	dateValue, notNull := descriptor.GetDate(tupleIdx, tuple)
	if notNull {
		ymd := uint32(
			dateValue.Year())<<9 |
			uint32(dateValue.Month())<<5 |
			uint32(dateValue.Day())
		temp := make([]byte, 4)
		binary.LittleEndian.PutUint32(temp, ymd)
		data = append(data, temp[:3]...)
	}
	return data, nil
}

func (d dateSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	// NOTE: MySQL still sends the old date type (not mysql.TypeNewDate), so for compatibility we use that here
	return mysql.TypeDate, 0
}

// timestampSerializer loads a TIMESTAMP type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type timestampSerializer struct{}

var _ typeSerializer = (*timestampSerializer)(nil)

func (t timestampSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	timeValue, notNull := descriptor.GetDatetime(tupleIdx, tuple)
	if notNull {
		data = make([]byte, 4)
		binary.BigEndian.PutUint32(data, uint32(timeValue.Unix()))

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
	}
	return data, nil
}

func (t timestampSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	// TypeTimestamp2 means use the new Timestamp format, which was introduced after MySQL 5.6.4,
	// has a more efficient binary representation, and supports fractional seconds.
	dtType := typ.(sql.DatetimeType)
	return mysql.TypeTimestamp2, uint16(dtType.Precision())
}

// datetimeSerializer loads a DATETIME type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type datetimeSerializer struct{}

var _ typeSerializer = (*datetimeSerializer)(nil)

func (d datetimeSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
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
	}
	return data, nil
}

func (d datetimeSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	// TypeDateTime2 means use the new DateTime format, which was introduced after MySQL 5.6.4,
	// has a more efficient binary representation, and supports fractional seconds.
	dtType := typ.(sql.DatetimeType)
	return mysql.TypeDateTime2, uint16(dtType.Precision())
}

// yearSerializer loads a YEAR type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type yearSerializer struct{}

var _ typeSerializer = (*yearSerializer)(nil)

func (y yearSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	intValue, notNull := descriptor.GetYear(tupleIdx, tuple)
	if notNull {
		return []byte{byte(intValue - 1900)}, nil
	}
	return data, nil
}

func (y yearSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	return mysql.TypeYear, 0
}

// stringSerializer loads a CHAR, VARCHAR, BINARY, or VARBINARY type value from Dolt's
// storage and encodes it into MySQL's binary encoding.
type stringSerializer struct{}

func (s *stringSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	var bytes []byte
	var notNull bool

	switch typ.Type() {
	case query.Type_VARBINARY, query.Type_BINARY:
		bytes, notNull = descriptor.GetBytes(tupleIdx, tuple)

	case query.Type_VARCHAR, query.Type_CHAR:
		var stringVal string
		stringVal, notNull = descriptor.GetString(tupleIdx, tuple)
		bytes = []byte(stringVal)

	default:
		return nil, fmt.Errorf("unsupported type %v", typ)
	}

	if notNull {
		return encodeBytes(bytes, typ)
	}
	return nil, nil
}

func (s *stringSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	switch typ.Type() {
	case query.Type_VARCHAR, query.Type_VARBINARY:
		sTyp := typ.(sql.StringType)
		maxFieldLengthInBytes := sTyp.MaxByteLength()
		return mysql.TypeVarchar, uint16(maxFieldLengthInBytes)

	case query.Type_CHAR, query.Type_BINARY:
		sTyp := typ.(sql.StringType)
		maxFieldLengthInBytes := uint16(sTyp.MaxByteLength())
		upperBits := (maxFieldLengthInBytes >> 8) << 12
		lowerBits := maxFieldLengthInBytes & 0xFF
		// This is one of the less obvious parts of the MySQL serialization protocol... Several types use
		// mysql.TypeString as their serialization type in binlog events (i.e. SET, ENUM, CHAR), so the first
		// metadata byte for this serialization type indicates what field type is using this serialization type
		// (i.e. SET, ENUM, or CHAR), and the second metadata byte indicates the number of bytes needed to serialize
		// a type value. However, for CHAR, that second byte isn't enough, since it can only represent up to 255
		// bytes. For sizes larger than that, we need to find two more bits. MySQL does this by reusing the third
		// and fourth bits from the first metadata byte. By XOR'ing them against the known mysql.TypeString value
		// in that byte, MySQL is able to reuse those two bits and extend the second metadata byte enough to
		// account for the max size of CHAR fields (255 chars).
		return mysql.TypeString, ((mysql.TypeString << 8) ^ upperBits) | lowerBits

	default:
		return 0, 0
	}
}

// bitSerializer loads a BIT type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type bitSerializer struct{}

var _ typeSerializer = (*bitSerializer)(nil)

func (b bitSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	// NOTE: descriptor.GetBit(tupleIdx, tuple) doesn't work here. BIT datatypes are described with a Uint64
	//       encoding, so trying to use GetBit results in an error. At the data level, both are stored with a
	//       uint64 value, so they are compatible, but we seem to only use Uint64 in the descriptor.
	bitValue, notNull := descriptor.GetUint64(tupleIdx, tuple)
	if notNull {
		bitType := typ.(gmstypes.BitType)
		numBytes := int((bitType.NumberOfBits() + 7) / 8)
		buffer := make([]byte, 8)
		binary.BigEndian.PutUint64(buffer, bitValue)
		data = buffer[len(buffer)-numBytes:]
	}
	return data, nil
}

func (b bitSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	bitType := typ.(gmstypes.BitType)
	// bitmap length is in metadata, as:
	// upper 8 bits: bytes length
	// lower 8 bits: bit length
	numBytes := bitType.NumberOfBits() / 8
	numBits := bitType.NumberOfBits() % 8
	return mysql.TypeBit, uint16(numBytes)<<8 | uint16(numBits)
}

// setSerializer loads a SET type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type setSerializer struct{}

var _ typeSerializer = (*setSerializer)(nil)

func (s setSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	setValue, notNull := descriptor.GetSet(tupleIdx, tuple)
	if notNull {
		setType := typ.(gmstypes.SetType)
		numElements := setType.NumberOfElements()
		numBytes := int((numElements + 7) / 8)
		buffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(buffer, setValue)
		data = buffer[:numBytes]
	}
	return data, nil
}

func (s setSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	setType := typ.(gmstypes.SetType)
	numElements := setType.NumberOfElements()
	numBytes := (numElements + 7) / 8
	return mysql.TypeString, mysql.TypeSet<<8 | numBytes
}

// enumSerializer loads an ENUM type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type enumSerializer struct{}

var _ typeSerializer = (*enumSerializer)(nil)

func (e enumSerializer) serialize(_ *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	enumValue, notNull := descriptor.GetEnum(tupleIdx, tuple)
	if notNull {
		enumType := typ.(gmstypes.EnumType)
		if enumType.NumberOfElements() <= 0xFF {
			data = []byte{byte(enumValue)}
		} else {
			data = make([]byte, 2)
			binary.LittleEndian.PutUint16(data, enumValue)
		}
	}
	return data, nil
}

func (e enumSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	enumType := typ.(gmstypes.EnumType)
	numElements := enumType.NumberOfElements()
	if numElements <= 0xFF {
		return mysql.TypeString, mysql.TypeEnum<<8 | 1
	} else {
		return mysql.TypeString, mysql.TypeEnum<<8 | 2
	}
}

// blobSerializer loads a BLOB type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type blobSerializer struct{}

var _ typeSerializer = (*blobSerializer)(nil)

func (b blobSerializer) serialize(ctx *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	addr, notNull := descriptor.GetBytesAddr(tupleIdx, tuple)
	if notNull {
		return encodeBytesFromAddress(ctx, addr, ns, typ)
	}
	return nil, nil
}

func (b blobSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	blobType := typ.(sql.StringType)
	if blobType.MaxByteLength() > 0xFFFFFF {
		return mysql.TypeBlob, uint16(4)
	} else if blobType.MaxByteLength() > 0xFFFF {
		return mysql.TypeBlob, uint16(3)
	} else if blobType.MaxByteLength() > 0xFF {
		return mysql.TypeBlob, uint16(2)
	} else {
		return mysql.TypeBlob, uint16(1)
	}
}

// textSerializer loads a JSON type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type textSerializer struct{}

var _ typeSerializer = (*textSerializer)(nil)

func (t textSerializer) serialize(ctx *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	addr, notNull := descriptor.GetStringAddr(tupleIdx, tuple)
	if notNull {
		return encodeBytesFromAddress(ctx, addr, ns, typ)
	}
	return nil, nil
}

func (t textSerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	blobType := typ.(sql.StringType)
	if blobType.MaxByteLength() > 0xFFFFFF {
		return mysql.TypeBlob, uint16(4)
	} else if blobType.MaxByteLength() > 0xFFFF {
		return mysql.TypeBlob, uint16(3)
	} else if blobType.MaxByteLength() > 0xFF {
		return mysql.TypeBlob, uint16(2)
	} else {
		return mysql.TypeBlob, uint16(1)
	}
}

// jsonSerializer loads a JSON type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type jsonSerializer struct{}

var _ typeSerializer = (*jsonSerializer)(nil)

func (j jsonSerializer) serialize(ctx *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	json, err := tree.GetField(ctx, descriptor, tupleIdx, tuple, ns)
	if err != nil {
		return nil, err
	}
	if json != nil {
		jsonDoc, ok := json.(sql.JSONWrapper)
		if !ok {
			return nil, fmt.Errorf("unsupported JSON type: %T", json)
		}

		jsonBuffer, err := encodeJsonDoc(jsonDoc)
		jsonLengthBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(jsonLengthBuffer, uint32(len(jsonBuffer)))
		data = append(data, jsonLengthBuffer...)
		data = append(data, jsonBuffer...)
		if err != nil {
			return nil, err
		}
	}
	return data, nil
}

func (j jsonSerializer) metadata(ctx *sql.Context, typ sql.Type) (byte, uint16) {
	jsonType := typ.(gmstypes.JsonType)
	maxByteLength := jsonType.MaxTextResponseByteLength(ctx)
	if maxByteLength > 0xFFFFFF {
		return mysql.TypeJSON, uint16(4)
	} else if maxByteLength > 0xFFFF {
		return mysql.TypeJSON, uint16(3)
	} else if maxByteLength > 0xFF {
		return mysql.TypeJSON, uint16(2)
	} else {
		return mysql.TypeJSON, uint16(1)
	}
}

// geometrySerializer loads a geometry type value from Dolt's storage and encodes it
// into MySQL's binary encoding.
type geometrySerializer struct{}

var _ typeSerializer = (*geometrySerializer)(nil)

func (g geometrySerializer) serialize(ctx *sql.Context, typ sql.Type, descriptor val.TupleDesc, tuple val.Tuple, tupleIdx int, ns tree.NodeStore) (data []byte, err error) {
	// NOTE: Using descriptor.GetGeometry() here will return the stored bytes, but
	//       we need to use tree.GetField() so that they get deserialized into WKB
	//       format bytes for the correct MySQL binlog serialization format.
	geometry, err := tree.GetField(ctx, descriptor, tupleIdx, tuple, ns)
	if err != nil {
		return nil, err
	}
	if geometry != nil {
		geoType := geometry.(gmstypes.GeometryValue)
		bytes := geoType.Serialize()
		bytesLengthBuffer := make([]byte, 4)
		binary.LittleEndian.PutUint32(bytesLengthBuffer, uint32(len(bytes)))
		data = append(data, bytesLengthBuffer...)
		data = append(data, bytes...)
	}
	return data, nil
}

func (g geometrySerializer) metadata(_ *sql.Context, typ sql.Type) (byte, uint16) {
	return mysql.TypeGeometry, uint16(4)
}

//
// Helper Functions
//

// encodeBytes encodes the bytes from a BINARY, VARBINARY, CHAR, or VARCHAR field, passed in |data|,
// into the returned byte slice, by first encoding the length of |data| and then encoding the bytes
// from data. As per MySQL's serialization protocol, the length field is a variable size depending on
// the maximum size of |typ|. Fields using 255 or less bytes use a single byte for the length of the
// data, and if a field is declared as larger than 255 bytes, then two bytes are used to encode the
// length of each data value.
func encodeBytes(data []byte, typ sql.Type) ([]byte, error) {
	// When the field size is greater than 255 bytes, the serialization format
	// requires us to use 2 bytes for the length of the field value.
	numBytesForLength := 1
	dataLength := len(data)
	if stringType, ok := typ.(sql.StringType); ok {
		if stringType.MaxByteLength() > 255 {
			numBytesForLength = 2
		}
	} else {
		return nil, fmt.Errorf("expected string type, got %T", typ)
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
