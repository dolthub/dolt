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

package schema

import (
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dolthub/vitess/go/vt/proto/query"

	"github.com/dolthub/dolt/go/gen/fb/serial"
)

// EncodingFromSqlType returns a serial.Encoding for a sql.Type.
func EncodingFromSqlType(typ sql.Type) serial.Encoding {
	if extendedType, ok := typ.(types.ExtendedType); ok {
		switch extendedType.MaxSerializedWidth() {
		case types.ExtendedTypeSerializedWidth_64K:
			return serial.EncodingExtended
		case types.ExtendedTypeSerializedWidth_Unbounded:
			return serial.EncodingExtendedAddr
		default:
			panic(fmt.Errorf("unknown serialization width"))
		}
	}
	return EncodingFromQueryType(typ.Type())
}

// Tests can set this variable to true in order to force Dolt to use TOAST encoding for TEXT and BLOB columns.
var UseToastTypes = false

// EncodingFromQueryType returns a serial.Encoding for a query.Type.
func EncodingFromQueryType(typ query.Type) serial.Encoding {
	switch typ {
	case query.Type_INT8:
		return serial.EncodingInt8
	case query.Type_UINT8:
		return serial.EncodingUint8
	case query.Type_INT16:
		return serial.EncodingInt16
	case query.Type_UINT16:
		return serial.EncodingUint16
	case query.Type_INT24:
		return serial.EncodingInt32
	case query.Type_UINT24:
		return serial.EncodingUint32
	case query.Type_INT32:
		return serial.EncodingInt32
	case query.Type_UINT32:
		return serial.EncodingUint32
	case query.Type_INT64:
		return serial.EncodingInt64
	case query.Type_UINT64:
		return serial.EncodingUint64
	case query.Type_FLOAT32:
		return serial.EncodingFloat32
	case query.Type_FLOAT64:
		return serial.EncodingFloat64
	case query.Type_BIT:
		return serial.EncodingUint64
	case query.Type_DECIMAL:
		return serial.EncodingDecimal
	case query.Type_YEAR:
		return serial.EncodingYear
	case query.Type_DATE:
		return serial.EncodingDate
	case query.Type_TIME:
		return serial.EncodingTime
	case query.Type_TIMESTAMP:
		return serial.EncodingDatetime
	case query.Type_DATETIME:
		return serial.EncodingDatetime
	case query.Type_ENUM:
		return serial.EncodingEnum
	case query.Type_SET:
		return serial.EncodingSet
	case query.Type_BINARY:
		return serial.EncodingBytes
	case query.Type_VARBINARY:
		return serial.EncodingBytes
	case query.Type_CHAR:
		return serial.EncodingString
	case query.Type_VARCHAR:
		return serial.EncodingString
	case query.Type_GEOMETRY:
		return serial.EncodingGeomAddr
	case query.Type_JSON:
		return serial.EncodingJSONAddr
	case query.Type_BLOB:
		if UseToastTypes {
			return serial.EncodingBytesToast
		}
		return serial.EncodingBytesAddr
	case query.Type_TEXT:
		if UseToastTypes {
			return serial.EncodingStringToast
		}
		return serial.EncodingStringAddr
	default:
		panic(fmt.Sprintf("unknown encoding %v", typ))
	}
}
