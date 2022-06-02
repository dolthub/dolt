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

package index

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	geo "github.com/dolthub/dolt/go/store/geometry"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

// todo(andy): this should go in GMS
func DenormalizeRow(sch sql.Schema, row sql.Row) (sql.Row, error) {
	var err error
	for i := range row {
		if row[i] == nil {
			continue
		}
		switch typ := sch[i].Type.(type) {
		case sql.DecimalType:
			row[i] = row[i].(decimal.Decimal).String()
		case sql.EnumType:
			row[i], err = typ.Unmarshal(int64(row[i].(uint16)))
		case sql.SetType:
			row[i], err = typ.Unmarshal(row[i].(uint64))
		default:
		}
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

// todo(andy): this should go in GMS
func NormalizeRow(sch sql.Schema, row sql.Row) (sql.Row, error) {
	var err error
	for i := range row {
		if row[i] == nil {
			continue
		}
		switch typ := sch[i].Type.(type) {
		case sql.DecimalType:
			row[i], err = decimal.NewFromString(row[i].(string))
		case sql.EnumType:
			var v int64
			v, err = typ.Marshal(row[i])
			row[i] = uint16(v)
		case sql.SetType:
			row[i], err = typ.Marshal(row[i])
		default:
		}
		if err != nil {
			return nil, err
		}
	}
	return row, nil
}

// GetField reads the value from the ith field of the Tuple as an interface{}.
func GetField(td val.TupleDesc, i int, tup val.Tuple) (v interface{}, err error) {
	var ok bool
	switch td.Types[i].Enc {
	case val.Int8Enc:
		v, ok = td.GetInt8(i, tup)
	case val.Uint8Enc:
		v, ok = td.GetUint8(i, tup)
	case val.Int16Enc:
		v, ok = td.GetInt16(i, tup)
	case val.Uint16Enc:
		v, ok = td.GetUint16(i, tup)
	case val.Int32Enc:
		v, ok = td.GetInt32(i, tup)
	case val.Uint32Enc:
		v, ok = td.GetUint32(i, tup)
	case val.Int64Enc:
		v, ok = td.GetInt64(i, tup)
	case val.Uint64Enc:
		v, ok = td.GetUint64(i, tup)
	case val.Float32Enc:
		v, ok = td.GetFloat32(i, tup)
	case val.Float64Enc:
		v, ok = td.GetFloat64(i, tup)
	case val.Bit64Enc:
		v, ok = td.GetBit(i, tup)
	case val.DecimalEnc:
		v, ok = td.GetDecimal(i, tup)
	case val.YearEnc:
		v, ok = td.GetYear(i, tup)
	case val.DateEnc:
		v, ok = td.GetDate(i, tup)
	case val.TimeEnc:
		var t int64
		t, ok = td.GetSqlTime(i, tup)
		if ok {
			v, err = deserializeTime(t)
		}
	case val.DatetimeEnc:
		v, ok = td.GetDatetime(i, tup)
	case val.EnumEnc:
		v, ok = td.GetEnum(i, tup)
	case val.SetEnc:
		v, ok = td.GetSet(i, tup)
	case val.StringEnc:
		v, ok = td.GetString(i, tup)
	case val.ByteStringEnc:
		v, ok = td.GetBytes(i, tup)
	case val.JSONEnc:
		var buf []byte
		buf, ok = td.GetJSON(i, tup)
		if ok {
			var doc sql.JSONDocument
			err = json.Unmarshal(buf, &doc.Val)
			v = doc
		}
	case val.GeometryEnc:
		var buf []byte
		buf, ok = td.GetGeometry(i, tup)
		if ok {
			v = deserializeGeometry(buf)
		}
	case val.Hash128Enc:
		v, ok = td.GetHash128(i, tup)
	default:
		panic("unknown val.encoding")
	}
	if !ok || err != nil {
		return nil, err
	}
	return v, err
}

// PutField writes an interface{} to the ith field of the Tuple being built.
func PutField(tb *val.TupleBuilder, i int, v interface{}) error {
	if v == nil {
		return nil // NULL
	}

	enc := tb.Desc.Types[i].Enc
	switch enc {
	case val.Int8Enc:
		tb.PutInt8(i, int8(convInt(v)))
	case val.Uint8Enc:
		tb.PutUint8(i, uint8(convUint(v)))
	case val.Int16Enc:
		tb.PutInt16(i, int16(convInt(v)))
	case val.Uint16Enc:
		tb.PutUint16(i, uint16(convUint(v)))
	case val.Int32Enc:
		tb.PutInt32(i, int32(convInt(v)))
	case val.Uint32Enc:
		tb.PutUint32(i, uint32(convUint(v)))
	case val.Int64Enc:
		tb.PutInt64(i, int64(convInt(v)))
	case val.Uint64Enc:
		tb.PutUint64(i, uint64(convUint(v)))
	case val.Float32Enc:
		tb.PutFloat32(i, v.(float32))
	case val.Float64Enc:
		tb.PutFloat64(i, v.(float64))
	case val.Bit64Enc:
		tb.PutBit(i, uint64(convUint(v)))
	case val.DecimalEnc:
		tb.PutDecimal(i, v.(decimal.Decimal))
	case val.YearEnc:
		tb.PutYear(i, v.(int16))
	case val.DateEnc:
		tb.PutDate(i, v.(time.Time))
	case val.TimeEnc:
		t, err := serializeTime(v)
		if err != nil {
			return err
		}
		tb.PutSqlTime(i, t)
	case val.DatetimeEnc:
		tb.PutDatetime(i, v.(time.Time))
	case val.EnumEnc:
		tb.PutEnum(i, v.(uint16))
	case val.SetEnc:
		tb.PutSet(i, v.(uint64))
	case val.StringEnc:
		tb.PutString(i, v.(string))
	case val.ByteStringEnc:
		if s, ok := v.(string); ok {
			v = []byte(s)
		}
		tb.PutByteString(i, v.([]byte))
	case val.GeometryEnc:
		tb.PutGeometry(i, serializeGeometry(v))
	case val.JSONEnc:
		buf, err := convJson(v)
		if err != nil {
			return err
		}
		tb.PutJSON(i, buf)
	case val.Hash128Enc:
		tb.PutHash128(i, v.([]byte))
	default:
		panic(fmt.Sprintf("unknown encoding %v %v", enc, v))
	}
	return nil
}

func convInt(v interface{}) int {
	switch i := v.(type) {
	case int:
		return i
	case int8:
		return int(i)
	case uint8:
		return int(i)
	case int16:
		return int(i)
	case uint16:
		return int(i)
	case int32:
		return int(i)
	case uint32:
		return int(i)
	case int64:
		return int(i)
	case uint64:
		return int(i)
	default:
		panic("impossible conversion")
	}
}

func convUint(v interface{}) uint {
	switch i := v.(type) {
	case uint:
		return i
	case int:
		return uint(i)
	case int8:
		return uint(i)
	case uint8:
		return uint(i)
	case int16:
		return uint(i)
	case uint16:
		return uint(i)
	case int32:
		return uint(i)
	case uint32:
		return uint(i)
	case int64:
		return uint(i)
	case uint64:
		return uint(i)
	default:
		panic("impossible conversion")
	}
}

func deserializeGeometry(buf []byte) (v interface{}) {
	srid, _, typ := geo.ParseEWKBHeader(buf)
	buf = buf[geo.EWKBHeaderSize:]
	switch typ {
	case geo.PointType:
		v = geo.DeserializePoint(buf, srid)
	case geo.LineStringType:
		v = geo.DeserializeLineString(buf, srid)
	case geo.PolygonType:
		v = geo.DeserializePolygon(srid, buf)
	default:
		panic(fmt.Sprintf("unknown geometry type %d", typ))
	}
	return
}

func serializeGeometry(v interface{}) []byte {
	switch t := v.(type) {
	case sql.Point:
		return geo.SerializePoint(t)
	case sql.LineString:
		return geo.SerializeLineString(t)
	case sql.Polygon:
		return geo.SerializePolygon(t)
	default:
		panic(fmt.Sprintf("unknown geometry %v", v))
	}
}

func convJson(v interface{}) (buf []byte, err error) {
	v, err = sql.JSON.Convert(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v.(sql.JSONDocument).Val)
}

func deserializeTime(v int64) (interface{}, error) {
	return typeinfo.TimeType.ConvertNomsValueToValue(types.Int(v))
}

func serializeTime(v interface{}) (int64, error) {
	i, err := typeinfo.TimeType.ConvertValueToNomsValue(nil, nil, v)
	if err != nil {
		return 0, err
	}
	return int64(i.(types.Int)), nil
}
