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
	"encoding/binary"
	"fmt"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function"

	"github.com/dolthub/dolt/go/store/val"
)

// GetField reads the value from the ith field of the Tuple as an interface{}.
func GetField(td val.TupleDesc, i int, tup val.Tuple) (v interface{}) {
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
	case val.DecimalEnc:
		v, ok = td.GetDecimal(i, tup)
	case val.TimeEnc:
		v, ok = td.GetSqlTime(i, tup)
	case val.YearEnc:
		v, ok = td.GetYear(i, tup)
	case val.TimestampEnc, val.DateEnc, val.DatetimeEnc:
		v, ok = td.GetTimestamp(i, tup)
	case val.StringEnc:
		v, ok = td.GetString(i, tup)
	case val.BytesEnc:
		v, ok = td.GetBytes(i, tup)
	case val.JSONEnc:
		var js interface{}
		js, ok = td.GetJSON(i, tup)
		if ok {
			v = sql.JSONDocument{Val: js}
		}
	case val.GeometryEnc:
		var geo []byte
		geo, ok = td.GetGeometry(i, tup)
		if ok {
			v = deserializeGeometry(geo)
		}
	default:
		panic("unknown val.encoding")
	}
	if !ok {
		return nil
	}
	return v
}

// PutField writes an interface{} to the ith field of the Tuple being built.
func PutField(tb *val.TupleBuilder, i int, v interface{}) {
	if v == nil {
		return // NULL
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
	case val.DecimalEnc:
		tb.PutDecimal(i, v.(string))
	case val.TimeEnc:
		tb.PutSqlTime(i, v.(string))
	case val.YearEnc:
		tb.PutYear(i, v.(int16))
	case val.DateEnc, val.DatetimeEnc, val.TimestampEnc:
		tb.PutTimestamp(i, v.(time.Time))
	case val.StringEnc:
		tb.PutString(i, v.(string))
	case val.BytesEnc:
		if s, ok := v.(string); ok {
			v = []byte(s)
		}
		tb.PutBytes(i, v.([]byte))
	case val.GeometryEnc:
		// todo(andy): remove GMS dependency
		tb.PutGeometry(i, serializeGeometry(v))
	case val.JSONEnc:
		// todo(andy): remove GMS dependency
		tb.PutJSON(i, v.(sql.JSONDocument).Val)
	default:
		panic(fmt.Sprintf("unknown encoding %v %v", enc, v))
	}
}

func deserializeGeometry(buf []byte) (v interface{}) {
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

// todo(andy): remove GMS dependency
//  have the engine pass serialized bytes

const (
	sridSize       = val.ByteSize(4)
	endianSize     = val.ByteSize(1)
	typeSize       = val.ByteSize(4)
	ewkbHeaderSize = sridSize + endianSize + typeSize
)

const (
	pointType      = uint32(1)
	linestringType = uint32(2)
	polygonType    = uint32(3)

	littleEndian = uint8(1)
)

type ewkbHeader struct {
	srid   uint32
	endian uint8
	typ    uint32
}

func (h ewkbHeader) writeTo(buf []byte) {
	expectSize(buf, ewkbHeaderSize)
	binary.LittleEndian.PutUint32(buf[:sridSize], h.srid)
	buf[sridSize] = h.endian
	binary.LittleEndian.PutUint32(buf[sridSize+endianSize:ewkbHeaderSize], h.typ)
}

func readHeaderFrom(buf []byte) (h ewkbHeader) {
	expectSize(buf, ewkbHeaderSize)
	h.srid = binary.LittleEndian.Uint32(buf[:sridSize])
	h.endian = uint8(buf[sridSize])
	h.typ = binary.LittleEndian.Uint32(buf[sridSize+endianSize : ewkbHeaderSize])
	return
}

func serializeGeometry(v interface{}) []byte {
	switch t := v.(type) {
	case sql.Point:
		return serializePoint(t)
	case sql.Linestring:
		return serializeLinestring(t)
	case sql.Polygon:
		return serializePolygon(t)
	default:
		panic(fmt.Sprintf("unknown geometry %v", v))
	}
}

func serializePoint(p sql.Point) (buf []byte) {
	pb := function.PointToBytes(p)
	buf = make([]byte, ewkbHeaderSize+val.ByteSize(len(pb)))
	copy(buf[ewkbHeaderSize:], pb)

	h := ewkbHeader{
		srid:   p.SRID,
		endian: littleEndian,
		typ:    pointType,
	}
	h.writeTo(buf[:ewkbHeaderSize])
	return
}

func serializeLinestring(l sql.Linestring) (buf []byte) {
	lb := function.LineToBytes(l)
	buf = make([]byte, ewkbHeaderSize+val.ByteSize(len(lb)))
	copy(buf[ewkbHeaderSize:], lb)

	h := ewkbHeader{
		srid:   l.SRID,
		endian: littleEndian,
		typ:    linestringType,
	}
	h.writeTo(buf[:ewkbHeaderSize])
	return
}

func serializePolygon(p sql.Polygon) (buf []byte) {
	pb := function.PolyToBytes(p)
	buf = make([]byte, ewkbHeaderSize+val.ByteSize(len(pb)))
	copy(buf[ewkbHeaderSize:], pb)

	h := ewkbHeader{
		srid:   p.SRID,
		endian: littleEndian,
		typ:    polygonType,
	}
	h.writeTo(buf[:ewkbHeaderSize])
	return
}

func expectSize(buf []byte, sz val.ByteSize) {
	if val.ByteSize(len(buf)) != sz {
		panic("byte slice is not of expected size")
	}
}
