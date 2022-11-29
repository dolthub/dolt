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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/val"
)

var ErrValueExceededMaxFieldSize = errors.New("value exceeded max field size of 65kb")

// GetField reads the value from the ith field of the Tuple as an interface{}.
func GetField(ctx context.Context, td val.TupleDesc, i int, tup val.Tuple, ns tree.NodeStore) (v interface{}, err error) {
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
			v = sql.Timespan(t)
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
	case val.BytesAddrEnc:
		var h hash.Hash
		h, ok = td.GetBytesAddr(i, tup)
		if ok {
			v, err = tree.NewByteArray(h, ns).ToBytes(ctx)
		}
	case val.JSONAddrEnc:
		var h hash.Hash
		h, ok = td.GetJSONAddr(i, tup)
		if ok {
			v, err = tree.NewJSONDoc(h, ns).ToJSONDocument(ctx)
		}
	case val.StringAddrEnc:
		var h hash.Hash
		h, ok = td.GetStringAddr(i, tup)
		if ok {
			v, err = tree.NewTextStorage(h, ns).ToString(ctx)
		}
	case val.CommitAddrEnc:
		v, ok = td.GetCommitAddr(i, tup)
	default:
		panic("unknown val.encoding")
	}
	if !ok || err != nil {
		return nil, err
	}
	return v, err
}

// PutField writes an interface{} to the ith field of the Tuple being built.
func PutField(ctx context.Context, ns tree.NodeStore, tb *val.TupleBuilder, i int, v interface{}) error {
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
		tb.PutSqlTime(i, int64(v.(sql.Timespan)))
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
			if len(s) > math.MaxUint16 {
				return ErrValueExceededMaxFieldSize
			}
			v = []byte(s)
		}
		tb.PutByteString(i, v.([]byte))
	case val.Hash128Enc:
		tb.PutHash128(i, v.([]byte))
	case val.GeometryEnc:
		geo := serializeGeometry(v)
		if len(geo) > math.MaxUint16 {
			return ErrValueExceededMaxFieldSize
		}
		tb.PutGeometry(i, serializeGeometry(v))
	case val.JSONAddrEnc:
		buf, err := convJson(v)
		if err != nil {
			return err
		}
		h, err := serializeBytesToAddr(ctx, ns, bytes.NewReader(buf), len(buf))
		if err != nil {
			return err
		}
		tb.PutJSONAddr(i, h)
	case val.BytesAddrEnc:
		h, err := serializeBytesToAddr(ctx, ns, bytes.NewReader(v.([]byte)), len(v.([]byte)))
		if err != nil {
			return err
		}
		tb.PutBytesAddr(i, h)
	case val.StringAddrEnc:
		//todo: v will be []byte after daylon's changes
		h, err := serializeBytesToAddr(ctx, ns, bytes.NewReader([]byte(v.(string))), len(v.(string)))
		if err != nil {
			return err
		}
		tb.PutStringAddr(i, h)
	case val.CommitAddrEnc:
		tb.PutCommitAddr(i, v.(hash.Hash))
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
	srid, _, typ, _ := sql.DeserializeEWKBHeader(buf)
	buf = buf[sql.EWKBHeaderSize:]
	switch typ {
	case sql.WKBPointID:
		v, _, _ = sql.DeserializePoint(buf, false, srid)
	case sql.WKBLineID:
		v, _, _ = sql.DeserializeLine(buf, false, srid)
	case sql.WKBPolyID:
		v, _, _ = sql.DeserializePoly(buf, false, srid)
	case sql.WKBMultiPointID:
		v, _, _ = sql.DeserializeMPoint(buf, false, srid)
	case sql.WKBMultiLineID:
		v, _, _ = sql.DeserializeMLine(buf, false, srid)
	case sql.WKBMultiPolyID:
		v, _, _ = sql.DeserializeMPoly(buf, false, srid)
	case sql.WKBGeomCollID:
		v, _, _ = sql.DeserializeGeomColl(buf, false, srid)
	default:
		panic(fmt.Sprintf("unknown geometry type %d", typ))
	}
	return
}

func serializeGeometry(v interface{}) []byte {
	switch t := v.(type) {
	case sql.GeometryValue:
		return t.Serialize()
	default:
		panic(fmt.Sprintf("unknown geometry %v", v))
	}
}

func serializeBytesToAddr(ctx context.Context, ns tree.NodeStore, r io.Reader, dataSize int) (hash.Hash, error) {
	bb := ns.BlobBuilder()
	bb.Init(dataSize)
	_, addr, err := bb.Chunk(ctx, r)
	if err != nil {
		return hash.Hash{}, err
	}
	return addr, nil
}

func convJson(v interface{}) (buf []byte, err error) {
	v, err = sql.JSON.Convert(v)
	if err != nil {
		return nil, err
	}
	return json.Marshal(v.(sql.JSONDocument).Val)
}
