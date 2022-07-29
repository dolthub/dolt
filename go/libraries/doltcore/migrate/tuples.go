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

package migrate

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/json"
	geo "github.com/dolthub/dolt/go/store/geometry"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type translator struct {
	builder *val.TupleBuilder

	// maps columns tags to ordinal position
	mapping map[uint64]int

	ns   tree.NodeStore
	pool pool.BuffPool
}

func tupleTranslatorsFromSchema(sch schema.Schema, ns tree.NodeStore) (kt, vt translator) {
	kd := shim.KeyDescriptorFromSchema(sch)
	vd := shim.ValueDescriptorFromSchema(sch)

	keyMap := sch.GetPKCols().TagToIdx
	valMap := sch.GetNonPKCols().TagToIdx

	if !schema.IsKeyless(sch) {
		kt = newTupleTranslator(ns, keyMap, kd)
		vt = newTupleTranslator(ns, valMap, vd)
		return
	}

	// for keyless tables, we must account for the id and cardinality columns
	keyMap2 := map[uint64]int{schema.KeylessRowIdTag: 0}
	valMap2 := map[uint64]int{schema.KeylessRowCardinalityTag: 0}

	// shift positions for other columns
	for tag, pos := range valMap {
		valMap2[tag] = pos + 1
	}
	// assert previous keyMap was empty
	assertTrue(len(keyMap) == 0)

	kt = newTupleTranslator(ns, keyMap2, kd)
	vt = newTupleTranslator(ns, valMap2, vd)
	return
}

func newTupleTranslator(ns tree.NodeStore, mapping map[uint64]int, desc val.TupleDesc) translator {
	return translator{
		builder: val.NewTupleBuilder(desc),
		mapping: mapping,
		ns:      ns,
		pool:    pool.NewBuffPool(),
	}
}

// TranslateTuple translates a types.Tuple into a val.Tuple.
func (t translator) TranslateTuple(ctx context.Context, tup types.Tuple) (val.Tuple, error) {
	if !isEven(tup.Len()) {
		return nil, fmt.Errorf("expected even-legnth tuple (len %d)", tup.Len())
	}

	var tag uint64
	err := tup.IterFields(func(i uint64, value types.Value) (stop bool, err error) {
		// even fields are column tags, odd fields are column values
		if isEven(i) {
			tag = uint64(value.(types.Uint))
		} else {
			// |tag| set in previous iteration
			pos, ok := t.mapping[tag]
			if ok {
				err = translateNomsField(ctx, t.ns, value, pos, t.builder)
				stop = err != nil
			} // else tombstone column
		}
		return
	})
	if err != nil {
		return nil, err
	}

	defer func() {
		if r := recover(); r != nil {
			panic(tup.String())
		}
	}()

	return t.builder.Build(t.pool), nil
}

func translateNomsField(ctx context.Context, ns tree.NodeStore, value types.Value, idx int, b *val.TupleBuilder) error {
	nk := value.Kind()
	switch nk {
	case types.NullKind:
		return nil // todo(andy): log warning?

	case types.UintKind:
		translateUintField(value.(types.Uint), idx, b)

	case types.IntKind:
		translateIntField(value.(types.Int), idx, b)

	case types.FloatKind:
		translateFloatField(value.(types.Float), idx, b)

	case types.TimestampKind:
		translateTimestampField(value.(types.Timestamp), idx, b)

	case types.BoolKind:
		b.PutBool(idx, bool(value.(types.Bool)))

	case types.StringKind:
		return translateStringField(ctx, ns, value.(types.String), idx, b)

	case types.UUIDKind:
		uuid := value.(types.UUID)
		b.PutHash128(idx, uuid[:])

	case types.InlineBlobKind:
		b.PutByteString(idx, value.(types.InlineBlob))

	case types.DecimalKind:
		b.PutDecimal(idx, decimal.Decimal(value.(types.Decimal)))

	case types.GeometryKind:
		v := value.(types.Geometry).Inner
		translateGeometryField(v, idx, b)

	case types.PointKind, types.LineStringKind, types.PolygonKind:
		translateGeometryField(value, idx, b)

	case types.JSONKind:
		return translateJSONField(ctx, ns, value.(types.JSON), idx, b)

	case types.BlobKind:
		return translateBlobField(ctx, ns, value.(types.Blob), idx, b)

	default:
		return fmt.Errorf("encountered unexpected NomsKind %s",
			types.KindToString[nk])
	}
	return nil
}

func translateUintField(value types.Uint, idx int, b *val.TupleBuilder) {
	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.Uint8Enc:
		b.PutUint8(idx, uint8(value))
	case val.Uint16Enc:
		b.PutUint16(idx, uint16(value))
	case val.Uint32Enc:
		b.PutUint32(idx, uint32(value))
	case val.Uint64Enc:
		b.PutUint64(idx, uint64(value))
	case val.EnumEnc:
		b.PutEnum(idx, uint16(value))
	case val.SetEnc:
		b.PutSet(idx, uint64(value))
	default:
		panic(fmt.Sprintf("unexpected encoding for uint (%d)", typ.Enc))
	}
}

func translateIntField(value types.Int, idx int, b *val.TupleBuilder) {
	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.Int8Enc:
		b.PutInt8(idx, int8(value))
	case val.Int16Enc:
		b.PutInt16(idx, int16(value))
	case val.Int32Enc:
		b.PutInt32(idx, int32(value))
	case val.Int64Enc:
		b.PutInt64(idx, int64(value))
	case val.YearEnc:
		b.PutYear(idx, int16(value))
	case val.TimeEnc:
		b.PutSqlTime(idx, int64(value))
	default:
		panic(fmt.Sprintf("unexpected encoding for int (%d)", typ.Enc))
	}
}

func translateFloatField(value types.Float, idx int, b *val.TupleBuilder) {
	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.Float32Enc:
		b.PutFloat32(idx, float32(value))
	case val.Float64Enc:
		b.PutFloat64(idx, float64(value))
	default:
		panic(fmt.Sprintf("unexpected encoding for float (%d)", typ.Enc))
	}
}

func translateStringField(ctx context.Context, ns tree.NodeStore, value types.String, idx int, b *val.TupleBuilder) error {
	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.StringEnc:
		b.PutString(idx, string(value))

	case val.StringAddrEnc:
		// note: previously, TEXT fields were serialized as types.String
		rd := strings.NewReader(string(value))
		t, err := tree.NewImmutableTreeFromReader(ctx, rd, ns, tree.DefaultFixedChunkLength)
		if err != nil {
			return err
		}
		b.PutStringAddr(idx, t.Addr)

	default:
		panic(fmt.Sprintf("unexpected encoding for string (%d)", typ.Enc))
	}
	return nil
}

func translateTimestampField(value types.Timestamp, idx int, b *val.TupleBuilder) {
	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.DateEnc:
		b.PutDate(idx, time.Time(value))
	case val.DatetimeEnc:
		b.PutDatetime(idx, time.Time(value))
	default:
		panic(fmt.Sprintf("unexpected encoding for timestamp (%d)", typ.Enc))
	}
}

func translateGeometryField(value types.Value, idx int, b *val.TupleBuilder) {
	nk := value.Kind()
	switch nk {
	case types.PointKind:
		p := typeinfo.ConvertTypesPointToSQLPoint(value.(types.Point))
		b.PutGeometry(idx, geo.SerializePoint(p))

	case types.LineStringKind:
		l := typeinfo.ConvertTypesLineStringToSQLLineString(value.(types.LineString))
		b.PutGeometry(idx, geo.SerializeLineString(l))

	case types.PolygonKind:
		p := typeinfo.ConvertTypesPolygonToSQLPolygon(value.(types.Polygon))
		b.PutGeometry(idx, geo.SerializePolygon(p))

	default:
		panic(fmt.Sprintf("unexpected NomsKind for geometry (%d)", nk))
	}
}

func translateJSONField(ctx context.Context, ns tree.NodeStore, value types.JSON, idx int, b *val.TupleBuilder) error {
	s, err := json.NomsJSONToString(ctx, json.NomsJSON(value))
	if err != nil {
		return err
	}
	buf := bytes.NewBuffer([]byte(s))

	t, err := tree.NewImmutableTreeFromReader(ctx, buf, ns, tree.DefaultFixedChunkLength)
	if err != nil {
		return err
	}
	b.PutJSONAddr(idx, t.Addr)
	return nil
}

func translateBlobField(ctx context.Context, ns tree.NodeStore, value types.Blob, idx int, b *val.TupleBuilder) error {
	t, err := tree.NewImmutableTreeFromReader(ctx, value.Reader(ctx), ns, tree.DefaultFixedChunkLength)
	if err != nil {
		return err
	}

	typ := b.Desc.Types[idx]
	switch typ.Enc {
	case val.BytesAddrEnc:
		b.PutBytesAddr(idx, t.Addr)
	case val.StringAddrEnc:
		b.PutStringAddr(idx, t.Addr)
	default:
		panic(fmt.Sprintf("unexpected encoding for blob (%d)", typ.Enc))
	}
	return nil
}

func isEven(n uint64) bool {
	return n%2 == 0
}
