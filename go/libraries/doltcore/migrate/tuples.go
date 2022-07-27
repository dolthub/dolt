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
	"context"
	"fmt"
	"time"

	"github.com/shopspring/decimal"

	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/prolly/shim"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/dolthub/dolt/go/store/val"
)

type translator struct {
	builder *val.TupleBuilder
	mapping map[uint64]int

	pool pool.BuffPool
}

func tupleTranslatorsFromSchema(sch schema.Schema, old types.ValueReadWriter) (kt, vt translator) {
	kd := shim.KeyDescriptorFromSchema(sch)
	kt = newTupleTranslator(sch.GetPKCols(), kd)
	vd := shim.ValueDescriptorFromSchema(sch)
	vt = newTupleTranslator(sch.GetNonPKCols(), vd)
	return
}

func newTupleTranslator(cols *schema.ColCollection, desc val.TupleDesc) translator {
	return translator{
		builder: val.NewTupleBuilder(desc),
		mapping: cols.TagToIdx,
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
			pos := t.mapping[tag]
			err = translateNomsField(value, pos, t.builder)
			stop = err != nil
		}
		return
	})
	if err != nil {
		return nil, err
	}
	return t.builder.Build(t.pool), nil
}

func translateNomsField(value types.Value, idx int, b *val.TupleBuilder) error {
	nk := value.Kind()
	switch nk {
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
		b.PutString(idx, string(value.(types.String)))

	case types.UUIDKind:
		uuid := value.(types.UUID)
		b.PutHash128(idx, uuid[:])

	case types.InlineBlobKind:
		b.PutByteString(idx, value.(types.InlineBlob))

	case types.DecimalKind:
		b.PutDecimal(idx, decimal.Decimal(value.(types.Decimal)))

	case types.PointKind, types.LineStringKind,
		types.PolygonKind, types.GeometryKind:
		translateGeometryField(value, idx, b)

	case types.JSONKind:
		translateJSONField(value.(types.JSON), idx, b)

	case types.BlobKind:
		translateBlobField(value.(types.Blob), idx, b)

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
		b.PutInt16(idx, int16(value))
	case val.TimeEnc:
		b.PutInt64(idx, int64(value))
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

func translateGeometryField(value types.Value, ids int, b *val.TupleBuilder) {
	panic("unimplemeted")
}

func translateJSONField(value types.JSON, idx int, b *val.TupleBuilder) {
	panic("unimplemeted")
}

func translateBlobField(value types.Blob, idx int, b *val.TupleBuilder) {
	panic("unimplemeted")
}

func isEven(n uint64) bool {
	return n%2 == 0
}
