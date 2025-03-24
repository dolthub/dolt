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

package tree

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/expression/function/spatial"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/pool"
	"github.com/dolthub/dolt/go/store/val"
)

type prollyFieldTest struct {
	name  string
	value interface{}
	typ   val.Type
}

func TestRoundTripProllyFields(t *testing.T) {
	tests := []prollyFieldTest{
		{
			name: "null",
			typ: val.Type{
				Enc:      val.Int8Enc,
				Nullable: true,
			},
			value: nil,
		},
		{
			name:  "int8",
			typ:   val.Type{Enc: val.Int8Enc},
			value: int8(-42),
		},
		{
			name:  "uint8",
			typ:   val.Type{Enc: val.Uint8Enc},
			value: uint8(42),
		},
		{
			name:  "int16",
			typ:   val.Type{Enc: val.Int16Enc},
			value: int16(-42),
		},
		{
			name:  "uint16",
			typ:   val.Type{Enc: val.Uint16Enc},
			value: uint16(42),
		},
		{
			name:  "int32",
			typ:   val.Type{Enc: val.Int32Enc},
			value: int32(-42),
		},
		{
			name:  "uint32",
			typ:   val.Type{Enc: val.Uint32Enc},
			value: uint32(42),
		},
		{
			name:  "int64",
			typ:   val.Type{Enc: val.Int64Enc},
			value: int64(-42),
		},
		{
			name:  "uint64",
			typ:   val.Type{Enc: val.Uint64Enc},
			value: uint64(42),
		},
		{
			name:  "float32",
			typ:   val.Type{Enc: val.Float32Enc},
			value: float32(math.Pi),
		},
		{
			name:  "float64",
			typ:   val.Type{Enc: val.Float64Enc},
			value: float64(-math.Pi),
		},
		{
			name:  "bit",
			typ:   val.Type{Enc: val.Bit64Enc},
			value: uint64(42),
		},
		{
			name:  "decimal",
			typ:   val.Type{Enc: val.DecimalEnc},
			value: mustParseDecimal("0.263419374632932747932030573792"),
		},
		{
			name:  "string",
			typ:   val.Type{Enc: val.StringEnc},
			value: "lorem ipsum",
		},
		{
			name:  "string",
			typ:   val.Type{Enc: val.StringAddrEnc},
			value: "lorem ipsum",
		},
		{
			name:  "bytes",
			typ:   val.Type{Enc: val.ByteStringEnc},
			value: []byte("lorem ipsum"),
		},
		{
			name:  "year",
			typ:   val.Type{Enc: val.YearEnc},
			value: int16(2022),
		},
		{
			name:  "date",
			typ:   val.Type{Enc: val.DateEnc},
			value: dateFromTime(time.Now().UTC()),
		},
		{
			name:  "time",
			typ:   val.Type{Enc: val.TimeEnc},
			value: mustParseTime(t, "11:22:00"),
		},
		{
			name:  "datetime",
			typ:   val.Type{Enc: val.DatetimeEnc},
			value: time.UnixMicro(time.Now().UTC().UnixMicro()).UTC(),
		},
		{
			name:  "timestamp",
			typ:   val.Type{Enc: val.DatetimeEnc},
			value: time.UnixMicro(time.Now().UTC().UnixMicro()).UTC(),
		},
		{
			name:  "json",
			typ:   val.Type{Enc: val.JSONAddrEnc},
			value: mustParseJson(t, `{"a": 1, "b": false}`),
		},
		{
			name:  "point",
			typ:   val.Type{Enc: val.GeomAddrEnc},
			value: mustParseGeometryType(t, "POINT(1 2)"),
		},
		{
			name:  "linestring",
			typ:   val.Type{Enc: val.GeomAddrEnc},
			value: mustParseGeometryType(t, "LINESTRING(1 2,3 4)"),
		},
		{
			name:  "polygon",
			typ:   val.Type{Enc: val.GeomAddrEnc},
			value: mustParseGeometryType(t, "POLYGON((0 0,1 1,1 0,0 0))"),
		},
		{
			name:  "binary",
			typ:   val.Type{Enc: val.BytesAddrEnc},
			value: []byte("lorem ipsum"),
		},
		{
			name:  "adaptive line binary short",
			typ:   val.Type{Enc: val.BytesAdaptiveEnc},
			value: []byte("lorem ipsum"),
		},
		{
			name:  "adaptive line binary long",
			typ:   val.Type{Enc: val.BytesAdaptiveEnc},
			value: make([]byte, (1 << 12)),
		},
		{
			name:  "adaptive line string short",
			typ:   val.Type{Enc: val.StringAdaptiveEnc},
			value: "lorem ipsum",
		},
		{
			name:  "adaptive line string long",
			typ:   val.Type{Enc: val.StringAdaptiveEnc},
			value: string(make([]byte, (1 << 12))),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testRoundTripProllyFields(t, test)
		})
	}
}

var testPool = pool.NewBuffPool()

func testRoundTripProllyFields(t *testing.T, test prollyFieldTest) {
	ctx := context.Background()
	desc := val.NewTupleDescriptor(test.typ)
	ns := NewTestNodeStore()
	builder := val.NewTupleBuilder(desc, ns)

	err := PutField(ctx, ns, builder, 0, test.value)
	require.NoError(t, err)

	tup, _ := builder.Build(testPool)

	v, err := GetField(ctx, desc, 0, tup, ns)
	require.NoError(t, err)

	v, err = sql.UnwrapAny(ctx, v)
	require.NoError(t, err)

	if js, ok := v.(sql.JSONWrapper); ok {
		v, err = js.ToInterface()
		require.NoError(t, err)
	}

	expectedValue, err := sql.UnwrapAny(ctx, test.value)
	assert.NoError(t, err)

	if js, ok := expectedValue.(sql.JSONWrapper); ok {
		expectedValue, err = js.ToInterface()
		require.NoError(t, err)
	}

	assert.Equal(t, expectedValue, v)

}

func mustParseGeometryType(t *testing.T, s string) (v interface{}) {
	// Determine type, and get data
	geomType, data, _, err := spatial.ParseWKTHeader(s)
	require.NoError(t, err)

	srid, order := uint32(0), false
	switch geomType {
	case "point":
		v, err = spatial.WKTToPoint(data, srid, order)
	case "linestring":
		v, err = spatial.WKTToLine(data, srid, order)
	case "polygon":
		v, err = spatial.WKTToPoly(data, srid, order)
	default:
		panic("unknown geometry type")
	}
	require.NoError(t, err)
	return
}

func mustParseJson(t *testing.T, s string) types.JSONDocument {
	var v interface{}
	err := json.Unmarshal([]byte(s), &v)
	require.NoError(t, err)
	return types.JSONDocument{Val: v}
}

func mustParseDecimal(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func mustParseTime(t *testing.T, s string) types.Timespan {
	val, err := types.Time.ConvertToTimespan(s)
	require.NoError(t, err)
	return val
}

func dateFromTime(t time.Time) time.Time {
	y, m, d := t.Year(), t.Month(), t.Day()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}

// TestGeometryEncoding contains tests that ensure backwards compatibility with the old geometry encoding.
//
//	Initially, Geometries were stored in line, but now they are stored out of band as BLOBs.
func TestGeometryEncoding(t *testing.T) {
	tests := []struct {
		name  string
		value interface{}
	}{
		{
			name:  "point",
			value: mustParseGeometryType(t, "POINT(1 2)"),
		},
		{
			name:  "linestring",
			value: mustParseGeometryType(t, "LINESTRING(1 2,3 4)"),
		},
		{
			name:  "polygon",
			value: mustParseGeometryType(t, "POLYGON((0 0,1 1,1 0,0 0))"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ns := NewTestNodeStore()
			oldDesc := val.NewTupleDescriptor(val.Type{Enc: val.GeometryEnc})
			builder := val.NewTupleBuilder(oldDesc, ns)
			b := serializeGeometry(test.value)
			builder.PutGeometry(0, b)
			tup, _ := builder.Build(testPool)

			var v interface{}
			var err error

			v, err = GetField(context.Background(), oldDesc, 0, tup, ns)
			assert.NoError(t, err)
			assert.Equal(t, test.value, v)

			newDesc := val.NewTupleDescriptor(val.Type{Enc: val.GeometryEnc})
			v, err = GetField(context.Background(), newDesc, 0, tup, ns)
			assert.NoError(t, err)
			assert.Equal(t, test.value, v)
		})
	}
}
