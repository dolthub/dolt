// Copyright 2021 Dolthub, Inc.
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

package val

import (
	"github.com/dolthub/go-mysql-server/sql"
	"math"
	"testing"
	"time"

	"github.com/shopspring/decimal"

	"github.com/stretchr/testify/assert"
)

func TestCompare(t *testing.T) {
	tests := []struct {
		typ  Type
		l, r []byte
		cmp  int
	}{
		// int
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(0), r: encInt(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(-1), r: encInt(0),
			cmp: -1,
		},
		{
			typ: Type{Enc: Int64Enc},
			l:   encInt(1), r: encInt(0),
			cmp: 1,
		},
		// uint
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(0), r: encUint(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(0), r: encUint(1),
			cmp: -1,
		},
		{
			typ: Type{Enc: Uint64Enc},
			l:   encUint(1), r: encUint(0),
			cmp: 1,
		},
		// float
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(0), r: encFloat(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(-1), r: encFloat(0),
			cmp: -1,
		},
		{
			typ: Type{Enc: Float64Enc},
			l:   encFloat(1), r: encFloat(0),
			cmp: 1,
		},
		// bit
		{
			typ: Type{Enc: Bit64Enc},
			l:   encBit(0), r: encBit(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: Bit64Enc},
			l:   encBit(0), r: encBit(1),
			cmp: -1,
		},
		{
			typ: Type{Enc: Bit64Enc},
			l:   encBit(1), r: encBit(0),
			cmp: 1,
		},
		// decimal
		{
			typ: Type{Enc: DecimalEnc},
			l:   encDecimal(decimalFromString("-3.7e0")), r: encDecimal(decimalFromString("-3.7e0")),
			cmp: 0,
		},
		{
			typ: Type{Enc: DecimalEnc},
			l:   encDecimal(decimalFromString("5.5729136e3")), r: encDecimal(decimalFromString("2634193746329327479.32030573792e-19")),
			cmp: 1,
		},
		{
			typ: Type{Enc: DecimalEnc},
			l:   encDecimal(decimalFromString("2634193746329327479.32030573792e-19")), r: encDecimal(decimalFromString("5.5729136e3")),
			cmp: -1,
		},
		// year
		{
			typ: Type{Enc: YearEnc},
			l:   encYear(2022), r: encYear(2022),
			cmp: 0,
		},
		{
			typ: Type{Enc: YearEnc},
			l:   encYear(2022), r: encYear(1999),
			cmp: 1,
		},
		{
			typ: Type{Enc: YearEnc},
			l:   encYear(2000), r: encYear(2022),
			cmp: -1,
		},
		// date
		{
			typ: Type{Enc: DateEnc},
			l:   encDate(2022, 05, 24), r: encDate(2022, 05, 24),
			cmp: 0,
		},
		{
			typ: Type{Enc: DateEnc},
			l:   encDate(2022, 12, 24), r: encDate(2022, 05, 24),
			cmp: 1,
		},
		{
			typ: Type{Enc: DateEnc},
			l:   encDate(1999, 04, 24), r: encDate(2022, 05, 24),
			cmp: -1,
		},
		// time
		{
			typ: Type{Enc: TimeEnc},
			l:   encTime(978220860), r: encTime(978220860),
			cmp: 0,
		},
		{
			typ: Type{Enc: TimeEnc},
			l:   encTime(599529660), r: encTime(-11644473600),
			cmp: 1,
		},
		{
			typ: Type{Enc: TimeEnc},
			l:   encTime(-11644473600), r: encTime(599529660),
			cmp: -1,
		},
		// datetime
		{
			typ: Type{Enc: DatetimeEnc},
			l:   encDatetime(time.Date(1999, 11, 01, 01, 01, 01, 00, time.UTC)),
			r:   encDatetime(time.Date(1999, 11, 01, 01, 01, 01, 00, time.UTC)),
			cmp: 0,
		},
		{
			typ: Type{Enc: DatetimeEnc},
			l:   encDatetime(time.Date(2000, 11, 01, 01, 01, 01, 00, time.UTC)),
			r:   encDatetime(time.Date(1999, 11, 01, 01, 01, 01, 00, time.UTC)),
			cmp: 1,
		},
		{
			typ: Type{Enc: DatetimeEnc},
			l:   encDatetime(time.Date(1999, 11, 01, 01, 01, 01, 00, time.UTC)),
			r:   encDatetime(time.Date(2000, 11, 01, 01, 01, 01, 00, time.UTC)),
			cmp: -1,
		},
		// enum
		{
			typ: Type{Enc: EnumEnc},
			l:   encEnum(0), r: encEnum(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: EnumEnc},
			l:   encEnum(0), r: encEnum(1),
			cmp: -1,
		},
		{
			typ: Type{Enc: EnumEnc},
			l:   encEnum(1), r: encEnum(0),
			cmp: 1,
		},
		// set
		{
			typ: Type{Enc: SetEnc},
			l:   encSet(0), r: encSet(0),
			cmp: 0,
		},
		{
			typ: Type{Enc: SetEnc},
			l:   encSet(0), r: encSet(1),
			cmp: -1,
		},
		{
			typ: Type{Enc: SetEnc},
			l:   encSet(1), r: encSet(0),
			cmp: 1,
		},
		// string
		{
			typ: Type{Enc: StringEnc},
			l:   encStr(""), r: encStr(""),
			cmp: 0,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr(""), r: encStr("a"),
			cmp: -1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr(""),
			cmp: 1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr("a"),
			cmp: 0,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("a"), r: encStr("b"),
			cmp: -1,
		},
		{
			typ: Type{Enc: StringEnc},
			l:   encStr("b"), r: encStr("a"),
			cmp: 1,
		},
		// z-address
		{
			typ: Type{Enc: StringEnc},
			l:   encCell(Cell{}),
			r:   encCell(Cell{}),
			cmp: 0,
		},
	}

	for _, test := range tests {
		ctx := sql.NewEmptyContext()
		act := compare(test.typ, test.l, test.r)
		assert.Equal(t, test.cmp, act, "expected %s %s %s ",
			TupleDesc{}.formatValue(ctx, test.typ.Enc, 0, test.l),
			fmtComparator(test.cmp),
			TupleDesc{}.formatValue(ctx, test.typ.Enc, 0, test.r))
	}
}

func fmtComparator(c int) string {
	if c == 0 {
		return "="
	} else if c < 0 {
		return "<"
	} else {
		return ">"
	}
}

func encInt(i int64) []byte {
	buf := make([]byte, uint64Size)
	writeInt64(buf, i)
	return buf
}

func encUint(u uint64) []byte {
	buf := make([]byte, int64Size)
	writeUint64(buf, u)
	return buf
}

func encFloat(f float64) []byte {
	buf := make([]byte, float64Size)
	writeFloat64(buf, f)
	return buf
}

func encBit(u uint64) []byte {
	buf := make([]byte, bit64Size)
	writeBit64(buf, u)
	return buf
}

func encDecimal(d decimal.Decimal) []byte {
	buf := make([]byte, sizeOfDecimal(d))
	writeDecimal(buf, d)
	return buf
}

func encStr(s string) []byte {
	buf := make([]byte, len(s)+1)
	writeString(buf, s)
	return buf
}

func encCell(c Cell) []byte {
	buf := make([]byte, cellSize)
	writeCell(buf, c)
	return buf
}

func encYear(y int16) []byte {
	buf := make([]byte, yearSize)
	writeYear(buf, y)
	return buf
}

func encDate(y, m, d int) []byte {
	date := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
	buf := make([]byte, dateSize)
	writeDate(buf, date)
	return buf
}

func encTime(t int64) []byte {
	buf := make([]byte, timeSize)
	writeTime(buf, t)
	return buf
}

func encDatetime(dt time.Time) []byte {
	buf := make([]byte, datetimeSize)
	writeDatetime(buf, dt)
	return buf
}

func encEnum(u uint16) []byte {
	buf := make([]byte, enumSize)
	writeEnum(buf, u)
	return buf
}

func encSet(u uint64) []byte {
	buf := make([]byte, setSize)
	writeSet(buf, u)
	return buf
}

func TestCodecRoundTrip(t *testing.T) {
	t.Run("round trip bool", func(t *testing.T) {
		roundTripBools(t)
	})
	t.Run("round trip ints", func(t *testing.T) {
		roundTripInts(t)
	})
	t.Run("round trip uints", func(t *testing.T) {
		roundTripUints(t)
	})
	t.Run("round trip floats", func(t *testing.T) {
		roundTripFloats(t)
	})
	t.Run("round trip years", func(t *testing.T) {
		roundTripYears(t)
	})
	t.Run("round trip dates", func(t *testing.T) {
		roundTripDates(t)
	})
	t.Run("round trip times", func(t *testing.T) {
		roundTripTimes(t)
	})
	t.Run("round trip datetimes", func(t *testing.T) {
		roundTripDatetimes(t)
	})
	t.Run("round trip decimal", func(t *testing.T) {
		roundTripDecimal(t)
	})
}

func roundTripBools(t *testing.T) {
	buf := make([]byte, 1)
	integers := []bool{true, false}
	for _, exp := range integers {
		writeBool(buf, exp)
		assert.Equal(t, exp, readBool(buf))
		zero(buf)
	}
}

func roundTripInts(t *testing.T) {
	buf := make([]byte, int8Size)
	integers := []int64{-1, 0, -1, math.MaxInt8, math.MinInt8}
	for _, value := range integers {
		exp := int8(value)
		writeInt8(buf, exp)
		assert.Equal(t, exp, readInt8(buf))
		zero(buf)
	}

	buf = make([]byte, int16Size)
	integers = append(integers, math.MaxInt16, math.MaxInt16)
	for _, value := range integers {
		exp := int16(value)
		writeInt16(buf, exp)
		assert.Equal(t, exp, readInt16(buf))
		zero(buf)
	}

	buf = make([]byte, int32Size)
	integers = append(integers, math.MaxInt32, math.MaxInt32)
	for _, value := range integers {
		exp := int32(value)
		writeInt32(buf, exp)
		assert.Equal(t, exp, readInt32(buf))
		zero(buf)
	}

	buf = make([]byte, int64Size)
	integers = append(integers, math.MaxInt64, math.MaxInt64)
	for _, value := range integers {
		exp := int64(value)
		writeInt64(buf, exp)
		assert.Equal(t, exp, readInt64(buf))
		zero(buf)
	}
}

func roundTripUints(t *testing.T) {
	buf := make([]byte, uint8Size)
	uintegers := []uint64{0, 1, math.MaxUint8}
	for _, value := range uintegers {
		exp := uint8(value)
		writeUint8(buf, exp)
		assert.Equal(t, exp, readUint8(buf))
		zero(buf)
	}

	buf = make([]byte, uint16Size)
	uintegers = append(uintegers, math.MaxUint16)
	for _, value := range uintegers {
		exp := uint16(value)
		WriteUint16(buf, exp)
		assert.Equal(t, exp, ReadUint16(buf))
		zero(buf)
	}

	buf = make([]byte, enumSize)
	for _, value := range uintegers {
		exp := uint16(value)
		writeEnum(buf, exp)
		assert.Equal(t, exp, readEnum(buf))
		zero(buf)
	}

	buf = make([]byte, uint32Size)
	uintegers = append(uintegers, math.MaxUint32)
	for _, value := range uintegers {
		exp := uint32(value)
		writeUint32(buf, exp)
		assert.Equal(t, exp, ReadUint32(buf))
		zero(buf)
	}

	buf = make([]byte, uint64Size)
	uintegers = append(uintegers, math.MaxUint64)
	for _, value := range uintegers {
		exp := uint64(value)
		writeUint64(buf, exp)
		assert.Equal(t, exp, readUint64(buf))
		zero(buf)
	}

	buf = make([]byte, bit64Size)
	for _, value := range uintegers {
		exp := uint64(value)
		writeBit64(buf, exp)
		assert.Equal(t, exp, readBit64(buf))
		zero(buf)
	}

	buf = make([]byte, setSize)
	for _, value := range uintegers {
		exp := uint64(value)
		writeSet(buf, exp)
		assert.Equal(t, exp, readSet(buf))
		zero(buf)
	}
}

func roundTripFloats(t *testing.T) {
	buf := make([]byte, float32Size)
	floats := []float64{-1, 0, 1, math.MaxFloat32, math.SmallestNonzeroFloat32}
	for _, value := range floats {
		exp := float32(value)
		writeFloat32(buf, exp)
		assert.Equal(t, exp, readFloat32(buf))
		zero(buf)
	}

	buf = make([]byte, float64Size)
	floats = append(floats, math.MaxFloat64, math.SmallestNonzeroFloat64)
	for _, value := range floats {
		exp := float64(value)
		writeFloat64(buf, exp)
		assert.Equal(t, exp, readFloat64(buf))
		zero(buf)
	}
}

func roundTripYears(t *testing.T) {
	years := []int16{
		1901,
		2022,
		2155,
	}

	buf := make([]byte, yearSize)
	for _, y := range years {
		writeYear(buf, y)
		assert.Equal(t, y, readYear(buf))
		zero(buf)
	}
}

func roundTripDates(t *testing.T) {
	dates := []time.Time{
		testDate(1000, 01, 01),
		testDate(2022, 05, 24),
		testDate(9999, 12, 31),
	}

	buf := make([]byte, dateSize)
	for _, d := range dates {
		writeDate(buf, d)
		assert.Equal(t, d, readDate(buf))
		zero(buf)
	}
}

func roundTripTimes(t *testing.T) {
	times := []int64{
		-1221681866,
		-11644473600,
		599529660,
		978220860,
	}

	buf := make([]byte, timeSize)
	for _, d := range times {
		writeTime(buf, d)
		assert.Equal(t, d, readTime(buf))
		zero(buf)
	}
}

func roundTripDatetimes(t *testing.T) {
	datetimes := []time.Time{
		time.Date(1000, 01, 01, 0, 0, 0, 0, time.UTC),
		time.UnixMicro(time.Now().UTC().UnixMicro()).UTC(),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
	}

	buf := make([]byte, datetimeSize)
	for _, dt := range datetimes {
		writeDatetime(buf, dt)
		assert.Equal(t, dt, readDatetime(buf))
		zero(buf)
	}
}

func roundTripDecimal(t *testing.T) {
	decimals := []decimal.Decimal{
		decimalFromString("0"),
		decimalFromString("1"),
		decimalFromString("-1"),
		decimalFromString("-3.7e0"),
		decimalFromString("0.00000000000000000003e20"),
		decimalFromString(".22"),
		decimalFromString("-.7863294659345624"),
		decimalFromString("2634193746329327479.32030573792e-19"),
		decimalFromString("7742"),
		decimalFromString("99999.999994"),
		decimalFromString("5.5729136e3"),
		decimalFromString("600e-2"),
		decimalFromString("-99999.999995"),
		decimalFromString("99999999999999999999999999999999999999999999999999999999999999999"),
		decimalFromString("99999999999999999999999999999999999999999999999999999999999999999.1"),
		decimalFromString("99999999999999999999999999999999999999999999999999999999999999999.99"),
		decimalFromString("16976349273982359874209023948672021737840592720387475.2719128737543572927374503832837350563300243035038234972093785"),
		decimalFromString("99999999999999999999999999999999999999999999999999999.9999999999999"),
	}

	for _, dec := range decimals {
		buf := make([]byte, sizeOfDecimal(dec))
		writeDecimal(buf, dec)
		actual := readDecimal(buf)
		assert.True(t, dec.Equal(actual), "%s != %s",
			dec.String(), actual.String())
		assert.Equal(t, dec, actual)
		zero(buf)
	}
}

func testDate(y, m, d int) (date time.Time) {
	return time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.UTC)
}

func decimalFromString(s string) decimal.Decimal {
	d, err := decimal.NewFromString(s)
	if err != nil {
		panic(err)
	}
	return d
}

func zero(buf []byte) {
	for i := range buf {
		buf[i] = 0
	}
}
