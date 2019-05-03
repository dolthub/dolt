// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datetime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func TestBasics(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	// Since we are using float64 in noms we cannot represent all possible times.
	dt := DateTime{time.Unix(1234567, 1234567)}

	nomsValue, err := marshal.Marshal(context.Background(), vs, dt)
	assert.NoError(err)

	var dt2 DateTime
	err = marshal.Unmarshal(context.Background(), nomsValue, &dt2)
	assert.NoError(err)

	assert.True(dt.Equal(dt2.Time))
}

func TestUnmarshal(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Struct, t time.Time) {
		var dt DateTime
		err := marshal.Unmarshal(context.Background(), v, &dt)
		assert.NoError(err)
		assert.True(dt.Equal(t))
	}

	for _, name := range []string{"DateTime", "Date", "xxx", ""} {
		test(types.NewStruct(name, types.StructData{
			"secSinceEpoch": types.Float(42),
		}), time.Unix(42, 0))
	}

	test(types.NewStruct("", types.StructData{
		"secSinceEpoch": types.Float(42),
		"extra":         types.String("field"),
	}), time.Unix(42, 0))
}

func TestUnmarshalInvalid(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Value) {
		var dt DateTime
		err := marshal.Unmarshal(context.Background(), v, &dt)
		assert.Error(err)
	}

	test(types.Float(42))
	test(types.NewStruct("DateTime", types.StructData{}))
	test(types.NewStruct("DateTime", types.StructData{
		"secSinceEpoch": types.String(42),
	}))
	test(types.NewStruct("DateTime", types.StructData{
		"SecSinceEpoch": types.Float(42),
	}))
	test(types.NewStruct("DateTime", types.StructData{
		"msSinceEpoch": types.Float(42),
	}))
}

func TestMarshal(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	test := func(dt DateTime, expected float64) {
		v, err := marshal.Marshal(context.Background(), vs, dt)
		assert.NoError(err)

		assert.True(types.NewStruct("DateTime", types.StructData{
			"secSinceEpoch": types.Float(expected),
		}).Equals(v))
	}

	test(DateTime{time.Unix(0, 0)}, 0)
	test(DateTime{time.Unix(42, 0)}, 42)
	test(DateTime{time.Unix(42, 123456789)}, 42.123456789)
	test(DateTime{time.Unix(123456789, 123456789)}, 123456789.123456789)
	test(DateTime{time.Unix(-42, 0)}, -42)
	test(DateTime{time.Unix(-42, -123456789)}, -42.123456789)
	test(DateTime{time.Unix(-123456789, -123456789)}, -123456789.123456789)
}

func TestMarshalType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	dt := DateTime{time.Unix(0, 0)}
	typ := marshal.MustMarshalType(dt)
	assert.Equal(DateTimeType, typ)

	v := marshal.MustMarshal(context.Background(), vs, dt)
	assert.Equal(typ, types.TypeOf(v))
}

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewView())
}

func TestZeroValues(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	dt1 := DateTime{}
	assert.True(dt1.IsZero())

	nomsDate, _ := dt1.MarshalNoms(vs)

	dt2 := DateTime{}
	marshal.Unmarshal(context.Background(), nomsDate, &dt2)
	assert.True(dt2.IsZero())

	dt3 := DateTime{}
	dt3.UnmarshalNoms(context.Background(), nomsDate)
	assert.True(dt3.IsZero())
}

func TestString(t *testing.T) {
	assert := assert.New(t)
	dt := DateTime{time.Unix(1234567, 1234567)}
	// Don't test the actual output since that
	assert.IsType(dt.String(), "s")
}

func TestEpoch(t *testing.T) {
	assert := assert.New(t)
	assert.Equal(Epoch, DateTime{time.Unix(0, 0)})
}

func TestHRSComment(t *testing.T) {
	a := assert.New(t)
	vs := newTestValueStore()

	dt := Now()
	mdt := marshal.MustMarshal(context.Background(), vs, dt)

	exp := dt.Format(time.RFC3339)
	s1 := types.EncodedValue(context.Background(), mdt)
	a.True(strings.Contains(s1, "{ // "+exp))

	RegisterHRSCommenter(time.UTC)
	exp = dt.In(time.UTC).Format((time.RFC3339))
	s1 = types.EncodedValue(context.Background(), mdt)
	a.True(strings.Contains(s1, "{ // "+exp))

	types.UnregisterHRSCommenter(datetypename, hrsEncodingName)
	s1 = types.EncodedValue(context.Background(), mdt)
	a.False(strings.Contains(s1, "{ // 20"))
}
