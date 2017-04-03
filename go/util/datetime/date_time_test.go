// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datetime

import (
	"testing"
	"time"

	"github.com/attic-labs/noms/go/marshal"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestBasics(t *testing.T) {
	assert := assert.New(t)

	dt := DateTime(time.Unix(123, 456))

	nomsValue, err := marshal.Marshal(dt)
	assert.NoError(err)

	var dt2 DateTime
	err = marshal.Unmarshal(nomsValue, &dt2)
	assert.NoError(err)

	assert.True(time.Time(dt).Equal(time.Time(dt2)))
}

func TestUnmarshal(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Struct, t time.Time) {
		var dt DateTime
		err := marshal.Unmarshal(v, &dt)
		assert.NoError(err)
		assert.True(time.Time(dt).Equal(t))
	}

	for _, name := range []string{"DateTime", "Date", "xxx", ""} {
		test(types.NewStruct(name, types.StructData{
			"secSinceEpoch": types.Number(42),
		}), time.Unix(42, 0))
	}

	test(types.NewStruct("", types.StructData{
		"secSinceEpoch": types.Number(42),
		"extra":         types.String("field"),
	}), time.Unix(42, 0))
}

func TestUnmarshalInvalid(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Value) {
		var dt DateTime
		err := marshal.Unmarshal(v, &dt)
		assert.Error(err)
	}

	test(types.Number(42))
	test(types.NewStruct("DateTime", types.StructData{}))
	test(types.NewStruct("DateTime", types.StructData{
		"secSinceEpoch": types.String(42),
	}))
	test(types.NewStruct("DateTime", types.StructData{
		"SecSinceEpoch": types.Number(42),
	}))
	test(types.NewStruct("DateTime", types.StructData{
		"msSinceEpoch": types.Number(42),
	}))
}

func TestMarshal(t *testing.T) {
	assert := assert.New(t)

	test := func(dt DateTime, expected float64) {
		v, err := marshal.Marshal(dt)
		assert.NoError(err)

		assert.True(types.NewStruct("DateTime", types.StructData{
			"secSinceEpoch": types.Number(expected),
		}).Equals(v))
	}

	test(DateTime(time.Unix(0, 0)), 0)
	test(DateTime(time.Unix(42, 0)), 42)
	test(DateTime(time.Unix(42, 123456789)), 42.123456789)
	test(DateTime(time.Unix(123456789, 123456789)), 123456789.123456789)
	test(DateTime(time.Unix(-42, 0)), -42)
	test(DateTime(time.Unix(-42, -123456789)), -42.123456789)
	test(DateTime(time.Unix(-123456789, -123456789)), -123456789.123456789)
}

func TestMarshalType(t *testing.T) {
	assert := assert.New(t)

	dt := DateTime(time.Unix(0, 0))
	typ := marshal.MustMarshalType(dt)
	assert.Equal(DateTimeType, typ)

	v := marshal.MustMarshal(dt)
	assert.Equal(typ, types.TypeOf(v))
}
