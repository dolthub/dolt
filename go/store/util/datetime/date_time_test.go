// Copyright 2019 Dolthub, Inc.
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
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datetime

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/marshal"
	"github.com/dolthub/dolt/go/store/types"
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
	err = marshal.Unmarshal(context.Background(), types.Format_Default, nomsValue, &dt2)
	assert.NoError(err)

	assert.True(dt.Equal(dt2.Time))
}

func mustStruct(st types.Struct, err error) types.Struct {
	d.PanicIfError(err)
	return st
}

func TestUnmarshal(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Struct, t time.Time) {
		var dt DateTime
		err := marshal.Unmarshal(context.Background(), types.Format_Default, v, &dt)
		assert.NoError(err)
		assert.True(dt.Equal(t))
	}

	for _, name := range []string{"DateTime", "Date", "xxx", ""} {
		test(mustStruct(types.NewStruct(types.Format_Default, name, types.StructData{
			"secSinceEpoch": types.Float(42),
		})), time.Unix(42, 0))
	}

	test(mustStruct(types.NewStruct(types.Format_Default, "", types.StructData{
		"secSinceEpoch": types.Float(42),
		"extra":         types.String("field"),
	})), time.Unix(42, 0))
}

func TestUnmarshalInvalid(t *testing.T) {
	assert := assert.New(t)

	test := func(v types.Value) {
		var dt DateTime
		err := marshal.Unmarshal(context.Background(), types.Format_Default, v, &dt)
		assert.Error(err)
	}

	test(types.Float(42))
	test(mustStruct(types.NewStruct(types.Format_Default, "DateTime", types.StructData{})))
	test(mustStruct(types.NewStruct(types.Format_Default, "DateTime", types.StructData{
		"secSinceEpoch": types.String("42"),
	})))
	test(mustStruct(types.NewStruct(types.Format_Default, "DateTime", types.StructData{
		"SecSinceEpoch": types.Float(42),
	})))
	test(mustStruct(types.NewStruct(types.Format_Default, "DateTime", types.StructData{
		"msSinceEpoch": types.Float(42),
	})))
}

func TestMarshal(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	test := func(dt DateTime, expected float64) {
		v, err := marshal.Marshal(context.Background(), vs, dt)
		assert.NoError(err)
		st, err := types.NewStruct(types.Format_Default, "DateTime", types.StructData{
			"secSinceEpoch": types.Float(expected),
		})
		assert.NoError(err)
		assert.True(st.Equals(v))
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
	typ, err := marshal.MarshalType(types.Format_Default, dt)
	assert.NoError(err)
	assert.Equal(DateTimeType, typ)

	v, err := marshal.Marshal(context.Background(), vs, dt)
	assert.NoError(err)
	typ2, err := types.TypeOf(v)
	assert.NoError(err)
	assert.Equal(typ, typ2)
}

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewViewWithDefaultFormat())
}

func TestZeroValues(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()
	defer vs.Close()

	dt1 := DateTime{}
	assert.True(dt1.IsZero())

	nomsDate, _ := dt1.MarshalNoms(vs)

	dt2 := DateTime{}
	marshal.Unmarshal(context.Background(), types.Format_Default, nomsDate, &dt2)
	assert.True(dt2.IsZero())

	dt3 := DateTime{}
	dt3.UnmarshalNoms(context.Background(), types.Format_Default, nomsDate)
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
	mdt, err := marshal.Marshal(context.Background(), vs, dt)
	a.NoError(err)

	exp := dt.Format(time.RFC3339)
	s1, err := types.EncodedValue(context.Background(), mdt)
	a.NoError(err)
	a.True(strings.Contains(s1, "{ // "+exp))

	RegisterHRSCommenter(time.UTC)
	exp = dt.In(time.UTC).Format((time.RFC3339))
	s1, err = types.EncodedValue(context.Background(), mdt)
	a.NoError(err)
	a.True(strings.Contains(s1, "{ // "+exp))

	types.UnregisterHRSCommenter(datetypename, hrsEncodingName)
	s1, err = types.EncodedValue(context.Background(), mdt)
	a.NoError(err)
	a.False(strings.Contains(s1, "{ // 20"))
}
