// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestReadToList(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	dataString := `a,1,true
b,2,false
`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B", "C"}
	kinds := KindSlice{types.StringKind, types.NumberKind, types.BoolKind}
	l, typ := ReadToList(r, "test", headers, kinds, ds)

	assert.Equal(uint64(2), l.Len())

	assert.Equal(types.StructKind, typ.Kind())

	desc, ok := typ.Desc.(types.StructDesc)
	assert.True(ok)
	assert.Equal(desc.Len(), 3)
	assert.Equal(types.StringKind, desc.Field("A").Kind())
	assert.Equal(types.NumberKind, desc.Field("B").Kind())
	assert.Equal(types.BoolKind, desc.Field("C").Kind())

	assert.True(l.Get(0).(types.Struct).Get("A").Equals(types.NewString("a")))
	assert.True(l.Get(1).(types.Struct).Get("A").Equals(types.NewString("b")))

	assert.True(l.Get(0).(types.Struct).Get("B").Equals(types.Number(1)))
	assert.True(l.Get(1).(types.Struct).Get("B").Equals(types.Number(2)))

	assert.True(l.Get(0).(types.Struct).Get("C").Equals(types.Bool(true)))
	assert.True(l.Get(1).(types.Struct).Get("C").Equals(types.Bool(false)))
}

func TestReadToMap(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	dataString := `a,1,true
b,2,false
`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B", "C"}
	kinds := KindSlice{types.StringKind, types.NumberKind, types.BoolKind}
	m := ReadToMap(r, headers, 0, kinds, ds)

	assert.Equal(uint64(2), m.Len())
	assert.True(m.Type().Equals(
		types.MakeMapType(types.StringType, types.MakeStructType("", map[string]*types.Type{
			"B": types.NumberType,
			"C": types.BoolType,
		}))))

	assert.True(m.Get(types.NewString("a")).Equals(types.NewStruct("", map[string]types.Value{
		"B": types.Number(1),
		"C": types.Bool(true),
	})))
	assert.True(m.Get(types.NewString("b")).Equals(types.NewStruct("", map[string]types.Value{
		"B": types.Number(2),
		"C": types.Bool(false),
	})))
}

func testTrailingHelper(t *testing.T, dataString string) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	l, typ := ReadToList(r, "test", headers, kinds, ds)

	assert.Equal(uint64(3), l.Len())

	assert.Equal(types.StructKind, typ.Kind())

	desc, ok := typ.Desc.(types.StructDesc)
	assert.True(ok)
	assert.Equal(desc.Len(), 2)
	assert.Equal(types.StringKind, desc.Field("A").Kind())
	assert.Equal(types.StringKind, desc.Field("B").Kind())
}

func TestReadTrailingHole(t *testing.T) {
	dataString := `a,b,
d,e,
g,h,
`
	testTrailingHelper(t, dataString)
}

func TestReadTrailingHoles(t *testing.T) {
	dataString := `a,b,,
d,e
g,h
`
	testTrailingHelper(t, dataString)
}

func TestReadTrailingValues(t *testing.T) {
	dataString := `a,b
d,e,f
g,h,i,j
`
	testTrailingHelper(t, dataString)
}

func TestReadParseError(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	dataString := `a,"b`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	func() {
		defer func() {
			r := recover()
			assert.NotNil(r)
			_, ok := r.(*csv.ParseError)
			assert.True(ok, "Should be a ParseError")
		}()
		ReadToList(r, "test", headers, kinds, ds)
	}()
}

func TestDuplicateHeaderName(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	dataString := "1,2\n3,4\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A", "A"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	assert.Panics(func() { ReadToList(r, "test", headers, kinds, ds) })
}

func TestEscapeFieldNames(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	dataString := "1\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A A"}
	kinds := KindSlice{types.NumberKind}
	l, _ := ReadToList(r, "test", headers, kinds, ds)
	assert.Equal(uint64(1), l.Len())
	assert.Equal(types.Number(1), l.Get(0).(types.Struct).Get(types.EscapeStructField("A A")))
}
