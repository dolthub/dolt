// Copyright 2016 Attic Labs, Inc. All rights reserved.
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

	assert.True(l.Get(0).(types.Struct).Get("A").Equals(types.String("a")))
	assert.True(l.Get(1).(types.Struct).Get("A").Equals(types.String("b")))

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
		types.MakeMapType(types.StringType, types.MakeStructType("",
			[]string{"B", "C"},
			[]*types.Type{
				types.NumberType,
				types.BoolType,
			}))))

	assert.True(m.Get(types.String("a")).Equals(types.NewStruct("", types.StructData{
		"B": types.Number(1),
		"C": types.Bool(true),
	})))
	assert.True(m.Get(types.String("b")).Equals(types.NewStruct("", types.StructData{
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
	dataString := "1,2\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A A", "B"}
	kinds := KindSlice{types.NumberKind, types.NumberKind}

	l, _ := ReadToList(r, "test", headers, kinds, ds)
	assert.Equal(uint64(1), l.Len())
	assert.Equal(types.Number(1), l.Get(0).(types.Struct).Get(types.EscapeStructField("A A")))

	r = NewCSVReader(bytes.NewBufferString(dataString), ',')
	m := ReadToMap(r, headers, 1, kinds, ds)
	assert.Equal(uint64(1), l.Len())
	assert.Equal(types.Number(1), m.Get(types.Number(2)).(types.Struct).Get(types.EscapeStructField("A A")))
}

func TestDefaults(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	dataString := "42,,,\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A", "B", "C", "D"}
	kinds := KindSlice{types.NumberKind, types.NumberKind, types.BoolKind, types.StringKind}

	l, _ := ReadToList(r, "test", headers, kinds, ds)
	assert.Equal(uint64(1), l.Len())
	row := l.Get(0).(types.Struct)
	assert.Equal(types.Number(42), row.Get("A"))
	assert.Equal(types.Number(0), row.Get("B"))
	assert.Equal(types.Bool(false), row.Get("C"))
	assert.Equal(types.String(""), row.Get("D"))
}

func TestBooleanStrings(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	dataString := "true,false\n1,0\ny,n\nY,N\nY,\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"T", "F"}
	kinds := KindSlice{types.BoolKind, types.BoolKind}

	l, _ := ReadToList(r, "test", headers, kinds, ds)
	assert.Equal(uint64(5), l.Len())
	for i := uint64(0); i < l.Len(); i++ {
		row := l.Get(i).(types.Struct)
		assert.True(types.Bool(true).Equals(row.Get("T")))
		assert.True(types.Bool(false).Equals(row.Get("F")))
	}
}
