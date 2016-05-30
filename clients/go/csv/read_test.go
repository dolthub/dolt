// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bytes"
	"encoding/csv"
	"testing"

	"github.com/attic-labs/noms/chunks"
	"github.com/attic-labs/noms/datas"
	"github.com/attic-labs/noms/types"
	"github.com/attic-labs/testify/assert"
)

func TestRead(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	dataString := `a,1,true
b,2,false
`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B", "C"}
	kinds := KindSlice{types.StringKind, types.NumberKind, types.BoolKind}
	l, typ := Read(r, "test", headers, kinds, ds)

	assert.Equal(uint64(2), l.Len())

	assert.Equal(types.StructKind, typ.Kind())

	desc, ok := typ.Desc.(types.StructDesc)
	assert.True(ok)
	assert.Len(desc.Fields, 3)
	assert.Equal(types.StringKind, desc.Fields["A"].Kind())
	assert.Equal(types.NumberKind, desc.Fields["B"].Kind())
	assert.Equal(types.BoolKind, desc.Fields["C"].Kind())

	assert.True(l.Get(0).(types.Struct).Get("A").Equals(types.NewString("a")))
	assert.True(l.Get(1).(types.Struct).Get("A").Equals(types.NewString("b")))

	assert.True(l.Get(0).(types.Struct).Get("B").Equals(types.Number(1)))
	assert.True(l.Get(1).(types.Struct).Get("B").Equals(types.Number(2)))

	assert.True(l.Get(0).(types.Struct).Get("C").Equals(types.Bool(true)))
	assert.True(l.Get(1).(types.Struct).Get("C").Equals(types.Bool(false)))
}

func testTrailingHelper(t *testing.T, dataString string) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())

	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	l, typ := Read(r, "test", headers, kinds, ds)

	assert.Equal(uint64(3), l.Len())

	assert.Equal(types.StructKind, typ.Kind())

	desc, ok := typ.Desc.(types.StructDesc)
	assert.True(ok)
	assert.Len(desc.Fields, 2)
	assert.Equal(types.StringKind, desc.Fields["A"].Kind())
	assert.Equal(types.StringKind, desc.Fields["B"].Kind())
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
		Read(r, "test", headers, kinds, ds)
	}()
}

func TestNormalizeHeaderName(t *testing.T) {
	assert := assert.New(t)

	assert.Equal("a", NormalizeHeaderName("a"))
	assert.Equal("ab", NormalizeHeaderName("ab"))
	assert.Equal("a0", NormalizeHeaderName("a0"))

	assert.Equal("a_b", NormalizeHeaderName("a b"))

	assert.Panics(func() { NormalizeHeaderName(" ") })
	assert.Panics(func() { NormalizeHeaderName("0") })
}

func TestDuplicateNormalizedHeaderNames(t *testing.T) {
	assert := assert.New(t)
	ds := datas.NewDatabase(chunks.NewMemoryStore())
	dataString := "1,2\n3,4\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A+", "A-"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	assert.Panics(func() { Read(r, "test", headers, kinds, ds) })
}
