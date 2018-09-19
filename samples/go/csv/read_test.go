// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package csv

import (
	"bytes"
	"encoding/csv"
	"math"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

var LIMIT = uint64(math.MaxUint64)

func TestReadToList(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())

	dataString := `a,1,true
b,2,false
`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B", "C"}
	kinds := KindSlice{types.StringKind, types.FloatKind, types.BoolKind}
	l := ReadToList(r, "test", headers, kinds, db, LIMIT)

	assert.Equal(uint64(2), l.Len())

	assert.True(l.Get(0).(types.Struct).Get("A").Equals(types.String("a")))
	assert.True(l.Get(1).(types.Struct).Get("A").Equals(types.String("b")))

	assert.True(l.Get(0).(types.Struct).Get("B").Equals(types.Float(1)))
	assert.True(l.Get(1).(types.Struct).Get("B").Equals(types.Float(2)))

	assert.True(l.Get(0).(types.Struct).Get("C").Equals(types.Bool(true)))
	assert.True(l.Get(1).(types.Struct).Get("C").Equals(types.Bool(false)))
}

func TestReadToMap(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())

	dataString := `a,1,true
b,2,false
`
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B", "C"}
	kinds := KindSlice{types.StringKind, types.FloatKind, types.BoolKind}
	m := ReadToMap(r, "test", headers, []string{"0"}, kinds, db, LIMIT)

	assert.Equal(uint64(2), m.Len())
	assert.True(types.TypeOf(m).Equals(
		types.MakeMapType(types.StringType, types.MakeStructType("test",
			types.StructField{"A", types.StringType, false},
			types.StructField{"B", types.FloaTType, false},
			types.StructField{"C", types.BoolType, false},
		))))

	assert.True(m.Get(types.String("a")).Equals(types.NewStruct("test", types.StructData{
		"A": types.String("a"),
		"B": types.Float(1),
		"C": types.Bool(true),
	})))
	assert.True(m.Get(types.String("b")).Equals(types.NewStruct("test", types.StructData{
		"A": types.String("b"),
		"B": types.Float(2),
		"C": types.Bool(false),
	})))
}

func testTrailingHelper(t *testing.T, dataString string) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db1 := datas.NewDatabase(storage.NewView())
	defer db1.Close()

	r := NewCSVReader(bytes.NewBufferString(dataString), ',')

	headers := []string{"A", "B"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	l := ReadToList(r, "test", headers, kinds, db1, LIMIT)
	assert.Equal(uint64(3), l.Len())

	storage = &chunks.MemoryStorage{}
	db2 := datas.NewDatabase(storage.NewView())
	defer db2.Close()
	r = NewCSVReader(bytes.NewBufferString(dataString), ',')
	m := ReadToMap(r, "test", headers, []string{"0"}, kinds, db2, LIMIT)
	assert.Equal(uint64(3), m.Len())
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

func TestEscapeStructFieldFromCSV(t *testing.T) {
	assert := assert.New(t)
	cases := []string{
		"a", "a",
		"1a", "a",
		"AaZz19_", "AaZz19_",
		"Q", "Q",
		"AQ", "AQ",
		"_content", "content",
		"Few ¢ents Short", "fewEntsShort",
		"CAMEL💩case letTerS", "camelcaseLetters",
		"https://picasaweb.google.com/data", "httpspicasawebgooglecomdata",
		"💩", "",
		"11 1💩", "",
		"-- A B", "aB",
		"-- A --", "a",
		"-- A -- B", "aB",
	}

	for i := 0; i < len(cases); i += 2 {
		orig, expected := cases[i], cases[i+1]
		assert.Equal(expected, EscapeStructFieldFromCSV(orig))
	}
}

func TestReadParseError(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())

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
		ReadToList(r, "test", headers, kinds, db, LIMIT)
	}()
}

func TestDuplicateHeaderName(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())
	dataString := "1,2\n3,4\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A", "A"}
	kinds := KindSlice{types.StringKind, types.StringKind}
	assert.Panics(func() { ReadToList(r, "test", headers, kinds, db, LIMIT) })
}

func TestEscapeFieldNames(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())
	dataString := "1,2\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A A", "B"}
	kinds := KindSlice{types.FloatKind, types.FloatKind}

	l := ReadToList(r, "test", headers, kinds, db, LIMIT)
	assert.Equal(uint64(1), l.Len())
	assert.Equal(types.Float(1), l.Get(0).(types.Struct).Get(EscapeStructFieldFromCSV("A A")))

	r = NewCSVReader(bytes.NewBufferString(dataString), ',')
	m := ReadToMap(r, "test", headers, []string{"1"}, kinds, db, LIMIT)
	assert.Equal(uint64(1), l.Len())
	assert.Equal(types.Float(1), m.Get(types.Float(2)).(types.Struct).Get(EscapeStructFieldFromCSV("A A")))
}

func TestDefaults(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())
	dataString := "42,,,\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"A", "B", "C", "D"}
	kinds := KindSlice{types.FloatKind, types.FloatKind, types.BoolKind, types.StringKind}

	l := ReadToList(r, "test", headers, kinds, db, LIMIT)
	assert.Equal(uint64(1), l.Len())
	row := l.Get(0).(types.Struct)
	assert.Equal(types.Float(42), row.Get("A"))
	assert.Equal(types.Float(0), row.Get("B"))
	assert.Equal(types.Bool(false), row.Get("C"))
	assert.Equal(types.String(""), row.Get("D"))
}

func TestBooleanStrings(t *testing.T) {
	assert := assert.New(t)
	storage := &chunks.MemoryStorage{}
	db := datas.NewDatabase(storage.NewView())
	dataString := "true,false\n1,0\ny,n\nY,N\nY,\n"
	r := NewCSVReader(bytes.NewBufferString(dataString), ',')
	headers := []string{"T", "F"}
	kinds := KindSlice{types.BoolKind, types.BoolKind}

	l := ReadToList(r, "test", headers, kinds, db, LIMIT)
	assert.Equal(uint64(5), l.Len())
	for i := uint64(0); i < l.Len(); i++ {
		row := l.Get(i).(types.Struct)
		assert.True(types.Bool(true).Equals(row.Get("T")))
		assert.True(types.Bool(false).Equals(row.Get("F")))
	}
}
