package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/attic-labs/noms/types"

	"github.com/stretchr/testify/assert"
)

func TestList(t *testingT) {
	assert := assert.New(t)

	l := NewInt32List()
	assert.Equal(uint64(0), l.Len())
	assert.True(l.Empty())
	l = l.Append(types.Int32(1), types.Int32(2))
	assert.Equal(uint64(2), l.Len())
	assert.False(l.Empty())
}

func TestMap(t *testingT) {
	assert := assert.New(t)

	m := NewStringFloat64Map()
	assert.Equal(uint64(0), m.Len())
	assert.True(m.Empty())

	m = m.Set(types.NewString("hi"), types.Float64(float64(42)))
	assert.Equal(uint64(1), m.Len())
	assert.False(m.Empty())
}

func TestSet(t *testingT) {
	assert := assert.New(t)

	s := NewBoolSet()
	assert.Equal(uint64(0), s.Len())
	assert.True(s.Empty())
	s = s.Insert(types.Bool(true), types.Bool(false))
	assert.Equal(uint64(2), s.Len())
	assert.False(s.Empty())
}

func TestStructTest(t *testingT) {
	assert := assert.New(t)

	s := NewTestStruct()
	s = s.SetTitle(types.NewString("Hello"))
	assert.True(s.Title().Equals(types.NewString("Hello")))
}

func TestCompound(t *testingT) {
	assert := assert.New(t)

	m := NewTestStructBoolSetMap()
	k := NewTestStruct()
	k = k.SetTitle(types.NewString("Hello"))
	v := NewBoolSet()
	m = m.Set(k, v)
	assert.Equal(m.Get(k), v)
}

func TestCustomName(t *testingT) {
	assert := assert.New(t)

	s := NewMyTestSet()
	assert.True(s.Empty())
	s = s.Insert(types.UInt32(32))
	assert.Equal(uint64(1), s.Len())
}

type testingT struct {
	testing.T
	errors int
}

func (t *testingT) Errorf(format string, args ...interface{}) {
	t.errors++
	fmt.Fprintf(os.Stderr, format, args...)
}

func main() {
	t := &testingT{}
	TestList(t)
	TestMap(t)
	TestSet(t)
	TestStructTest(t)
	TestCompound(t)
	TestCustomName(t)
	os.Exit(t.errors)
}
