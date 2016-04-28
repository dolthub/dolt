package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenericStructEquals(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{
		Field{"x", BoolType, false},
		Field{"o", StringType, true},
	})

	data1 := structData{"x": Bool(true)}
	s1 := newStructFromData(data1, typ)
	data2 := structData{"x": Bool(true), "extra": NewString("is ignored")}
	s2 := newStructFromData(data2, typ)

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), false},
	})

	b := Bool(true)

	data1 := structData{"r": NewRef(b.Ref())}
	s1 := newStructFromData(data1, typ)

	assert.Len(s1.Chunks(), 1)
	assert.Equal(b.Ref(), s1.Chunks()[0].TargetRef())
}

func TestGenericStructChunksOptional(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), true},
	})

	b := Bool(true)

	data1 := structData{}
	s1 := newStructFromData(data1, typ)

	assert.Len(s1.Chunks(), 0)

	data2 := structData{"r": NewRef(b.Ref())}
	s2 := newStructFromData(data2, typ)

	assert.Len(s2.Chunks(), 1)
	assert.Equal(b.Ref(), s2.Chunks()[0].TargetRef())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S2", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true)})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("o")
	assert.False(ok)

	_, ok = s.MaybeGet("x")
	assert.False(ok)

	s2 := NewStruct(typ, map[string]Value{"b": Bool(false), "o": NewString("hi")})
	assert.True(s2.Get("b").Equals(Bool(false)))
	o, ok := s2.MaybeGet("o")
	assert.True(ok)
	assert.True(NewString("hi").Equals(o))

	assert.Panics(func() { NewStruct(typ, nil) })
	assert.Panics(func() { NewStruct(typ, map[string]Value{"o": NewString("hi")}) })
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S3", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true)})
	s2 := s.Set("b", Bool(false))

	assert.Panics(func() { s.Set("b", Number(1)) })
	assert.Panics(func() { s.Set("x", Number(1)) })

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}
