package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenericStructEquals(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", TypeMap{
		"x": BoolType,
		"s": StringType,
	})

	data1 := structData{"x": Bool(true), "s": NewString("hi")}
	s1 := newStructFromData(data1, typ)
	data2 := structData{"x": Bool(true), "s": NewString("hi")}
	s2 := newStructFromData(data2, typ)

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", TypeMap{
		"r": MakeRefType(BoolType),
	})

	b := Bool(true)

	data1 := structData{"r": NewRef(b.Ref())}
	s1 := newStructFromData(data1, typ)

	assert.Len(s1.Chunks(), 1)
	assert.Equal(b.Ref(), s1.Chunks()[0].TargetRef())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S2", TypeMap{
		"b": BoolType,
		"o": StringType,
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true), "o": NewString("hi")})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("missing")
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

	typ := MakeStructType("S3", TypeMap{
		"b": BoolType,
		"o": StringType,
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true), "o": NewString("hi")})
	s2 := s.Set("b", Bool(false))

	assert.Panics(func() { s.Set("b", Number(1)) })
	assert.Panics(func() { s.Set("x", Number(1)) })

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}
