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
	}, []Field{})

	data1 := structData{"x": Bool(true)}
	s1 := newStructFromData(data1, 0, nil, typ)
	data2 := structData{"x": Bool(true), "extra": NewString("is ignored")}
	s2 := newStructFromData(data2, 0, nil, typ)

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), false},
	}, []Field{})

	b := Bool(true)

	data1 := structData{"r": NewRef(b.Ref())}
	s1 := newStructFromData(data1, 0, nil, typ)

	assert.Len(s1.Chunks(), 1)
	assert.Equal(b.Ref(), s1.Chunks()[0].TargetRef())
}

func TestGenericStructChunksOptional(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), true},
	}, []Field{})

	b := Bool(true)

	data1 := structData{}
	s1 := newStructFromData(data1, 0, nil, typ)

	assert.Len(s1.Chunks(), 0)

	data2 := structData{"r": NewRef(b.Ref())}
	s2 := newStructFromData(data2, 0, nil, typ)

	assert.Len(s2.Chunks(), 1)
	assert.Equal(b.Ref(), s2.Chunks()[0].TargetRef())
}

func TestGenericStructChunksUnion(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S1", []Field{}, []Field{
		Field{"r", MakeRefType(BoolType), false},
		Field{"s", StringType, false},
	})

	b := Bool(true)

	s1 := NewStruct(typ, structData{"s": NewString("hi")})

	assert.Len(s1.Chunks(), 0)

	s2 := NewStruct(typ, structData{"r": NewRef(b.Ref())})

	assert.Len(s2.Chunks(), 1)
	assert.Equal(b.Ref(), s2.Chunks()[0].TargetRef())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S2", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	}, []Field{})

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

func TestGenericStructNewUnion(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S3", []Field{}, []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, false},
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true)})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("o")
	assert.False(ok)
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S3", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	}, []Field{})

	s := NewStruct(typ, map[string]Value{"b": Bool(true)})
	s2 := s.Set("b", Bool(false))

	assert.Panics(func() { s.Set("b", Number(1)) })
	assert.Panics(func() { s.Set("x", Number(1)) })

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}

func TestGenericStructSetUnion(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("S3", []Field{}, []Field{
		Field{"b", BoolType, false},
		Field{"s", StringType, false},
	})

	s := NewStruct(typ, map[string]Value{"b": Bool(true)})
	assert.Equal(uint32(0), s.UnionIndex())
	assert.True(Bool(true).Equals(s.UnionValue()))
	s2 := s.Set("s", NewString("hi"))
	assert.Equal(uint32(1), s2.UnionIndex())
	assert.True(NewString("hi").Equals(s2.UnionValue()))

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}
