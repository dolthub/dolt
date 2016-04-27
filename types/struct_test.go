package types

import (
	"testing"

	"github.com/attic-labs/noms/ref"
	"github.com/stretchr/testify/assert"
)

func TestGenericStructEquals(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S1", []Field{
		Field{"x", BoolType, false},
		Field{"o", StringType, true},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	data1 := structData{"x": Bool(true)}
	s1 := newStructFromData(data1, 0, nil, typ, typeDef)
	data2 := structData{"x": Bool(true), "extra": NewString("is ignored")}
	s2 := newStructFromData(data2, 0, nil, typ, typeDef)

	assert.True(s1.Equals(s2))
	assert.True(s2.Equals(s1))
}

func TestGenericStructChunks(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), false},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	b := Bool(true)

	data1 := structData{"r": NewRef(b.Ref())}
	s1 := newStructFromData(data1, 0, nil, typ, typeDef)

	assert.Len(s1.Chunks(), 2)
	assert.Equal(pkgRef, s1.Chunks()[0].TargetRef())
	assert.Equal(b.Ref(), s1.Chunks()[1].TargetRef())
}

func TestGenericStructChunksOptional(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S1", []Field{
		Field{"r", MakeRefType(BoolType), true},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	b := Bool(true)

	data1 := structData{}
	s1 := newStructFromData(data1, 0, nil, typ, typeDef)

	assert.Len(s1.Chunks(), 1)
	assert.Equal(pkgRef, s1.Chunks()[0].TargetRef())

	data2 := structData{"r": NewRef(b.Ref())}
	s2 := newStructFromData(data2, 0, nil, typ, typeDef)

	assert.Len(s2.Chunks(), 2)
	assert.Equal(pkgRef, s2.Chunks()[0].TargetRef())
	assert.Equal(b.Ref(), s2.Chunks()[1].TargetRef())
}

func TestGenericStructChunksUnion(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S1", []Field{}, []Field{
		Field{"r", MakeRefType(BoolType), false},
		Field{"s", StringType, false},
	})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	b := Bool(true)

	s1 := NewStruct(typ, typeDef, structData{"s": NewString("hi")})

	assert.Len(s1.Chunks(), 1)
	assert.Equal(pkgRef, s1.Chunks()[0].TargetRef())

	s2 := NewStruct(typ, typeDef, structData{"r": NewRef(b.Ref())})

	assert.Len(s2.Chunks(), 2)
	assert.Equal(pkgRef, s2.Chunks()[0].TargetRef())
	assert.Equal(b.Ref(), s2.Chunks()[1].TargetRef())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S2", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	s := NewStruct(typ, typeDef, map[string]Value{"b": Bool(true)})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("o")
	assert.False(ok)

	_, ok = s.MaybeGet("x")
	assert.False(ok)

	s2 := NewStruct(typ, typeDef, map[string]Value{"b": Bool(false), "o": NewString("hi")})
	assert.True(s2.Get("b").Equals(Bool(false)))
	o, ok := s2.MaybeGet("o")
	assert.True(ok)
	assert.True(NewString("hi").Equals(o))

	assert.Panics(func() { NewStruct(typ, typeDef, nil) })
	assert.Panics(func() { NewStruct(typ, typeDef, map[string]Value{"o": NewString("hi")}) })
}

func TestGenericStructNewUnion(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S3", []Field{}, []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, false},
	})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	s := NewStruct(typ, typeDef, map[string]Value{"b": Bool(true)})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("o")
	assert.False(ok)
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S3", []Field{
		Field{"b", BoolType, false},
		Field{"o", StringType, true},
	}, []Field{})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	s := NewStruct(typ, typeDef, map[string]Value{"b": Bool(true)})
	s2 := s.Set("b", Bool(false))

	assert.Panics(func() { s.Set("b", Number(1)) })
	assert.Panics(func() { s.Set("x", Number(1)) })

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}

func TestGenericStructSetUnion(t *testing.T) {
	assert := assert.New(t)

	typeDef := MakeStructType("S3", []Field{}, []Field{
		Field{"b", BoolType, false},
		Field{"s", StringType, false},
	})
	pkg := NewPackage([]*Type{typeDef}, []ref.Ref{})
	pkgRef := RegisterPackage(&pkg)
	typ := MakeType(pkgRef, 0)

	s := NewStruct(typ, typeDef, map[string]Value{"b": Bool(true)})
	assert.Equal(uint32(0), s.UnionIndex())
	assert.True(Bool(true).Equals(s.UnionValue()))
	s2 := s.Set("s", NewString("hi"))
	assert.Equal(uint32(1), s2.UnionIndex())
	assert.True(NewString("hi").Equals(s2.UnionValue()))

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}
