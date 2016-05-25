package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
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

	data1 := structData{"r": NewRef(b)}
	s1 := newStructFromData(data1, typ)

	assert.Len(s1.Chunks(), 1)
	assert.Equal(b.Hash(), s1.Chunks()[0].TargetHash())
}

func TestGenericStructNew(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct("S2", map[string]Value{"b": Bool(true), "o": NewString("hi")})
	assert.True(s.Get("b").Equals(Bool(true)))
	_, ok := s.MaybeGet("missing")
	assert.False(ok)

	s2 := NewStruct("S2", map[string]Value{"b": Bool(false), "o": NewString("hi")})
	assert.True(s2.Get("b").Equals(Bool(false)))
	o, ok := s2.MaybeGet("o")
	assert.True(ok)
	assert.True(NewString("hi").Equals(o))

	typ := MakeStructType("S2", TypeMap{
		"b": BoolType,
		"o": StringType,
	})
	assert.Panics(func() { NewStructWithType(typ, nil) })
	assert.Panics(func() { NewStructWithType(typ, map[string]Value{"o": NewString("hi")}) })
}

func TestGenericStructSet(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct("S3", map[string]Value{"b": Bool(true), "o": NewString("hi")})
	s2 := s.Set("b", Bool(false))

	assert.Panics(func() { s.Set("b", Number(1)) })
	assert.Panics(func() { s.Set("x", Number(1)) })

	s3 := s2.Set("b", Bool(true))
	assert.True(s.Equals(s3))
}

func TestStructDiff(t *testing.T) {
	assert := assert.New(t)

	assertDiff := func(expect []string, s1, s2 Struct) {
		actual := StructDiff(s1, s2)
		assert.Equal(len(expect), len(actual))
		for i, _ := range actual {
			assert.Equal(expect[i], actual[i])
		}
	}

	s1 := NewStruct("", map[string]Value{"a": Bool(true), "b": NewString("hi"), "c": Number(4)})

	assertDiff([]string{}, s1,
		NewStruct("", map[string]Value{"a": Bool(true), "b": NewString("hi"), "c": Number(4)}))

	assertDiff([]string{"a", "b"}, s1,
		NewStruct("", map[string]Value{"a": Bool(false), "b": NewString("bye"), "c": Number(4)}))

	assertDiff([]string{"b", "c"}, s1,
		NewStruct("", map[string]Value{"a": Bool(true), "b": NewString("bye"), "c": Number(5)}))

	assertDiff([]string{"a", "c"}, s1,
		NewStruct("", map[string]Value{"a": Bool(false), "b": NewString("hi"), "c": Number(10)}))

	s2 := NewStruct("", map[string]Value{
		"a": NewList(Number(0), Number(1)),
		"b": NewMap(NewString("foo"), Bool(false), NewString("bar"), Bool(true)),
		"c": NewSet(Number(0), Number(1), NewString("foo")),
	})

	assertDiff([]string{}, s2,
		NewStruct("", map[string]Value{
			"a": NewList(Number(0), Number(1)),
			"b": NewMap(NewString("foo"), Bool(false), NewString("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), NewString("foo")),
		}))

	assertDiff([]string{"a", "b"}, s2,
		NewStruct("", map[string]Value{
			"a": NewList(Number(1), Number(1)),
			"b": NewMap(NewString("foo"), Bool(true), NewString("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), NewString("foo")),
		}))

	assertDiff([]string{"a", "c"}, s2,
		NewStruct("", map[string]Value{
			"a": NewList(Number(0)),
			"b": NewMap(NewString("foo"), Bool(false), NewString("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(2), NewString("foo")),
		}))

	assertDiff([]string{"b", "c"}, s2,
		NewStruct("", map[string]Value{
			"a": NewList(Number(0), Number(1)),
			"b": NewMap(NewString("boo"), Bool(false), NewString("bar"), Bool(true)),
			"c": NewSet(Number(0), Number(1), NewString("bar")),
		}))
}
