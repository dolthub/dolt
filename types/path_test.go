package types

import (
	"testing"

	"github.com/attic-labs/testify/assert"
)

func assertPathResolvesTo(assert *assert.Assertions, expect, ref Value, p Path) {
	if expect == nil {
		assert.Nil(p.Resolve(ref))
		return
	}

	assert.True(expect.Equals(p.Resolve(ref)))
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("", structData{
		"foo": NewString("foo"),
		"bar": Bool(false),
		"baz": Number(203),
	})

	assertPathResolvesTo(assert, NewString("foo"), v, NewPath().AddField("foo"))
	assertPathResolvesTo(assert, Bool(false), v, NewPath().AddField("bar"))
	assertPathResolvesTo(assert, Number(203), v, NewPath().AddField("baz"))
	assertPathResolvesTo(assert, nil, v, NewPath().AddField("notHere"))
}

func TestPathList(t *testing.T) {
	assert := assert.New(t)

	v := NewList(Number(1), Number(3), NewString("foo"), Bool(false))

	assertPathResolvesTo(assert, Number(1), v, NewPath().AddIndex(Number(0)))
	assertPathResolvesTo(assert, Number(3), v, NewPath().AddIndex(Number(1)))
	assertPathResolvesTo(assert, NewString("foo"), v, NewPath().AddIndex(Number(2)))
	assertPathResolvesTo(assert, Bool(false), v, NewPath().AddIndex(Number(3)))
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(4)))
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(-4)))
}

func TestPathMap(t *testing.T) {
	assert := assert.New(t)

	v := NewMap(
		Number(1), NewString("foo"),
		NewString("two"), NewString("bar"),
		Bool(false), Number(23),
		Number(2.3), Number(4.5),
	)

	assertPathResolvesTo(assert, NewString("foo"), v, NewPath().AddIndex(Number(1)))
	assertPathResolvesTo(assert, NewString("bar"), v, NewPath().AddIndex(NewString("two")))
	assertPathResolvesTo(assert, Number(23), v, NewPath().AddIndex(Bool(false)))
	assertPathResolvesTo(assert, Number(4.5), v, NewPath().AddIndex(Number(2.3)))
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(4)))
}

func TestPathMutli(t *testing.T) {
	assert := assert.New(t)

	m1 := NewMap(
		NewString("a"), NewString("foo"),
		NewString("b"), NewString("bar"),
		NewString("c"), NewString("car"),
	)

	m2 := NewMap(
		NewString("d"), NewString("dar"),
		Bool(false), NewString("earth"),
	)

	l := NewList(m1, m2)

	s := NewStruct("", structData{
		"foo": l,
	})

	assertPathResolvesTo(assert, l, s, NewPath().AddField("foo"))
	assertPathResolvesTo(assert, m1, s, NewPath().AddField("foo").AddIndex(Number(0)))
	assertPathResolvesTo(assert, NewString("foo"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("a")))
	assertPathResolvesTo(assert, NewString("bar"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("b")))
	assertPathResolvesTo(assert, NewString("car"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("c")))
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("x")))
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(2)).AddIndex(NewString("c")))
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("notHere").AddIndex(Number(0)).AddIndex(NewString("c")))
	assertPathResolvesTo(assert, m2, s, NewPath().AddField("foo").AddIndex(Number(1)))
	assertPathResolvesTo(assert, NewString("dar"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(NewString("d")))
	assertPathResolvesTo(assert, NewString("earth"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(Bool(false)))
}

func TestPathToString(t *testing.T) {
	assert := assert.New(t)
	assert.Equal("[0]", NewPath().AddIndex(Number(0)).String())
	assert.Equal("[\"0\"][\"1\"][\"100\"]", NewPath().AddIndex(NewString("0")).AddIndex(NewString("1")).AddIndex(NewString("100")).String())
	assert.Equal(".foo[0].bar[4.5][false]", NewPath().AddField("foo").AddIndex(Number(0)).AddField("bar").AddIndex(Number(4.5)).AddIndex(Bool(false)).String())
}
