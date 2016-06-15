// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

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

func assertPathStringResolvesTo(assert *assert.Assertions, expect, ref Value, str string) {
	p, err := NewPath().AddPath(str)
	assert.NoError(err)
	assertPathResolvesTo(assert, expect, ref, p)
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("", structData{
		"foo": NewString("foo"),
		"bar": Bool(false),
		"baz": Number(203),
	})

	assertPathResolvesTo(assert, NewString("foo"), v, NewPath().AddField("foo"))
	assertPathStringResolvesTo(assert, NewString("foo"), v, `.foo`)
	assertPathResolvesTo(assert, Bool(false), v, NewPath().AddField("bar"))
	assertPathStringResolvesTo(assert, Bool(false), v, `.bar`)
	assertPathResolvesTo(assert, Number(203), v, NewPath().AddField("baz"))
	assertPathStringResolvesTo(assert, Number(203), v, `.baz`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddField("notHere"))
	assertPathStringResolvesTo(assert, nil, v, `.notHere`)
}

func TestPathList(t *testing.T) {
	assert := assert.New(t)

	v := NewList(Number(1), Number(3), NewString("foo"), Bool(false))

	assertPathResolvesTo(assert, Number(1), v, NewPath().AddIndex(Number(0)))
	assertPathStringResolvesTo(assert, Number(1), v, `[0]`)
	assertPathResolvesTo(assert, Number(3), v, NewPath().AddIndex(Number(1)))
	assertPathStringResolvesTo(assert, Number(3), v, `[1]`)
	assertPathResolvesTo(assert, NewString("foo"), v, NewPath().AddIndex(Number(2)))
	assertPathStringResolvesTo(assert, NewString("foo"), v, `[2]`)
	assertPathResolvesTo(assert, Bool(false), v, NewPath().AddIndex(Number(3)))
	assertPathStringResolvesTo(assert, Bool(false), v, `[3]`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(4)))
	assertPathStringResolvesTo(assert, nil, v, `[4]`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(-4)))
	assertPathStringResolvesTo(assert, nil, v, `[-4]`)
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
	assertPathStringResolvesTo(assert, NewString("foo"), v, `[1]`)
	assertPathResolvesTo(assert, NewString("bar"), v, NewPath().AddIndex(NewString("two")))
	assertPathStringResolvesTo(assert, NewString("bar"), v, `["two"]`)
	assertPathResolvesTo(assert, Number(23), v, NewPath().AddIndex(Bool(false)))
	assertPathStringResolvesTo(assert, Number(23), v, `[false]`)
	assertPathResolvesTo(assert, Number(4.5), v, NewPath().AddIndex(Number(2.3)))
	assertPathStringResolvesTo(assert, Number(4.5), v, `[2.3]`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(4)))
	assertPathStringResolvesTo(assert, nil, v, `[4]`)
}

func TestPathMulti(t *testing.T) {
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
	assertPathStringResolvesTo(assert, l, s, `.foo`)
	assertPathResolvesTo(assert, m1, s, NewPath().AddField("foo").AddIndex(Number(0)))
	assertPathStringResolvesTo(assert, m1, s, `.foo[0]`)
	assertPathResolvesTo(assert, NewString("foo"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("a")))
	assertPathStringResolvesTo(assert, NewString("foo"), s, `.foo[0]["a"]`)
	assertPathResolvesTo(assert, NewString("bar"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("b")))
	assertPathStringResolvesTo(assert, NewString("bar"), s, `.foo[0]["b"]`)
	assertPathResolvesTo(assert, NewString("car"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("c")))
	assertPathStringResolvesTo(assert, NewString("car"), s, `.foo[0]["c"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(NewString("x")))
	assertPathStringResolvesTo(assert, nil, s, `.foo[0]["x"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(2)).AddIndex(NewString("c")))
	assertPathStringResolvesTo(assert, nil, s, `.foo[2]["c"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("notHere").AddIndex(Number(0)).AddIndex(NewString("c")))
	assertPathStringResolvesTo(assert, nil, s, `.notHere[0]["c"]`)
	assertPathResolvesTo(assert, m2, s, NewPath().AddField("foo").AddIndex(Number(1)))
	assertPathStringResolvesTo(assert, m2, s, `.foo[1]`)
	assertPathResolvesTo(assert, NewString("dar"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(NewString("d")))
	assertPathStringResolvesTo(assert, NewString("dar"), s, `.foo[1]["d"]`)
	assertPathResolvesTo(assert, NewString("earth"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(Bool(false)))
	assertPathStringResolvesTo(assert, NewString("earth"), s, `.foo[1][false]`)
}

func TestPathToAndFromString(t *testing.T) {
	assert := assert.New(t)

	test := func(str string, p Path) {
		assert.Equal(str, p.String())
		p2, err := NewPath().AddPath(str)
		assert.NoError(err)
		assert.Equal(p, p2)
	}

	test("[0]", NewPath().AddIndex(Number(0)))
	test("[\"0\"][\"1\"][\"100\"]", NewPath().AddIndex(NewString("0")).AddIndex(NewString("1")).AddIndex(NewString("100")))
	test(".foo[0].bar[4.5][false]", NewPath().AddField("foo").AddIndex(Number(0)).AddField("bar").AddIndex(Number(4.5)).AddIndex(Bool(false)))
}

func TestPathImmutability(t *testing.T) {
	assert := assert.New(t)
	p1 := NewPath().AddField("/").AddField("value").AddField("data").AddIndex(Number(1)).AddField("data")
	p2 := p1.AddField("x")
	p3 := p1.AddField("y")
	p4 := p3.AddIndex(Number(19))
	assert.Equal("./.value.data[1].data", p1.String())
	assert.Equal("./.value.data[1].data.x", p2.String())
	assert.Equal("./.value.data[1].data.y", p3.String())
	assert.Equal("./.value.data[1].data.y[19]", p4.String())
}

func TestPathParseSuccess(t *testing.T) {
	assert := assert.New(t)

	test := func(str string, expectPath Path) {
		p, err := NewPath().AddPath(str)
		assert.NoError(err)
		assert.Equal(expectPath, p)
	}

	test(".foo", NewPath().AddField("foo"))
	test(".Q", NewPath().AddField("Q"))
	test(".QQ", NewPath().AddField("QQ"))
	test("[true]", NewPath().AddIndex(Bool(true)))
	test("[false]", NewPath().AddIndex(Bool(false)))
	test("[42]", NewPath().AddIndex(Number(42)))
	test("[1e4]", NewPath().AddIndex(Number(1e4)))
	test("[1.]", NewPath().AddIndex(Number(1.)))
	test("[1.345]", NewPath().AddIndex(Number(1.345)))
	test(`[""]`, NewPath().AddIndex(NewString("")))
	test(`["42"]`, NewPath().AddIndex(NewString("42")))
	test("[\"line\nbreak\rreturn\"]", NewPath().AddIndex(NewString("line\nbreak\rreturn")))
	test(`["qu\\ote\""]`, NewPath().AddIndex(NewString(`qu\ote"`)))
	test(`["π"]`, NewPath().AddIndex(NewString("π")))
	test(`["[[br][]acke]]ts"]`, NewPath().AddIndex(NewString("[[br][]acke]]ts")))
	test(`["xπy✌z"]`, NewPath().AddIndex(NewString("xπy✌z")))
	test(`["ಠ_ಠ"]`, NewPath().AddIndex(NewString("ಠ_ಠ")))
}

func TestPathParseErrors(t *testing.T) {
	assert := assert.New(t)

	test := func(str, expectError string) {
		p, err := NewPath().AddPath(str)
		assert.Equal(Path{}, p)
		if err != nil {
			assert.Equal(expectError, err.Error())
		} else {
			assert.Fail("Expected " + expectError)
		}
	}

	test("", "Empty path")
	test("foo", "f is not a valid operator")
	test(".", "Invalid field ")
	test("[", "Path ends in [")
	test(".#", "Invalid field #")
	test(". ", "Invalid field  ")
	test(". invalid.field", "Invalid field  invalid.field")
	test(".foo.", "Invalid field ")
	test(".foo.#invalid.field", "Invalid field #invalid.field")
	test(".foo!", "! is not a valid operator")
	test(".foo!bar", "! is not a valid operator")
	test(".foo[", "Path ends in [")
	test(".foo[.bar", "[ is missing closing ]")
	test(".foo]", "] is missing opening [")
	test(".foo].bar", "] is missing opening [")
	test(".foo[]", "Invalid index ")
	test(".foo[[]", "Invalid index [")
	test(".foo[[]]", "Invalid index [")
	test(".foo[42.1.2]", "Invalid index 42.1.2")
	test(".foo[1f4]", "Invalid index 1f4")
	test(".foo[hello]", "Invalid index hello")
	test(".foo['hello']", "Invalid index 'hello'")
	test(`.foo[\]`, `Invalid index \`)
	test(`.foo[\\]`, `Invalid index \\`)
	test(`.foo["hello]`, "[ is missing closing ]")
	test(`.foo["hello`, "[ is missing closing ]")
	test(`.foo["`, "[ is missing closing ]")
	test(`.foo["\`, "[ is missing closing ]")
	test(`.foo["]`, "[ is missing closing ]")
	test(".foo[#sha1-invalid]", "Invalid index #sha1-invalid")
	test(`.foo["hello\nworld"]`, `Only " and \ can be escaped`)
}
