// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/testify/assert"
)

func assertPathResolvesTo(assert *assert.Assertions, expect, ref Value, p Path) {
	actual := p.Resolve(ref)
	if expect == nil {
		assert.Nil(actual)
	} else if actual == nil {
		assert.Fail("", "Expected %s, but got nil", EncodedValue(expect))
	} else {
		assert.True(expect.Equals(actual), "Expected %s, but got %s", EncodedValue(expect), EncodedValue(actual))
	}
}

func assertPathStringResolvesTo(assert *assert.Assertions, expect, ref Value, str string) {
	p, err := NewPath().AddPath(str)
	assert.NoError(err)
	assertPathResolvesTo(assert, expect, ref, p)
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("", structData{
		"foo": String("foo"),
		"bar": Bool(false),
		"baz": Number(203),
	})

	assertPathResolvesTo(assert, String("foo"), v, NewPath().AddField("foo"))
	assertPathStringResolvesTo(assert, String("foo"), v, `.foo`)
	assertPathResolvesTo(assert, Bool(false), v, NewPath().AddField("bar"))
	assertPathStringResolvesTo(assert, Bool(false), v, `.bar`)
	assertPathResolvesTo(assert, Number(203), v, NewPath().AddField("baz"))
	assertPathStringResolvesTo(assert, Number(203), v, `.baz`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddField("notHere"))
	assertPathStringResolvesTo(assert, nil, v, `.notHere`)
}

func TestPathList(t *testing.T) {
	assert := assert.New(t)

	v := NewList(Number(1), Number(3), String("foo"), Bool(false))

	assertPathResolvesTo(assert, Number(1), v, NewPath().AddIndex(Number(0)))
	assertPathStringResolvesTo(assert, Number(1), v, `[0]`)
	assertPathResolvesTo(assert, Number(3), v, NewPath().AddIndex(Number(1)))
	assertPathStringResolvesTo(assert, Number(3), v, `[1]`)
	assertPathResolvesTo(assert, String("foo"), v, NewPath().AddIndex(Number(2)))
	assertPathStringResolvesTo(assert, String("foo"), v, `[2]`)
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
		Number(1), String("foo"),
		String("two"), String("bar"),
		Bool(false), Number(23),
		Number(2.3), Number(4.5),
	)

	assertPathResolvesTo(assert, String("foo"), v, NewPath().AddIndex(Number(1)))
	assertPathStringResolvesTo(assert, String("foo"), v, `[1]`)
	assertPathResolvesTo(assert, String("bar"), v, NewPath().AddIndex(String("two")))
	assertPathStringResolvesTo(assert, String("bar"), v, `["two"]`)
	assertPathResolvesTo(assert, Number(23), v, NewPath().AddIndex(Bool(false)))
	assertPathStringResolvesTo(assert, Number(23), v, `[false]`)
	assertPathResolvesTo(assert, Number(4.5), v, NewPath().AddIndex(Number(2.3)))
	assertPathStringResolvesTo(assert, Number(4.5), v, `[2.3]`)
	assertPathResolvesTo(assert, nil, v, NewPath().AddIndex(Number(4)))
	assertPathStringResolvesTo(assert, nil, v, `[4]`)
}

func TestPathHashIndex(t *testing.T) {
	assert := assert.New(t)

	b := Bool(true)
	br := NewRef(b)
	i := Number(0)
	s := String("foo")
	l := NewList(b, i, s)
	lr := NewRef(l)
	m := NewMap(
		b, br,
		br, i,
		i, s,
		l, lr,
		lr, b,
	)

	hashStr := func(v Value) string {
		return fmt.Sprintf("[#%s]", v.Hash())
	}

	// Primitives are only addressable by their values.
	assertPathResolvesTo(assert, nil, m, NewPath().AddHashIndex(b.Hash()))
	assertPathStringResolvesTo(assert, nil, m, hashStr(b))
	assertPathResolvesTo(assert, nil, m, NewPath().AddHashIndex(i.Hash()))
	assertPathStringResolvesTo(assert, nil, m, hashStr(i))
	assertPathResolvesTo(assert, nil, m, NewPath().AddHashIndex(s.Hash()))
	assertPathStringResolvesTo(assert, nil, m, hashStr(s))

	// Other values are only addressable by their hashes.
	assertPathResolvesTo(assert, i, m, NewPath().AddHashIndex(br.Hash()))
	assertPathStringResolvesTo(assert, i, m, hashStr(br))
	assertPathResolvesTo(assert, lr, m, NewPath().AddHashIndex(l.Hash()))
	assertPathStringResolvesTo(assert, lr, m, hashStr(l))
	assertPathResolvesTo(assert, b, m, NewPath().AddHashIndex(lr.Hash()))
	assertPathStringResolvesTo(assert, b, m, hashStr(lr))

	// Lists cannot be addressed by hashes, obviously.
	assertPathResolvesTo(assert, nil, l, NewPath().AddHashIndex(i.Hash()))
	assertPathStringResolvesTo(assert, nil, l, hashStr(i))
}

func TestPathHashIndexOfSingletonMap(t *testing.T) {
	// This test is to make sure we don't accidentally return |b| if it's the only element.
	assert := assert.New(t)
	b := Bool(true)
	m := NewMap(b, b)
	assertPathResolvesTo(assert, nil, m, NewPath().AddHashIndex(b.Hash()))
}

func TestPathMulti(t *testing.T) {
	assert := assert.New(t)

	m1 := NewMap(
		String("a"), String("foo"),
		String("b"), String("bar"),
		String("c"), String("car"),
	)

	m2 := NewMap(
		String("d"), String("dar"),
		Bool(false), String("earth"),
		m1, String("fire"),
	)

	l := NewList(m1, m2)

	s := NewStruct("", structData{
		"foo": l,
	})

	assertPathResolvesTo(assert, l, s, NewPath().AddField("foo"))
	assertPathStringResolvesTo(assert, l, s, `.foo`)
	assertPathResolvesTo(assert, m1, s, NewPath().AddField("foo").AddIndex(Number(0)))
	assertPathStringResolvesTo(assert, m1, s, `.foo[0]`)
	assertPathResolvesTo(assert, String("foo"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(String("a")))
	assertPathStringResolvesTo(assert, String("foo"), s, `.foo[0]["a"]`)
	assertPathResolvesTo(assert, String("bar"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(String("b")))
	assertPathStringResolvesTo(assert, String("bar"), s, `.foo[0]["b"]`)
	assertPathResolvesTo(assert, String("car"), s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(String("c")))
	assertPathStringResolvesTo(assert, String("car"), s, `.foo[0]["c"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(0)).AddIndex(String("x")))
	assertPathStringResolvesTo(assert, nil, s, `.foo[0]["x"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("foo").AddIndex(Number(2)).AddIndex(String("c")))
	assertPathStringResolvesTo(assert, nil, s, `.foo[2]["c"]`)
	assertPathResolvesTo(assert, nil, s, NewPath().AddField("notHere").AddIndex(Number(0)).AddIndex(String("c")))
	assertPathStringResolvesTo(assert, nil, s, `.notHere[0]["c"]`)
	assertPathResolvesTo(assert, m2, s, NewPath().AddField("foo").AddIndex(Number(1)))
	assertPathStringResolvesTo(assert, m2, s, `.foo[1]`)
	assertPathResolvesTo(assert, String("dar"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(String("d")))
	assertPathStringResolvesTo(assert, String("dar"), s, `.foo[1]["d"]`)
	assertPathResolvesTo(assert, String("earth"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddIndex(Bool(false)))
	assertPathStringResolvesTo(assert, String("earth"), s, `.foo[1][false]`)
	assertPathResolvesTo(assert, String("fire"), s, NewPath().AddField("foo").AddIndex(Number(1)).AddHashIndex(m1.Hash()))
	assertPathStringResolvesTo(assert, String("fire"), s, fmt.Sprintf(`.foo[1][#%s]`, m1.Hash().String()))
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
	test("[\"0\"][\"1\"][\"100\"]", NewPath().AddIndex(String("0")).AddIndex(String("1")).AddIndex(String("100")))
	test(".foo[0].bar[4.5][false]", NewPath().AddField("foo").AddIndex(Number(0)).AddField("bar").AddIndex(Number(4.5)).AddIndex(Bool(false)))
	h := Number(42).Hash() // arbitrary hash
	test(fmt.Sprintf(".foo[#%s]", h.String()), NewPath().AddField("foo").AddHashIndex(h))
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
	test(`[""]`, NewPath().AddIndex(String("")))
	test(`["42"]`, NewPath().AddIndex(String("42")))
	test("[\"line\nbreak\rreturn\"]", NewPath().AddIndex(String("line\nbreak\rreturn")))
	test(`["qu\\ote\""]`, NewPath().AddIndex(String(`qu\ote"`)))
	test(`["π"]`, NewPath().AddIndex(String("π")))
	test(`["[[br][]acke]]ts"]`, NewPath().AddIndex(String("[[br][]acke]]ts")))
	test(`["xπy✌z"]`, NewPath().AddIndex(String("xπy✌z")))
	test(`["ಠ_ಠ"]`, NewPath().AddIndex(String("ಠ_ಠ")))
	test(`["ಠ_ಠ"]`, NewPath().AddIndex(String("ಠ_ಠ")))
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
	test(".foo[]", "Empty index value")
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
	test(".foo[#]", "Invalid hash ")
	test(".foo[#sha1-invalid]", "Invalid hash sha1-invalid")
	test(`.foo["hello\nworld"]`, `Only " and \ can be escaped`)
}
