// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/hash"
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

func TestPathIndex(t *testing.T) {
	assert := assert.New(t)

	var v Value
	resolvesTo := func(exp, val Value, str string) {
		// Indices resolve to |exp|.
		assertPathResolvesTo(assert, exp, v, NewPath().AddIndex(val))
		assertPathStringResolvesTo(assert, exp, v, str)
		// Keys resolve to themselves.
		if exp != nil {
			exp = val
		}
		assertPathResolvesTo(assert, exp, v, NewPath().AddKeyIndex(val))
		assertPathStringResolvesTo(assert, exp, v, str+"@key")
	}

	v = NewList(Number(1), Number(3), String("foo"), Bool(false))

	resolvesTo(Number(1), Number(0), "[0]")
	resolvesTo(Number(3), Number(1), "[1]")
	resolvesTo(String("foo"), Number(2), "[2]")
	resolvesTo(Bool(false), Number(3), "[3]")
	resolvesTo(nil, Number(4), "[4]")
	resolvesTo(nil, Number(-4), "[-4]")

	v = NewMap(
		Number(1), String("foo"),
		String("two"), String("bar"),
		Bool(false), Number(23),
		Number(2.3), Number(4.5),
	)

	resolvesTo(String("foo"), Number(1), "[1]")
	resolvesTo(String("bar"), String("two"), `["two"]`)
	resolvesTo(Number(23), Bool(false), "[false]")
	resolvesTo(Number(4.5), Number(2.3), "[2.3]")
	resolvesTo(nil, Number(4), "[4]")
}

func TestPathHashIndex(t *testing.T) {
	assert := assert.New(t)

	b := Bool(true)
	br := NewRef(b)
	i := Number(0)
	str := String("foo")
	l := NewList(b, i, str)
	lr := NewRef(l)
	m := NewMap(
		b, br,
		br, i,
		i, str,
		l, lr,
		lr, b,
	)
	s := NewSet(b, br, i, str, l, lr)

	hashStr := func(v Value) string {
		return fmt.Sprintf("[#%s]", v.Hash())
	}

	resolvesTo := func(col, exp, val Value) {
		// Values resolve to |exp|.
		assertPathResolvesTo(assert, exp, col, NewPath().AddHashIndex(val.Hash()))
		assertPathStringResolvesTo(assert, exp, col, hashStr(val))
		// Keys resolve to themselves.
		if exp != nil {
			exp = val
		}
		assertPathResolvesTo(assert, exp, col, NewPath().AddHashKeyIndex(val.Hash()))
		assertPathStringResolvesTo(assert, exp, col, hashStr(val)+"@key")
	}

	// Primitives are only addressable by their values.
	resolvesTo(m, nil, b)
	resolvesTo(m, nil, i)
	resolvesTo(m, nil, str)
	resolvesTo(s, nil, b)
	resolvesTo(s, nil, i)
	resolvesTo(s, nil, str)

	// Other values are only addressable by their hashes.
	resolvesTo(m, i, br)
	resolvesTo(m, lr, l)
	resolvesTo(m, b, lr)
	resolvesTo(s, br, br)
	resolvesTo(s, l, l)
	resolvesTo(s, lr, lr)

	// Lists cannot be addressed by hashes, obviously.
	resolvesTo(l, nil, i)
}

func TestPathHashIndexOfSingletonCollection(t *testing.T) {
	// This test is to make sure we don't accidentally return |b| if it's the only element.
	assert := assert.New(t)

	resolvesToNil := func(col, val Value) {
		assertPathResolvesTo(assert, nil, col, NewPath().AddHashIndex(val.Hash()))
		assertPathStringResolvesTo(assert, nil, col, fmt.Sprintf("[#%s]", val.Hash()))
	}

	b := Bool(true)
	resolvesToNil(NewMap(b, b), b)
	resolvesToNil(NewSet(b), b)
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
	assertPathResolvesTo(assert, m1, s, NewPath().AddField("foo").AddIndex(Number(1)).AddHashKeyIndex(m1.Hash()))
	assertPathStringResolvesTo(assert, m1, s, fmt.Sprintf(`.foo[1][#%s]@key`, m1.Hash().String()))
	assertPathResolvesTo(assert, String("car"), s,
		NewPath().AddField("foo").AddIndex(Number(1)).AddHashKeyIndex(m1.Hash()).AddIndex(String("c")))
	assertPathStringResolvesTo(assert, String("car"), s,
		fmt.Sprintf(`.foo[1][#%s]@key["c"]`, m1.Hash().String()))
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
		expectStr := str
		switch expectStr { // Human readable serialization special cases.
		case "[1e4]":
			expectStr = "[10000]"
		case "[1.]":
			expectStr = "[1]"
		case "[\"line\nbreak\rreturn\"]":
			expectStr = `["line\nbreak\rreturn"]`
		}
		assert.Equal(expectStr, p.String())
	}

	test(".foo", NewPath().AddField("foo"))
	test(".Q", NewPath().AddField("Q"))
	test(".QQ", NewPath().AddField("QQ"))
	test("[true]", NewPath().AddIndex(Bool(true)))
	test("[false]", NewPath().AddIndex(Bool(false)))
	test("[false]@key", NewPath().AddKeyIndex(Bool(false)))
	test("[42]", NewPath().AddIndex(Number(42)))
	test("[42]@key", NewPath().AddKeyIndex(Number(42)))
	test("[1e4]", NewPath().AddIndex(Number(1e4)))
	test("[1.]", NewPath().AddIndex(Number(1.)))
	test("[1.345]", NewPath().AddIndex(Number(1.345)))
	test(`[""]`, NewPath().AddIndex(String("")))
	test(`["42"]`, NewPath().AddIndex(String("42")))
	test(`["42"]@key`, NewPath().AddKeyIndex(String("42")))
	test("[\"line\nbreak\rreturn\"]", NewPath().AddIndex(String("line\nbreak\rreturn")))
	test(`["qu\\ote\""]`, NewPath().AddIndex(String(`qu\ote"`)))
	test(`["π"]`, NewPath().AddIndex(String("π")))
	test(`["[[br][]acke]]ts"]`, NewPath().AddIndex(String("[[br][]acke]]ts")))
	test(`["xπy✌z"]`, NewPath().AddIndex(String("xπy✌z")))
	test(`["ಠ_ಠ"]`, NewPath().AddIndex(String("ಠ_ಠ")))
	test("[\"0\"][\"1\"][\"100\"]", NewPath().AddIndex(String("0")).AddIndex(String("1")).AddIndex(String("100")))
	test(".foo[0].bar[4.5][false]", NewPath().AddField("foo").AddIndex(Number(0)).AddField("bar").AddIndex(Number(4.5)).AddIndex(Bool(false)))

	h := Number(42).Hash() // arbitrary hash
	test(fmt.Sprintf(".foo[#%s]", h.String()), NewPath().AddField("foo").AddHashIndex(h))
	test(fmt.Sprintf(".bar[#%s]@key", h.String()), NewPath().AddField("bar").AddHashKeyIndex(h))
}

func TestPathParseErrors(t *testing.T) {
	assert := assert.New(t)

	test := func(str, expectError string) {
		p, err := ParsePath(str)
		assert.Equal(Path{}, p)
		if err != nil {
			assert.Equal(expectError, err.Error())
		} else {
			assert.Fail("Expected " + expectError)
		}
	}

	test("", "Empty path")
	test(".", "Invalid field: ")
	test("[", "Path ends in [")
	test("]", "] is missing opening [")
	test(".#", "Invalid field: #")
	test(". ", "Invalid field:  ")
	test(". invalid.field", "Invalid field:  invalid.field")
	test(".foo.", "Invalid field: ")
	test(".foo.#invalid.field", "Invalid field: #invalid.field")
	test(".foo!", "Invalid operator: !")
	test(".foo!bar", "Invalid operator: !")
	test(".foo#", "Invalid operator: #")
	test(".foo#bar", "Invalid operator: #")
	test(".foo[", "Path ends in [")
	test(".foo[.bar", "[ is missing closing ]")
	test(".foo]", "] is missing opening [")
	test(".foo].bar", "] is missing opening [")
	test(".foo[]", "Empty index value")
	test(".foo[[]", "Invalid index: [")
	test(".foo[[]]", "Invalid index: [")
	test(".foo[42.1.2]", "Invalid index: 42.1.2")
	test(".foo[1f4]", "Invalid index: 1f4")
	test(".foo[hello]", "Invalid index: hello")
	test(".foo['hello']", "Invalid index: 'hello'")
	test(`.foo[\]`, `Invalid index: \`)
	test(`.foo[\\]`, `Invalid index: \\`)
	test(`.foo["hello]`, "[ is missing closing ]")
	test(`.foo["hello`, "[ is missing closing ]")
	test(`.foo["`, "[ is missing closing ]")
	test(`.foo["\`, "[ is missing closing ]")
	test(`.foo["]`, "[ is missing closing ]")
	test(".foo[#]", "Invalid hash: ")
	test(".foo[#sha1-invalid]", "Invalid hash: sha1-invalid")
	test(`.foo["hello\nworld"]`, `Only " and \ can be escaped`)
	test(".foo[42]bar", "Invalid operator: b")
	test("#foo", "Invalid operator: #")
	test("!foo", "Invalid operator: !")
	test("@foo", "Invalid operator: @")
	test("@key", "Invalid operator: @")
	test(fmt.Sprintf(".foo[#%s]@soup", hash.FromData([]byte{42}).String()), "Unsupported annotation: @soup")
}
