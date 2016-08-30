// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"testing"

	"github.com/attic-labs/noms/go/hash"
	"github.com/attic-labs/testify/assert"
)

func hashIdx(v Value) string {
	return fmt.Sprintf("[#%s]", v.Hash().String())
}

func assertResolvesTo(assert *assert.Assertions, expect, ref Value, str string) {
	p, err := ParsePath(str)
	assert.NoError(err)
	actual := p.Resolve(ref)
	if expect == nil {
		if actual != nil {
			assert.Fail("", "Expected nil, but got %s", EncodedValue(actual))
		}
	} else if actual == nil {
		assert.Fail("", "Expected %s, but got nil", EncodedValue(expect))
	} else {
		assert.True(expect.Equals(actual), "Expected %s, but got %s", EncodedValue(expect), EncodedValue(actual))
	}
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("", StructData{
		"foo": String("foo"),
		"bar": Bool(false),
		"baz": Number(203),
	})

	assertResolvesTo(assert, String("foo"), v, `.foo`)
	assertResolvesTo(assert, Bool(false), v, `.bar`)
	assertResolvesTo(assert, Number(203), v, `.baz`)
	assertResolvesTo(assert, nil, v, `.notHere`)

	v2 := NewStruct("", StructData{
		"v1": v,
	})

	assertResolvesTo(assert, String("foo"), v2, `.v1.foo`)
	assertResolvesTo(assert, Bool(false), v2, `.v1.bar`)
	assertResolvesTo(assert, Number(203), v2, `.v1.baz`)
	assertResolvesTo(assert, nil, v2, `.v1.notHere`)
	assertResolvesTo(assert, nil, v2, `.notHere.v1`)
}

func TestPathIndex(t *testing.T) {
	assert := assert.New(t)

	var v Value
	resolvesTo := func(exp, val Value, str string) {
		// Indices resolve to |exp|.
		assertResolvesTo(assert, exp, v, str)
		// Keys resolve to themselves.
		if exp != nil {
			exp = val
		}
		assertResolvesTo(assert, exp, v, str+"@key")
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

	resolvesTo := func(col, exp, val Value) {
		// Values resolve to |exp|.
		assertResolvesTo(assert, exp, col, hashIdx(val))
		// Keys resolve to themselves.
		if exp != nil {
			exp = val
		}
		assertResolvesTo(assert, exp, col, hashIdx(val)+"@key")
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
		assertResolvesTo(assert, nil, col, hashIdx(val))
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

	s := NewStruct("", StructData{
		"foo": l,
	})

	assertResolvesTo(assert, l, s, `.foo`)
	assertResolvesTo(assert, m1, s, `.foo[0]`)
	assertResolvesTo(assert, String("foo"), s, `.foo[0]["a"]`)
	assertResolvesTo(assert, String("bar"), s, `.foo[0]["b"]`)
	assertResolvesTo(assert, String("car"), s, `.foo[0]["c"]`)
	assertResolvesTo(assert, nil, s, `.foo[0]["x"]`)
	assertResolvesTo(assert, nil, s, `.foo[2]["c"]`)
	assertResolvesTo(assert, nil, s, `.notHere[0]["c"]`)
	assertResolvesTo(assert, m2, s, `.foo[1]`)
	assertResolvesTo(assert, String("dar"), s, `.foo[1]["d"]`)
	assertResolvesTo(assert, String("earth"), s, `.foo[1][false]`)
	assertResolvesTo(assert, String("fire"), s, fmt.Sprintf(`.foo[1]%s`, hashIdx(m1)))
	assertResolvesTo(assert, m1, s, fmt.Sprintf(`.foo[1]%s@key`, hashIdx(m1)))
	assertResolvesTo(assert, String("car"), s, fmt.Sprintf(`.foo[1]%s@key["c"]`, hashIdx(m1)))
}

func TestPathParseSuccess(t *testing.T) {
	assert := assert.New(t)

	test := func(str string) {
		p, err := ParsePath(str)
		assert.NoError(err)
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

	h := Number(42).Hash() // arbitrary hash

	test(".foo")
	test(".Q")
	test(".QQ")
	test("[true]")
	test("[false]")
	test("[false]@key")
	test("[42]")
	test("[42]@key")
	test("[1e4]")
	test("[1.]")
	test("[1.345]")
	test(`[""]`)
	test(`["42"]`)
	test(`["42"]@key`)
	test("[\"line\nbreak\rreturn\"]")
	test(`["qu\\ote\""]`)
	test(`["π"]`)
	test(`["[[br][]acke]]ts"]`)
	test(`["xπy✌z"]`)
	test(`["ಠ_ಠ"]`)
	test(`["0"]["1"]["100"]`)
	test(".foo[0].bar[4.5][false]")
	test(fmt.Sprintf(".foo[#%s]", h.String()))
	test(fmt.Sprintf(".bar[#%s]@key", h.String()))
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
	test(".foo[.bar", "Invalid index: .bar")
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
	test(`.foo["hello"`, "[ is missing closing ]")
	test(`.foo["`, "[ is missing closing ]")
	test(`.foo["\`, "[ is missing closing ]")
	test(`.foo["]`, "[ is missing closing ]")
	test(".foo[#]", "Invalid hash: ")
	test(".foo[#invalid]", "Invalid hash: invalid")
	test(`.foo["hello\nworld"]`, `Only " and \ can be escaped`)
	test(".foo[42]bar", "Invalid operator: b")
	test("#foo", "Invalid operator: #")
	test("!foo", "Invalid operator: !")
	test("@foo", "Invalid operator: @")
	test("@key", "Invalid operator: @")
	test(fmt.Sprintf(".foo[#%s]@soup", hash.FromData([]byte{42}).String()), "Unsupported annotation: @soup")
}
