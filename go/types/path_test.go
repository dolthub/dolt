// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"fmt"
	"testing"

	"bytes"

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
	resolvesTo := func(expVal, expKey Value, str string) {
		assertResolvesTo(assert, expVal, v, str)
		assertResolvesTo(assert, expKey, v, str+"@key")
	}

	v = NewList(Number(1), Number(3), String("foo"), Bool(false))

	resolvesTo(Number(1), Number(0), "[0]")
	resolvesTo(Number(3), Number(1), "[1]")
	resolvesTo(String("foo"), Number(2), "[2]")
	resolvesTo(Bool(false), Number(3), "[3]")
	resolvesTo(nil, nil, "[4]")
	resolvesTo(nil, nil, "[-5]")
	resolvesTo(Number(1), Number(0), "[-4]")
	resolvesTo(Number(3), Number(1), "[-3]")
	resolvesTo(String("foo"), Number(2), "[-2]")
	resolvesTo(Bool(false), Number(3), "[-1]")

	v = NewMap(
		Bool(false), Number(23),
		Number(1), String("foo"),
		Number(2.3), Number(4.5),
		String("two"), String("bar"),
	)

	resolvesTo(String("foo"), Number(1), "[1]")
	resolvesTo(String("bar"), String("two"), `["two"]`)
	resolvesTo(Number(23), Bool(false), "[false]")
	resolvesTo(Number(4.5), Number(2.3), "[2.3]")
	resolvesTo(nil, nil, "[4]")
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

	resolvesTo := func(col, key, expVal, expKey Value) {
		assertResolvesTo(assert, expVal, col, hashIdx(key))
		assertResolvesTo(assert, expKey, col, hashIdx(key)+"@key")
	}

	// Primitives are only addressable by their values.
	resolvesTo(m, b, nil, nil)
	resolvesTo(m, i, nil, nil)
	resolvesTo(m, str, nil, nil)
	resolvesTo(s, b, nil, nil)
	resolvesTo(s, i, nil, nil)
	resolvesTo(s, str, nil, nil)

	// Other values are only addressable by their hashes.
	resolvesTo(m, br, i, br)
	resolvesTo(m, l, lr, l)
	resolvesTo(m, lr, b, lr)
	resolvesTo(s, br, br, br)
	resolvesTo(s, l, l, l)
	resolvesTo(s, lr, lr, lr)

	// Lists cannot be addressed by hashes, obviously.
	resolvesTo(l, i, nil, nil)
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
		Bool(false), String("earth"),
		String("d"), String("dar"),
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
	assertResolvesTo(assert, String("foo"), s, `.foo[0]@at(0)`)
	assertResolvesTo(assert, String("bar"), s, `.foo[0]@at(1)`)
	assertResolvesTo(assert, String("car"), s, `.foo[0]@at(2)`)
	assertResolvesTo(assert, nil, s, `.foo[0]["x"]`)
	assertResolvesTo(assert, nil, s, `.foo[2]["c"]`)
	assertResolvesTo(assert, nil, s, `.notHere[0]["c"]`)
	assertResolvesTo(assert, m2, s, `.foo[1]`)
	assertResolvesTo(assert, String("dar"), s, `.foo[1]["d"]`)
	assertResolvesTo(assert, String("earth"), s, `.foo[1][false]`)
	assertResolvesTo(assert, String("fire"), s, fmt.Sprintf(`.foo[1]%s`, hashIdx(m1)))
	assertResolvesTo(assert, m1, s, fmt.Sprintf(`.foo[1]%s@key`, hashIdx(m1)))
	assertResolvesTo(assert, String("car"), s, fmt.Sprintf(`.foo[1]%s@key["c"]`, hashIdx(m1)))
	assertResolvesTo(assert, String("fire"), s, `.foo[1]@at(2)`)
	assertResolvesTo(assert, m1, s, `.foo[1]@at(2)@key`)
	assertResolvesTo(assert, String("car"), s, `.foo[1]@at(2)@key@at(2)`)
	assertResolvesTo(assert, String("fire"), s, `.foo[1]@at(-1)`)
	assertResolvesTo(assert, m1, s, `.foo[1]@at(-1)@key`)
	assertResolvesTo(assert, String("car"), s, `.foo[1]@at(-1)@key@at(-1)`)
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
	test(".foo@type")
	test(".Q")
	test(".QQ")
	test("[true]")
	test("[true]@type")
	test("[false]")
	test("[false]@key")
	test("[false]@key@type")
	test("[false]@key@type@at(42)")
	test("[42]")
	test("[42]@key")
	test("[42]@at(-101)")
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
	test("@foo", "Unsupported annotation: @foo")
	test("@key", "Cannot use @key annotation at beginning of path")
	test(".foo@key", "Cannot use @key annotation on: .foo")
	test(".foo@key()", "@key annotation does not support arguments")
	test(".foo@key(42)", "@key annotation does not support arguments")
	test(".foo@type()", "@type annotation does not support arguments")
	test(".foo@type(42)", "@type annotation does not support arguments")
	test(".foo@at", "@at annotation requires a position argument")
	test(".foo@at()", "@at annotation requires a position argument")
	test(fmt.Sprintf(".foo[#%s]@soup", hash.Of([]byte{42}).String()), "Unsupported annotation: @soup")
}

func TestPathEquals(t *testing.T) {
	assert := assert.New(t)
	equalPaths := []string{
		`[1]`,
		`["one"]`,
		`.two.three`,
		`["yo"]@key`,
	}
	notEqualPaths := [][]string{
		{`[1]`, `[2]`},
		{`["one"]`, `["two"]`},
		{`.two.three`, `.two.four`},
		{`["yo"]@key`, `["yo"]`},
	}

	assert.True(Path{}.Equals(Path{}))
	for _, s := range equalPaths {
		p, err := ParsePath(s)
		assert.NoError(err)
		assert.True(p.Equals(p))
	}

	simple, err := ParsePath(`["one"].two`)
	assert.NoError(err)
	assert.False(Path{}.Equals(simple))
	for _, a := range notEqualPaths {
		s0, s1 := a[0], a[1]
		p0, err := ParsePath(s0)
		assert.NoError(err)
		p1, err := ParsePath(s1)
		assert.NoError(err)
		assert.False(p0.Equals(p1))
	}
}

func TestPathCanBePathIndex(t *testing.T) {
	assert := assert.New(t)

	assert.True(ValueCanBePathIndex(Bool(true)))
	assert.True(ValueCanBePathIndex(Number(5)))
	assert.True(ValueCanBePathIndex(String("yes")))

	assert.False(ValueCanBePathIndex(NewRef(String("yes"))))
	assert.False(ValueCanBePathIndex(NewBlob(bytes.NewReader([]byte("yes")))))
}

func TestCopyPath(t *testing.T) {
	assert := assert.New(t)

	testCases := []string{
		``,
		`["key"]`,
		`["key"].field1`,
		`["key"]@key.field1`,
	}

	for _, s1 := range testCases {
		expected, err := ParsePath(s1 + `["anIndex"]`)
		assert.NoError(err)
		var p Path
		if s1 != "" {
			p, err = ParsePath(s1)
		}
		assert.NoError(err)
		p1 := p.Append(NewIndexPath(String("anIndex")))
		if len(p) > 0 {
			p[0] = expected[1] // if p1 really is a copy, this shouldn't be noticed
		}
		assert.Equal(expected, p1)
	}
}

func TestMustParsePath(t *testing.T) {
	for _, good := range []string{".good", "[\"good\"]"} {
		assert.NotNil(t, MustParsePath(good))
	}
	for _, bad := range []string{"", "bad", "[bad]", "!", "💩"} {
		assert.Panics(t, func() { MustParsePath(bad) })
	}
}

func TestPathType(t *testing.T) {
	assert := assert.New(t)

	m := NewMap(
		String("string"), String("foo"),
		String("bool"), Bool(false),
		String("number"), Number(42),
		String("List<number|string>"), NewList(Number(42), String("foo")),
		String("Map<Bool, Bool>"), NewMap(Bool(true), Bool(false)))

	m.IterAll(func(k, cv Value) {
		ks := k.(String)
		assertResolvesTo(assert, cv.Type(), m, fmt.Sprintf("[\"%s\"]@type", ks))
	})

	assertResolvesTo(assert, StringType, m, `["string"]@key@type`)
	assertResolvesTo(assert, m.Type(), m, `@type`)
	s := NewStruct("", StructData{
		"str": String("foo"),
		"num": Number(42),
	})
	assertResolvesTo(assert, s.Get("str").Type(), s, ".str@type")
	assertResolvesTo(assert, s.Get("num").Type(), s, ".num@type")
}

func TestPathAtAnnotation(t *testing.T) {
	assert := assert.New(t)

	var v Value
	resolvesTo := func(expVal, expKey Value, str string) {
		assertResolvesTo(assert, expVal, v, str)
		assertResolvesTo(assert, expKey, v, str+"@key")
	}

	v = NewList(Number(1), Number(3), String("foo"), Bool(false))

	resolvesTo(Number(1), nil, "@at(0)")
	resolvesTo(Number(3), nil, "@at(1)")
	resolvesTo(String("foo"), nil, "@at(2)")
	resolvesTo(Bool(false), nil, "@at(3)")
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Number(1), nil, "@at(-4)")
	resolvesTo(Number(3), nil, "@at(-3)")
	resolvesTo(String("foo"), nil, "@at(-2)")
	resolvesTo(Bool(false), nil, "@at(-1)")

	v = NewSet(
		Bool(false),
		Number(1),
		Number(2.3),
		String("two"),
	)

	resolvesTo(Bool(false), Bool(false), "@at(0)")
	resolvesTo(Number(1), Number(1), "@at(1)")
	resolvesTo(Number(2.3), Number(2.3), "@at(2)")
	resolvesTo(String("two"), String("two"), `@at(3)`)
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Bool(false), Bool(false), "@at(-4)")
	resolvesTo(Number(1), Number(1), "@at(-3)")
	resolvesTo(Number(2.3), Number(2.3), "@at(-2)")
	resolvesTo(String("two"), String("two"), `@at(-1)`)

	v = NewMap(
		Bool(false), Number(23),
		Number(1), String("foo"),
		Number(2.3), Number(4.5),
		String("two"), String("bar"),
	)

	resolvesTo(Number(23), Bool(false), "@at(0)")
	resolvesTo(String("foo"), Number(1), "@at(1)")
	resolvesTo(Number(4.5), Number(2.3), "@at(2)")
	resolvesTo(String("bar"), String("two"), `@at(3)`)
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Number(23), Bool(false), "@at(-4)")
	resolvesTo(String("foo"), Number(1), "@at(-3)")
	resolvesTo(Number(4.5), Number(2.3), "@at(-2)")
	resolvesTo(String("bar"), String("two"), `@at(-1)`)
}
