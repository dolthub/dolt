// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/hash"
	"github.com/stretchr/testify/assert"
)

func hashIdx(v Value) string {
	// TODO(binformat)
	return fmt.Sprintf("[#%s]", v.Hash(Format_7_18).String())
}

func assertResolvesTo(assert *assert.Assertions, expect, ref Value, str string) {
	assertResolvesToWithVR(assert, expect, ref, str, nil)
}

func assertResolvesToWithVR(assert *assert.Assertions, expect, ref Value, str string, vr ValueReader) {
	p, err := ParsePath(str, Format_7_18)
	assert.NoError(err)
	actual := p.Resolve(context.Background(), Format_7_18, ref, vr)
	if expect == nil {
		if actual != nil {
			assert.Fail("", "Expected nil, but got %s", EncodedValue(context.Background(), actual))
		}
	} else if actual == nil {
		assert.Fail("", "Expected %s, but got nil", EncodedValue(context.Background(), expect))
	} else {
		assert.True(expect.Equals(actual), "Expected %s, but got %s", EncodedValue(context.Background(), expect), EncodedValue(context.Background(), actual))
	}
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)

	v := NewStruct("", StructData{
		"foo": String("foo"),
		"bar": Bool(false),
		"baz": Float(203),
	})

	assertResolvesTo(assert, String("foo"), v, `.foo`)
	assertResolvesTo(assert, Bool(false), v, `.bar`)
	assertResolvesTo(assert, Float(203), v, `.baz`)
	assertResolvesTo(assert, nil, v, `.notHere`)

	v2 := NewStruct("", StructData{
		"v1": v,
	})

	assertResolvesTo(assert, String("foo"), v2, `.v1.foo`)
	assertResolvesTo(assert, Bool(false), v2, `.v1.bar`)
	assertResolvesTo(assert, Float(203), v2, `.v1.baz`)
	assertResolvesTo(assert, nil, v2, `.v1.notHere`)
	assertResolvesTo(assert, nil, v2, `.notHere.v1`)
}

func TestPathStructType(t *testing.T) {
	assert := assert.New(t)

	typ := MakeStructType("MyStruct",
		StructField{Name: "foo", Type: StringType},
		StructField{Name: "bar", Type: BoolType},
		StructField{Name: "baz", Type: FloaTType},
	)

	assertResolvesTo(assert, StringType, typ, `.foo`)
	assertResolvesTo(assert, BoolType, typ, `.bar`)
	assertResolvesTo(assert, FloaTType, typ, `.baz`)
	assertResolvesTo(assert, nil, typ, `.notHere`)

	typ2 := MakeStructType("",
		StructField{Name: "typ", Type: typ},
	)

	assertResolvesTo(assert, typ, typ2, `.typ`)
	assertResolvesTo(assert, StringType, typ2, `.typ.foo`)
	assertResolvesTo(assert, BoolType, typ2, `.typ.bar`)
	assertResolvesTo(assert, FloaTType, typ2, `.typ.baz`)
	assertResolvesTo(assert, nil, typ2, `.typ.notHere`)
	assertResolvesTo(assert, nil, typ2, `.notHere.typ`)
}

func TestPathIndex(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	var v Value
	resolvesTo := func(expVal, expKey Value, str string) {
		assertResolvesTo(assert, expVal, v, str)
		assertResolvesTo(assert, expKey, v, str+"@key")
	}

	// TODO(binformat)
	v = NewList(context.Background(), Format_7_18, vs, Float(1), Float(3), String("foo"), Bool(false))

	resolvesTo(Float(1), Float(0), "[0]")
	resolvesTo(Float(3), Float(1), "[1]")
	resolvesTo(String("foo"), Float(2), "[2]")
	resolvesTo(Bool(false), Float(3), "[3]")
	resolvesTo(nil, nil, "[4]")
	resolvesTo(nil, nil, "[-5]")
	resolvesTo(Float(1), Float(0), "[-4]")
	resolvesTo(Float(3), Float(1), "[-3]")
	resolvesTo(String("foo"), Float(2), "[-2]")
	resolvesTo(Bool(false), Float(3), "[-1]")

	v = NewMap(context.Background(), Format_7_18, vs,
		Bool(false), Float(23),
		Float(1), String("foo"),
		Float(2.3), Float(4.5),
		String("two"), String("bar"),
	)

	resolvesTo(String("foo"), Float(1), "[1]")
	resolvesTo(String("bar"), String("two"), `["two"]`)
	resolvesTo(Float(23), Bool(false), "[false]")
	resolvesTo(Float(4.5), Float(2.3), "[2.3]")
	resolvesTo(nil, nil, "[4]")
}

func TestPathIndexType(t *testing.T) {
	assert := assert.New(t)

	st := MakeSetType(FloaTType)
	lt := MakeListType(st)
	mt := MakeMapType(st, lt)
	ut := MakeUnionType(lt, mt, st)

	assertResolvesTo(assert, FloaTType, st, "[0]")
	assertResolvesTo(assert, FloaTType, st, "[-1]")
	assertResolvesTo(assert, FloaTType, st, "@at(0)")
	assertResolvesTo(assert, nil, st, "[1]")
	assertResolvesTo(assert, nil, st, "[-2]")

	assertResolvesTo(assert, st, lt, "[0]")
	assertResolvesTo(assert, st, lt, "[-1]")
	assertResolvesTo(assert, FloaTType, lt, "[0][0]")
	assertResolvesTo(assert, FloaTType, lt, "@at(0)@at(0)")
	assertResolvesTo(assert, nil, lt, "[1]")
	assertResolvesTo(assert, nil, lt, "[-2]")

	assertResolvesTo(assert, st, mt, "[0]")
	assertResolvesTo(assert, st, mt, "[-2]")
	assertResolvesTo(assert, lt, mt, "[1]")
	assertResolvesTo(assert, lt, mt, "[-1]")
	assertResolvesTo(assert, FloaTType, mt, "[1][0][0]")
	assertResolvesTo(assert, FloaTType, mt, "@at(1)@at(0)@at(0)")
	assertResolvesTo(assert, nil, mt, "[2]")
	assertResolvesTo(assert, nil, mt, "[-3]")

	assertResolvesTo(assert, lt, ut, "[0]")
	assertResolvesTo(assert, lt, ut, "[-3]")
	assertResolvesTo(assert, mt, ut, "[1]")
	assertResolvesTo(assert, mt, ut, "[-2]")
	assertResolvesTo(assert, st, ut, "[2]")
	assertResolvesTo(assert, st, ut, "[-1]")
	assertResolvesTo(assert, FloaTType, ut, "[1][1][0][0]")
	assertResolvesTo(assert, FloaTType, ut, "@at(1)@at(1)@at(0)@at(0)")
	assertResolvesTo(assert, nil, ut, "[3]")
	assertResolvesTo(assert, nil, ut, "[-4]")
}

func TestPathHashIndex(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	b := Bool(true)
	br := NewRef(b, Format_7_18)
	i := Float(0)
	str := String("foo")
	// TODO(binformat)
	l := NewList(context.Background(), Format_7_18, vs, b, i, str)
	lr := NewRef(l, Format_7_18)
	m := NewMap(context.Background(), Format_7_18, vs,
		b, br,
		br, i,
		i, str,
		l, lr,
		lr, b,
	)
	s := NewSet(context.Background(), vs, b, br, i, str, l, lr)

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

	vs := newTestValueStore()

	resolvesToNil := func(col, val Value) {
		assertResolvesTo(assert, nil, col, hashIdx(val))
	}

	b := Bool(true)
	resolvesToNil(NewMap(context.Background(), Format_7_18, vs, b, b), b)
	resolvesToNil(NewSet(context.Background(), vs, b), b)
}

func TestPathMulti(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	m1 := NewMap(context.Background(), Format_7_18, vs,
		String("a"), String("foo"),
		String("b"), String("bar"),
		String("c"), String("car"),
	)

	m2 := NewMap(context.Background(), Format_7_18, vs,
		Bool(false), String("earth"),
		String("d"), String("dar"),
		m1, String("fire"),
	)

	// TODO(binformat)
	l := NewList(context.Background(), Format_7_18, vs, m1, m2)

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
	assertResolvesTo(assert, nil, s, `.foo[0]@at(3)`)
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
		p, err := ParsePath(str, Format_7_18)
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

	// TODO(binformat)
	h := Float(42).Hash(Format_7_18) // arbitrary hash

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
		p, err := ParsePath(str, Format_7_18)
		assert.Equal(Path{}, p)
		if err != nil {
			assert.Equal(expectError, err.Error())
		} else {
			assert.Fail("Expected " + expectError)
		}
	}

	test("", "empty path")
	test(".", "invalid field: ")
	test("[", "path ends in [")
	test("]", "] is missing opening [")
	test(".#", "invalid field: #")
	test(". ", "invalid field:  ")
	test(". invalid.field", "invalid field:  invalid.field")
	test(".foo.", "invalid field: ")
	test(".foo.#invalid.field", "invalid field: #invalid.field")
	test(".foo!", "invalid operator: !")
	test(".foo!bar", "invalid operator: !")
	test(".foo#", "invalid operator: #")
	test(".foo#bar", "invalid operator: #")
	test(".foo[", "path ends in [")
	test(".foo[.bar", "invalid index: .bar")
	test(".foo]", "] is missing opening [")
	test(".foo].bar", "] is missing opening [")
	test(".foo[]", "empty index value")
	test(".foo[[]", "invalid index: [")
	test(".foo[[]]", "invalid index: [")
	test(".foo[42.1.2]", "invalid index: 42.1.2")
	test(".foo[1f4]", "invalid index: 1f4")
	test(".foo[hello]", "invalid index: hello")
	test(".foo['hello']", "invalid index: 'hello'")
	test(`.foo[\]`, `invalid index: \`)
	test(`.foo[\\]`, `invalid index: \\`)
	test(`.foo["hello]`, "[ is missing closing ]")
	test(`.foo["hello`, "[ is missing closing ]")
	test(`.foo["hello"`, "[ is missing closing ]")
	test(`.foo["`, "[ is missing closing ]")
	test(`.foo["\`, "[ is missing closing ]")
	test(`.foo["]`, "[ is missing closing ]")
	test(".foo[#]", "invalid hash: ")
	test(".foo[#invalid]", "invalid hash: invalid")
	test(`.foo["hello\nworld"]`, `only " and \ can be escaped`)
	test(".foo[42]bar", "invalid operator: b")
	test("#foo", "invalid operator: #")
	test("!foo", "invalid operator: !")
	test("@foo", "unsupported annotation: @foo")
	test("@key", "cannot use @key annotation at beginning of path")
	test(".foo@key", "cannot use @key annotation on: .foo")
	test(".foo@key()", "@key annotation does not support arguments")
	test(".foo@key(42)", "@key annotation does not support arguments")
	test(".foo@type()", "@type annotation does not support arguments")
	test(".foo@type(42)", "@type annotation does not support arguments")
	test(".foo@at", "@at annotation requires a position argument")
	test(".foo@at()", "@at annotation requires a position argument")
	test(".foo@at(", "@at annotation requires a position argument")
	test(".foo@at(42", "@at annotation requires a position argument")
	test(fmt.Sprintf(".foo[#%s]@soup", hash.Of([]byte{42}).String()), "unsupported annotation: @soup")
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
		p, err := ParsePath(s, Format_7_18)
		assert.NoError(err)
		assert.True(p.Equals(p))
	}

	simple, err := ParsePath(`["one"].two`, Format_7_18)
	assert.NoError(err)
	assert.False(Path{}.Equals(simple))
	for _, a := range notEqualPaths {
		s0, s1 := a[0], a[1]
		p0, err := ParsePath(s0, Format_7_18)
		assert.NoError(err)
		p1, err := ParsePath(s1, Format_7_18)
		assert.NoError(err)
		assert.False(p0.Equals(p1))
	}
}

func TestPathCanBePathIndex(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	assert.True(ValueCanBePathIndex(Bool(true)))
	assert.True(ValueCanBePathIndex(Float(5)))
	assert.True(ValueCanBePathIndex(String("yes")))

	assert.False(ValueCanBePathIndex(NewRef(String("yes"), Format_7_18)))
	// TODO(binformat)
	assert.False(ValueCanBePathIndex(NewBlob(context.Background(), Format_7_18, vs, bytes.NewReader([]byte("yes")))))
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
		expected, err := ParsePath(s1 + `["anIndex"]`, Format_7_18)
		assert.NoError(err)
		var p Path
		if s1 != "" {
			p, err = ParsePath(s1, Format_7_18)
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
		assert.NotNil(t, MustParsePath(good, Format_7_18))
	}
	for _, bad := range []string{"", "bad", "[bad]", "!", "💩"} {
		assert.Panics(t, func() { MustParsePath(bad, Format_7_18) })
	}
}

func TestPathType(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	m := NewMap(context.Background(), Format_7_18, vs,
		String("string"), String("foo"),
		String("bool"), Bool(false),
		String("number"), Float(42),
		// TODO(binformat)
		String("List<number|string>"), NewList(context.Background(), Format_7_18, vs, Float(42), String("foo")),
		String("Map<Bool, Bool>"), NewMap(context.Background(), Format_7_18, vs, Bool(true), Bool(false)))

	m.IterAll(context.Background(), func(k, cv Value) {
		ks := k.(String)
		assertResolvesTo(assert, TypeOf(cv), m, fmt.Sprintf("[\"%s\"]@type", ks))
	})

	assertResolvesTo(assert, StringType, m, `["string"]@key@type`)
	assertResolvesTo(assert, TypeOf(m), m, `@type`)
	s := NewStruct("", StructData{
		"str": String("foo"),
		"num": Float(42),
	})
	assertResolvesTo(assert, TypeOf(s.Get("str")), s, ".str@type")
	assertResolvesTo(assert, TypeOf(s.Get("num")), s, ".num@type")
}

func TestPathTarget(t *testing.T) {
	assert := assert.New(t)

	s := NewStruct("", StructData{
		"foo": String("bar"),
	})
	vs := newTestValueStore()
	r := vs.WriteValue(context.Background(), s)
	s2 := NewStruct("", StructData{
		"ref": r,
	})

	assertResolvesToWithVR(assert, nil, String("notref"), `@target`, vs)
	assertResolvesToWithVR(assert, s, r, `@target`, vs)
	assertResolvesToWithVR(assert, String("bar"), r, `@target.foo`, vs)
	assertResolvesToWithVR(assert, String("bar"), s2, `.ref@target.foo`, vs)
}

func TestPathAtAnnotation(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	var v Value
	resolvesTo := func(expVal, expKey Value, str string) {
		assertResolvesTo(assert, expVal, v, str)
		assertResolvesTo(assert, expKey, v, str+"@key")
	}

	// TODO(binformat)
	v = NewList(context.Background(), Format_7_18, vs, Float(1), Float(3), String("foo"), Bool(false))

	resolvesTo(Float(1), nil, "@at(0)")
	resolvesTo(Float(3), nil, "@at(1)")
	resolvesTo(String("foo"), nil, "@at(2)")
	resolvesTo(Bool(false), nil, "@at(3)")
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Float(1), nil, "@at(-4)")
	resolvesTo(Float(3), nil, "@at(-3)")
	resolvesTo(String("foo"), nil, "@at(-2)")
	resolvesTo(Bool(false), nil, "@at(-1)")

	v = NewSet(context.Background(), vs,
		Bool(false),
		Float(1),
		Float(2.3),
		String("two"),
	)

	resolvesTo(Bool(false), Bool(false), "@at(0)")
	resolvesTo(Float(1), Float(1), "@at(1)")
	resolvesTo(Float(2.3), Float(2.3), "@at(2)")
	resolvesTo(String("two"), String("two"), `@at(3)`)
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Bool(false), Bool(false), "@at(-4)")
	resolvesTo(Float(1), Float(1), "@at(-3)")
	resolvesTo(Float(2.3), Float(2.3), "@at(-2)")
	resolvesTo(String("two"), String("two"), `@at(-1)`)

	v = NewMap(context.Background(), Format_7_18, vs,
		Bool(false), Float(23),
		Float(1), String("foo"),
		Float(2.3), Float(4.5),
		String("two"), String("bar"),
	)

	resolvesTo(Float(23), Bool(false), "@at(0)")
	resolvesTo(String("foo"), Float(1), "@at(1)")
	resolvesTo(Float(4.5), Float(2.3), "@at(2)")
	resolvesTo(String("bar"), String("two"), `@at(3)`)
	resolvesTo(nil, nil, "@at(4)")
	resolvesTo(nil, nil, "@at(-5)")
	resolvesTo(Float(23), Bool(false), "@at(-4)")
	resolvesTo(String("foo"), Float(1), "@at(-3)")
	resolvesTo(Float(4.5), Float(2.3), "@at(-2)")
	resolvesTo(String("bar"), String("two"), `@at(-1)`)
}
