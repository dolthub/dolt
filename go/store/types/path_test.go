// Copyright 2019 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package types

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/hash"
)

func hashIdx(nbf *NomsBinFormat, v Value) string {
	return fmt.Sprintf("[#%s]", mustHash(v.Hash(nbf)).String())
}

func assertResolvesTo(assert *assert.Assertions, expect, ref Value, str string) {
	assertResolvesToWithVR(assert, expect, ref, str, nil)
}

func assertResolvesToWithVR(assert *assert.Assertions, expect, ref Value, str string, vr ValueReader) {
	p, err := ParsePath(str)
	assert.NoError(err)
	actual, err := p.Resolve(context.Background(), ref, vr)
	assert.NoError(err)
	if expect == nil {
		if actual != nil {
			assert.Fail("", "Expected nil, but got %s", mustString(EncodedValue(context.Background(), actual)))
		}
	} else if actual == nil {
		assert.Fail("", "Expected %s, but got nil", mustString(EncodedValue(context.Background(), expect)))
	} else {
		assert.True(expect.Equals(actual), "Expected %s, but got %s", mustString(EncodedValue(context.Background(), expect)), mustString(EncodedValue(context.Background(), actual)))
	}
}

func TestPathStruct(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	v, err := NewStruct(vs.Format(), "", StructData{
		"foo": String("foo"),
		"bar": Bool(false),
		"baz": Float(203),
	})

	require.NoError(t, err)
	assertResolvesTo(assert, String("foo"), v, `.foo`)
	assertResolvesTo(assert, Bool(false), v, `.bar`)
	assertResolvesTo(assert, Float(203), v, `.baz`)
	assertResolvesTo(assert, nil, v, `.notHere`)

	v2, err := NewStruct(vs.Format(), "", StructData{
		"v1": v,
	})

	require.NoError(t, err)
	assertResolvesTo(assert, String("foo"), v2, `.v1.foo`)
	assertResolvesTo(assert, Bool(false), v2, `.v1.bar`)
	assertResolvesTo(assert, Float(203), v2, `.v1.baz`)
	assertResolvesTo(assert, nil, v2, `.v1.notHere`)
	assertResolvesTo(assert, nil, v2, `.notHere.v1`)
}

func TestPathStructType(t *testing.T) {
	assert := assert.New(t)

	typ, err := MakeStructType("MyStruct",
		StructField{Name: "foo", Type: PrimitiveTypeMap[StringKind]},
		StructField{Name: "bar", Type: PrimitiveTypeMap[BoolKind]},
		StructField{Name: "baz", Type: PrimitiveTypeMap[FloatKind]},
	)

	require.NoError(t, err)
	assertResolvesTo(assert, PrimitiveTypeMap[StringKind], typ, `.foo`)
	assertResolvesTo(assert, PrimitiveTypeMap[BoolKind], typ, `.bar`)
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], typ, `.baz`)
	assertResolvesTo(assert, nil, typ, `.notHere`)

	typ2, err := MakeStructType("",
		StructField{Name: "typ", Type: typ},
	)

	require.NoError(t, err)
	assertResolvesTo(assert, typ, typ2, `.typ`)
	assertResolvesTo(assert, PrimitiveTypeMap[StringKind], typ2, `.typ.foo`)
	assertResolvesTo(assert, PrimitiveTypeMap[BoolKind], typ2, `.typ.bar`)
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], typ2, `.typ.baz`)
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

	var err error
	v, err = NewList(context.Background(), vs, Float(1), Float(3), String("foo"), Bool(false))
	require.NoError(t, err)

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

	v, err = NewMap(context.Background(), vs,
		Bool(false), Float(23),
		Float(1), String("foo"),
		Float(2.3), Float(4.5),
		String("two"), String("bar"),
	)

	require.NoError(t, err)
	resolvesTo(String("foo"), Float(1), "[1]")
	resolvesTo(String("bar"), String("two"), `["two"]`)
	resolvesTo(Float(23), Bool(false), "[false]")
	resolvesTo(Float(4.5), Float(2.3), "[2.3]")
	resolvesTo(nil, nil, "[4]")
}

func TestPathIndexType(t *testing.T) {
	assert := assert.New(t)

	st, err := MakeSetType(PrimitiveTypeMap[FloatKind])
	require.NoError(t, err)
	lt, err := MakeListType(st)
	require.NoError(t, err)
	mt, err := MakeMapType(st, lt)
	require.NoError(t, err)
	ut, err := MakeUnionType(lt, mt, st)
	require.NoError(t, err)

	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], st, "[0]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], st, "[-1]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], st, "@at(0)")
	assertResolvesTo(assert, nil, st, "[1]")
	assertResolvesTo(assert, nil, st, "[-2]")

	assertResolvesTo(assert, st, lt, "[0]")
	assertResolvesTo(assert, st, lt, "[-1]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], lt, "[0][0]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], lt, "@at(0)@at(0)")
	assertResolvesTo(assert, nil, lt, "[1]")
	assertResolvesTo(assert, nil, lt, "[-2]")

	assertResolvesTo(assert, st, mt, "[0]")
	assertResolvesTo(assert, st, mt, "[-2]")
	assertResolvesTo(assert, lt, mt, "[1]")
	assertResolvesTo(assert, lt, mt, "[-1]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], mt, "[1][0][0]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], mt, "@at(1)@at(0)@at(0)")
	assertResolvesTo(assert, nil, mt, "[2]")
	assertResolvesTo(assert, nil, mt, "[-3]")

	assertResolvesTo(assert, lt, ut, "[0]")
	assertResolvesTo(assert, lt, ut, "[-3]")
	assertResolvesTo(assert, mt, ut, "[1]")
	assertResolvesTo(assert, mt, ut, "[-2]")
	assertResolvesTo(assert, st, ut, "[2]")
	assertResolvesTo(assert, st, ut, "[-1]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], ut, "[1][1][0][0]")
	assertResolvesTo(assert, PrimitiveTypeMap[FloatKind], ut, "@at(1)@at(1)@at(0)@at(0)")
	assertResolvesTo(assert, nil, ut, "[3]")
	assertResolvesTo(assert, nil, ut, "[-4]")
}

func TestPathHashIndex(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	b := Bool(true)
	br, err := NewRef(b, vs.Format())
	require.NoError(t, err)
	i := Float(0)
	str := String("foo")
	l, err := NewList(context.Background(), vs, b, i, str)
	require.NoError(t, err)
	lr, err := NewRef(l, vs.Format())
	require.NoError(t, err)
	m, err := NewMap(context.Background(), vs,
		b, br,
		br, i,
		i, str,
		l, lr,
		lr, b,
	)
	require.NoError(t, err)
	s, err := NewSet(context.Background(), vs, b, br, i, str, l, lr)
	require.NoError(t, err)

	resolvesTo := func(col, key, expVal, expKey Value) {
		assertResolvesTo(assert, expVal, col, hashIdx(vs.Format(), key))
		assertResolvesTo(assert, expKey, col, hashIdx(vs.Format(), key)+"@key")
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
		assertResolvesTo(assert, nil, col, hashIdx(vs.Format(), val))
	}

	b := Bool(true)
	resolvesToNil(mustValue(NewMap(context.Background(), vs, b, b)), b)
	resolvesToNil(mustValue(NewSet(context.Background(), vs, b)), b)
}

func TestPathMulti(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	m1, err := NewMap(context.Background(), vs,
		String("a"), String("foo"),
		String("b"), String("bar"),
		String("c"), String("car"),
	)

	require.NoError(t, err)

	m2, err := NewMap(context.Background(), vs,
		Bool(false), String("earth"),
		String("d"), String("dar"),
		m1, String("fire"),
	)

	require.NoError(t, err)

	l, err := NewList(context.Background(), vs, m1, m2)

	require.NoError(t, err)

	s, err := NewStruct(vs.Format(), "", StructData{
		"foo": l,
	})

	require.NoError(t, err)

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
	assertResolvesTo(assert, String("fire"), s, fmt.Sprintf(`.foo[1]%s`, hashIdx(vs.Format(), m1)))
	assertResolvesTo(assert, m1, s, fmt.Sprintf(`.foo[1]%s@key`, hashIdx(vs.Format(), m1)))
	assertResolvesTo(assert, String("car"), s, fmt.Sprintf(`.foo[1]%s@key["c"]`, hashIdx(vs.Format(), m1)))
	assertResolvesTo(assert, String("fire"), s, `.foo[1]@at(2)`)
	assertResolvesTo(assert, m1, s, `.foo[1]@at(2)@key`)
	assertResolvesTo(assert, String("car"), s, `.foo[1]@at(2)@key@at(2)`)
	assertResolvesTo(assert, String("fire"), s, `.foo[1]@at(-1)`)
	assertResolvesTo(assert, m1, s, `.foo[1]@at(-1)@key`)
	assertResolvesTo(assert, String("car"), s, `.foo[1]@at(-1)@key@at(-1)`)
}

func TestPathParseSuccess(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	test := func(str string) {
		p, err := ParsePath(str)
		require.NoError(t, err)
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

	h, err := Float(42).Hash(vs.Format()) // arbitrary hash
	require.NoError(t, err)

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
		p, err := ParsePath(s)
		require.NoError(t, err)
		assert.True(p.Equals(p))
	}

	simple, err := ParsePath(`["one"].two`)
	require.NoError(t, err)
	assert.False(Path{}.Equals(simple))
	for _, a := range notEqualPaths {
		s0, s1 := a[0], a[1]
		p0, err := ParsePath(s0)
		require.NoError(t, err)
		p1, err := ParsePath(s1)
		require.NoError(t, err)
		assert.False(p0.Equals(p1))
	}
}

func TestPathCanBePathIndex(t *testing.T) {
	assert := assert.New(t)

	vs := newTestValueStore()

	assert.True(ValueCanBePathIndex(Bool(true)))
	assert.True(ValueCanBePathIndex(Float(5)))
	assert.True(ValueCanBePathIndex(String("yes")))

	assert.False(ValueCanBePathIndex(mustValue(NewRef(String("yes"), vs.Format()))))
	assert.False(ValueCanBePathIndex(mustBlob(NewBlob(context.Background(), vs, bytes.NewReader([]byte("yes"))))))
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
		require.NoError(t, err)
		var p Path
		if s1 != "" {
			p, err = ParsePath(s1)
		}
		require.NoError(t, err)
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

	vs := newTestValueStore()

	m, err := NewMap(context.Background(), vs,
		String("string"), String("foo"),
		String("bool"), Bool(false),
		String("number"), Float(42),
		String("List<number|string>"), mustList(NewList(context.Background(), vs, Float(42), String("foo"))),
		String("Map<Bool, Bool>"), mustMap(NewMap(context.Background(), vs, Bool(true), Bool(false))))
	require.NoError(t, err)

	err = m.IterAll(context.Background(), func(k, cv Value) error {
		ks := k.(String)
		t, err := TypeOf(cv)

		if err != nil {
			return err
		}

		assertResolvesTo(assert, t, m, fmt.Sprintf("[\"%s\"]@type", ks))
		return nil
	})

	require.NoError(t, err)
	assertResolvesTo(assert, PrimitiveTypeMap[StringKind], m, `["string"]@key@type`)
	assertResolvesTo(assert, mustType(TypeOf(m)), m, `@type`)
	s, err := NewStruct(vs.Format(), "", StructData{
		"str": String("foo"),
		"num": Float(42),
	})
	require.NoError(t, err)

	str, ok, err := s.MaybeGet("str")
	require.NoError(t, err)
	assert.True(ok)
	num, ok, err := s.MaybeGet("num")
	require.NoError(t, err)
	assert.True(ok)
	assertResolvesTo(assert, mustType(TypeOf(str)), s, ".str@type")
	assertResolvesTo(assert, mustType(TypeOf(num)), s, ".num@type")
}

func TestPathTarget(t *testing.T) {
	assert := assert.New(t)
	vs := newTestValueStore()

	s, err := NewStruct(vs.Format(), "", StructData{
		"foo": String("bar"),
	})
	require.NoError(t, err)
	r, err := vs.WriteValue(context.Background(), s)
	require.NoError(t, err)
	s2, err := NewStruct(vs.Format(), "", StructData{
		"ref": r,
	})

	require.NoError(t, err)
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

	var err error
	v, err = NewList(context.Background(), vs, Float(1), Float(3), String("foo"), Bool(false))
	require.NoError(t, err)

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

	v, err = NewSet(context.Background(), vs,
		Bool(false),
		Float(1),
		Float(2.3),
		String("two"),
	)
	require.NoError(t, err)

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

	v, err = NewMap(context.Background(), vs,
		Bool(false), Float(23),
		Float(1), String("foo"),
		Float(2.3), Float(4.5),
		String("two"), String("bar"),
	)
	require.NoError(t, err)

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
