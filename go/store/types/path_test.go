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
