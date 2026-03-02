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
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/util/test"
)

func assertWriteHRSEqual(t *testing.T, expected string, v Value) {
	assert := assert.New(t)
	var buf bytes.Buffer
	w := &hrsWriter{w: &buf, floatFormat: 'g'}
	w.Write(context.Background(), v)
	assert.Equal(test.RemoveHashes(expected), test.RemoveHashes(buf.String()))
}

func TestWriteHumanReadablePrimitiveValues(t *testing.T) {
	assertWriteHRSEqual(t, "true", Bool(true))
	assertWriteHRSEqual(t, "false", Bool(false))

	assertWriteHRSEqual(t, "0", Float(0))
	assertWriteHRSEqual(t, "42", Float(42))

	assertWriteHRSEqual(t, "-42", Float(-42))

	assertWriteHRSEqual(t, "3.1415926535", Float(3.1415926535))
	assertWriteHRSEqual(t, "314159.26535", Float(3.1415926535e5))
	assertWriteHRSEqual(t, "3.1415926535e+20", Float(3.1415926535e20))

	assertWriteHRSEqual(t, `"abc"`, String("abc"))
	assertWriteHRSEqual(t, `" "`, String(" "))
	assertWriteHRSEqual(t, `"\t"`, String("\t"))
	assertWriteHRSEqual(t, `"\t"`, String("	"))
	assertWriteHRSEqual(t, `"\n"`, String("\n"))
	assertWriteHRSEqual(t, `"\n"`, String(`
`))
	assertWriteHRSEqual(t, `"\r"`, String("\r"))
	assertWriteHRSEqual(t, `"\r\n"`, String("\r\n"))
	assertWriteHRSEqual(t, `"\xff"`, String("\xff"))
	assertWriteHRSEqual(t, `"💩"`, String("\xf0\x9f\x92\xa9"))
	assertWriteHRSEqual(t, `"💩"`, String("💩"))
	assertWriteHRSEqual(t, `"\a"`, String("\007"))
	assertWriteHRSEqual(t, `"☺"`, String("\u263a"))
}

func TestWriteHumanReadableRef(t *testing.T) {
	vs := newTestValueStore()

	x := Float(42)
	rv, err := vs.WriteValue(context.Background(), x)
	require.NoError(t, err)
	assertWriteHRSEqual(t, "#0123456789abcdefghijklmnopqrstuv", rv)
}

func TestWriteHumanReadableStruct(t *testing.T) {
	vrw := newTestValueStore()
	str := mustValue(NewStruct(vrw.Format(), "S1", StructData{
		"x": Float(1),
		"y": Float(2),
	}))
	assertWriteHRSEqual(t, "struct S1 {\n  x: 1,\n  y: 2,\n}", str)
}

func TestWriteHumanReadableType(t *testing.T) {
	assertWriteHRSEqual(t, "Bool", PrimitiveTypeMap[BoolKind])
	assertWriteHRSEqual(t, "Blob", PrimitiveTypeMap[BlobKind])
	assertWriteHRSEqual(t, "String", PrimitiveTypeMap[StringKind])
	assertWriteHRSEqual(t, "Float", PrimitiveTypeMap[FloatKind])
	assertWriteHRSEqual(t, "UUID", PrimitiveTypeMap[UUIDKind])
	assertWriteHRSEqual(t, "Int", PrimitiveTypeMap[IntKind])
	assertWriteHRSEqual(t, "Uint", PrimitiveTypeMap[UintKind])
	assertWriteHRSEqual(t, "InlineBlob", PrimitiveTypeMap[InlineBlobKind])
	assertWriteHRSEqual(t, "Null", PrimitiveTypeMap[NullKind])

	assertWriteHRSEqual(t, "List<Float>", mustType(MakeListType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Set<Float>", mustType(MakeSetType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Ref<Float>", mustType(MakeRefType(PrimitiveTypeMap[FloatKind])))
	assertWriteHRSEqual(t, "Map<Float, String>", mustType(MakeMapType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertWriteHRSEqual(t, "Float | String", mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))
	assertWriteHRSEqual(t, "Bool", mustType(MakeUnionType(PrimitiveTypeMap[BoolKind])))
	assertWriteHRSEqual(t, "Union<>", mustType(MakeUnionType()))
	assertWriteHRSEqual(t, "List<Float | String>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[FloatKind], PrimitiveTypeMap[StringKind])))))
	assertWriteHRSEqual(t, "List<Int | Uint>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[IntKind], PrimitiveTypeMap[UintKind])))))
	assertWriteHRSEqual(t, "List<Int | Null>", mustType(MakeListType(mustType(MakeUnionType(PrimitiveTypeMap[IntKind], PrimitiveTypeMap[NullKind])))))
	assertWriteHRSEqual(t, "List<Union<>>", mustType(MakeListType(mustType(MakeUnionType()))))
}

func TestRecursiveStruct(t *testing.T) {
	// struct A {
	//   b: A
	//   c: List<A>
	//   d: struct D {
	//     e: D
	//     f: A
	//   }
	// }

	a := mustType(MakeStructType("A",
		StructField{MakeCycleType("A"), "b", false},
		StructField{mustType(MakeListType(MakeCycleType("A"))), "c", false},
		StructField{mustType(MakeStructType("D",
			StructField{MakeCycleType("D"), "e", false},
			StructField{MakeCycleType("A"), "f", false},
		)), "d", false},
	))

	assertWriteHRSEqual(t, `Struct A {
  b: Cycle<A>,
  c: List<Cycle<A>>,
  d: Struct D {
    e: Cycle<D>,
    f: Cycle<A>,
  },
}`, a)

	d, _ := a.Desc.(StructDesc).Field("d")

	assertWriteHRSEqual(t, `Struct D {
  e: Cycle<D>,
  f: Struct A {
    b: Cycle<A>,
    c: List<Cycle<A>>,
    d: Cycle<D>,
  },
}`, d)
}

func TestUnresolvedRecursiveStruct(t *testing.T) {
	// struct A {
	//   a: A
	//   b: Cycle<1> (unresolved)
	// }
	a := mustType(MakeStructType("A",
		StructField{MakeCycleType("A"), "a", false},
		StructField{MakeCycleType("X"), "b", false},
	))

	assertWriteHRSEqual(t, `Struct A {
  a: Cycle<A>,
  b: UnresolvedCycle<X>,
}`, a)
}

type errorWriter struct {
	err error
}

func (w *errorWriter) Write(p []byte) (int, error) {
	return 0, w.err
}

func TestWriteHumanReadableWriterError(t *testing.T) {
	assert := assert.New(t)
	err := errors.New("test")
	w := &errorWriter{err}
	assert.Equal(err, WriteEncodedValue(context.Background(), w, Float(42)))
}

func TestEmptyCollections(t *testing.T) {
	vrw := newTestValueStore()

	a, err := MakeStructType("Nothing")
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Struct Nothing {}", a)
	b, err := NewStruct(vrw.Format(), "Rien", StructData{})
	require.NoError(t, err)
	assertWriteHRSEqual(t, "struct Rien {}", b)
}

func TestWriteHumanReadableStructOptionalFields(t *testing.T) {
	typ, err := MakeStructType("S1",
		StructField{PrimitiveTypeMap[BoolKind], "a", false},
		StructField{PrimitiveTypeMap[BoolKind], "b", true})
	require.NoError(t, err)
	assertWriteHRSEqual(t, "Struct S1 {\n  a: Bool,\n  b?: Bool,\n}", typ)
}

type TestCommenter struct {
	prefix   string
	testType *Type
}

func (c TestCommenter) Comment(ctx context.Context, v Value) string {
	if !(mustType(v.typeOf()).Equals(c.testType)) {
		return ""
	}

	v, _, err := v.(Struct).MaybeGet("Name")
	d.PanicIfError(err)

	return c.prefix + string(v.(String))
}

func TestRegisterCommenter(t *testing.T) {
	a := assert.New(t)
	vrw := newTestValueStore()

	tt, err := NewStruct(vrw.Format(), "TestType1", StructData{"Name": String("abc-123")})
	a.NoError(err)
	nt, err := NewStruct(vrw.Format(), "TestType2", StructData{"Name": String("abc-123")})
	a.NoError(err)

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest: ", testType: mustType(tt.typeOf())})

	s1, err := EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.True(strings.Contains(s1, "// MyTest: abc-123"))
	s1, err = EncodedValue(context.Background(), nt)
	a.NoError(err)
	a.False(strings.Contains(s1, "// MyTest: abc-123"))

	RegisterHRSCommenter("TestType1", "mylib1", TestCommenter{prefix: "MyTest2: ", testType: mustType(tt.typeOf())})
	s1, err = EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.True(strings.Contains(s1, "// MyTest2: abc-123"))

	UnregisterHRSCommenter("TestType1", "mylib1")
	s1, err = EncodedValue(context.Background(), tt)
	a.NoError(err)
	a.False(strings.Contains(s1, "// MyTest2: abc-123"))
}
