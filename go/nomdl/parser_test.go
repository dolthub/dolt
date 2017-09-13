// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"bytes"
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/types"
	"github.com/stretchr/testify/assert"
)

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewView())
}

func assertParseType(t *testing.T, code string, expected *types.Type) {
	t.Run(code, func(t *testing.T) {
		actual, err := ParseType(code)
		assert.NoError(t, err)
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", expected.Describe(), actual.Describe())
	})
}

func assertParse(t *testing.T, vrw types.ValueReadWriter, code string, expected types.Value) {
	t.Run(code, func(t *testing.T) {
		actual, err := Parse(vrw, code)
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", types.EncodedValue(expected), types.EncodedValue(actual))
	})
}

func assertParseError(t *testing.T, code, msg string) {
	t.Run(code, func(t *testing.T) {
		vrw := newTestValueStore()
		p := New(vrw, strings.NewReader(code), ParserOptions{
			Filename: "example",
		})
		err := catchSyntaxError(func() {
			p.parseValue()
		})
		if assert.Error(t, err) {
			assert.Equal(t, msg, err.Error())
		}
	})
}

func TestSimpleTypes(t *testing.T) {
	assertParseType(t, "Blob", types.BlobType)
	assertParseType(t, "Bool", types.BoolType)
	assertParseType(t, "Number", types.NumberType)
	assertParseType(t, "String", types.StringType)
	assertParseType(t, "Value", types.ValueType)
	assertParseType(t, "Type", types.TypeType)
}

func TestWhitespace(t *testing.T) {
	for _, r := range " \t\n\r" {
		assertParseType(t, string(r)+"Blob", types.BlobType)
		assertParseType(t, "Blob"+string(r), types.BlobType)
	}
}

func TestComments(t *testing.T) {
	assertParseType(t, "/* */Blob", types.BlobType)
	assertParseType(t, "Blob/* */", types.BlobType)
	assertParseType(t, "Blob//", types.BlobType)
	assertParseType(t, "//\nBlob", types.BlobType)
}

func TestCompoundTypes(t *testing.T) {
	assertParseType(t, "List<>", types.MakeListType(types.MakeUnionType()))
	assertParseType(t, "List<Bool>", types.MakeListType(types.BoolType))
	assertParseError(t, "List<Bool, Number>", `Unexpected token ",", expected ">", example:1:11`)
	assertParseError(t, "List<Bool", `Unexpected token EOF, expected ">", example:1:10`)
	assertParseError(t, "List<", `Unexpected token EOF, expected Ident, example:1:6`)
	assertParseError(t, "List", `Unexpected token EOF, expected "<", example:1:5`)

	assertParseType(t, "Set<>", types.MakeSetType(types.MakeUnionType()))
	assertParseType(t, "Set<Bool>", types.MakeSetType(types.BoolType))
	assertParseError(t, "Set<Bool, Number>", `Unexpected token ",", expected ">", example:1:10`)
	assertParseError(t, "Set<Bool", `Unexpected token EOF, expected ">", example:1:9`)
	assertParseError(t, "Set<", `Unexpected token EOF, expected Ident, example:1:5`)
	assertParseError(t, "Set", `Unexpected token EOF, expected "<", example:1:4`)

	assertParseError(t, "Ref<>", `Unexpected token ">", expected Ident, example:1:6`)
	assertParseType(t, "Ref<Bool>", types.MakeRefType(types.BoolType))
	assertParseError(t, "Ref<Number, Bool>", `Unexpected token ",", expected ">", example:1:12`)
	assertParseError(t, "Ref<Number", `Unexpected token EOF, expected ">", example:1:11`)
	assertParseError(t, "Ref<", `Unexpected token EOF, expected Ident, example:1:5`)
	assertParseError(t, "Ref", `Unexpected token EOF, expected "<", example:1:4`)

	// Cannot use Equals on unresolved cycles.
	ct := MustParseType("Cycle<Abc>")
	assert.Equal(t, ct, types.MakeCycleType("Abc"))

	assertParseError(t, "Cycle<-123>", `Unexpected token "-", expected Ident, example:1:8`)
	assertParseError(t, "Cycle<12.3>", `Unexpected token Float, expected Ident, example:1:11`)
	assertParseError(t, "Cycle<>", `Unexpected token ">", expected Ident, example:1:8`)
	assertParseError(t, "Cycle<", `Unexpected token EOF, expected Ident, example:1:7`)
	assertParseError(t, "Cycle", `Unexpected token EOF, expected "<", example:1:6`)

	assertParseType(t, "Map<>", types.MakeMapType(types.MakeUnionType(), types.MakeUnionType()))
	assertParseType(t, "Map<Bool, String>", types.MakeMapType(types.BoolType, types.StringType))
	assertParseError(t, "Map<Bool,>", `Unexpected token ">", expected Ident, example:1:11`)
	assertParseError(t, "Map<,Bool>", `Unexpected token ",", expected Ident, example:1:6`)
	assertParseError(t, "Map<,>", `Unexpected token ",", expected Ident, example:1:6`)
	assertParseError(t, "Map<Bool,Bool", `Unexpected token EOF, expected ">", example:1:14`)
	assertParseError(t, "Map<Bool,", `Unexpected token EOF, expected Ident, example:1:10`)
	assertParseError(t, "Map<Bool", `Unexpected token EOF, expected ",", example:1:9`)
	assertParseError(t, "Map<", `Unexpected token EOF, expected Ident, example:1:5`)
	assertParseError(t, "Map", `Unexpected token EOF, expected "<", example:1:4`)
}

func TestStructTypes(t *testing.T) {
	assertParseType(t, "Struct {}", types.MakeStructTypeFromFields("", types.FieldMap{}))
	assertParseType(t, "Struct S {}", types.MakeStructTypeFromFields("S", types.FieldMap{}))

	assertParseType(t, `Struct S {
                x: Number
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
	}))

	assertParseType(t, `Struct S {
	        x: Number,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
	}))

	assertParseType(t, `Struct S {
	        x: Number,
	        y: String
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.StringType,
	}))

	assertParseType(t, `Struct S {
	        x: Number,
	        y: String,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.StringType,
	}))

	assertParseType(t, `Struct S {
	        x: Number,
	        y: Struct {
	                z: String,
	        },
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.MakeStructTypeFromFields("", types.FieldMap{
			"z": types.StringType,
		}),
	}))

	assertParseType(t, `Struct S {
                x?: Number,
                y: String,
        }`, types.MakeStructType("S",
		types.StructField{Name: "x", Type: types.NumberType, Optional: true},
		types.StructField{Name: "y", Type: types.StringType, Optional: false},
	))

	assertParseError(t, `Struct S {
	        x: Number
	        y: String
	}`, `Unexpected token Ident, expected "}", example:3:11`)

	assertParseError(t, `Struct S {,}`, `Unexpected token ",", expected Ident, example:1:12`)
	assertParseError(t, `Struct S {`, `Unexpected token EOF, expected Ident, example:1:11`)
	assertParseError(t, `Struct S { x }`, `Unexpected token "}", expected ":", example:1:15`)
	assertParseError(t, `Struct S { x`, `Unexpected token EOF, expected ":", example:1:13`)
	assertParseError(t, `Struct S { x: }`, `Unexpected token "}", expected Ident, example:1:16`)
	assertParseError(t, `Struct S { x: `, `Unexpected token EOF, expected Ident, example:1:15`)
	assertParseError(t, `Struct S { x?: `, `Unexpected token EOF, expected Ident, example:1:16`)
	assertParseError(t, `Struct S { x? `, `Unexpected token EOF, expected ":", example:1:15`)
	assertParseError(t, `Struct S { x? Bool`, `Unexpected token Ident, expected ":", example:1:19`)
	assertParseError(t, `Struct S { x: Bool`, `Unexpected token EOF, expected "}", example:1:19`)
	assertParseError(t, `Struct S { x: Bool,`, `Unexpected token EOF, expected Ident, example:1:20`)
	assertParseError(t, `Struct S { x: Bool,,`, `Unexpected token ",", expected Ident, example:1:21`)

	assertParseError(t, `Struct S {`, `Unexpected token EOF, expected Ident, example:1:11`)
	assertParseError(t, `Struct S `, `Unexpected token EOF, expected "{", example:1:10`)
	assertParseError(t, `Struct {`, `Unexpected token EOF, expected Ident, example:1:9`)
	assertParseError(t, `Struct`, `Unexpected token EOF, expected "{", example:1:7`)
}

func TestUnionTypes(t *testing.T) {
	assertParseType(t, "Blob | Bool", types.MakeUnionType(types.BlobType, types.BoolType))
	assertParseType(t, "Bool | Number | String", types.MakeUnionType(types.BoolType, types.NumberType, types.StringType))
	assertParseType(t, "List<Bool | Number>", types.MakeListType(types.MakeUnionType(types.BoolType, types.NumberType)))
	assertParseType(t, "Map<Bool | Number, Bool | Number>",
		types.MakeMapType(
			types.MakeUnionType(types.BoolType, types.NumberType),
			types.MakeUnionType(types.BoolType, types.NumberType),
		),
	)
	assertParseType(t, `Struct S {
                x: Number | Bool
                }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.NumberType),
	}))
	assertParseType(t, `Struct S {
                x: Number | Bool,
                y: String
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.NumberType),
		"y": types.StringType,
	}))

	assertParseError(t, "Bool |", "Unexpected token EOF, expected Ident, example:1:7")
	assertParseError(t, "Bool | Number |", "Unexpected token EOF, expected Ident, example:1:16")
	assertParseError(t, "Bool | | ", `Unexpected token "|", expected Ident, example:1:9`)
	assertParseError(t, "", `Unexpected token EOF, example:1:1`)
}

func TestValuePrimitives(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Number", types.NumberType)
	assertParse(t, vs, "Number | String", types.MakeUnionType(types.NumberType, types.StringType))

	assertParse(t, vs, "true", types.Bool(true))
	assertParse(t, vs, "false", types.Bool(false))

	assertParse(t, vs, "0", types.Number(0))
	assertParse(t, vs, "1", types.Number(1))
	assertParse(t, vs, "1.1", types.Number(1.1))
	assertParse(t, vs, "1.1e1", types.Number(1.1e1))
	assertParse(t, vs, "1e1", types.Number(1e1))
	assertParse(t, vs, "1e-1", types.Number(1e-1))
	assertParse(t, vs, "1e+1", types.Number(1e+1))

	assertParse(t, vs, "+0", types.Number(0))
	assertParse(t, vs, "+1", types.Number(1))
	assertParse(t, vs, "+1.1", types.Number(1.1))
	assertParse(t, vs, "+1.1e1", types.Number(1.1e1))
	assertParse(t, vs, "+1e1", types.Number(1e1))
	assertParse(t, vs, "+1e-1", types.Number(1e-1))
	assertParse(t, vs, "+1e+1", types.Number(1e+1))

	assertParse(t, vs, "-0", types.Number(-0))
	assertParse(t, vs, "-1", types.Number(-1))
	assertParse(t, vs, "-1.1", types.Number(-1.1))
	assertParse(t, vs, "-1.1e1", types.Number(-1.1e1))
	assertParse(t, vs, "-1e1", types.Number(-1e1))
	assertParse(t, vs, "-1e-1", types.Number(-1e-1))
	assertParse(t, vs, "-1e+1", types.Number(-1e+1))

	assertParse(t, vs, `"a"`, types.String("a"))
	assertParse(t, vs, `""`, types.String(""))
	assertParse(t, vs, `"\""`, types.String("\""))
	assertParseError(t, `"\"`, "Invalid string \"\\\", example:1:4")
	assertParseError(t, `"abc`, "Invalid string \"abc, example:1:5")
	assertParseError(t, `"`, "Invalid string \", example:1:2")
	assertParseError(t, `"
"`, "Invalid string \"\n, example:2:1")

	assertParseError(t, "`", "Unexpected token \"`\", example:1:2")
}

func TestValueList(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "[]", types.NewList(vs))

	assertParse(t, vs, "[42]", types.NewList(vs, types.Number(42)))
	assertParse(t, vs, "[42,]", types.NewList(vs, types.Number(42)))

	assertParseError(t, "[", "Unexpected token EOF, example:1:2")
	assertParseError(t, "[,", "Unexpected token \",\", example:1:3")
	assertParseError(t, "[42", "Unexpected token EOF, expected \"]\", example:1:4")
	assertParseError(t, "[42,", "Unexpected token EOF, example:1:5")
	assertParseError(t, "[,]", "Unexpected token \",\", example:1:3")

	assertParse(t, vs, `[42,
                Bool,
        ]`, types.NewList(vs, types.Number(42), types.BoolType))
	assertParse(t, vs, `[42,
                Bool
        ]`, types.NewList(vs, types.Number(42), types.BoolType))
}

func TestValueSet(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "set {}", types.NewSet(vs))

	assertParse(t, vs, "set {42}", types.NewSet(vs, types.Number(42)))
	assertParse(t, vs, "set {42,}", types.NewSet(vs, types.Number(42)))

	assertParseError(t, "set", "Unexpected token EOF, expected \"{\", example:1:4")
	assertParseError(t, "set {", "Unexpected token EOF, example:1:6")
	assertParseError(t, "set {,", "Unexpected token \",\", example:1:7")
	assertParseError(t, "set {42", "Unexpected token EOF, expected \"}\", example:1:8")
	assertParseError(t, "set {42,", "Unexpected token EOF, example:1:9")
	assertParseError(t, "set {,}", "Unexpected token \",\", example:1:7")

	assertParse(t, vs, `set {42,
                Bool,
        }`, types.NewSet(vs, types.Number(42), types.BoolType))
	assertParse(t, vs, `set {42,
                Bool
        }`, types.NewSet(vs, types.Number(42), types.BoolType))
}

func TestValueMap(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "map {}", types.NewMap(vs))

	assertParse(t, vs, "map {42: true}", types.NewMap(vs, types.Number(42), types.Bool(true)))
	assertParse(t, vs, "map {42: true,}", types.NewMap(vs, types.Number(42), types.Bool(true)))

	assertParseError(t, "map", "Unexpected token EOF, expected \"{\", example:1:4")
	assertParseError(t, "map {", "Unexpected token EOF, example:1:6")
	assertParseError(t, "map {,", "Unexpected token \",\", example:1:7")
	assertParseError(t, "map {42", "Unexpected token EOF, expected \":\", example:1:8")
	assertParseError(t, "map {42,", "Unexpected token \",\", expected \":\", example:1:9")
	assertParseError(t, "map {42:", "Unexpected token EOF, example:1:9")
	assertParseError(t, "map {42: true", "Unexpected token EOF, expected \"}\", example:1:14")
	assertParseError(t, "map {,}", "Unexpected token \",\", example:1:7")

	assertParse(t, vs, `map {42:
                Bool,
        }`, types.NewMap(vs, types.Number(42), types.BoolType))
	assertParse(t, vs, `map {42:
                Bool
        }`, types.NewMap(vs, types.Number(42), types.BoolType))
}

func TestValueType(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Bool", types.BoolType)
	assertParse(t, vs, "Number", types.NumberType)
	assertParse(t, vs, "String", types.StringType)
}

func TestValueStruct(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "struct {}", types.NewStruct("", nil))
	assertParseError(t, "struct", "Unexpected token EOF, expected \"{\", example:1:7")
	assertParseError(t, "struct {", "Unexpected token EOF, expected Ident, example:1:9")

	assertParse(t, vs, "struct name {}", types.NewStruct("name", nil))
	assertParseError(t, "struct name", "Unexpected token EOF, expected \"{\", example:1:12")
	assertParseError(t, "struct name {", "Unexpected token EOF, expected Ident, example:1:14")

	assertParse(t, vs, "struct name {a: 42}", types.NewStruct("name", types.StructData{"a": types.Number(42)}))
	assertParse(t, vs, "struct name {a: 42,}", types.NewStruct("name", types.StructData{"a": types.Number(42)}))
	assertParseError(t, "struct name {a", "Unexpected token EOF, expected \":\", example:1:15")
	assertParseError(t, "struct name {a: ", "Unexpected token EOF, example:1:17")
	assertParseError(t, "struct name {a,", "Unexpected token \",\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a}", "Unexpected token \"}\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a: 42", "Unexpected token EOF, expected \"}\", example:1:19")
	assertParseError(t, "struct name {a: 42,", "Unexpected token EOF, expected Ident, example:1:20")
	assertParseError(t, "struct name {a:}", "Unexpected token \"}\", example:1:17")

	assertParse(t, vs, "struct name {b: 42, a: true}", types.NewStruct("name", types.StructData{"b": types.Number(42), "a": types.Bool(true)}))
	assertParse(t, vs, `struct name {
                b: 42,
                a: true,
        }`, types.NewStruct("name", types.StructData{"b": types.Number(42), "a": types.Bool(true)}))

	assertParse(t, vs, "struct name {a: Struct {}}", types.NewStruct("name", types.StructData{"a": types.MakeStructType("")}))
}

func TestValueBlob(t *testing.T) {
	vs := newTestValueStore()

	test := func(code string, bs ...byte) {
		assertParse(t, vs, code, types.NewBlob(vs, bytes.NewBuffer(bs)))
	}

	test("blob {}")
	test("blob {// comment\n}")
	test("blob {10}", 0x10)
	test("blob {10/* comment */}", 0x10)
	test("blob {0000ff}", 0, 0, 0xff)
	test("blob {00 00 ff}", 0, 0, 0xff)
	test("blob { 00\n00\nff }", 0, 0, 0xff)
	test("blob { ffffffff ffffffff ffffffff ffffffff}",
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	)
	test("blob { ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff}",
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
		0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	)

	assertParseError(t, "blob", "Unexpected token EOF, expected \"{\", example:1:5")
	assertParseError(t, "blob {", "Unexpected token EOF, example:1:7")
	assertParseError(t, "blob { 00", "Unexpected token EOF, example:1:10")
	assertParseError(t, "blob {hh}", "Invalid blob \"hh\", example:1:9")
	assertParseError(t, "blob {0}", "Invalid blob \"0\", example:1:8")
	assertParseError(t, "blob {00 0}", "Invalid blob \"0\", example:1:11")
	assertParseError(t, "blob {ff 0 0}", "Invalid blob \"0\", example:1:11")

}

func TestRoundTrips(t *testing.T) {
	vs := newTestValueStore()

	test := func(v types.Value) {
		code := types.EncodedValue(v)
		assertParse(t, vs, code, v)
	}

	test(types.Number(0))
	test(types.Number(42))
	test(types.Number(-0))
	test(types.Number(-42))
	test(types.Number(0.05))
	test(types.Number(-0.05))
	test(types.Number(1e50))
	test(types.Number(-1e50))

	test(types.Bool(true))
	test(types.Bool(false))

	test(types.String(""))
	test(types.String("a"))
	test(types.String("\""))
	test(types.String("'"))
	test(types.String("`"))

	test(types.NewEmptyBlob(vs))
	test(types.NewBlob(vs, bytes.NewBufferString("abc")))

	test(types.NewList(vs))
	test(types.NewList(vs, types.Number(42), types.Bool(true), types.String("abc")))

	test(types.NewSet(vs))
	test(types.NewSet(vs, types.Number(42), types.Bool(true), types.String("abc")))

	test(types.NewMap(vs))
	test(types.NewMap(vs, types.Number(42), types.Bool(true), types.String("abc"), types.NewMap(vs)))

	test(types.NewStruct("", nil))
	test(types.NewStruct("Number", nil))
	test(types.NewStruct("Number", types.StructData{
		"Number": types.NumberType,
	}))

	test(types.MakeStructType("S", types.StructField{
		Name: "cycle", Type: types.MakeCycleType("S"), Optional: true,
	}))
}
