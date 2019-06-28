// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/liquidata-inc/ld/dolt/go/store/chunks"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
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
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", expected.Describe(context.Background()), actual.Describe(context.Background()))
	})
}

func assertParse(t *testing.T, vrw types.ValueReadWriter, code string, expected types.Value) {
	t.Run(code, func(t *testing.T) {
		actual, err := Parse(context.Background(), vrw, code)
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", types.EncodedValue(context.Background(), expected), types.EncodedValue(context.Background(), actual))
	})
}

func assertParseError(t *testing.T, code, msg string) {
	t.Run(code, func(t *testing.T) {
		vrw := newTestValueStore()
		p := New(vrw, strings.NewReader(code), ParserOptions{
			Filename: "example",
		})
		err := catchSyntaxError(func() {
			p.parseValue(context.Background())
		})
		if assert.Error(t, err) {
			assert.Equal(t, msg, err.Error())
		}
	})
}

func TestSimpleTypes(t *testing.T) {
	assertParseType(t, "Blob", types.BlobType)
	assertParseType(t, "Bool", types.BoolType)
	assertParseType(t, "Float", types.FloaTType)
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
	assertParseError(t, "List<Bool, Float>", `Unexpected token ",", expected ">", example:1:11`)
	assertParseError(t, "List<Bool", `Unexpected token EOF, expected ">", example:1:10`)
	assertParseError(t, "List<", `Unexpected token EOF, expected Ident, example:1:6`)
	assertParseError(t, "List", `Unexpected token EOF, expected "<", example:1:5`)

	assertParseType(t, "Set<>", types.MakeSetType(types.MakeUnionType()))
	assertParseType(t, "Set<Bool>", types.MakeSetType(types.BoolType))
	assertParseError(t, "Set<Bool, Float>", `Unexpected token ",", expected ">", example:1:10`)
	assertParseError(t, "Set<Bool", `Unexpected token EOF, expected ">", example:1:9`)
	assertParseError(t, "Set<", `Unexpected token EOF, expected Ident, example:1:5`)
	assertParseError(t, "Set", `Unexpected token EOF, expected "<", example:1:4`)

	assertParseError(t, "Ref<>", `Unexpected token ">", expected Ident, example:1:6`)
	assertParseType(t, "Ref<Bool>", types.MakeRefType(types.BoolType))
	assertParseError(t, "Ref<Float, Bool>", `Unexpected token ",", expected ">", example:1:11`)
	assertParseError(t, "Ref<Float", `Unexpected token EOF, expected ">", example:1:10`)
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
                x: Float
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.FloaTType,
	}))

	assertParseType(t, `Struct S {
	        x: Float,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.FloaTType,
	}))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: String
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.FloaTType,
		"y": types.StringType,
	}))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: String,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.FloaTType,
		"y": types.StringType,
	}))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: Struct {
	                z: String,
	        },
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.FloaTType,
		"y": types.MakeStructTypeFromFields("", types.FieldMap{
			"z": types.StringType,
		}),
	}))

	assertParseType(t, `Struct S {
                x?: Float,
                y: String,
        }`, types.MakeStructType("S",
		types.StructField{Name: "x", Type: types.FloaTType, Optional: true},
		types.StructField{Name: "y", Type: types.StringType, Optional: false},
	))

	assertParseError(t, `Struct S {
	        x: Float
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
	assertParseType(t, "Bool | Float | String", types.MakeUnionType(types.BoolType, types.FloaTType, types.StringType))
	assertParseType(t, "List<Bool | Float>", types.MakeListType(types.MakeUnionType(types.BoolType, types.FloaTType)))
	assertParseType(t, "Map<Bool | Float, Bool | Float>",
		types.MakeMapType(
			types.MakeUnionType(types.BoolType, types.FloaTType),
			types.MakeUnionType(types.BoolType, types.FloaTType),
		),
	)
	assertParseType(t, `Struct S {
                x: Float | Bool
                }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.FloaTType),
	}))
	assertParseType(t, `Struct S {
                x: Float | Bool,
                y: String
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.FloaTType),
		"y": types.StringType,
	}))

	assertParseError(t, "Bool |", "Unexpected token EOF, expected Ident, example:1:7")
	assertParseError(t, "Bool | Float |", "Unexpected token EOF, expected Ident, example:1:15")
	assertParseError(t, "Bool | | ", `Unexpected token "|", expected Ident, example:1:9`)
	assertParseError(t, "", `Unexpected token EOF, example:1:1`)
}

func TestValuePrimitives(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Float", types.FloaTType)
	assertParse(t, vs, "Float | String", types.MakeUnionType(types.FloaTType, types.StringType))

	assertParse(t, vs, "true", types.Bool(true))
	assertParse(t, vs, "false", types.Bool(false))

	assertParse(t, vs, "0", types.Float(0))
	assertParse(t, vs, "1", types.Float(1))
	assertParse(t, vs, "1.1", types.Float(1.1))
	assertParse(t, vs, "1.1e1", types.Float(1.1e1))
	assertParse(t, vs, "1e1", types.Float(1e1))
	assertParse(t, vs, "1e-1", types.Float(1e-1))
	assertParse(t, vs, "1e+1", types.Float(1e+1))

	assertParse(t, vs, "+0", types.Float(0))
	assertParse(t, vs, "+1", types.Float(1))
	assertParse(t, vs, "+1.1", types.Float(1.1))
	assertParse(t, vs, "+1.1e1", types.Float(1.1e1))
	assertParse(t, vs, "+1e1", types.Float(1e1))
	assertParse(t, vs, "+1e-1", types.Float(1e-1))
	assertParse(t, vs, "+1e+1", types.Float(1e+1))

	assertParse(t, vs, "-0", types.Float(-0))
	assertParse(t, vs, "-1", types.Float(-1))
	assertParse(t, vs, "-1.1", types.Float(-1.1))
	assertParse(t, vs, "-1.1e1", types.Float(-1.1e1))
	assertParse(t, vs, "-1e1", types.Float(-1e1))
	assertParse(t, vs, "-1e-1", types.Float(-1e-1))
	assertParse(t, vs, "-1e+1", types.Float(-1e+1))

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
	// TODO(binformat)
	assertParse(t, vs, "[]", types.NewList(context.Background(), types.Format_7_18, vs))

	assertParse(t, vs, "[42]", types.NewList(context.Background(), types.Format_7_18, vs, types.Float(42)))
	assertParse(t, vs, "[42,]", types.NewList(context.Background(), types.Format_7_18, vs, types.Float(42)))

	assertParseError(t, "[", "Unexpected token EOF, example:1:2")
	assertParseError(t, "[,", "Unexpected token \",\", example:1:3")
	assertParseError(t, "[42", "Unexpected token EOF, expected \"]\", example:1:4")
	assertParseError(t, "[42,", "Unexpected token EOF, example:1:5")
	assertParseError(t, "[,]", "Unexpected token \",\", example:1:3")

	// TODO(binformat)
	assertParse(t, vs, `[42,
                Bool,
        ]`, types.NewList(context.Background(), types.Format_7_18, vs, types.Float(42), types.BoolType))
	assertParse(t, vs, `[42,
                Bool
        ]`, types.NewList(context.Background(), types.Format_7_18, vs, types.Float(42), types.BoolType))
}

func TestValueSet(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "set {}", types.NewSet(context.Background(), vs))

	assertParse(t, vs, "set {42}", types.NewSet(context.Background(), vs, types.Float(42)))
	assertParse(t, vs, "set {42,}", types.NewSet(context.Background(), vs, types.Float(42)))

	assertParseError(t, "set", "Unexpected token EOF, expected \"{\", example:1:4")
	assertParseError(t, "set {", "Unexpected token EOF, example:1:6")
	assertParseError(t, "set {,", "Unexpected token \",\", example:1:7")
	assertParseError(t, "set {42", "Unexpected token EOF, expected \"}\", example:1:8")
	assertParseError(t, "set {42,", "Unexpected token EOF, example:1:9")
	assertParseError(t, "set {,}", "Unexpected token \",\", example:1:7")

	assertParse(t, vs, `set {42,
                Bool,
        }`, types.NewSet(context.Background(), vs, types.Float(42), types.BoolType))
	assertParse(t, vs, `set {42,
                Bool
        }`, types.NewSet(context.Background(), vs, types.Float(42), types.BoolType))
}

func TestValueMap(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "map {}", types.NewMap(context.Background(), types.Format_7_18, vs))

	assertParse(t, vs, "map {42: true}", types.NewMap(context.Background(), types.Format_7_18, vs, types.Float(42), types.Bool(true)))
	assertParse(t, vs, "map {42: true,}", types.NewMap(context.Background(), types.Format_7_18, vs, types.Float(42), types.Bool(true)))

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
        }`, types.NewMap(context.Background(), types.Format_7_18, vs, types.Float(42), types.BoolType))
	assertParse(t, vs, `map {42:
                Bool
        }`, types.NewMap(context.Background(), types.Format_7_18, vs, types.Float(42), types.BoolType))
}

func TestValueType(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Bool", types.BoolType)
	assertParse(t, vs, "Float", types.FloaTType)
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

	assertParse(t, vs, "struct name {a: 42}", types.NewStruct("name", types.StructData{"a": types.Float(42)}))
	assertParse(t, vs, "struct name {a: 42,}", types.NewStruct("name", types.StructData{"a": types.Float(42)}))
	assertParseError(t, "struct name {a", "Unexpected token EOF, expected \":\", example:1:15")
	assertParseError(t, "struct name {a: ", "Unexpected token EOF, example:1:17")
	assertParseError(t, "struct name {a,", "Unexpected token \",\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a}", "Unexpected token \"}\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a: 42", "Unexpected token EOF, expected \"}\", example:1:19")
	assertParseError(t, "struct name {a: 42,", "Unexpected token EOF, expected Ident, example:1:20")
	assertParseError(t, "struct name {a:}", "Unexpected token \"}\", example:1:17")

	assertParse(t, vs, "struct name {b: 42, a: true}", types.NewStruct("name", types.StructData{"b": types.Float(42), "a": types.Bool(true)}))
	assertParse(t, vs, `struct name {
                b: 42,
                a: true,
        }`, types.NewStruct("name", types.StructData{"b": types.Float(42), "a": types.Bool(true)}))

	assertParse(t, vs, "struct name {a: Struct {}}", types.NewStruct("name", types.StructData{"a": types.MakeStructType("")}))
}

func TestValueBlob(t *testing.T) {
	vs := newTestValueStore()

	test := func(code string, bs ...byte) {
		// TODO(binformat)
		assertParse(t, vs, code, types.NewBlob(context.Background(), types.Format_7_18, vs, bytes.NewBuffer(bs)))
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
		code := types.EncodedValue(context.Background(), v)
		assertParse(t, vs, code, v)
	}

	test(types.Float(0))
	test(types.Float(42))
	test(types.Float(-0))
	test(types.Float(-42))
	test(types.Float(0.05))
	test(types.Float(-0.05))
	test(types.Float(1e50))
	test(types.Float(-1e50))

	test(types.Bool(true))
	test(types.Bool(false))

	test(types.String(""))
	test(types.String("a"))
	test(types.String("\""))
	test(types.String("'"))
	test(types.String("`"))

	// TODO(binformat)
	test(types.NewEmptyBlob(vs, types.Format_7_18))
	test(types.NewBlob(context.Background(), types.Format_7_18, vs, bytes.NewBufferString("abc")))

	// TODO(binformat)
	test(types.NewList(context.Background(), types.Format_7_18, vs))
	test(types.NewList(context.Background(), types.Format_7_18, vs, types.Float(42), types.Bool(true), types.String("abc")))

	test(types.NewSet(context.Background(), vs))
	test(types.NewSet(context.Background(), vs, types.Float(42), types.Bool(true), types.String("abc")))

	test(types.NewMap(context.Background(), types.Format_7_18, vs))
	test(types.NewMap(context.Background(), types.Format_7_18, vs, types.Float(42), types.Bool(true), types.String("abc"), types.NewMap(context.Background(), types.Format_7_18, vs)))

	test(types.NewStruct("", nil))
	test(types.NewStruct("Float", nil))
	test(types.NewStruct("Float", types.StructData{
		"Float": types.FloaTType,
	}))

	test(types.MakeStructType("S", types.StructField{
		Name: "cycle", Type: types.MakeCycleType("S"), Optional: true,
	}))
}
