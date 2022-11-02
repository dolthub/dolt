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
// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/d"
	"github.com/dolthub/dolt/go/store/types"
)

func mustString(str string, err error) string {
	d.PanicIfError(err)
	return str
}

func mustType(typ *types.Type, err error) *types.Type {
	d.PanicIfError(err)
	return typ
}

func mustValue(val types.Value, err error) types.Value {
	d.PanicIfError(err)
	return val
}

func newTestValueStore() *types.ValueStore {
	st := &chunks.TestStorage{}
	return types.NewValueStore(st.NewViewWithDefaultFormat())
}

func assertParseType(t *testing.T, code string, expected *types.Type) {
	t.Run(code, func(t *testing.T) {
		actual, err := ParseType(code)
		require.NoError(t, err)
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", mustString(expected.Describe(context.Background())), mustString(actual.Describe(context.Background())))
	})
}

func assertParse(t *testing.T, vrw types.ValueReadWriter, code string, expected types.Value) {
	t.Run(code, func(t *testing.T) {
		actual, err := Parse(context.Background(), vrw, code)
		if !assert.NoError(t, err) {
			return
		}
		assert.True(t, expected.Equals(actual))
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
	assertParseType(t, "Blob", types.PrimitiveTypeMap[types.BlobKind])
	assertParseType(t, "Bool", types.PrimitiveTypeMap[types.BoolKind])
	assertParseType(t, "Float", types.PrimitiveTypeMap[types.FloatKind])
	assertParseType(t, "String", types.PrimitiveTypeMap[types.StringKind])
	assertParseType(t, "Value", types.PrimitiveTypeMap[types.ValueKind])
	assertParseType(t, "Type", types.PrimitiveTypeMap[types.TypeKind])
}

func TestWhitespace(t *testing.T) {
	for _, r := range " \t\n\r" {
		assertParseType(t, string(r)+"Blob", types.PrimitiveTypeMap[types.BlobKind])
		assertParseType(t, "Blob"+string(r), types.PrimitiveTypeMap[types.BlobKind])
	}
}

func TestComments(t *testing.T) {
	assertParseType(t, "/* */Blob", types.PrimitiveTypeMap[types.BlobKind])
	assertParseType(t, "Blob/* */", types.PrimitiveTypeMap[types.BlobKind])
	assertParseType(t, "Blob//", types.PrimitiveTypeMap[types.BlobKind])
	assertParseType(t, "//\nBlob", types.PrimitiveTypeMap[types.BlobKind])
}

func TestCompoundTypes(t *testing.T) {
	assertParseType(t, "List<>", mustType(types.MakeListType(mustType(types.MakeUnionType()))))
	assertParseType(t, "List<Bool>", mustType(types.MakeListType(types.PrimitiveTypeMap[types.BoolKind])))
	assertParseError(t, "List<Bool, Float>", `Unexpected token ",", expected ">", example:1:11`)
	assertParseError(t, "List<Bool", `Unexpected token EOF, expected ">", example:1:10`)
	assertParseError(t, "List<", `Unexpected token EOF, expected Ident, example:1:6`)
	assertParseError(t, "List", `Unexpected token EOF, expected "<", example:1:5`)

	assertParseType(t, "Set<>", mustType(types.MakeSetType(mustType(types.MakeUnionType()))))
	assertParseType(t, "Set<Bool>", mustType(types.MakeSetType(types.PrimitiveTypeMap[types.BoolKind])))
	assertParseError(t, "Set<Bool, Float>", `Unexpected token ",", expected ">", example:1:10`)
	assertParseError(t, "Set<Bool", `Unexpected token EOF, expected ">", example:1:9`)
	assertParseError(t, "Set<", `Unexpected token EOF, expected Ident, example:1:5`)
	assertParseError(t, "Set", `Unexpected token EOF, expected "<", example:1:4`)

	assertParseError(t, "Ref<>", `Unexpected token ">", expected Ident, example:1:6`)
	assertParseType(t, "Ref<Bool>", mustType(types.MakeRefType(types.PrimitiveTypeMap[types.BoolKind])))
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

	assertParseType(t, "Map<>", mustType(types.MakeMapType(mustType(types.MakeUnionType()), mustType(types.MakeUnionType()))))
	assertParseType(t, "Map<Bool, String>", mustType(types.MakeMapType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.StringKind])))
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
	assertParseType(t, "Struct {}", mustType(types.MakeStructTypeFromFields("", types.FieldMap{})))
	assertParseType(t, "Struct S {}", mustType(types.MakeStructTypeFromFields("S", types.FieldMap{})))

	assertParseType(t, `Struct S {
                x: Float
        }`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{"x": types.PrimitiveTypeMap[types.FloatKind]})))

	assertParseType(t, `Struct S {
	        x: Float,
	}`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{"x": types.PrimitiveTypeMap[types.FloatKind]})))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: String
	}`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{"x": types.PrimitiveTypeMap[types.FloatKind], "y": types.PrimitiveTypeMap[types.StringKind]})))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: String,
	}`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{"x": types.PrimitiveTypeMap[types.FloatKind], "y": types.PrimitiveTypeMap[types.StringKind]})))

	assertParseType(t, `Struct S {
	        x: Float,
	        y: Struct {
	                z: String,
	        },
	}`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.PrimitiveTypeMap[types.FloatKind],
		"y": mustType(types.MakeStructTypeFromFields("", types.FieldMap{"z": types.PrimitiveTypeMap[types.StringKind]})),
	})))

	assertParseType(t, `Struct S {
                x?: Float,
                y: String,
        }`, mustType(types.MakeStructType("S",
		types.StructField{Name: "x", Type: types.PrimitiveTypeMap[types.FloatKind], Optional: true},
		types.StructField{Name: "y", Type: types.PrimitiveTypeMap[types.StringKind], Optional: false},
	)))

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
	assertParseType(t, "Blob | Bool", mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BlobKind], types.PrimitiveTypeMap[types.BoolKind])))
	assertParseType(t, "Bool | Float | String", mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind], types.PrimitiveTypeMap[types.StringKind])))
	assertParseType(t, "List<Bool | Float>", mustType(types.MakeListType(mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind])))))
	assertParseType(t, "Map<Bool | Float, Bool | Float>",
		mustType(types.MakeMapType(
			mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind])),
			mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind])),
		)),
	)
	assertParseType(t, `Struct S {
                x: Float | Bool
                }`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind])),
	})))
	assertParseType(t, `Struct S {
                x: Float | Bool,
                y: String
        }`, mustType(types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.BoolKind], types.PrimitiveTypeMap[types.FloatKind])),
		"y": types.PrimitiveTypeMap[types.StringKind],
	})))

	assertParseError(t, "Bool |", "Unexpected token EOF, expected Ident, example:1:7")
	assertParseError(t, "Bool | Float |", "Unexpected token EOF, expected Ident, example:1:15")
	assertParseError(t, "Bool | | ", `Unexpected token "|", expected Ident, example:1:9`)
	assertParseError(t, "", `Unexpected token EOF, example:1:1`)
}

func TestValuePrimitives(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Float", types.PrimitiveTypeMap[types.FloatKind])
	assertParse(t, vs, "Float | String", mustType(types.MakeUnionType(types.PrimitiveTypeMap[types.FloatKind], types.PrimitiveTypeMap[types.StringKind])))

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
	assertParse(t, vs, "[]", mustValue(types.NewList(context.Background(), vs)))

	assertParse(t, vs, "[42]", mustValue(types.NewList(context.Background(), vs, types.Float(42))))
	assertParse(t, vs, "[42,]", mustValue(types.NewList(context.Background(), vs, types.Float(42))))

	assertParseError(t, "[", "Unexpected token EOF, example:1:2")
	assertParseError(t, "[,", "Unexpected token \",\", example:1:3")
	assertParseError(t, "[42", "Unexpected token EOF, expected \"]\", example:1:4")
	assertParseError(t, "[42,", "Unexpected token EOF, example:1:5")
	assertParseError(t, "[,]", "Unexpected token \",\", example:1:3")

	assertParse(t, vs, `[42,
                Bool,
        ]`, mustValue(types.NewList(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
	assertParse(t, vs, `[42,
                Bool
        ]`, mustValue(types.NewList(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
}

func TestValueSet(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "set {}", mustValue(types.NewSet(context.Background(), vs)))

	assertParse(t, vs, "set {42}", mustValue(types.NewSet(context.Background(), vs, types.Float(42))))
	assertParse(t, vs, "set {42,}", mustValue(types.NewSet(context.Background(), vs, types.Float(42))))

	assertParseError(t, "set", "Unexpected token EOF, expected \"{\", example:1:4")
	assertParseError(t, "set {", "Unexpected token EOF, example:1:6")
	assertParseError(t, "set {,", "Unexpected token \",\", example:1:7")
	assertParseError(t, "set {42", "Unexpected token EOF, expected \"}\", example:1:8")
	assertParseError(t, "set {42,", "Unexpected token EOF, example:1:9")
	assertParseError(t, "set {,}", "Unexpected token \",\", example:1:7")

	assertParse(t, vs, `set {42,
                Bool,
        }`, mustValue(types.NewSet(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
	assertParse(t, vs, `set {42,
                Bool
        }`, mustValue(types.NewSet(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
}

func TestValueMap(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "map {}", mustValue(types.NewMap(context.Background(), vs)))

	assertParse(t, vs, "map {42: true}", mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.Bool(true))))
	assertParse(t, vs, "map {42: true,}", mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.Bool(true))))

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
        }`, mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
	assertParse(t, vs, `map {42:
                Bool
        }`, mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.PrimitiveTypeMap[types.BoolKind])))
}

func TestValueType(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "Bool", types.PrimitiveTypeMap[types.BoolKind])
	assertParse(t, vs, "Float", types.PrimitiveTypeMap[types.FloatKind])
	assertParse(t, vs, "String", types.PrimitiveTypeMap[types.StringKind])
}

func TestValueStruct(t *testing.T) {
	vs := newTestValueStore()
	assertParse(t, vs, "struct {}", mustValue(types.NewStruct(vs.Format(), "", nil)))
	assertParseError(t, "struct", "Unexpected token EOF, expected \"{\", example:1:7")
	assertParseError(t, "struct {", "Unexpected token EOF, expected Ident, example:1:9")

	assertParse(t, vs, "struct name {}", mustValue(types.NewStruct(vs.Format(), "name", nil)))
	assertParseError(t, "struct name", "Unexpected token EOF, expected \"{\", example:1:12")
	assertParseError(t, "struct name {", "Unexpected token EOF, expected Ident, example:1:14")

	assertParse(t, vs, "struct name {a: 42}", mustValue(types.NewStruct(vs.Format(), "name", types.StructData{"a": types.Float(42)})))
	assertParse(t, vs, "struct name {a: 42,}", mustValue(types.NewStruct(vs.Format(), "name", types.StructData{"a": types.Float(42)})))
	assertParseError(t, "struct name {a", "Unexpected token EOF, expected \":\", example:1:15")
	assertParseError(t, "struct name {a: ", "Unexpected token EOF, example:1:17")
	assertParseError(t, "struct name {a,", "Unexpected token \",\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a}", "Unexpected token \"}\", expected \":\", example:1:16")
	assertParseError(t, "struct name {a: 42", "Unexpected token EOF, expected \"}\", example:1:19")
	assertParseError(t, "struct name {a: 42,", "Unexpected token EOF, expected Ident, example:1:20")
	assertParseError(t, "struct name {a:}", "Unexpected token \"}\", example:1:17")

	assertParse(t, vs, "struct name {b: 42, a: true}", mustValue(types.NewStruct(vs.Format(), "name", types.StructData{"b": types.Float(42), "a": types.Bool(true)})))
	assertParse(t, vs, `struct name {
                b: 42,
                a: true,
        }`, mustValue(types.NewStruct(vs.Format(), "name", types.StructData{"b": types.Float(42), "a": types.Bool(true)})))

	assertParse(t, vs, "struct name {a: Struct {}}", mustValue(types.NewStruct(vs.Format(), "name", types.StructData{"a": mustType(types.MakeStructType(""))})))
}

func TestValueBlob(t *testing.T) {
	vs := newTestValueStore()

	test := func(code string, bs ...byte) {
		assertParse(t, vs, code, mustValue(types.NewBlob(context.Background(), vs, bytes.NewBuffer(bs))))
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
		code := mustString(types.EncodedValue(context.Background(), v))
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

	test(mustValue(types.NewEmptyBlob(vs)))
	test(mustValue(types.NewBlob(context.Background(), vs, bytes.NewBufferString("abc"))))

	test(mustValue(types.NewList(context.Background(), vs)))
	test(mustValue(types.NewList(context.Background(), vs, types.Float(42), types.Bool(true), types.String("abc"))))

	test(mustValue(types.NewSet(context.Background(), vs)))
	test(mustValue(types.NewSet(context.Background(), vs, types.Float(42), types.Bool(true), types.String("abc"))))

	test(mustValue(types.NewMap(context.Background(), vs)))
	test(mustValue(types.NewMap(context.Background(), vs, types.Float(42), types.Bool(true), types.String("abc"), mustValue(types.NewMap(context.Background(), vs)))))

	test(mustValue(types.NewStruct(vs.Format(), "", nil)))
	test(mustValue(types.NewStruct(vs.Format(), "Float", nil)))
	test(mustValue(types.NewStruct(vs.Format(), "Float", types.StructData{
		"Float": types.PrimitiveTypeMap[types.FloatKind],
	})))

	test(mustType(types.MakeStructType("S", types.StructField{
		Name: "cycle", Type: types.MakeCycleType("S"), Optional: true,
	})))
}
