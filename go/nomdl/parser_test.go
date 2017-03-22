// Copyright 2017 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package nomdl

import (
	"strings"
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func assertParseType(t *testing.T, code string, expected *types.Type) {
	t.Run(code, func(t *testing.T) {
		actual, err := ParseType(code)
		assert.NoError(t, err)
		assert.True(t, expected.Equals(actual), "Expected: %s, Actual: %s", expected.Describe(), actual.Describe())
	})
}

func assertParseError(t *testing.T, code, msg string) {
	t.Run(code, func(t *testing.T) {
		p := New(strings.NewReader(code), ParserOptions{
			Filename: "example",
		})
		err := catchSyntaxError(func() {
			typ := p.parseType()
			assert.Nil(t, typ)
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
	assertParseError(t, "List<", `Unexpected token EOF, example:1:6`)
	assertParseError(t, "List", `Unexpected token EOF, expected "<", example:1:5`)

	assertParseType(t, "Set<>", types.MakeSetType(types.MakeUnionType()))
	assertParseType(t, "Set<Bool>", types.MakeSetType(types.BoolType))
	assertParseError(t, "Set<Bool, Number>", `Unexpected token ",", expected ">", example:1:10`)
	assertParseError(t, "Set<Bool", `Unexpected token EOF, expected ">", example:1:9`)
	assertParseError(t, "Set<", `Unexpected token EOF, example:1:5`)
	assertParseError(t, "Set", `Unexpected token EOF, expected "<", example:1:4`)

	assertParseError(t, "Ref<>", `Unexpected token ">", example:1:6`)
	assertParseType(t, "Ref<Bool>", types.MakeRefType(types.BoolType))
	assertParseError(t, "Ref<Number, Bool>", `Unexpected token ",", expected ">", example:1:12`)
	assertParseError(t, "Ref<Number", `Unexpected token EOF, expected ">", example:1:11`)
	assertParseError(t, "Ref<", `Unexpected token EOF, example:1:5`)
	assertParseError(t, "Ref", `Unexpected token EOF, expected "<", example:1:4`)

	assertParseType(t, "Cycle<42>", types.MakeCycleType(42))
	assertParseError(t, "Cycle<-123>", `Unexpected token "-", expected Int, example:1:8`)
	assertParseError(t, "Cycle<12.3>", `Unexpected token Float, expected Int, example:1:11`)
	assertParseError(t, "Cycle<>", `Unexpected token ">", expected Int, example:1:8`)
	assertParseError(t, "Cycle<", `Unexpected token EOF, expected Int, example:1:7`)
	assertParseError(t, "Cycle", `Unexpected token EOF, expected "<", example:1:6`)

	assertParseType(t, "Map<>", types.MakeMapType(types.MakeUnionType(), types.MakeUnionType()))
	assertParseType(t, "Map<Bool, String>", types.MakeMapType(types.BoolType, types.StringType))
	assertParseError(t, "Map<Bool,>", `Unexpected token ">", example:1:11`)
	assertParseError(t, "Map<,Bool>", `Unexpected token ",", example:1:6`)
	assertParseError(t, "Map<,>", `Unexpected token ",", example:1:6`)
	assertParseError(t, "Map<Bool,Bool", `Unexpected token EOF, expected ">", example:1:14`)
	assertParseError(t, "Map<Bool,", `Unexpected token EOF, example:1:10`)
	assertParseError(t, "Map<Bool", `Unexpected token EOF, expected ",", example:1:9`)
	assertParseError(t, "Map<", `Unexpected token EOF, example:1:5`)
	assertParseError(t, "Map", `Unexpected token EOF, expected "<", example:1:4`)
}

func TestStructTypes(t *testing.T) {
	assertParseType(t, "struct {}", types.MakeStructTypeFromFields("", types.FieldMap{}))
	assertParseType(t, "struct S {}", types.MakeStructTypeFromFields("S", types.FieldMap{}))

	assertParseType(t, `struct S {
                x: Number
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
	}))

	assertParseType(t, `struct S {
	        x: Number,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
	}))

	assertParseType(t, `struct S {
	        x: Number,
	        y: String
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.StringType,
	}))

	assertParseType(t, `struct S {
	        x: Number,
	        y: String,
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.StringType,
	}))

	assertParseType(t, `struct S {
	        x: Number,
	        y: struct {
	                z: String,
	        },
	}`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.NumberType,
		"y": types.MakeStructTypeFromFields("", types.FieldMap{
			"z": types.StringType,
		}),
	}))

	assertParseError(t, `struct S {
	        x: Number
	        y: String
	}`, `Unexpected token Ident, expected "}", example:3:11`)

	assertParseError(t, `struct S {,}`, `Unexpected token ",", expected Ident, example:1:12`)
	assertParseError(t, `struct S {`, `Unexpected token EOF, expected Ident, example:1:11`)
	assertParseError(t, `struct S { x }`, `Unexpected token "}", expected ":", example:1:15`)
	assertParseError(t, `struct S { x`, `Unexpected token EOF, expected ":", example:1:13`)
	assertParseError(t, `struct S { x: }`, `Unexpected token "}", example:1:16`)
	assertParseError(t, `struct S { x: `, `Unexpected token EOF, example:1:15`)
	assertParseError(t, `struct S { x: Bool`, `Unexpected token EOF, expected "}", example:1:19`)
	assertParseError(t, `struct S { x: Bool,`, `Unexpected token EOF, expected Ident, example:1:20`)
	assertParseError(t, `struct S { x: Bool,,`, `Unexpected token ",", expected Ident, example:1:21`)

	assertParseError(t, `struct S {`, `Unexpected token EOF, expected Ident, example:1:11`)
	assertParseError(t, `struct S `, `Unexpected token EOF, expected "{", example:1:10`)
	assertParseError(t, `struct {`, `Unexpected token EOF, expected Ident, example:1:9`)
	assertParseError(t, `struct`, `Unexpected token EOF, expected "{", example:1:7`)
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
	assertParseType(t, `struct S {
                x: Number | Bool
                }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.NumberType),
	}))
	assertParseType(t, `struct S {
                x: Number | Bool,
                y: String
        }`, types.MakeStructTypeFromFields("S", types.FieldMap{
		"x": types.MakeUnionType(types.BoolType, types.NumberType),
		"y": types.StringType,
	}))

	assertParseError(t, "Bool |", "Unexpected token EOF, example:1:7")
	assertParseError(t, "Bool | Number |", "Unexpected token EOF, example:1:16")
	assertParseError(t, "Bool | | ", `Unexpected token "|", example:1:9`)
	assertParseError(t, "", `Unexpected token EOF, example:1:1`)

}
