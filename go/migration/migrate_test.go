// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package migration

import (
	"bytes"
	"testing"

	"github.com/attic-labs/noms/go/chunks"
	v7chunks "github.com/attic-labs/noms/go/chunks"
	"github.com/attic-labs/noms/go/datas"
	v7datas "github.com/attic-labs/noms/go/datas"
	"github.com/attic-labs/noms/go/types"
	v7types "github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestMigrateFromVersion7(t *testing.T) {
	sourceStore := v7datas.NewDatabase(v7chunks.NewMemoryStore())
	sinkStore := datas.NewDatabase(chunks.NewMemoryStore())

	test := func(expected types.Value, source v7types.Value) {
		actual, err := MigrateFromVersion7(source, sourceStore, sinkStore)
		assert.NoError(t, err)
		assert.True(t, actual.Equals(expected))
	}

	test(types.Bool(true), v7types.Bool(true))
	test(types.Bool(false), v7types.Bool(false))

	test(types.Number(-42), v7types.Number(-42))
	test(types.Number(-1.23456789), v7types.Number(-1.23456789))
	test(types.Number(0), v7types.Number(0))
	test(types.Number(1.23456789), v7types.Number(1.23456789))
	test(types.Number(42), v7types.Number(42))

	test(types.String(""), v7types.String(""))
	test(types.String("Hello World"), v7types.String("Hello World"))
	test(types.String("💩"), v7types.String("💩"))

	test(types.NewBlob(bytes.NewBuffer([]byte{})), v7types.NewBlob(bytes.NewBuffer([]byte{})))
	test(types.NewBlob(bytes.NewBufferString("hello")), v7types.NewBlob(bytes.NewBufferString("hello")))

	test(types.NewList(), v7types.NewList())
	test(types.NewList(types.Bool(true)), v7types.NewList(v7types.Bool(true)))
	test(types.NewList(types.Bool(true), types.String("hi")), v7types.NewList(v7types.Bool(true), v7types.String("hi")))

	test(types.NewSet(), v7types.NewSet())
	test(types.NewSet(types.Bool(true)), v7types.NewSet(v7types.Bool(true)))
	test(types.NewSet(types.Bool(true), types.String("hi")), v7types.NewSet(v7types.Bool(true), v7types.String("hi")))

	test(types.NewMap(), v7types.NewMap())
	test(types.NewMap(types.Bool(true), types.String("hi")), v7types.NewMap(v7types.Bool(true), v7types.String("hi")))

	test(types.NewStruct("", types.StructData{}), v7types.NewStruct("", v7types.StructData{}))
	test(types.NewStruct("xyz", types.StructData{}), v7types.NewStruct("xyz", v7types.StructData{}))
	test(types.NewStruct("T", types.StructData{}), v7types.NewStruct("T", v7types.StructData{}))

	test(types.NewStruct("T", types.StructData{
		"x": types.Number(42),
		"s": types.String("hi"),
		"b": types.Bool(false),
	}), v7types.NewStruct("T", v7types.StructData{
		"x": v7types.Number(42),
		"s": v7types.String("hi"),
		"b": v7types.Bool(false),
	}))

	test(
		types.NewStructWithType(
			types.MakeStructType("", []string{"a"}, []*types.Type{types.NumberType}),
			[]types.Value{types.Number(42)},
		),
		v7types.NewStructWithType(
			v7types.MakeStructType("", []string{"a"}, []*v7types.Type{v7types.NumberType}),
			[]v7types.Value{v7types.Number(42)},
		),
	)

	test(
		types.NewStructWithType(
			types.MakeStructType("",
				[]string{"a"},
				[]*types.Type{types.MakeListType(types.MakeCycleType(0))},
			),
			[]types.Value{types.NewList()},
		),
		v7types.NewStructWithType(
			v7types.MakeStructType("",
				[]string{"a"},
				[]*v7types.Type{v7types.MakeListType(v7types.MakeCycleType(0))},
			),
			[]v7types.Value{v7types.NewList()},
		),
	)

	r := sourceStore.WriteValue(v7types.Number(123))
	test(types.NewRef(types.Number(123)), r)
	v := sinkStore.ReadValue(types.Number(123).Hash())
	assert.True(t, types.Number(123).Equals(v))

	// Types
	test(types.BoolType, v7types.BoolType)
	test(types.NumberType, v7types.NumberType)
	test(types.StringType, v7types.StringType)
	test(types.BlobType, v7types.BlobType)
	test(types.TypeType, v7types.TypeType)
	test(types.ValueType, v7types.ValueType)
	test(types.MakeListType(types.NumberType), v7types.MakeListType(types.NumberType))
	test(types.MakeListType(types.NumberType).Type(), v7types.MakeListType(types.NumberType).Type())

	test(types.MakeListType(types.NumberType), v7types.MakeListType(v7types.NumberType))
	test(types.MakeSetType(types.NumberType), v7types.MakeSetType(v7types.NumberType))
	test(types.MakeRefType(types.NumberType), v7types.MakeRefType(v7types.NumberType))
	test(types.MakeMapType(types.NumberType, types.StringType), v7types.MakeMapType(v7types.NumberType, v7types.StringType))
	test(types.MakeUnionType(), v7types.MakeUnionType())
	test(types.MakeUnionType(types.StringType, types.BoolType), v7types.MakeUnionType(v7types.StringType, v7types.BoolType))

	test(types.MakeCycleType(42), v7types.MakeCycleType(42))

	commitFieldNames := []string{"parents", "value"}
	commit := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})

	commit7 := v7types.MakeStructType("Commit", commitFieldNames, []*v7types.Type{
		v7types.MakeSetType(v7types.MakeRefType(v7types.MakeStructType("Commit", commitFieldNames, []*v7types.Type{
			v7types.MakeSetType(v7types.MakeRefType(v7types.MakeCycleType(0))),
			v7types.MakeUnionType(v7types.NumberType, v7types.StringType),
		}))),
		v7types.StringType,
	})
	test(commit, commit7)
}
