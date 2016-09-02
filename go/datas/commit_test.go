// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import (
	"testing"

	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/testify/assert"
)

func TestNewCommit(t *testing.T) {
	assert := assert.New(t)

	commitFieldNames := []string{MetaField, ParentsField, ValueField}

	assertTypeEquals := func(e, a *types.Type) {
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", a.Describe(), e.Describe())
	}

	commit := NewCommit(types.Number(1), types.NewSet(), types.EmptyStruct)
	at := commit.Type()
	et := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
		types.NumberType,
	})
	assertTypeEquals(et, at)

	// Committing another Number
	commit2 := NewCommit(types.Number(2), types.NewSet(types.NewRef(commit)), types.EmptyStruct)
	at2 := commit2.Type()
	et2 := et
	assertTypeEquals(et2, at2)

	// Now commit a String
	commit3 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)), types.EmptyStruct)
	at3 := commit3.Type()
	et3 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.EmptyStructType,
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et3, at3)

	// Now commit a String with MetaInfo
	meta := types.NewStruct("Meta", types.StructData{"date": types.String("some date"), "number": types.Number(9)})
	metaType := types.MakeStructType("Meta", []string{"date", "number"}, []*types.Type{types.StringType, types.NumberType})
	assertTypeEquals(metaType, meta.Type())
	commit4 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)), meta)
	at4 := commit4.Type()
	et4 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		metaType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.MakeUnionType(types.EmptyStructType, metaType),
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et4, at4)

	// Merge-commit with different parent types
	commit5 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2), types.NewRef(commit3)), types.EmptyStruct)
	at5 := commit5.Type()
	et5 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.EmptyStructType,
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et5, at5)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)
	metaCommit := types.NewStruct("Commit", types.StructData{
		"value":   types.Number(9),
		"parents": types.NewSet(),
		"meta":    types.EmptyStruct,
	})
	assert.True(IsCommitType(metaCommit.Type()))

	noMetaCommit := types.NewStruct("Commit", types.StructData{
		"value":   types.Number(9),
		"parents": types.NewSet(),
	})
	assert.False(IsCommitType(noMetaCommit.Type()))
}
