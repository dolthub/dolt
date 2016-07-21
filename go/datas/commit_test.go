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

	// Commiting another Number
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
	meta := types.NewStruct("Meta", map[string]types.Value{"date": types.String("some date"), "number": types.Number(9)})
	commit4 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)), meta)
	at4 := commit4.Type()
	et4 := types.MakeStructType("Commit", commitFieldNames, []*types.Type{
		types.EmptyStructType,
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", commitFieldNames, []*types.Type{
			types.EmptyStructType,
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et4, at4)
}

func TestCommitWithoutMetaField(t *testing.T) {
	assert := assert.New(t)
	metaCommit := types.NewStruct("Commit", map[string]types.Value{
		"value":   types.Number(9),
		"parents": types.NewSet(),
		"meta":    types.EmptyStruct,
	})
	assert.True(IsCommitType(metaCommit.Type()))

	noMetaCommit := types.NewStruct("Commit", map[string]types.Value{
		"value":   types.Number(9),
		"parents": types.NewSet(),
	})
	assert.True(IsCommitType(noMetaCommit.Type()))

	badCommit := types.NewStruct("Commit", map[string]types.Value{
		"value":    types.Number(9),
		"parents1": types.NewSet(),
	})
	assert.False(IsCommitType(badCommit.Type()))

	badMetaCommit := types.NewStruct("Commit", map[string]types.Value{
		"value":    types.Number(9),
		"parents1": types.NewSet(),
		"meta":     types.String("one"),
	})
	assert.False(IsCommitType(badMetaCommit.Type()))
}
