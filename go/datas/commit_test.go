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
	assertTypeEquals := func(e, a *types.Type) {
		assert.True(a.Equals(e), "Actual: %s\nExpected %s", a.Describe(), e.Describe())
	}

	commit := NewCommit(types.Number(1), types.NewSet())
	at := commit.Type()
	et := types.MakeStructType("Commit", []string{
		ParentsField, ValueField,
	}, []*types.Type{
		types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
		types.NumberType,
	})
	assertTypeEquals(et, at)

	// Commiting another Number
	commit2 := NewCommit(types.Number(2), types.NewSet(types.NewRef(commit)))
	at2 := commit2.Type()
	et2 := et
	assertTypeEquals(et2, at2)

	// Now commit a String
	commit3 := NewCommit(types.String("Hi"), types.NewSet(types.NewRef(commit2)))
	at3 := commit3.Type()
	et3 := types.MakeStructType("Commit", []string{
		ParentsField, ValueField,
	}, []*types.Type{
		types.MakeSetType(types.MakeRefType(types.MakeStructType("Commit", []string{
			ParentsField, ValueField,
		}, []*types.Type{
			types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
			types.MakeUnionType(types.NumberType, types.StringType),
		}))),
		types.StringType,
	})
	assertTypeEquals(et3, at3)
}
