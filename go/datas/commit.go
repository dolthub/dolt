// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import "github.com/attic-labs/noms/go/types"

var commitType *types.Type
var refOfCommitType *types.Type

const (
	ParentsField = "parents"
	ValueField   = "value"
)

func init() {
	// struct Commit {
	//   parents: Set<Ref<Commit>>
	//   value: Value
	// }

	commitType = types.MakeStructType("Commit", types.TypeMap{
		ParentsField: types.MakeSetType(types.MakeRefType(types.MakeCycleType(0))),
		ValueField:   types.ValueType,
	})

	refOfCommitType = types.MakeRefType(commitType)
}

func NewCommit() types.Struct {
	initialFields := map[string]types.Value{
		ValueField:   types.String(""),
		ParentsField: types.NewSet(),
	}

	return types.NewStructWithType(commitType, initialFields)
}

func typeForMapOfStringToRefOfCommit() *types.Type {
	return types.MakeMapType(types.StringType, refOfCommitType)
}

func NewMapOfStringToRefOfCommit() types.Map {
	return types.NewMap()
}

func typeForSetOfRefOfCommit() *types.Type {
	return types.MakeSetType(refOfCommitType)
}

func CommitType() *types.Type {
	return commitType
}
