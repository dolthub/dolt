// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package datas

import "github.com/attic-labs/noms/types"

var commitType *types.Type

const (
	ParentsField = "parents"
	ValueField   = "value"
)

func init() {
	structName := "Commit"

	// struct Commit {
	//   parents: Set<Ref<Commit>>
	//   value: Value
	// }

	fieldTypes := types.TypeMap{
		ParentsField: nil,
		ValueField:   types.ValueType,
	}
	commitType = types.MakeStructType(structName, fieldTypes)
	commitType.Desc.(types.StructDesc).Fields[ParentsField] = types.MakeSetType(types.MakeRefType(commitType))
}

func NewCommit() types.Struct {
	initialFields := map[string]types.Value{
		ValueField:   types.NewString(""),
		ParentsField: types.NewSet(),
	}

	return types.NewStructWithType(commitType, initialFields)
}

func typeForMapOfStringToRefOfCommit() *types.Type {
	return types.MakeMapType(types.StringType, types.MakeRefType(commitType))
}

func NewMapOfStringToRefOfCommit() types.Map {
	return types.NewMap()
}

func typeForSetOfRefOfCommit() *types.Type {
	return types.MakeSetType(types.MakeRefType(commitType))
}
