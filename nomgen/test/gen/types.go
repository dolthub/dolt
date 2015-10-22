package main

import (
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.ListDef"),
		types.NewString("elem"), types.NewString("int32")))

	testSet := ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.SetDef"),
		types.NewString("elem"), types.NewString("bool")))

	ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), types.NewString("float64")))

	testStruct := ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("TestStruct"),
		types.NewString("title"), types.NewString("string")))

	ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.MapDef"),
		types.NewString("key"), testStruct,
		types.NewString("value"), testSet))

	ng.AddType(types.NewMap(
		types.NewString("$typeDef"), types.NewString("noms.SetDef"),
		types.NewString("$name"), types.NewString("MyTestSet"),
		types.NewString("elem"), types.NewString("uint32")))

	ng.WriteGo("main")
}
