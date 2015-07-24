package main

import (
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	pitch := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Pitch"),
		types.NewString("X"), types.NewString("float64"),
		types.NewString("Z"), types.NewString("float64")))

	pitchList := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.ListDef"),
		types.NewString("elem"), pitch))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), pitchList))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), types.NewString("string")))

	ng.WriteGo("main")
}
