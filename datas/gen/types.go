package main

import (
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	commit := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Commit"),
		types.NewString("value"), types.NewString("value"),
		// grump... circular definition :(
		types.NewString("parents"), types.NewString("set")))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), commit))

	ng.WriteGo("datas")
}
