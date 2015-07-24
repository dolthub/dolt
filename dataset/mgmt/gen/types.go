package main

import (
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	dataset := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Dataset"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("heads"), types.NewString("value")))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), dataset))

	ng.WriteGo("mgmt")
}
