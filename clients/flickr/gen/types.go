package main

import (
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	album := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Album"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("title"), types.NewString("string"),
		types.NewString("photos"), util.PhotoSetTypeDef))

	albumMap := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), album))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("User"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("name"), types.NewString("string"),
		types.NewString("oAuthToken"), types.NewString("string"),
		types.NewString("oAuthSecret"), types.NewString("string"),
		types.NewString("albums"), albumMap))

	ng.WriteGo("main")
}
