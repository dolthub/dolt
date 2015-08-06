package main

import (
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

	photoset := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Photoset"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("title"), types.NewString("string"),
		types.NewString("photos"), util.PhotoSetTypeDef))

	photosetSet := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), photoset))

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("User"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("name"), types.NewString("string"),
		types.NewString("oAuthToken"), types.NewString("string"),
		types.NewString("oAuthSecret"), types.NewString("string"),
		types.NewString("photosets"), photosetSet))

	ng.WriteGo("main")
}
