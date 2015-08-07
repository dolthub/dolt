package main

import (
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")
	ng.AddType(util.PhotoTypeDef)
	ng.AddType(util.PhotoSetTypeDef)

	ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), util.PhotoSetTypeDef))

	ng.WriteGo("main")
}
