package util

import "github.com/attic-labs/noms/types"

var (
	PhotoTypeDef    types.Map
	PhotoSetTypeDef types.Map
)

func init() {
	stringSet := types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), types.NewString("string"))

	PhotoTypeDef = types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Photo"),
		types.NewString("height"), types.NewString("uint32"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("image"), types.NewString("blob"),
		types.NewString("tags"), stringSet,
		types.NewString("title"), types.NewString("string"),
		types.NewString("url"), types.NewString("string"),
		types.NewString("width"), types.NewString("uint32"),
	)

	PhotoSetTypeDef = types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), PhotoTypeDef)
}
