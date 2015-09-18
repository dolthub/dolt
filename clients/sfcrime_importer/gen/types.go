package main

import (
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	ng := nomgen.New("types.go")

    Geoposition := types.NewMap(
        types.NewString("$type"), types.NewString("noms.StructDef"),
        types.NewString("$name"), types.NewString("Geoposition"),
        types.NewString("latitude"), types.NewString("float32"),
        types.NewString("longitude"), types.NewString("float32"),
    )

    Incident := ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Incident"),
		types.NewString("ID"), types.NewString("int64"),
		types.NewString("Category"), types.NewString("string"),
		types.NewString("Description"), types.NewString("string"),
		types.NewString("DayOfWeek"), types.NewString("string"),
		types.NewString("Date"), types.NewString("string"),
		types.NewString("Time"), types.NewString("string"),
		types.NewString("PdDistrict"), types.NewString("string"),
		types.NewString("Resolution"), types.NewString("string"),
		types.NewString("Address"), types.NewString("string"),
		types.NewString("geoposition"), Geoposition,
		types.NewString("PdID"), types.NewString("string"),
	))

    ng.AddType(types.NewMap(
		types.NewString("$type"), types.NewString("noms.ListDef"),
		types.NewString("elem"), Incident))

	ng.WriteGo("main")
}
