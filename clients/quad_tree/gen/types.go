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

    Georectangle := ng.AddType(types.NewMap(
        types.NewString("$type"), types.NewString("noms.StructDef"),
        types.NewString("$name"), types.NewString("Georectangle"),
        types.NewString("TopLeft"), Geoposition,
        types.NewString("BottomRight"), Geoposition,
    ))

    Geonode := ng.AddType(types.NewMap(
        types.NewString("$type"), types.NewString("noms.StructDef"),
        types.NewString("$name"), types.NewString("Geonode"),
        types.NewString("geoposition"), Geoposition,
    ))

    GeonodeList := ng.AddType(types.NewMap(
        types.NewString("$type"), types.NewString("noms.ListDef"),
        types.NewString("elem"), Geonode))

    QuadTreeDef := ng.AddType(types.NewMap(
        types.NewString("$type"), types.NewString("noms.StructDef"),
        types.NewString("$name"), types.NewString("QuadTree"),
        types.NewString("Nodes"), GeonodeList,
        types.NewString("Children"), types.NewString("map"),
        types.NewString("Depth"), types.NewString("uint8"),
        types.NewString("NumDescendents"), types.NewString("uint64"),
        types.NewString("Path"), types.NewString("string"),
        types.NewString("Georectangle"), Georectangle,
    ))

//    QuadTreeDefListDef := 
    ng.AddType(types.NewMap(
        types.NewString("$type"), types.NewString("noms.MapDef"),
        types.NewString("key"), types.NewString("string"),
        types.NewString("value"), QuadTreeDef,
    ))

    ng.WriteGo("main")
}
