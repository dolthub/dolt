package main

import (
	"flag"
	"os"

	"github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/nomgen"
	"github.com/attic-labs/noms/types"
)

func main() {
	outFile := flag.String("o", "", "output file")
	flag.Parse()
	if *outFile == "" {
		flag.Usage()
		return
	}

	f, err := os.OpenFile(*outFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	defer f.Close()
	dbg.Chk.NoError(err)
	ng := nomgen.New(f)

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
