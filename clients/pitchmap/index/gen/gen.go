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

	pitch := types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("Pitch"),
		types.NewString("X"), types.NewString("float64"),
		types.NewString("Z"), types.NewString("float64"),
	)

	pitchList := types.NewMap(
		types.NewString("$type"), types.NewString("noms.ListDef"),
		types.NewString("$name"), types.NewString("PitchList"),
		types.NewString("elem"), pitch,
	)

	stringPitchListMap := types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), pitchList,
	)

	stringStringMap := types.NewMap(
		types.NewString("$type"), types.NewString("noms.MapDef"),
		types.NewString("key"), types.NewString("string"),
		types.NewString("value"), types.NewString("string"),
	)

	f, err := os.OpenFile(*outFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	defer f.Close()
	dbg.Chk.NoError(err)
	ng := nomgen.New(f)
	ng.WriteGo("main", pitch, pitchList, stringPitchListMap, stringStringMap)
}
