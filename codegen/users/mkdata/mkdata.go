package main

import (
	"flag"
	"fmt"

	"github.com/attic-labs/noms/chunks"
	. "github.com/attic-labs/noms/dbg"
	"github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/types"
)

var (
	outDir = flag.String("out", "", "blah")
)

func main() {
	flag.Parse()
	if *outDir == "" {
		flag.Usage()
		return
	}

	cs := chunks.NewFileStore(*outDir, "root")

	app := types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("App"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("value"), types.NewString("value"),
	)

	user := types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("User"),
		types.NewString("email"), types.NewString("string"),
		types.NewString("apps"), types.NewMap(
			types.NewString("$type"), types.NewString("noms.SetDef"),
			types.NewString("elem"), app,
		),
	)

	ref, err := enc.WriteValue(user, cs)
	Chk.NoError(err)
	fmt.Println(ref.String())
}
