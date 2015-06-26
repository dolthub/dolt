package main

import (
	"flag"
	"os"

	"github.com/attic-labs/noms/codegen/nomgen"
	. "github.com/attic-labs/noms/dbg"
	_ "github.com/attic-labs/noms/enc"
	"github.com/attic-labs/noms/types"
)

func main() {
	outFile := flag.String("o", "", "output file")
	flag.Parse()
	if *outFile == "" {
		flag.Usage()
		return
	}

	app := types.NewMap(
		types.NewString("$type"), types.NewString("noms.StructDef"),
		types.NewString("$name"), types.NewString("App"),
		types.NewString("id"), types.NewString("string"),
		types.NewString("root"), types.NewString("value"),
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

	userSet := types.NewMap(
		types.NewString("$type"), types.NewString("noms.SetDef"),
		types.NewString("elem"), user)

	f, err := os.OpenFile(*outFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	defer f.Close()
	Chk.NoError(err)
	ng := nomgen.New(f)
	ng.WriteGo(userSet, "user")
}
