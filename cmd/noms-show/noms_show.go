package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path"

	"github.com/attic-labs/noms/clients/go/flags"
	"github.com/attic-labs/noms/clients/go/util"
	"github.com/attic-labs/noms/types"
)

var (
	showHelp = flag.Bool("help", false, "show help text")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <object>\n", path.Base(os.Args[0]))
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSee \"Spelling Objects\" at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.\n\n")
	}

	flag.Parse()
	if *showHelp {
		flag.Usage()
		return
	}

	if len(flag.Args()) != 1 {
		util.CheckError(errors.New("expected exactly one argument"))
	}

	spec, err := flags.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	database, value, err := spec.Value()
	util.CheckError(err)

	types.WriteEncodedValueWithTags(os.Stdout, value)
	fmt.Fprintf(os.Stdout, "\n")
	database.Close()
}
