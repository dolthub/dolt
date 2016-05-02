package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/clients/flags"
	"github.com/attic-labs/noms/clients/util"
	"github.com/attic-labs/noms/types"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s <object>\n\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "See \"Spelling Objects\" at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.\n\n")
	}

	flag.Parse()
	if len(flag.Args()) != 1 {
		util.CheckError(errors.New("expected exactly one argument"))
	}

	spec, err := flags.ParsePathSpec(flag.Arg(0))
	util.CheckError(err)
	datastore, value, err := spec.Value()
	util.CheckError(err)

	fmt.Println(types.EncodedValueWithTags(value))
	datastore.Close()
}
