// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/constants"
	flag "github.com/juju/gnuflag"
)

var nomsVersion = &util.Command{
	Run:       runVersion,
	UsageLine: "version ",
	Short:     "Display noms version",
	Long:      "version prints the Noms data version and build identifier",
	Flags:     setupVersionFlags,
	Nargs:     0,
}

func setupVersionFlags() *flag.FlagSet {
	return flag.NewFlagSet("version", flag.ExitOnError)
}

func runVersion(args []string) int {
	fmt.Fprintf(os.Stdout, "format version: %v\n", constants.NomsVersion)
	fmt.Fprintf(os.Stdout, "built from %v\n", constants.NomsGitSHA)
	return 0
}
