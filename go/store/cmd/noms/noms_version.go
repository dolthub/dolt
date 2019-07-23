// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"os"

	flag "github.com/juju/gnuflag"

	"github.com/liquidata-inc/ld/dolt/go/store/cmd/noms/util"
	"github.com/liquidata-inc/ld/dolt/go/store/constants"
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

func runVersion(ctx context.Context, args []string) int {
	fmt.Fprintf(os.Stdout, "format version: %v\n", constants.NomsVersion)
	fmt.Fprintf(os.Stdout, "built from %v\n", constants.NomsGitSHA)
	return 0
}
