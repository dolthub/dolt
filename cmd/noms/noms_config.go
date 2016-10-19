// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	flag "github.com/juju/gnuflag"
)

var nomsConfig = &util.Command{
	Run:       runConfig,
	UsageLine: "config ",
	Short:     "Display noms config info",
	Long:      "Prints the active configuration if a .nomsconfig file is present",
	Flags:     setupConfigFlags,
	Nargs:     0,
}

func setupConfigFlags() *flag.FlagSet {
	return flag.NewFlagSet("config", flag.ExitOnError)
}

func runConfig(args []string) int {
	c, err := config.FindNomsConfig()
	if err == config.NoConfig {
		fmt.Fprintf(os.Stdout, "no config active\n")
	} else {
		d.CheckError(err)
		fmt.Fprintf(os.Stdout, "%s\n", c.String())
	}
	return 0
}
