// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"context"
	"fmt"
	"os"

	"github.com/attic-labs/noms/cmd/util"
	flag "github.com/juju/gnuflag"
	"github.com/liquidata-inc/ld/dolt/go/store/go/config"
	"github.com/liquidata-inc/ld/dolt/go/store/go/d"
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

func runConfig(ctx context.Context, args []string) int {
	c, err := config.FindNomsConfig()
	if err == config.ErrNoConfig {
		fmt.Fprintf(os.Stdout, "no config active\n")
	} else {
		d.CheckError(err)
		fmt.Fprintf(os.Stdout, "%s\n", c.String())
	}
	return 0
}
