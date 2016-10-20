// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/diff"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var summarize bool

var nomsDiff = &util.Command{
	Run:       runDiff,
	UsageLine: "diff [--summarize] <object1> <object2>",
	Short:     "Shows the difference between two objects",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object arguments.",
	Flags:     setupDiffFlags,
	Nargs:     2,
}

func setupDiffFlags() *flag.FlagSet {
	diffFlagSet := flag.NewFlagSet("diff", flag.ExitOnError)
	diffFlagSet.BoolVar(&summarize, "summarize", false, "Writes a summary of the changes instead")
	outputpager.RegisterOutputpagerFlags(diffFlagSet)
	verbose.RegisterVerboseFlags(diffFlagSet)

	return diffFlagSet
}

func runDiff(args []string) int {
	cfg := config.NewResolver()
	db1, value1, err := cfg.GetPath(args[0])
	d.CheckErrorNoUsage(err)
	if value1 == nil {
		d.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[0]))
	}
	defer db1.Close()

	db2, value2, err := cfg.GetPath(args[1])
	d.CheckErrorNoUsage(err)
	if value2 == nil {
		d.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", args[1]))
	}
	defer db2.Close()

	if summarize {
		diff.Summary(value1, value2)
		return 0
	}

	pgr := outputpager.Start()
	defer pgr.Stop()

	diff.PrintDiff(pgr.Writer, value1, value2, false)
	return 0
}
