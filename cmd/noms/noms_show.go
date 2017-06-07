// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/config"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/go/util/verbose"
	flag "github.com/juju/gnuflag"
)

var nomsShow = &util.Command{
	Run:       runShow,
	UsageLine: "show [flags] <object>",
	Short:     "Shows a serialization of a Noms object",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupShowFlags,
	Nargs:     1,
}

var showRaw = false
var showStats = false

func setupShowFlags() *flag.FlagSet {
	showFlagSet := flag.NewFlagSet("show", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(showFlagSet)
	verbose.RegisterVerboseFlags(showFlagSet)
	showFlagSet.BoolVar(&showRaw, "raw", false, "If true, dumps the raw binary version of the data")
	showFlagSet.BoolVar(&showStats, "stats", false, "If true, reports statistics related to the value")
	return showFlagSet
}

func runShow(args []string) int {
	cfg := config.NewResolver()
	database, value, err := cfg.GetPath(args[0])
	d.CheckErrorNoUsage(err)
	defer database.Close()

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", args[0])
		return 0
	}

	if showRaw && showStats {
		fmt.Fprintln(os.Stderr, "--raw and --stats are mutually exclusive")
		return 0
	}

	if showRaw {
		ch := types.EncodeValue(value)
		buf := bytes.NewBuffer(ch.Data())
		_, err = io.Copy(os.Stdout, buf)
		d.CheckError(err)
		return 0
	}

	if showStats {
		types.WriteValueStats(os.Stdout, value, database)
		return 0
	}

	pgr := outputpager.Start()
	defer pgr.Stop()

	types.WriteEncodedValue(pgr.Writer, value)
	fmt.Fprintln(pgr.Writer)
	return 0
}
