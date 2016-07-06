// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/types"
	"github.com/attic-labs/noms/go/util/outputpager"
)

var nomsShow = &nomsCommand{
	Run:       runShow,
	UsageLine: "show <object>",
	Short:     "Shows a serialization of a Noms object",
	Long:      "See Spelling Objects at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.",
	Flags:     setupShowFlags,
	Nargs:     1,
}

func setupShowFlags() *flag.FlagSet {
	showFlagSet := flag.NewFlagSet("show", flag.ExitOnError)
	outputpager.RegisterOutputpagerFlags(showFlagSet)
	return showFlagSet
}

func runShow(args []string) int {
	database, value, err := spec.GetPath(args[0])
	d.CheckErrorNoUsage(err)

	if value == nil {
		fmt.Fprintf(os.Stderr, "Object not found: %s\n", args[0])
		return 0
	}

	waitChan := outputpager.PageOutput(!outputpager.NoPager)

	w := bufio.NewWriter(os.Stdout)
	types.WriteEncodedValueWithTags(w, value)
	fmt.Fprintf(w, "\n")
	w.Flush()
	database.Close()

	if waitChan != nil {
		os.Stdout.Close()
		<-waitChan
	}
	return 0
}
