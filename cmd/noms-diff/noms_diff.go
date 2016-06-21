// Copyright 2016 The Noms Authors. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/attic-labs/noms/cmd/noms-diff/diff"
	"github.com/attic-labs/noms/go/spec"
	"github.com/attic-labs/noms/go/util/outputpager"
	"github.com/attic-labs/noms/samples/go/util"
)

const (
	addPrefix = "+   "
	subPrefix = "-   "
)

var (
	showHelp = flag.Bool("help", false, "show help text")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "Shows the difference between two objects\n")
		fmt.Fprintln(os.Stderr, "Usage: noms diff <object1> <object2>\n")
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "\nSee \"Spelling Objects\" at https://github.com/attic-labs/noms/blob/master/doc/spelling.md for details on the object argument.\n\n")
	}

	flag.Parse()
	if *showHelp {
		flag.Usage()
		return
	}

	if len(flag.Args()) != 2 {
		util.CheckErrorNoUsage(errors.New("Expected exactly two arguments"))
	}

	db1, value1, err := spec.GetPath(flag.Arg(0))
	util.CheckErrorNoUsage(err)
	if value1 == nil {
		util.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", flag.Arg(0)))
	}
	defer db1.Close()

	db2, value2, err := spec.GetPath(flag.Arg(1))
	util.CheckErrorNoUsage(err)
	if value2 == nil {
		util.CheckErrorNoUsage(fmt.Errorf("Object not found: %s", flag.Arg(1)))
	}
	defer db2.Close()

	waitChan := outputpager.PageOutput(!*outputpager.NoPager)

	diff.Diff(os.Stdout, value1, value2)
	fmt.Fprintf(os.Stdout, "\n")

	if waitChan != nil {
		os.Stdout.Close()
		<-waitChan
	}
}
