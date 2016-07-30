// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"strings"

	flag "github.com/tsuru/gnuflag"
)

type nomsCommand struct {
	// Run runs the command.
	// The args are the arguments after the command name.
	Run func(args []string) int

	// UsageLine is the one-line usage message.
	// The first word in the line is taken to be the command name.
	UsageLine string

	// Short is the short description shown in the 'noms help' output.
	Short string

	// Long is the long message shown in the 'noms help <this-command>' output.
	Long string

	// Flag is a set of flags specific to this command.
	Flags func() *flag.FlagSet

	// Nargs is the minimum number of arguments expected after flags, specific to this command.
	Nargs int
}

// Name returns the command's name: the first word in the usage line.
func (nc *nomsCommand) Name() string {
	name := nc.UsageLine
	i := strings.Index(name, " ")
	if i >= 0 {
		name = name[:i]
	}
	return name
}

func countFlags(flags *flag.FlagSet) int {
	if flags == nil {
		return 0
	} else {
		n := 0
		flags.VisitAll(func(f *flag.Flag) {
			n++
		})
		return n
	}
}

func (nc *nomsCommand) Usage() {
	fmt.Fprintf(os.Stderr, "usage: %s\n\n", nc.UsageLine)
	fmt.Fprintf(os.Stderr, "%s\n", strings.TrimSpace(nc.Long))
	flags := nc.Flags()
	if countFlags(flags) > 0 {
		fmt.Fprintf(os.Stderr, "\noptions:\n")
		flags.PrintDefaults()
	}
	os.Exit(1)
}
