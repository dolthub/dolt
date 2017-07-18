// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/util/exit"
	flag "github.com/juju/gnuflag"
)

var commands = []*util.Command{
	nomsCommit,
	nomsConfig,
	nomsDiff,
	nomsDs,
	nomsLog,
	nomsMerge,
	nomsRoot,
	nomsServe,
	nomsShow,
	nomsSync,
	nomsVersion,
}

var actions = []string{
	"interacting with",
	"poking at",
	"goofing with",
	"dancing with",
	"playing with",
	"contemplation of",
	"showing off",
	"jiggerypokery of",
	"singing to",
	"nomming on",
}

func usageString() string {
	i := rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(actions))
	return fmt.Sprintf(`Noms is a tool for %s Noms data.`, actions[i])
}

func main() {
	util.InitHelp(path.Base(os.Args[0]), commands, usageString())

	flag.Usage = util.Usage
	flag.Parse(false)

	args := flag.Args()
	if len(args) < 1 {
		util.Usage()
		return
	}

	if args[0] == "help" {
		util.Help(args[1:])
		return
	}

	// Don't prefix log messages with timestamp when running interactively
	log.SetFlags(0)

	for _, cmd := range commands {
		if cmd.Name() == args[0] {
			flags := cmd.Flags()
			flags.Usage = cmd.Usage

			flags.Parse(true, args[1:])
			args = flags.Args()
			if cmd.Nargs != 0 && len(args) < cmd.Nargs {
				cmd.Usage()
			}
			exitCode := cmd.Run(args)
			if exitCode != 0 {
				exit.Exit(exitCode)
			}
			return
		}
	}

	fmt.Fprintf(os.Stderr, "noms: unknown command %q\n", args[0])
	util.Usage()
}
