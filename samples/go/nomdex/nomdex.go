// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"fmt"
	"os"
	"path"

	"github.com/attic-labs/noms/cmd/util"
	"github.com/attic-labs/noms/go/d"
	"github.com/attic-labs/noms/go/util/exit"
	flag "github.com/juju/gnuflag"
)

var commands = []*util.Command{
	update,
	find,
}

var usageLine = `Nomdex builds indexes to support fast data access.`

func main() {
	progName := path.Base(os.Args[0])
	util.InitHelp(progName, commands, usageLine)
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

func printError(err error, msgAndArgs ...interface{}) bool {
	if err != nil {
		err := d.Unwrap(err)
		switch len(msgAndArgs) {
		case 0:
			fmt.Fprintf(os.Stderr, "error: %s\n", err)
		case 1:
			fmt.Fprintf(os.Stderr, "%s%s\n", msgAndArgs[0], err)
		default:
			format, ok := msgAndArgs[0].(string)
			if ok {
				s1 := fmt.Sprintf(format, msgAndArgs[1:]...)
				fmt.Fprintf(os.Stderr, "%s%s\n", s1, err)
			} else {
				fmt.Fprintf(os.Stderr, "error: %s\n", err)
			}
		}
	}
	return err != nil
}
