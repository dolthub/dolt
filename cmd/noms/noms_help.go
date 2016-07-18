// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

package main

import (
	"bufio"
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"text/template"
	"time"
)

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

var usageTemplate = `Noms is a tool for {{.Action}} Noms data.

Usage:

	noms command [arguments]

The commands are:
{{range .Commands}}
	{{.Name | printf "%-11s"}} {{.Short}}{{end}}

Use "noms help [command]" for more information about a command.

`

var helpTemplate = `usage: noms {{.UsageLine}}

{{.Long | trim}}
`

// tmpl executes the given template text on data, writing the result to w.
func tmpl(w io.Writer, text string, data interface{}) {
	t := template.New("top")
	t.Funcs(template.FuncMap{"trim": strings.TrimSpace})
	template.Must(t.Parse(text))
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
}

func printUsage(w io.Writer) {
	bw := bufio.NewWriter(w)
	data := struct {
		Commands []*nomsCommand
		Action   string
	}{
		commands,
		actions[rand.New(rand.NewSource(time.Now().UnixNano())).Intn(len(actions))],
	}
	tmpl(bw, usageTemplate, data)
	bw.Flush()
}

func usage() {
	printUsage(os.Stderr)
	os.Exit(1)
}

// help implements the 'help' command.
func help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'noms help'.
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: noms help command\n\nToo many arguments given.\n")
		os.Exit(1) // failed at 'noms help'
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			tmpl(os.Stdout, helpTemplate, cmd)
			flags := cmd.Flags()
			if countFlags(flags) > 0 {
				fmt.Fprintf(os.Stdout, "\noptions:\n")
				flags.PrintDefaults()
			}
			// not exit 2: succeeded at 'noms help cmd'.
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q\n", arg)
	usage() // failed at 'noms help cmd'
}
