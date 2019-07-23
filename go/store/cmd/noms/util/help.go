// Copyright 2019 Liquidata, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This file incorporates work covered by the following copyright and
// permission notice:
//
// Copyright 2016 Attic Labs, Inc. All rights reserved.
// Licensed under the Apache License, version 2.0:
// http://www.apache.org/licenses/LICENSE-2.0

// This is the Help facility used by the noms utility. It is packaged in a separate util can be used by other programs as well.
package util

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"text/template"
)

var usageTemplate = `{{.UsageLine}}

Usage:

	{{.ProgName}} command [arguments]

The commands are:
{{range .Commands}}
	{{.Name | printf "%-11s"}} {{.Short}}{{end}}

Use "{{.ProgName}} help [command]" for more information about a command.

`

var helpTemplate = `usage: {{.ProgName}} {{.Cmd.UsageLine}}

{{.Cmd.Long | trim}}
`

var (
	commands  = []*Command{}
	usageLine = ""
	progName  = ""
)

func InitHelp(name string, cmds []*Command, usage string) {
	progName = name
	commands = cmds
	usageLine = usage
}

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
		ProgName  string
		Commands  []*Command
		UsageLine string
	}{
		progName,
		commands,
		usageLine,
	}
	tmpl(bw, usageTemplate, data)
	bw.Flush()
}

func Usage() {
	printUsage(os.Stderr)
	os.Exit(1)
}

// help implements the 'help' command.
func Help(args []string) {
	if len(args) == 0 {
		printUsage(os.Stdout)
		// not exit 2: succeeded at 'help'.
		return
	}
	if len(args) != 1 {
		fmt.Fprintf(os.Stderr, "usage: %s help command\n\nToo many arguments given.\n", progName)
		os.Exit(1) // failed at 'help'
	}

	arg := args[0]

	for _, cmd := range commands {
		if cmd.Name() == arg {
			data := struct {
				ProgName string
				Cmd      *Command
			}{
				progName,
				cmd,
			}
			tmpl(os.Stdout, helpTemplate, data)
			flags := cmd.Flags()
			if countFlags(flags) > 0 {
				fmt.Fprintf(os.Stdout, "\noptions:\n")
				flags.PrintDefaults()
			}
			// not exit 2: succeeded at 'help cmd'.
			return
		}
	}

	fmt.Fprintf(os.Stderr, "Unknown help topic %#q\n", arg)
	Usage() // failed at 'help cmd'
}
