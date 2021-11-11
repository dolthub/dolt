// Copyright 2021 Dolthub, Inc.
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

package commands

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type DumpZshCmd struct {
	DoltCommand cli.SubCommandHandler
}

func (d DumpZshCmd) Name() string {
	return "dump-zsh"
}

func (d DumpZshCmd) Description() string {
	return "Creates a zsh autocomp file for the current dolt commands"
}

func (d DumpZshCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(fileParamName, "", "file", "The file to write CLI docs to")
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	fileStr := apr.GetValueOrDefault(fileParamName, "cli.md")

	exists, _ := dEnv.FS.Exists(fileStr)
	if exists {
		cli.PrintErrln(fileStr + " exists")
		usage()
		return 1
	}

	wr, err := dEnv.FS.OpenForWrite(fileStr, os.ModePerm)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	// _dolt() {
	//    local line state
	//
	//    _arguments -C \
	//               "1: :->cmds" \
	//               "*::arg:->args"
	//
	//    case "$state" in
	//        cmds)
	//            _values "dolt command" \
	//                    "add[add a table to the staging area]" \
	//                    "table[table commands]"
	//            ;;
	//        args)
	//            case $line[1] in
	//                add)
	//                    _add_cmd
	//                    ;;
	//                table)
	//                    _table_cmd
	//                    ;;
	//            esac
	//            ;;
	//    esac
	// }

	_, err = wr.Write([]byte("#compdef _dolt dolt\n\n"))
	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump zsh.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	err = d.dumpZsh(wr, d.DoltCommand.Name(), d.DoltCommand.Subcommands)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump docs.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	return 0

}

const (
	subCmdFmt = `
_%s() {
    local line state

    _arguments -C \
               "1: :->cmds" \
               "*::arg:->args"
    case "$state" in
        cmds)
            _values "%s command" \
%s
            ;;
        args)
            case $line[1] in
%s
            esac
            ;;
    esac
}
`

	noSubFmt = `
_%s() {
    _arguments \
               
%s
}
`

	argumentFmt = `               '(--%s)--%s[%s]'`

	cmdValueFmt = `                    "%s[%s]""`

	argSwitchFmt = `                %s)
                    _%s
                    ;;
`
)

func (d DumpZshCmd) dumpZsh(wr io.Writer, cmdStr string, subCommands []cli.Command) error {

	var subCmds []string
	var subArgs []string
	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if hidden {
			continue
		}

		subCmds = append(subCmds, fmt.Sprintf(cmdValueFmt, curr.Name(), curr.Description()))
		subArgs = append(subArgs, fmt.Sprintf(argSwitchFmt, curr.Name(), curr.Name()))
	}

	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if hidden {
			continue
		}

		if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
			err := d.dumpZsh(wr, subCmdHandler.Name(), subCmdHandler.Subcommands)

			if err != nil {
				return err
			}
		} else {
			currCmdStr := fmt.Sprintf("%s %s", cmdStr, curr.Name())
			err := curr.CreateMarkdown(wr, currCmdStr)

			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (d DumpZshCmd) dumpZshLeaf(wr io.Writer, command cli.Command) error {
	//command.CreateMarkdown()
	return nil
}


func (d DumpZshCmd) CreateMarkdown(writer io.Writer, commandStr string) error {
	return nil
}

// Hidden should return true if this command should be hidden from the help text
func (d DumpZshCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (d DumpZshCmd) RequiresRepo() bool {
	return false
}
