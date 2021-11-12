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
	"regexp"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

type DumpZshCmd struct {
	DoltCommand cli.SubCommandHandler
}

func (d DumpZshCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(fileParamName, "", "file", "The file to write zsh comp file to")
	ap.SupportsFlag("includeHidden", "", "Include hidden commands")
	return ap
}

func (d DumpZshCmd) Name() string {
	return "dump-zsh"
}

func (d DumpZshCmd) Description() string {
	return "Creates a zsh autocomp file for the current dolt commands"
}

func (d DumpZshCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := d.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	fileStr := apr.GetValueOrDefault(fileParamName, "_dolt")

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

	_, err = wr.Write([]byte("#compdef _dolt dolt\n"))
	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump zsh.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	err = d.dumpZsh(wr, d.DoltCommand.Name(), d.DoltCommand.Subcommands, apr.Contains("includeHidden"))

	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump zsh.").AddCause(err).Build()
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

	lineJoiner = " \\\n"

	leafCmdFmt = `
_%s() {
    _arguments -s \
%s
}
`

	noOptCmdFmt = `
_%s() {
}
`


	singleArgumentFmt = `               '(%s)%s[%s]'`

	singleArgumentFmtNoHelp = `               '(%s)%s'`

	multiArgumentFmt = `               {%s}'[%s]'`

	multiArgumentFmtNoHelp = `               {%s}'%s'`

	cmdValueFmt = `                    "%s[%s]"`

	argSwitchFmt = `                %s)
                    _%s
                    ;;
`
)

func (d DumpZshCmd) dumpZsh(wr io.Writer, cmdStr string, subCommands []cli.Command, includeHidden bool) error {

	var subCmds []string
	var subArgs []string

	for _, sub := range subCommands {
		var hidden bool
		if hidCmd, ok := sub.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if hidden && !includeHidden {
			continue
		}

		subCmds = append(subCmds, fmt.Sprintf(cmdValueFmt, sub.Name(), sub.Description()))
		subArgs = append(subArgs, fmt.Sprintf(argSwitchFmt, sub.Name(), fmt.Sprintf("%s_%s", cmdStr, sub.Name())))

		subCmdStr := fmt.Sprintf("%s_%s", cmdStr, sub.Name())

		if subCmdHandler, ok := sub.(cli.SubCommandHandler); ok {
			err := d.dumpZsh(wr, subCmdStr, subCmdHandler.Subcommands, includeHidden)
			if err != nil {
				return err
			}
		} else {
			err := d.dumpZshLeaf(wr, subCmdStr, sub)
			if err != nil {
				return err
			}
		}
	}

	functionStr := fmt.Sprintf(subCmdFmt, cmdStr, cmdStr, strings.Join(subCmds, lineJoiner), strings.Join(subArgs, ""))

	_, err := wr.Write([]byte(functionStr))
	return err
}

func (d DumpZshCmd) dumpZshLeaf(wr io.Writer, cmdString string, command cli.Command) error {
	ap := command.ArgParser()
	var args []string
	if len(ap.Supported) > 0 || len(ap.ArgListHelp) > 0 {
		// TODO: args that aren't flags
		// for _, kvTuple := range cmdDoc.ArgParser.ArgListHelp {
		//
		// }
		for _, opt := range ap.Supported {
			args = append(args, formatOption(opt))
		}

		_, err := wr.Write([]byte(fmt.Sprintf(leafCmdFmt, cmdString, strings.Join(args, lineJoiner))))
		return err
	}

	_, err := wr.Write([]byte(fmt.Sprintf(noOptCmdFmt, cmdString)))
	return err
}

var markdownRegex = regexp.MustCompile(`\{\{[\.a-zA-Z]+\}\}`)

func formatOption(opt *argparser.Option) string {
	var formatString string

	both := false
	// TODO: valdesc?
	if opt.Abbrev == "" && opt.Name != "" {
		formatString = fmt.Sprintf("--%s", opt.Name)
	} else if opt.Abbrev != "" && opt.Name == "" {
		formatString = fmt.Sprintf("-%s", opt.Abbrev)
	} else if opt.Abbrev != "" && opt.Name != "" {
		both = true
		formatString = fmt.Sprintf("-%s,--%s", opt.Abbrev, opt.Name)
	} else {
		panic("short and long name both empty")
	}

	if len(opt.Desc) > 0 {
		desc := opt.Desc

		// Various sanitation steps
		desc = strings.ReplaceAll(desc, "'", "''")
		desc = string(markdownRegex.ReplaceAll([]byte(desc), []byte("")))

		if strings.Contains(desc, "\n") {
			// Truncate any multi-line help text
			desc = desc[:strings.Index(desc, "\n")]
		}
		if both {
			return fmt.Sprintf(multiArgumentFmt, formatString, desc)
		} else {
			return fmt.Sprintf(singleArgumentFmt, formatString, formatString, desc)
		}
	} else {
		if both {
			return fmt.Sprintf(multiArgumentFmtNoHelp, formatString, formatString)
		} else {
			return fmt.Sprintf(singleArgumentFmtNoHelp, formatString, formatString)
		}
	}
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
