// Copyright 2020 Liquidata, Inc.
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
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"os"
	"path/filepath"
	"strings"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

const (
	dirParamName = "dir"
)

type DumpDocsCmd struct {
	DoltCommand cli.SubCommandHandler
}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd *DumpDocsCmd) Name() string {
	return "dump-docs"
}

// Description returns a description of the command
func (cmd *DumpDocsCmd) Description() string {
	return "dumps all documentation in md format to a directory"
}

// Hidden should return true if this command should be hidden from the help text
func (cmd *DumpDocsCmd) Hidden() bool {
	return true
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd *DumpDocsCmd) RequiresRepo() bool {
	return false
}

// BuildCommandDocumentation creates a markdown file containing the helptext for the command at the given path
func (cmd *DumpDocsCmd) GetCommandDocumentation(commandStr string) cli.CommandDocumentation {
	return cli.CommandDocumentation{}
}

// Exec executes the command
func (cmd *DumpDocsCmd) Exec(_ context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(dirParamName, "", "dir", "The directory where the md files should be dumped")
	help, usage := cli.HelpAndUsagePrinters(cli.BuildCommandDocumentation(commandStr, cli.CommandDocumentationContent{}, ap))
	apr := cli.ParseArgs(ap, args, help)

	dirStr := apr.GetValueOrDefault(dirParamName, ".")

	exists, isDir := dEnv.FS.Exists(dirStr)

	if !exists {
		cli.PrintErrln(dirStr + " does not exist.")
		usage()
		return 1
	} else if !isDir {
		cli.PrintErrln(dirStr + " is a file, not a directory.")
		usage()
		return 1
	}

	err := cmd.dumpDocs(dEnv, dirStr, cmd.DoltCommand.Name(), cmd.DoltCommand.Subcommands)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump docs.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	return 0
}

func (cmd *DumpDocsCmd) dumpDocs(dEnv *env.DoltEnv, dirStr, cmdStr string, subCommands []cli.Command) error {
	var result []string

	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if !hidden {
			if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
				err := cmd.dumpDocs(dEnv, dirStr, cmdStr+" "+subCmdHandler.Name(), subCmdHandler.Subcommands)

				if err != nil {
					return err
				}
			} else {
				currCmdStr := fmt.Sprintf("%s %s", cmdStr, curr.Name())
				cmdDoc := curr.GetCommandDocumentation(currCmdStr)
				if cmdDoc.CommandStr != "" {
					currCmdMkd, err := cmdDoc.CmdDocToMd()
					if err != nil {
						return err
					}
					result = append(result, currCmdMkd)
				}

			}
		}
	}

	markdownDoc := strings.Join(result, "")
	absPath := filepath.Join(dirStr, "cli.md")
	wr, err := dEnv.FS.OpenForWrite(absPath, os.ModePerm)
	if err != nil {
		return err
	}
	return iohelp.WriteIfNoErr(wr, []byte(markdownDoc), err)
}
