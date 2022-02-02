// Copyright 2020 Dolthub, Inc.
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

const (
	fileParamName  = "file"
	cliMdDocHeader = "" +
		"---\n" +
		"title: CLI\n" +
		"---\n\n" +
		"# CLI\n\n"
)

type DumpDocsCmd struct {
	DoltCommand cli.SubCommandHandler
}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd *DumpDocsCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	return nil
}

func (cmd *DumpDocsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsString(fileParamName, "", "file", "The file to write CLI docs to")
	return ap
}

// Exec executes the command
func (cmd *DumpDocsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()

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

	_, err = wr.Write([]byte(cliMdDocHeader))
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	err = cmd.dumpDocs(wr, cmd.DoltCommand.Name(), cmd.DoltCommand.Subcommands)

	if err != nil {
		verr := errhand.BuildDError("error: Failed to dump docs.").AddCause(err).Build()
		cli.PrintErrln(verr.Verbose())

		return 1
	}

	return 0
}

func (cmd *DumpDocsCmd) dumpDocs(wr io.Writer, cmdStr string, subCommands []cli.Command) error {
	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if !hidden {
			if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
				err := cmd.dumpDocs(wr, cmdStr+" "+subCmdHandler.Name(), subCmdHandler.Subcommands)

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
	}

	return nil
}

func CreateMarkdown(wr io.Writer, cmdDoc cli.CommandDocumentation) error {
	markdownDoc, err := cmdDoc.CmdDocToMd()
	if err != nil {
		return err
	}
	_, err = wr.Write([]byte(markdownDoc))
	return err
}
