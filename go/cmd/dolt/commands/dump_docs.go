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
	"sort"

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
		"# Command Line Interface Reference\n\n"
)

type DumpDocsCmd struct {
	DoltCommand      cli.SubCommandHandler
	GlobalDocs       *cli.CommandDocumentation
	GlobalSpecialMsg string
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

func (cmd *DumpDocsCmd) Docs() *cli.CommandDocumentation {
	return nil
}

func (cmd *DumpDocsCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 0)
	ap.SupportsString(fileParamName, "", "file", "The file to write CLI docs to")
	return ap
}

// Exec executes the command
func (cmd *DumpDocsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cli.CommandDocumentationContent{}, ap))
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

	doltUsage := cmd.DoltCommand.GetUsage("dolt")
	doltUsageMarkdown := fmt.Sprintf("```\n$ dolt\n%s```\n\n", doltUsage)
	_, err = wr.Write([]byte(doltUsageMarkdown))
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	err = cmd.writeGlobalArgumentsSection(wr)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}

	verr := cmd.dumpDocs(wr, cmd.DoltCommand.Name(), cmd.DoltCommand.Subcommands)

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func (cmd *DumpDocsCmd) dumpDocs(wr io.Writer, cmdStr string, subCommands []cli.Command) errhand.VerboseError {
	sort.Slice(subCommands, func(i, j int) bool {
		return subCommands[i].Name() < subCommands[j].Name()
	})

	for _, curr := range subCommands {
		var hidden bool
		if hidCmd, ok := curr.(cli.HiddenCommand); ok {
			hidden = hidCmd.Hidden()
		}

		if !hidden {
			if subCmdHandler, ok := curr.(cli.SubCommandHandler); ok {
				var verr errhand.VerboseError
				if subCmdHandler.Unspecified != nil {
					verr = cmd.dumpDocs(wr, cmdStr, []cli.Command{subCmdHandler.Unspecified})
					if verr != nil {
						return verr
					}
				}
				verr = cmd.dumpDocs(wr, cmdStr+" "+subCmdHandler.Name(), subCmdHandler.Subcommands)
				if verr != nil {
					return verr
				}
			} else {
				docs := curr.Docs()

				if docs != nil {
					docs.CommandStr = fmt.Sprintf("%s %s", cmdStr, curr.Name())
					err := CreateMarkdown(wr, docs)
					if err != nil {
						return errhand.BuildDError("error: Failed to create markdown for command: %s %s.", cmdStr, curr.Name()).AddCause(err).Build()
					}
				}
			}
		}
	}

	return nil
}

func CreateMarkdown(wr io.Writer, cmdDoc *cli.CommandDocumentation) error {
	markdownDoc, err := cmdDoc.CmdDocToMd()
	if err != nil {
		return err
	}
	_, err = wr.Write([]byte(markdownDoc))
	return err
}

func (cmd *DumpDocsCmd) writeGlobalArgumentsSection(wr io.Writer) error {
	cmd.GlobalDocs.ShortDesc = cmd.GlobalSpecialMsg
	markdownDoc, err := cmd.GlobalDocs.GlobalCmdDocToMd()
	if err != nil {
		return err
	}

	_, err = wr.Write([]byte(markdownDoc))
	return err
}
