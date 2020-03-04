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

package commands

import (
	"context"
	"fmt"
	"strings"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/store/hash"
)

const (
	numLinesParam = "number"
)

var logShortDesc = `Show commit logs`

var logLongDesc = `Shows the commit logs

The command takes options to control what is shown and how.`


var logSynopsis = []string{
		`[-n {{.LessThan}}num_commits{{.GreaterThan}}] [{{.LessThan}}commit{{.GreaterThan}}]`,
	}

var logDocumentation = cli.CommandDocumentation{
	ShortDesc: loginShortDesc,
	LongDesc: logLongDesc,
	Synopsis: logSynopsis,
}

type commitLoggerFunc func(*doltdb.CommitMeta, []hash.Hash, hash.Hash)

func logToStdOutFunc(cm *doltdb.CommitMeta, parentHashes []hash.Hash, ch hash.Hash) {
	cli.Println(color.YellowString("commit %s", ch.String()))

	if len(parentHashes) > 1 {
		printMerge(parentHashes)
	}

	printAuthor(cm)
	printDate(cm)
	printDesc(cm)
}

func printMerge(hashes []hash.Hash) {
	cli.Print("Merge:")
	for _, h := range hashes {
		cli.Print(" " + h.String())
	}
	cli.Println()
}

func printAuthor(cm *doltdb.CommitMeta) {
	cli.Printf("Author: %s <%s>\n", cm.Name, cm.Email)
}

func printDate(cm *doltdb.CommitMeta) {
	timeStr := cm.FormatTS()
	cli.Println("Date:  ", timeStr)
}

func printDesc(cm *doltdb.CommitMeta) {
	formattedDesc := "\n\t" + strings.Replace(cm.Description, "\n", "\n\t", -1) + "\n"
	cli.Println(formattedDesc)
}

type LogCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd LogCmd) Name() string {
	return "log"
}

// Description returns a description of the command
func (cmd LogCmd) Description() string {
	return "Show commit logs."
}

// EventType returns the type of the event to log
func (cmd LogCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_LOG
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd LogCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := createLogArgParser()
	return CreateMarkdown(fs, path, commandStr, logDocumentation, ap)
}

func createLogArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsInt(numLinesParam, "n", "num_commits", "Limit the number of commits to output")
	return ap
}

// Exec executes the command
func (cmd LogCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	return logWithLoggerFunc(ctx, commandStr, args, dEnv, logToStdOutFunc)
}

func logWithLoggerFunc(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, loggerFunc commitLoggerFunc) int {
	ap := createLogArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, logDocumentation, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() > 1 {
		usage()
		return 1
	}

	cs, err := parseCommitSpec(dEnv, apr)
	if err != nil {
		cli.PrintErr(err)
		return 1
	}

	numLines := apr.GetIntOrDefault(numLinesParam, -1)
	return logCommits(ctx, dEnv, cs, loggerFunc, numLines)
}

func parseCommitSpec(dEnv *env.DoltEnv, apr *argparser.ArgParseResults) (*doltdb.CommitSpec, error) {
	if apr.NArg() == 0 || apr.Arg(0) == "--" {
		return dEnv.RepoState.CWBHeadSpec(), nil
	}

	comSpecStr := apr.Arg(0)
	cs, err := doltdb.NewCommitSpec(comSpecStr, dEnv.RepoState.CWBHeadRef().String())

	if err != nil {
		return nil, fmt.Errorf("invalid commit %s\n", comSpecStr)
	}

	return cs, nil
}

func logCommits(ctx context.Context, dEnv *env.DoltEnv, cs *doltdb.CommitSpec, loggerFunc commitLoggerFunc, numLines int) int {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs)

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	h, err := commit.HashOf()

	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
		return 1
	}

	commits, err := commitwalk.GetTopNTopoOrderedCommits(ctx, dEnv.DoltDB, h, numLines)

	if err != nil {
		cli.PrintErrln("Error retrieving commit.")
		return 1
	}

	for _, comm := range commits {
		meta, err := comm.GetCommitMeta()

		if err != nil {
			cli.PrintErrln("error: failed to get commit metadata")
			return 1
		}

		pHashes, err := comm.ParentHashes(ctx)

		if err != nil {
			cli.PrintErrln("error: failed to get parent hashes")
			return 1
		}

		cmHash, err := comm.HashOf()

		if err != nil {
			cli.PrintErrln("error: failed to get commit hash")
			return 1
		}
		loggerFunc(meta, pHashes, cmHash)
	}

	return 0
}
