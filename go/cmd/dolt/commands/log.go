// Copyright 2019 Dolthub, Inc.
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
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/commitwalk"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/hash"
)

const (
	numLinesParam = "number"
)

var logDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show commit logs`,
	LongDesc: `Shows the commit logs

The command takes options to control what is shown and how.`,
	Synopsis: []string{
		`[-n {{.LessThan}}num_commits{{.GreaterThan}}] [{{.LessThan}}commit{{.GreaterThan}}] [[--] {{.LessThan}}table{{.GreaterThan}}]`,
	},
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
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, logDocs, ap))
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
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, logDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 2 {
		usage()
		return 1
	}

	numLines := apr.GetIntOrDefault(numLinesParam, -1)

	// Just dolt log
	if apr.NArg() == 0 {
		return logCommits(ctx, dEnv, dEnv.RepoState.CWBHeadSpec(), loggerFunc, numLines)
	} else if apr.NArg() == 1 { // dolt log <ref/table>
		argIsRef := actions.ValidateIsRef(ctx, apr.Arg(0), dEnv.DoltDB, dEnv.RepoStateReader())

		if argIsRef {
			cs, err := doltdb.NewCommitSpec(apr.Arg(0))
			if err != nil {
				cli.PrintErrln(color.HiRedString("invalid commit %s\n", apr.Arg(0)))
			}
			return logCommits(ctx, dEnv, cs, loggerFunc, numLines)
		} else {
			return logTableCommits(ctx, dEnv, loggerFunc, dEnv.RepoState.CWBHeadSpec(), apr.Arg(0), numLines)
		}
	} else { // dolt log ref table
		cs, err := doltdb.NewCommitSpec(apr.Arg(0))
		if err != nil {
			cli.PrintErrln(color.HiRedString("invalid commit %s\n", apr.Arg(0)))
		}
		return logTableCommits(ctx, dEnv, loggerFunc, cs, apr.Arg(1), numLines)
	}
}

func logCommits(ctx context.Context, dEnv *env.DoltEnv, cs *doltdb.CommitSpec, loggerFunc commitLoggerFunc, numLines int) int {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())

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

func tableExists(ctx context.Context, commit *doltdb.Commit, tableName string) (bool, error) {
	rv, err := commit.GetRootValue()
	if err != nil {
		return false, err
	}

	_, ok, err := rv.GetTable(ctx, tableName)
	if err != nil {
		return false, err
	}

	return ok, nil
}

func logTableCommits(ctx context.Context, dEnv *env.DoltEnv, loggerFunc commitLoggerFunc, cs *doltdb.CommitSpec, tableName string, numLines int) int {
	commit, err := dEnv.DoltDB.Resolve(ctx, cs, dEnv.RepoState.CWBHeadRef())
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: cannot get HEAD commit for current branch."))
		return 1
	}

	// Check that the table exists in the head commit
	exists, err := tableExists(ctx, commit, tableName)
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit info"))
		return 1
	}

	if !exists {
		cli.PrintErrln(color.HiRedString("error: table %s does not exist", tableName))
		return 1
	}

	h, err := commit.HashOf()
	if err != nil {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit hash"))
		return 1
	}

	itr, err := commitwalk.GetTopologicalOrderIterator(ctx, dEnv.DoltDB, h)
	if err != nil && err != io.EOF {
		cli.PrintErrln(color.HiRedString("Fatal error: failed to get commit history"))
		return 1
	}

	var prevCommit *doltdb.Commit = nil
	var prevHash hash.Hash
	for {
		// If we reached the limit then break
		if numLines == 0 {
			break
		}

		h, c, err := itr.Next(ctx)
		if err == io.EOF {
			break
		} else if err != nil {
			cli.PrintErrln("Fatal Error: failed to get commit history")
			return 1
		}

		if prevCommit == nil {
			prevCommit = c
			prevHash = h
			continue
		}

		parentRV, err := c.GetRootValue()
		if err != nil {
			cli.PrintErrln("error: failed to get parent root value")
			return 1
		}

		childRV, err := prevCommit.GetRootValue()
		if err != nil {
			cli.PrintErrln("error: failed to get child root value")
			return 1
		}

		ok, err := didTableChangeBetweenRootValues(ctx, childRV, parentRV, tableName)
		if err != nil {
			return 1 // helper handles logic
		}

		if ok {
			meta, err := prevCommit.GetCommitMeta()
			if err != nil {
				cli.PrintErrln("error: failed to get commit metadata")
				return 1
			}

			ph, err := prevCommit.ParentHashes(ctx)
			if err != nil {
				cli.PrintErrln("error: failed to get parent hashes")
				return 1
			}

			loggerFunc(meta, ph, prevHash)
			numLines--
		}

		prevCommit = c
		prevHash = h
	}

	return 0
}

func didTableChangeBetweenRootValues(ctx context.Context, child, parent *doltdb.RootValue, tableName string) (bool, error) {
	childHash, ok, err := child.GetTableHash(ctx, tableName)

	if !ok {
		return false, nil
	}
	if err != nil {
		cli.PrintErrln(color.HiRedString("error: failed to retrieve table %s from root value", tableName))
	}

	parentHash, ok, err := parent.GetTableHash(ctx, tableName)

	// If the table didn't exist in the parent then we know there was a change
	if !ok {
		return true, nil
	}

	if err != nil {
		cli.PrintErrln(color.HiRedString("error: failed to retrieve table %s from parent root value", tableName))
	}

	return childHash != parentHash, nil
}
