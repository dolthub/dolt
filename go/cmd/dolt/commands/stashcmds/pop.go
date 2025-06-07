// Copyright 2023 Dolthub, Inc.
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

package stashcmds

import (
	"context"
	"strconv"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var stashPopDocs = cli.CommandDocumentationContent{
	ShortDesc: "Remove a single stash from the stash list and apply it on top of the current working set.",
	LongDesc: `Apply a single stash at given index and drop that stash entry from the stash list (e.g. 'dolt stash pop stash@{1}' will apply and drop the stash entry at index 1 in the stash list).

Applying the stash entry can fail with conflicts; in this case, the stash entry is not removed from the stash list. You need to resolve the conflicts by hand and call dolt stash drop manually afterwards.
`,
	Synopsis: []string{
		"{{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashPopCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashPopCmd) Name() string {
	return "pop"
}

// Description returns a description of the command
func (cmd StashPopCmd) Description() string {
	return "Remove a single stash from the stash list and apply it on top of the current working set."
}

func (cmd StashPopCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashPopDocs, ap)
}

func (cmd StashPopCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

// EventType returns the type of the event to log
func (cmd StashPopCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH_POP
}

// Exec executes the command
func (cmd StashPopCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	if !dEnv.DoltDB(ctx).Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashPopDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	_, sqlCtx, closer, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}
	defer closer()

	var idx = 0
	if apr.NArg() == 1 {
		stashName := apr.Args[0]
		stashName = strings.TrimSuffix(strings.TrimPrefix(stashName, "stash@{"), "}")
		idx, err = strconv.Atoi(stashName)
		if err != nil {
			cli.Printf("error: %s is not a valid reference", stashName)
			return 1
		}
	}

	workingRoot, err := dEnv.WorkingRoot(sqlCtx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	success, err := applyStashAtIdx(sqlCtx, dEnv, workingRoot, idx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	ret := commands.StatusCmd{}.Exec(sqlCtx, "status", []string{}, dEnv, cliCtx)
	if ret != 0 || !success {
		cli.Println("The stash entry is kept in case you need it again.")
		return 1
	}

	cli.Println()
	err = dropStashAtIdx(sqlCtx, dEnv, idx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	return 0
}

func applyStashAtIdx(ctx *sql.Context, dEnv *env.DoltEnv, curWorkingRoot doltdb.RootValue, idx int) (bool, error) {
	stashRoot, headCommit, meta, err := dEnv.DoltDB(ctx).GetStashRootAndHeadCommitAtIdx(ctx, idx, DoltCliRef)
	if err != nil {
		return false, err
	}

	hch, err := headCommit.HashOf()
	if err != nil {
		return false, err
	}
	headCommitSpec, err := doltdb.NewCommitSpec(hch.String())
	if err != nil {
		return false, err
	}
	headRef, err := dEnv.RepoStateReader().CWBHeadRef(ctx)
	if err != nil {
		return false, err
	}
	optCmt, err := dEnv.DoltDB(ctx).Resolve(ctx, headCommitSpec, headRef)
	if err != nil {
		return false, err
	}
	parentCommit, ok := optCmt.ToCommit()
	if !ok {
		// Should not be possible to get into this situation. The parent of the stashed commit
		// Must have been present at the time it was created
		return false, doltdb.ErrGhostCommitEncountered
	}

	parentRoot, err := parentCommit.GetRootValue(ctx)
	if err != nil {
		return false, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return false, err
	}

	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(ctx), Tempdir: tmpDir}
	result, err := merge.MergeRoots(ctx, curWorkingRoot, stashRoot, parentRoot, stashRoot, parentCommit, opts, merge.MergeOpts{IsCherryPick: false})
	if err != nil {
		return false, err
	}

	var tablesWithConflict []doltdb.TableName
	for tbl, stats := range result.Stats {
		if stats.HasConflicts() {
			tablesWithConflict = append(tablesWithConflict, tbl)
		}
	}

	if len(tablesWithConflict) > 0 {
		tblNames := strings.Join(doltdb.FlattenTableNames(tablesWithConflict), "', '")
		cli.Printf("error: Your local changes to the following tables would be overwritten by applying stash %d:\n"+
			"\t{'%s'}\n"+
			"Please commit your changes or stash them before you merge.\nAborting\n", idx, tblNames)
		return false, nil
	}

	err = dEnv.UpdateWorkingRoot(ctx, result.Root)
	if err != nil {
		return false, err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return false, err
	}

	// added tables need to be staged
	// since these tables are coming from a stash, don't filter for ignored table names.
	roots, err = actions.StageTables(ctx, roots, doltdb.ToTableNames(meta.TablesToStage, doltdb.DefaultSchemaName), false)
	if err != nil {
		return false, err
	}

	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		return false, err
	}

	return true, nil
}

func handleStashPopErr(usage cli.UsagePrinter, err error) int {
	cli.Println("The stash entry is kept in case you need it again.")
	return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}
