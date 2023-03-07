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
	LongDesc: `Applying the state can fail with conflicts; in this case, it is not removed from the stash list. 

You need to resolve the conflicts by hand and call dolt stash drop manually afterwards.
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
	ap := argparser.NewArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd StashPopCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH_POP
}

// Exec executes the command
func (cmd StashPopCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if !dEnv.DoltDB.Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashPopDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	if apr.NArg() > 1 {
		usage()
		return 1
	}

	var idx = 0
	var err error
	if apr.NArg() == 1 {
		stashName := apr.Args[0]
		stashName = strings.TrimSuffix(strings.TrimPrefix(stashName, "stash@{"), "}")
		idx, err = strconv.Atoi(stashName)
		if err != nil {
			cli.Printf("error: %s is not a valid reference", stashName)
			return 1
		}
	}

	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	success, err := applyStashAtIdx(ctx, dEnv, workingRoot, idx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	ret := commands.StatusCmd{}.Exec(ctx, "status", []string{}, dEnv)
	if ret != 0 || !success {
		cli.Println("The stash entry is kept in case you need it again.")
		return 1
	}

	cli.Println()
	err = dropStashAtIdx(ctx, dEnv, idx)
	if err != nil {
		return handleStashPopErr(usage, err)
	}

	return 0
}

func applyStashAtIdx(ctx context.Context, dEnv *env.DoltEnv, curWorkingRoot *doltdb.RootValue, idx int) (bool, error) {
	stashRoot, headCommit, meta, err := dEnv.DoltDB.GetStashRootAndHeadCommitAtIdx(ctx, idx)
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
	parentCommit, err := dEnv.DoltDB.Resolve(ctx, headCommitSpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return false, err
	}
	parentRoot, err := parentCommit.GetRootValue(ctx)
	if err != nil {
		return false, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return false, err
	}

	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: tmpDir}
	mergedRoot, mergeStats, err := merge.MergeRoots(ctx, curWorkingRoot, stashRoot, parentRoot, stashRoot, parentCommit, opts, merge.MergeOpts{IsCherryPick: false})
	if err != nil {
		return false, err
	}

	var tablesWithConflict []string
	for tbl, stats := range mergeStats {
		if stats.Conflicts > 0 {
			tablesWithConflict = append(tablesWithConflict, tbl)
		}
	}

	if len(tablesWithConflict) > 0 {
		tblNames := strings.Join(tablesWithConflict, "', '")
		cli.Printf("error: Your local changes to the following tables would be overwritten by applying stash %d:\n"+
			"\t{'%s'}\n"+
			"Please commit your changes or stash them before you merge.\nAborting\n", idx, tblNames)
		return false, nil
	}

	err = dEnv.UpdateWorkingRoot(ctx, mergedRoot)
	if err != nil {
		return false, err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return false, err
	}

	// added tables need to be staged
	roots, err = actions.StageTables(ctx, roots, meta.TablesToStage)
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
