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
	"errors"
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/diff"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

var ErrStashNotSupportedForOldFormat = errors.New("stash is not supported for old storage format")

var StashCommands = cli.NewSubCommandHandlerWithUnspecified("stash", "Stash the changes in a dirty working directory away.", false, StashCmd{}, []cli.Command{
	StashClearCmd{},
	StashDropCmd{},
	StashListCmd{},
	StashPopCmd{},
})

const (
	IncludeUntrackedFlag = "include-untracked"
)

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: "Stash the changes in a dirty working directory away.",
	LongDesc: `Use dolt stash when you want to record the current state of the working directory and the index, but want to go back to a clean working directory. 

The command saves your local modifications away and reverts the working directory to match the HEAD commit.
`,
	Synopsis: []string{
		"list",
		"pop {{.LessThan}}stash{{.GreaterThan}}",
		"clear",
		"drop {{.LessThan}}stash{{.GreaterThan}}",
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(IncludeUntrackedFlag, "u", "All untracked files (added tables) are also stashed.")
	return ap
}

// EventType returns the type of the event to log
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	if !dEnv.DoltDB.Format().UsesFlatbuffers() {
		cli.PrintErrln(ErrStashNotSupportedForOldFormat.Error())
		return 1
	}
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	if apr.NArg() > 0 {
		usage()
		return 1
	}

	err := stashChanges(ctx, dEnv, apr)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}

func stashChanges(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) error {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return err
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return err
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return err
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return err
	}

	roots, err := dEnv.Roots(ctx)
	if err != nil {
		return fmt.Errorf("couldn't get working root, cause: %s", err.Error())
	}

	if headHash.Equal(stagedHash) {
		if headHash.Equal(workingHash) {
			cli.Println("No local changes to save")
			return nil
		} else if has, err := hasNonUntrackedFilesWorkingSetChanges(ctx, roots); err != nil {
			return err
		} else if !apr.Contains(IncludeUntrackedFlag) && !has {
			// if the only changes in working set are untracked files, then no local changes to save
			cli.Println("No local changes to save")
			return nil
		}
	}

	roots, err = actions.StageModifiedAndDeletedTables(ctx, roots)
	if err != nil {
		return err
	}

	// all tables with changes that are going to be stashed are staged at this point

	allTblsToBeStashed, addedTbls, droppedTbls, err := stashedTableSets(ctx, roots)
	if err != nil {
		return err
	}

	// stage untracked files to include them in the stash,
	// but do not include them in added table set,
	// because they should not be staged when popped.
	if apr.Contains(IncludeUntrackedFlag) {
		allTblsToBeStashed, err = doltdb.UnionTableNames(ctx, roots.Staged, roots.Working)
		if err != nil {
			return err
		}

		roots, err = actions.StageTables(ctx, roots, allTblsToBeStashed)
		if err != nil {
			return err
		}
	}

	curHeadRef := dEnv.RepoStateReader().CWBHeadRef()
	curBranchName := curHeadRef.String()
	commitSpec, err := doltdb.NewCommitSpec(curBranchName)
	if err != nil {
		return err
	}
	commit, err := dEnv.DoltDB.Resolve(ctx, commitSpec, curHeadRef)
	if err != nil {
		return err
	}
	commitMeta, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return err
	}

	err = dEnv.DoltDB.AddStash(ctx, commit, roots.Staged, datas.NewStashMeta(curBranchName, commitMeta.Description, addedTbls, droppedTbls))
	if err != nil {
		return err
	}

	// setting STAGED to current HEAD RootValue resets staged set of changed, so
	// these changes are now in working set of changes, which needs to be checked out
	roots.Staged = roots.Head
	roots, err = actions.MoveTablesFromHeadToWorking(ctx, roots, allTblsToBeStashed)
	if err != nil {
		return err
	}

	err = dEnv.UpdateRoots(ctx, roots)
	if err != nil {
		return err
	}

	commitHash, err := commit.HashOf()
	if err != nil {
		return err
	}
	cli.Println(fmt.Sprintf("Saved working directory and index state WIP on %s: %s %s", curBranchName, commitHash.String(), commitMeta.Description))
	return nil
}

// hasNonUntrackedFilesWorkingSetChanges returns true if there exists a change in working set that is not added table.
// Untracked files are part of working set changes, but should not be stashed unless staged or --include-untracked flag is used.
func hasNonUntrackedFilesWorkingSetChanges(ctx context.Context, roots doltdb.Roots) (bool, error) {
	_, unstaged, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return false, err
	}

	for _, tableDelta := range unstaged {
		if !tableDelta.IsAdd() {
			return true, nil
		}
	}

	return false, nil
}

// stashedTableSets returns array of table names for all tables that are stashed, added tables and deleted tables.
// These table names are determined from all tables in the staged set of changes as they are being stashed only.
func stashedTableSets(ctx context.Context, roots doltdb.Roots) ([]string, []string, []string, error) {
	var addedTbls []string
	var droppedTbls []string
	var allTbls []string
	staged, _, err := diff.GetStagedUnstagedTableDeltas(ctx, roots)
	if err != nil {
		return nil, nil, nil, err
	}

	for _, tableDelta := range staged {
		tblName := tableDelta.ToName
		if tableDelta.IsAdd() {
			addedTbls = append(addedTbls, tableDelta.ToName)
		}
		if tableDelta.IsDrop() {
			tblName = tableDelta.FromName
			droppedTbls = append(droppedTbls, tableDelta.FromName)
		}
		allTbls = append(allTbls, tblName)
	}

	return allTbls, addedTbls, droppedTbls, nil
}
