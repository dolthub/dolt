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

package commands

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

var stashDocs = cli.CommandDocumentationContent{
	ShortDesc: `Stash the changes in a dirty working directory away.`,
	LongDesc: `

`,
	Synopsis: []string{
		`{{.LessThan}}stash{{.GreaterThan}}`,
	},
}

type StashCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd StashCmd) Name() string {
	return "stash"
}

// Description returns a description of the command.
func (cmd StashCmd) Description() string {
	return "Stash the changes in a dirty working directory away."
}

func (cmd StashCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateStashArgParser()
	return cli.NewCommandDocumentation(stashDocs, ap)
}

func (cmd StashCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateStashArgParser()
}

// EventType returns the type of the event to log.
func (cmd StashCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_STASH
}

// Exec executes the command.
func (cmd StashCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateStashArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, stashDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	// TODO: these needs to be subcommands NOT flag options...
	var err error
	switch {
	case apr.Contains(cli.ListFlag):
		err = listStashes(ctx, dEnv)
	case apr.Contains(cli.PopFlag):
		idx := 0
		val, has := apr.GetValue(cli.PopFlag)
		if has && val != "" {
			idx, err = strconv.Atoi(val)
			if err != nil {
				return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
			}
		}
		err = popStash(ctx, dEnv, idx)
	case apr.Contains(cli.ClearFlag):
		err = clearStashes(ctx, dEnv)
	default:
		err = stashChanges(ctx, dEnv)
	}
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return 0
}

func listStashes(ctx context.Context, dEnv *env.DoltEnv) error {
	stashes, err := dEnv.DoltDB.GetStashes(ctx)
	if err != nil {
		return err
	}

	for _, stash := range stashes {
		commitHash, err := stash.HeadCommit.HashOf()
		if err != nil {
			return err
		}
		ch := commitHash.String()
		s := fmt.Sprintf("%s: WIP on %s: %s %s", stash.Name, stash.BranchName, ch, stash.Description)
		cli.Println(s)
	}
	return nil
}

func popStash(ctx context.Context, dEnv *env.DoltEnv, idx int) error {
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return err
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return err
	}
	//stagedRoot, err := dEnv.StagedRoot(ctx)
	//if err != nil {
	//	return errhand.VerboseErrorFromError(err)
	//}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return err
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return err
	}
	//stagedHash, err := stagedRoot.HashOf()
	//if err != nil {
	//	return err
	//}

	if headHash.Equal(workingHash) {
		// TODO: could you update working set with the stash root?
	}

	mergedRoot, err := popStashAtIdx(ctx, dEnv, workingRoot, idx)
	if err != nil {
		return err
	}

	// TODO: to reset the working set, is setting it to head sufficient?
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(mergedRoot))
	if err != nil {
		return err
	}

	return nil
}

func popStashAtIdx(ctx context.Context, dEnv *env.DoltEnv, workingRoot *doltdb.RootValue, idx int) (*doltdb.RootValue, error) {
	stashRoot, headCommit, err := dEnv.DoltDB.GetStashAtIdx(ctx, idx)
	if err != nil {
		return nil, err
	}

	hch, err := headCommit.HashOf()
	if err != nil {
		return nil, err
	}
	headSpec, err := doltdb.NewCommitSpec(hch.String())
	if err != nil {
		return nil, err
	}
	parentCm, err := dEnv.DoltDB.Resolve(ctx, headSpec, dEnv.RepoStateReader().CWBHeadRef())
	if err != nil {
		return nil, err
	}
	parentRoot, err := parentCm.GetRootValue(ctx)
	if err != nil {
		return nil, err
	}

	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, err
	}

	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: tmpDir}
	mo := merge.MergeOpts{IsCherryPick: true}
	mergedRoot, mergeStats, err := merge.MergeRoots(ctx, workingRoot, stashRoot, parentRoot, stashRoot, parentCm, opts, mo)
	if err != nil {
		return nil, err
	}

	var tablesWithConflict []string
	for tbl, stats := range mergeStats {
		if stats.Conflicts > 0 {
			tablesWithConflict = append(tablesWithConflict, tbl)
		}
	}

	if len(tablesWithConflict) > 0 {
		tblNames := strings.Join(tablesWithConflict, "', '")
		return nil, fmt.Errorf("conflicts in table {'%s'}", tblNames)
	}

	err = dEnv.DoltDB.RemoveStashAtIdx(ctx, idx)
	if err != nil {
		return nil, err
	}

	return mergedRoot, nil
}

func clearStashes(ctx context.Context, dEnv *env.DoltEnv) error {
	return nil
}

func stashChanges(ctx context.Context, dEnv *env.DoltEnv) error {
	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
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

	if headHash.Equal(workingHash) && headHash.Equal(stagedHash) {
		return fmt.Errorf("No local changes to save")
	}

	curHeadRef := dEnv.RepoStateReader().CWBHeadRef()
	curBranch := curHeadRef.String()
	cms, err := doltdb.NewCommitSpec(curBranch)
	if err != nil {
		return err
	}
	commit, err := dEnv.DoltDB.Resolve(ctx, cms, curHeadRef)
	if err != nil {
		return err
	}
	cmm, err := commit.GetCommitMeta(ctx)
	if err != nil {
		return err
	}

	err = dEnv.DoltDB.AddStash(ctx, commit, workingRoot, datas.NewStashMeta(curBranch, cmm.Description))
	if err != nil {
		return err
	}

	// TODO: to reset the working set, is setting it to head sufficient?
	ws, err := dEnv.WorkingSet(ctx)
	if err != nil {
		return err
	}

	err = dEnv.UpdateWorkingSet(ctx, ws.WithWorkingRoot(headRoot))
	if err != nil {
		return err
	}

	return nil
}
