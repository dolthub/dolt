// Copyright 2022 Dolthub, Inc.
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
	"gopkg.in/src-d/go-errors.v1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var cherryPickDocs = cli.CommandDocumentationContent{
	ShortDesc: `Apply the changes introduced by an existing commit.`,
	LongDesc: `
Applies the changes from an existing commit and creates a new commit from the current HEAD. This requires your working tree to be clean (no modifications from the HEAD commit).

Cherry-picking merge commits or commits with schema changes or rename or drop tables is not currently supported. Row data changes are allowed as long as the two table schemas are exactly identical.

If applying the row data changes from the cherry-picked commit results in a data conflict, the cherry-pick operation is aborted and no changes are made to the working tree or committed.
`,
	Synopsis: []string{
		`{{.LessThan}}commit{{.GreaterThan}}`,
	},
}

// TODO: Would be nice to have a documentation URL to send people to, since resolving conflicts
//
//	and violations is a little bit advanced.
var ErrCherryPickConflictsOrViolations = errors.NewKind("error: Unable to apply commit cleanly due to conflicts " +
	"or constraint violations. Please resolve the conflicts and/or constraint violations, then use `dolt add` " +
	"to add the tables to the staged set, and `dolt commit` to commit the changes and finish cherry-picking. \n" +
	"To undo all changes from this cherry-pick operation, use `dolt cherry-pick --abort`.")

type CherryPickCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command.
func (cmd CherryPickCmd) Name() string {
	return "cherry-pick"
}

// Description returns a description of the command.
func (cmd CherryPickCmd) Description() string {
	return "Apply the changes introduced by an existing commit."
}

func (cmd CherryPickCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateCherryPickArgParser()
	return cli.NewCommandDocumentation(cherryPickDocs, ap)
}

func (cmd CherryPickCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCherryPickArgParser()
}

// EventType returns the type of the event to log.
func (cmd CherryPickCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CHERRY_PICK
}

// Exec executes the command.
func (cmd CherryPickCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateCherryPickArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cherryPickDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}
	// This command creates a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(dEnv) {
		return 1
	}

	if apr.Contains(cli.AbortParam) {
		ws, err := dEnv.WorkingSet(ctx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("fatal: unable to load working set: %v", err).Build(), nil)
		}

		if !ws.MergeActive() {
			return HandleVErrAndExitCode(errhand.BuildDError("error: There is no cherry-pick merge to abort").Build(), nil)
		}

		return HandleVErrAndExitCode(abortMerge(ctx, dEnv), usage)
	}

	// TODO : support single commit cherry-pick only for now
	if apr.NArg() == 0 {
		usage()
		return 1
	} else if apr.NArg() > 1 {
		return HandleVErrAndExitCode(errhand.BuildDError("cherry-picking multiple commits is not supported yet").SetPrintUsage().Build(), usage)
	}

	cherryStr := apr.Arg(0)
	if len(cherryStr) == 0 {
		verr := errhand.BuildDError("error: cannot cherry-pick empty string").Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	verr := cherryPick(ctx, dEnv, cliCtx, cherryStr)
	return HandleVErrAndExitCode(verr, usage)
}

// cherryPick returns error if any step of cherry-picking fails. It receives cherry-picked commit and performs cherry-picking and commits.
func cherryPick(ctx context.Context, dEnv *env.DoltEnv, cliCtx cli.CliContext, cherryStr string) errhand.VerboseError {
	// check for clean working state
	headRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingRoot, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedRoot, err := dEnv.StagedRoot(ctx)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	stagedHash, err := stagedRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if !headHash.Equal(stagedHash) {
		return errhand.BuildDError("Please commit your staged changes before using cherry-pick.").Build()
	}

	if !headHash.Equal(workingHash) {
		return errhand.BuildDError("error: your local changes would be overwritten by cherry-pick.\nhint: commit your changes (dolt commit -am \"<message>\") or reset them (dolt reset --hard) to proceed.").Build()
	}

	mergeResult, commitMsg, err := mergeCherryPickedCommit(ctx, dEnv, workingRoot, cherryStr)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}
	newWorkingRoot := mergeResult.Root
	mapOfMergeStats := mergeResult.Stats

	workingHash, err = newWorkingRoot.HashOf()
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	if headHash.Equal(workingHash) {
		cli.Println("No changes were made.")
		return nil
	}

	err = dEnv.UpdateWorkingRoot(ctx, newWorkingRoot)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	// Stage all tables that don't have merge conflicts or constraint violations
	for tableName, mergeStats := range mapOfMergeStats {
		if !mergeStats.HasArtifacts() {
			res := AddCmd{}.Exec(ctx, "add", []string{tableName}, dEnv, cliCtx)
			if res != 0 {
				return errhand.BuildDError("dolt add failed").AddCause(err).Build()
			}
		}
	}

	printSuccessStats(mapOfMergeStats)

	if mergeResult.HasMergeArtifacts() {
		return errhand.VerboseErrorFromError(ErrCherryPickConflictsOrViolations.New())
	} else {
		commitParams := []string{"-m", commitMsg}
		res := CommitCmd{}.Exec(ctx, "commit", commitParams, dEnv, cliCtx)
		if res != 0 {
			return errhand.BuildDError("dolt commit failed").AddCause(err).Build()
		}
	}

	return nil
}

// mergeCherryPickedCommit executes a merge to cherry-pick the specified ref specification from
// |cherryStr| and apply it to the specified |workingRoot| in this |dEnv|. The MergeResult is
// returned, along with the commit message for the specified |cherryStr|, and any error encountered.
func mergeCherryPickedCommit(ctx context.Context, dEnv *env.DoltEnv, workingRoot *doltdb.RootValue, cherryStr string) (*merge.Result, string, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, "", err
	}
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: tmpDir}

	cherrySpec, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	headRef, err := dEnv.RepoStateReader().CWBHeadRef()
	if err != nil {
		return nil, "", err
	}
	cherryCm, err := dEnv.DoltDB.Resolve(ctx, cherrySpec, headRef)
	if err != nil {
		return nil, "", err
	}

	if len(cherryCm.DatasParents()) > 1 {
		return nil, "", errhand.BuildDError("cherry-picking a merge commit is not supported.").Build()
	}
	if len(cherryCm.DatasParents()) == 0 {
		return nil, "", errhand.BuildDError("cherry-picking a commit without parents is not supported.").Build()
	}

	cherryCM, err := cherryCm.GetCommitMeta(ctx)
	if err != nil {
		return nil, "", err
	}
	commitMsg := cherryCM.Description

	cherryRoot, err := cherryCm.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}

	parentCm, err := dEnv.DoltDB.ResolveParent(ctx, cherryCm, 0)
	if err != nil {
		return nil, "", err
	}
	parentRoot, err := parentCm.GetRootValue(ctx)
	if err != nil {
		return nil, "", err
	}

	// use parent of cherry-pick as ancestor to merge
	mo := merge.MergeOpts{IsCherryPick: true}
	result, err := merge.MergeRoots(ctx, workingRoot, cherryRoot, parentRoot, cherryCm, parentCm, opts, mo)
	if err != nil {
		return nil, "", err
	}

	// If any of the merge stats show a data or schema conflict or a constraint
	// violation, record that a merge is in progress.
	for _, stats := range result.Stats {
		if stats.HasArtifacts() {
			ws, err := dEnv.WorkingSet(ctx)
			if err != nil {
				return nil, "", err
			}
			newWorkingSet := ws.StartMerge(cherryCm, cherryStr)
			err = dEnv.UpdateWorkingSet(ctx, newWorkingSet)
			if err != nil {
				return nil, "", err
			}

			break
		}
	}

	return result, commitMsg, nil
}
