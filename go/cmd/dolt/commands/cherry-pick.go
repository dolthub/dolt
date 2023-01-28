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
	"errors"
	"fmt"
	"strings"

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
func (cmd CherryPickCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
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

	verr := cherryPick(ctx, dEnv, cherryStr)
	return HandleVErrAndExitCode(verr, usage)
}

// cherryPick returns error if any step of cherry-picking fails. It receives cherry-picked commit and performs cherry-picking and commits.
func cherryPick(ctx context.Context, dEnv *env.DoltEnv, cherryStr string) errhand.VerboseError {
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

	newWorkingRoot, commitMsg, err := getCherryPickedRootValue(ctx, dEnv, workingRoot, cherryStr)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

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
	res := AddCmd{}.Exec(ctx, "add", []string{"-A"}, dEnv)
	if res != 0 {
		return errhand.BuildDError("dolt add failed").AddCause(err).Build()
	}

	commitParams := []string{"-m", commitMsg}
	res = CommitCmd{}.Exec(ctx, "commit", commitParams, dEnv)
	if res != 0 {
		return errhand.BuildDError("dolt commit failed").AddCause(err).Build()
	}

	return nil
}

// getCherryPickedRootValue returns updated RootValue for current HEAD after cherry-pick commit is merged successfully and
// commit message of cherry-picked commit.
func getCherryPickedRootValue(ctx context.Context, dEnv *env.DoltEnv, workingRoot *doltdb.RootValue, cherryStr string) (*doltdb.RootValue, string, error) {
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return nil, "", err
	}
	opts := editor.Options{Deaf: dEnv.BulkDbEaFactory(), Tempdir: tmpDir}

	cherrySpec, err := doltdb.NewCommitSpec(cherryStr)
	if err != nil {
		return nil, "", err
	}
	cherryCm, err := dEnv.DoltDB.Resolve(ctx, cherrySpec, dEnv.RepoStateReader().CWBHeadRef())
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
	mergedRoot, mergeStats, err := merge.MergeRoots(ctx, workingRoot, cherryRoot, parentRoot, cherryCm, parentCm, opts, mo)
	if err != nil {
		return nil, "", err
	}

	var tablesWithConflict []string
	for tbl, stats := range mergeStats {
		if stats.Conflicts > 0 {
			tablesWithConflict = append(tablesWithConflict, tbl)
		}
	}

	if len(tablesWithConflict) > 0 {
		tblNames := strings.Join(tablesWithConflict, "', '")
		return nil, "", errors.New(fmt.Sprintf("conflicts in table {'%s'}", tblNames))
	}

	return mergedRoot, commitMsg, nil
}
