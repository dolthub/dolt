// Copyright 2021 Dolthub, Inc.
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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var revertDocs = cli.CommandDocumentationContent{
	ShortDesc: "Undo the changes introduced in a commit",
	LongDesc: "Removes the changes made in a commit (or series of commits) from the working set, and then automatically " +
		"commits the result. This is done by way of a three-way merge. Given a specific commit " +
		"(e.g. {{.EmphasisLeft}}HEAD~1{{.EmphasisRight}}), this is similar to applying the patch from " +
		"{{.EmphasisLeft}}HEAD~1..HEAD~2{{.EmphasisRight}}, giving us a patch of what to remove to effectively remove the " +
		"influence of the specified commit. If multiple commits are specified, then this process is repeated for each " +
		"commit in the order specified. This requires a clean working set." +
		"\n\nAny conflicts or constraint violations caused by the merge cause the command to fail.",
	Synopsis: []string{
		"<revision>...",
	},
}

type RevertCmd struct{}

var _ cli.Command = RevertCmd{}

// Name implements the interface cli.Command.
func (cmd RevertCmd) Name() string {
	return "revert"
}

// Description implements the interface cli.Command.
func (cmd RevertCmd) Description() string {
	return "Undo the changes introduced in a commit."
}

func (cmd RevertCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateRevertArgParser()
	return cli.NewCommandDocumentation(revertDocs, ap)
}

func (cmd RevertCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateRevertArgParser()
}

// Exec implements the interface cli.Command.
func (cmd RevertCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateRevertArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, revertDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// This command creates a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
		return 1
	}

	if apr.NArg() < 1 {
		usage()
		return 1
	}

	if dEnv.IsLocked() {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(env.ErrActiveServerLock.New(dEnv.LockFile())), help)
	}

	_, sqlCtx, closer, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(err.Error())
		return 1
	}
	defer closer()

	headCommit, err := dEnv.HeadCommit(sqlCtx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	headRoot, err := headCommit.GetRootValue(sqlCtx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	workingRoot, err := dEnv.WorkingRoot(sqlCtx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	headHash, err := headRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	workingHash, err := workingRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if !headHash.Equal(workingHash) {
		cli.PrintErrln("You must commit any changes before using revert.")
		return 1
	}

	headRef := dEnv.RepoState.CWBHeadRef()
	commits := make([]*doltdb.Commit, apr.NArg())
	for i, arg := range apr.Args {
		commitSpec, err := doltdb.NewCommitSpec(arg)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		commit, err := dEnv.DoltDB.Resolve(sqlCtx, commitSpec, headRef)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
		commits[i] = commit
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: tmpDir}
	workingRoot, revertMessage, err := merge.Revert(sqlCtx, dEnv.DoltDB, workingRoot, headCommit, commits, opts)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	workingHash, err = workingRoot.HashOf()
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if headHash.Equal(workingHash) {
		cli.Println("No changes were made.")
		return 0
	}

	err = dEnv.UpdateWorkingRoot(sqlCtx, workingRoot)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	res := AddCmd{}.Exec(sqlCtx, "add", []string{"-A"}, dEnv, cliCtx)
	if res != 0 {
		return res
	}

	// Pass in the final parameters for the author string.
	commitParams := []string{"-m", revertMessage}
	authorStr, ok := apr.GetValue(cli.AuthorParam)
	if ok {
		commitParams = append(commitParams, "--author", authorStr)
	}

	return CommitCmd{}.Exec(sqlCtx, "commit", commitParams, dEnv, cliCtx)
}
