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
	"path"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	dirParamName = "dir"
)

var readTablesDocs = cli.CommandDocumentationContent{
	ShortDesc: "Fetch table(s) at a specific commit into a new dolt repo",
	LongDesc: "A shallow clone operation will retrieve the state of table(s) from a remote repository at a given commit. " +
		"Retrieved data is placed into the working state of a newly created local Dolt repository. Changes to the data cannot " +
		"be submitted back to the remote repository, and the shallow clone cannot be converted into a regular clone of a " +
		"repository.",
	Synopsis: []string{
		"[--dir <directory>] {{.LessThan}}remote-url{{.GreaterThan}} {{.LessThan}}commit{{.GreaterThan}} [{{.LessThan}}table{{.GreaterThan}}...]",
	},
}

// ReadTablesCmd is the implementation of the shallow-clone command
type ReadTablesCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ReadTablesCmd) Name() string {
	return "read-tables"
}

// Description returns a description of the command
func (cmd ReadTablesCmd) Description() string {
	return readTablesDocs.ShortDesc
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ReadTablesCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, readTablesDocs, ap))
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd ReadTablesCmd) RequiresRepo() bool {
	return false
}

func (cmd ReadTablesCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = [][2]string{
		{"remote-repo", "Remote repository to retrieve data from"},
		{"commit", "Branch or commit hash representing a point in time to retrieve tables from"},
		{"table", " Optional tables to retrieve.  If omitted, all tables are retrieved."},
	}
	ap.SupportsString(dirParamName, "d", "directory", "directory to create and put retrieved table data.")
	return ap
}

// Exec executes the command
func (cmd ReadTablesCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()

	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, readTablesDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() < 2 {
		return HandleVErrAndExitCode(errhand.BuildDError("Missing required arguments").SetPrintUsage().Build(), usage)
	}

	urlStr := apr.Arg(0)
	commitStr := apr.Arg(1)
	tblNames := apr.Args[2:]

	_, err := earl.Parse(urlStr)

	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Invalid remote url").AddCause(err).Build(), usage)
	}

	dir := apr.GetValueOrDefault(dirParamName, path.Base(urlStr))

	if dir == "" {
		return HandleVErrAndExitCode(errhand.BuildDError(`parameter %s has an invalid value of ""`, dirParamName).Build(), usage)
	}

	scheme, remoteUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, urlStr)

	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Invalid remote url").AddCause(err).Build(), usage)
	}

	remoteUrlParams, verr := parseRemoteArgs(apr, scheme, remoteUrl)

	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	srcDB, srcRoot, verr := getRemoteDBAtCommit(ctx, remoteUrl, remoteUrlParams, commitStr, dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	branches, err := srcDB.GetBranches(ctx)
	if verr != nil {
		BuildVerrAndExit("Failed to get remote branches", err)
	}

	dEnv, verr = initializeShallowCloneRepo(ctx, dEnv, srcDB.Format(), dir, env.GetDefaultBranch(dEnv, branches))
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	destRoot, err := dEnv.WorkingRoot(ctx)

	if err != nil {
		return BuildVerrAndExit("Failed to read working root", err)
	}

	if len(tblNames) == 0 {
		tblNames, err = srcRoot.GetTableNames(ctx)

		if err != nil {
			return BuildVerrAndExit("Unable to read tables.", err)
		}
	}

	for _, tblName := range tblNames {
		destRoot, verr = pullTableValue(ctx, dEnv, srcDB, srcRoot, destRoot, downloadLanguage, tblName, commitStr)

		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	err = dEnv.UpdateWorkingRoot(ctx, destRoot)

	if err != nil {
		return BuildVerrAndExit("Unable to update the working root for local database.", err)
	}

	return 0
}

func pullTableValue(ctx context.Context, dEnv *env.DoltEnv, srcDB *doltdb.DoltDB, srcRoot, destRoot *doltdb.RootValue, language progLanguage, tblName, commitStr string) (*doltdb.RootValue, errhand.VerboseError) {
	tbl, ok, err := srcRoot.GetTable(ctx, tblName)

	if !ok {
		return nil, errhand.BuildDError("No table named '%s' at '%s'", tblName, commitStr).Build()
	} else if err != nil {
		return nil, errhand.BuildDError("Failed reading table '%s' from remote database.", tblName).AddCause(err).Build()
	}

	tblHash, err := tbl.HashOf()

	if err != nil {
		return nil, errhand.BuildDError("Unable to read from remote database.").AddCause(err).Build()
	}

	newCtx, cancelFunc := context.WithCancel(ctx)
	cli.Println("Retrieving", tblName)
	runProgFunc := buildProgStarter(language)
	wg, progChan, pullerEventCh := runProgFunc(newCtx)
	err = dEnv.DoltDB.PullChunks(ctx, dEnv.TempTableFilesDir(), srcDB, tblHash, progChan, pullerEventCh)
	stopProgFuncs(cancelFunc, wg, progChan, pullerEventCh)
	if err != nil {
		return nil, errhand.BuildDError("Failed reading chunks for remote table '%s' at '%s'", tblName, commitStr).AddCause(err).Build()
	}

	destRoot, err = destRoot.SetTableHash(ctx, tblName, tblHash)

	if err != nil {
		return nil, errhand.BuildDError("Unable to write to local database.").AddCause(err).Build()
	}

	return destRoot, nil
}

func getRemoteDBAtCommit(ctx context.Context, remoteUrl string, remoteUrlParams map[string]string, commitStr string, dEnv *env.DoltEnv) (*doltdb.DoltDB, *doltdb.RootValue, errhand.VerboseError) {
	_, srcDB, verr := createRemote(ctx, "temp", remoteUrl, remoteUrlParams, dEnv)

	if verr != nil {
		return nil, nil, verr
	}

	cs, err := doltdb.NewCommitSpec(commitStr)

	if err != nil {
		return nil, nil, errhand.BuildDError("Invalid Commit '%s'", commitStr).Build()
	}

	cm, err := srcDB.Resolve(ctx, cs, nil)

	if err != nil {
		return nil, nil, errhand.BuildDError("Failed to find commit '%s'", commitStr).Build()
	}

	srcRoot, err := cm.GetRootValue(ctx)

	if err != nil {
		return nil, nil, errhand.BuildDError("Failed to read from database").AddCause(err).Build()
	}

	return srcDB, srcRoot, nil
}

func initializeShallowCloneRepo(ctx context.Context, dEnv *env.DoltEnv, nbf *types.NomsBinFormat, dir, branchName string) (*env.DoltEnv, errhand.VerboseError) {
	var err error
	dEnv, err = actions.EnvForClone(ctx, nbf, env.NoRemote, dir, dEnv.FS, dEnv.Version, env.GetCurrentUserHomeDir)

	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	err = actions.InitEmptyClonedRepo(ctx, dEnv)
	if err != nil {
		return nil, errhand.BuildDError("Unable to initialize repo.").AddCause(err).Build()
	}

	err = dEnv.InitializeRepoState(ctx, branchName)
	if err != nil {
		return nil, errhand.BuildDError("Unable to initialize repo.").AddCause(err).Build()
	}

	return dEnv, nil
}
