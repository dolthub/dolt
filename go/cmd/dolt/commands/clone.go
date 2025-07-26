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
	"os"
	"path"
	"strings"

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/creds"
	"github.com/dolthub/dolt/go/libraries/doltcore/dbfactory"
	"github.com/dolthub/dolt/go/libraries/doltcore/dconfig"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/events"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/earl"
	"github.com/dolthub/dolt/go/store/types"
)

var cloneDocs = cli.CommandDocumentationContent{
	ShortDesc: "Clone a data repository into a new directory",
	LongDesc: `Clones a repository into a newly created directory, creates remote-tracking branches for each branch in the cloned repository (visible using {{.LessThan}}dolt branch -a{{.GreaterThan}}), and creates and checks out an initial branch that is forked from the cloned repository's currently active branch.

After the clone, a plain {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} without arguments will update all the remote-tracking branches, and a {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} without arguments will in addition merge the remote branch into the current branch.

This default configuration is achieved by creating references to the remote branch heads under {{.LessThan}}refs/remotes/origin{{.GreaterThan}}  and by creating a remote named 'origin'.
`,
	Synopsis: []string{
		"[-remote {{.LessThan}}remote{{.GreaterThan}}] [-branch {{.LessThan}}branch{{.GreaterThan}}]  [--aws-region {{.LessThan}}region{{.GreaterThan}}] [--aws-creds-type {{.LessThan}}creds-type{{.GreaterThan}}] [--aws-creds-file {{.LessThan}}file{{.GreaterThan}}] [--aws-creds-profile {{.LessThan}}profile{{.GreaterThan}}] {{.LessThan}}remote-url{{.GreaterThan}} {{.LessThan}}new-dir{{.GreaterThan}}",
	},
}

type CloneCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CloneCmd) Name() string {
	return "clone"
}

// Description returns a description of the command
func (cmd CloneCmd) Description() string {
	return "Clone from a remote data repository."
}

// RequiresRepo should return false if this interface is implemented, and the command does not have the requirement
// that it be run from within a data repository directory
func (cmd CloneCmd) RequiresRepo() bool {
	return false
}

func (cmd CloneCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(cloneDocs, ap)
}

func (cmd CloneCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateCloneArgParser()
}

// EventType returns the type of the event to log
func (cmd CloneCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CLONE
}

// Exec executes the command
func (cmd CloneCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, cloneDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	verr := clone(ctx, apr, dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	return 0
}

func clone(ctx context.Context, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) errhand.VerboseError {
	remoteName := apr.GetValueOrDefault(cli.RemoteParam, "origin")
	branch := apr.GetValueOrDefault(cli.BranchParam, "")
	singleBranch := apr.Contains(cli.SingleBranchFlag)
	dir, urlStr, verr := parseArgs(apr)
	if verr != nil {
		return verr
	}

	dEnv.UserPassConfig, verr = getRemoteUserAndPassConfig(apr)
	if verr != nil {
		return verr
	}

	userDirExists, _ := dEnv.FS.Exists(dir)

	// Check for a valid dolthub url and replace the urlStr with the parsed repoName.
	repoName, ok := validateAndParseDolthubUrl(urlStr)
	if ok {
		urlStr = repoName
	}

	scheme, remoteUrl, err := env.GetAbsRemoteUrl(dEnv.FS, dEnv.Config, urlStr)

	if err != nil {
		return errhand.BuildDError("error: '%s' is not valid.", urlStr).Build()
	}
	var params map[string]string
	params, verr = parseRemoteArgs(apr, scheme, remoteUrl)
	if verr != nil {
		return verr
	}

	var r env.Remote
	var srcDB *doltdb.DoltDB
	r, srcDB, verr = createRemote(ctx, remoteName, remoteUrl, params, dEnv)
	if verr != nil {
		return verr
	}

	// Create a new Dolt env for the clone
	clonedEnv, err := actions.EnvForClone(ctx, srcDB.ValueReadWriter().Format(), r, dir, dEnv.FS, dEnv.Version, env.GetCurrentUserHomeDir)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	depth, ok := apr.GetInt(cli.DepthFlag)
	if !ok {
		depth = -1
	}

	// Nil out the old Dolt env so we don't accidentally operate on the wrong database
	dEnv = nil

	err = actions.CloneRemote(ctx, srcDB, remoteName, branch, singleBranch, depth, clonedEnv)
	if err != nil {
		// If we're cloning into a directory that already exists do not erase it. Otherwise
		// make best effort to delete the directory we created.
		if userDirExists {
			clonedEnv.FS.Delete(dbfactory.DoltDir, true)
		} else {
			clonedEnv.FS.Delete(".", true)
		}
		return errhand.VerboseErrorFromError(err)
	}

	evt := events.GetEventFromContext(ctx)
	u, err := earl.Parse(remoteUrl)
	if err == nil {
		if u.Scheme != "" {
			evt.SetAttribute(eventsapi.AttributeID_REMOTE_URL_SCHEME, u.Scheme)
		}
	}

	err = clonedEnv.RepoStateWriter().UpdateBranch(clonedEnv.RepoState.CWBHeadRef().GetPath(), env.BranchConfig{
		Merge:  clonedEnv.RepoState.Head,
		Remote: remoteName,
	})
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	return nil
}

func parseArgs(apr *argparser.ArgParseResults) (string, string, errhand.VerboseError) {
	if apr.NArg() < 1 || apr.NArg() > 2 {
		return "", "", errhand.BuildDError("").SetPrintUsage().Build()
	}

	urlStr := apr.Arg(0)
	_, err := earl.Parse(urlStr)

	if err != nil {
		return "", "", errhand.BuildDError("error: invalid remote url: %s", urlStr).Build()
	}

	var dir string
	if apr.NArg() == 2 {
		dir = apr.Arg(1)
	} else {
		dir = path.Base(urlStr)
		if dir == "." {
			dir = path.Dir(urlStr)
		} else if dir == "/" {
			return "", "", errhand.BuildDError("Could not infer repo name.  Please explicitly define a directory for this url").Build()
		}
	}

	return dir, urlStr, nil
}

func createRemote(ctx context.Context, remoteName, remoteUrl string, params map[string]string, dEnv *env.DoltEnv) (env.Remote, *doltdb.DoltDB, errhand.VerboseError) {
	cli.Printf("cloning %s\n", remoteUrl)

	r := env.NewRemote(remoteName, remoteUrl, params)
	ddb, err := r.GetRemoteDB(ctx, types.Format_Default, dEnv)
	if err != nil {
		bdr := errhand.BuildDError("error: failed to get remote db").AddCause(err)
		return env.NoRemote, nil, bdr.Build()
	}

	return r, ddb, nil
}

// validateAndParseDolthubUrl validates and returns a Dolthub repo link's repository name. For example, given this url: https://www.dolthub.com/repositories/user/test
// the function would return 'user/test'. Note this function correctly does not handle removing additional path extensions. The url: https://www.dolthub.com/repositories/user/test/pulls
// would return 'user/test/pulls' and eventually error later in the code base.
func validateAndParseDolthubUrl(urlStr string) (string, bool) {
	u, err := earl.Parse(urlStr)

	if err != nil {
		return "", false
	}

	if u.Scheme == dbfactory.HTTPSScheme && u.Host == "www.dolthub.com" {
		// Get the actual repo name and convert the remote
		split := strings.Split(u.Path, "/")

		if len(split) > 2 {
			// the path is of the form /repositories/user/repoName
			return strings.Join(split[2:], "/"), true
		}
	}

	return "", false
}

func getRemoteUserAndPassConfig(apr *argparser.ArgParseResults) (*creds.DoltCredsForPass, errhand.VerboseError) {
	if !apr.Contains(cli.UserFlag) {
		return nil, nil
	}
	pass, found := os.LookupEnv(dconfig.EnvDoltRemotePassword)
	if !found {
		return nil, errhand.BuildDError("error: must set DOLT_REMOTE_PASSWORD environment variable to use --user param").Build()
	}
	return &creds.DoltCredsForPass{
		Username: apr.GetValueOrDefault(cli.UserFlag, ""),
		Password: pass,
	}, nil
}
