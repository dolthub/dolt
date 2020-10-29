// Copyright 2019 Liquidata, Inc.
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
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var pullDocs = cli.CommandDocumentationContent{
	ShortDesc: "Fetch from and integrate with another repository or a local branch",
	LongDesc: `Incorporates changes from a remote repository into the current branch. In its default mode, {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} is shorthand for {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} followed by {{.EmphasisLeft}}dolt merge <remote>/<branch>{{.EmphasisRight}}.

More precisely, dolt pull runs {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} with the given parameters and calls {{.EmphasisLeft}}dolt merge{{.EmphasisRight}} to merge the retrieved branch {{.EmphasisLeft}}HEAD{{.EmphasisRight}} into the current branch.
`,
	Synopsis: []string{
		"{{.LessThan}}remote{{.GreaterThan}}",
	},
}

type PullCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd PullCmd) Name() string {
	return "pull"
}

// Description returns a description of the command
func (cmd PullCmd) Description() string {
	return "Fetch from a dolt remote data repository and merge."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd PullCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, pullDocs, ap))
}

func (cmd PullCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(squashParam, "", "Merges changes to the working set without updating the commit history")
	return ap
}

// EventType returns the type of the event to log
func (cmd PullCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PULL
}

// Exec executes the command
func (cmd PullCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, pullDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	verr := pullFromRemote(ctx, dEnv, apr)

	return HandleVErrAndExitCode(verr, usage)
}

func pullFromRemote(ctx context.Context, dEnv *env.DoltEnv, apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() > 1 {
		return errhand.BuildDError("dolt pull takes at most one arg").SetPrintUsage().Build()
	}

	branch := dEnv.RepoState.CWBHeadRef()

	var remoteName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	}

	refSpecs, verr := dEnv.GetRefSpecs(remoteName)
	if verr != nil {
		return verr
	}

	if len(refSpecs) == 0 {
		return errhand.BuildDError("error: no refspec for remote").Build()
	}

	remote := dEnv.RepoState.Remotes[refSpecs[0].GetRemote()]

	for _, refSpec := range refSpecs {
		remoteTrackRef := refSpec.DestRef(branch)

		if remoteTrackRef != nil {
			squash := apr.Contains(squashParam)
			verr = pullRemoteBranch(ctx, squash, dEnv, remote, branch, remoteTrackRef)

			if verr != nil {
				return verr
			}
		}
	}

	srcDB, err := remote.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	verr = fetchFollowTags(ctx, dEnv, srcDB, dEnv.DoltDB)

	if verr != nil {
		return verr
	}

	return nil
}

func pullRemoteBranch(ctx context.Context, squash bool, dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef) errhand.VerboseError {
	srcDB, err := r.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	srcDBCommit, verr := fetchRemoteBranch(ctx, dEnv, r, srcDB, dEnv.DoltDB, srcRef, destRef)

	if verr != nil {
		return verr
	}

	err = dEnv.DoltDB.FastForward(ctx, destRef, srcDBCommit)

	if err != nil {
		return errhand.BuildDError("error: fetch failed").AddCause(err).Build()
	}

	return mergeCommitSpec(ctx, squash, dEnv, destRef.String())
}
