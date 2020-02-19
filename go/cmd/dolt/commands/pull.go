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

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/ref"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
)

var pullShortDesc = "Fetch from and integrate with another repository or a local branch"
var pullLongDesc = "Incorporates changes from a remote repository into the current branch. In its default mode, " +
	"<b>dolt pull</b> is shorthand for <b>dolt fetch</b> followed by <b>dolt merge <remote>/<branch></b>." +
	"\n" +
	"\nMore precisely, dolt pull runs dolt fetch with the given parameters and calls dolt merge to merge the retrieved " +
	"branch heads into the current branch."
var pullSynopsis = []string{
	"<remote>",
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
	return cli.CreateMarkdown(fs, path, commandStr, pullShortDesc, pullLongDesc, pullSynopsis, ap)
}

func (cmd PullCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	return ap
}

// EventType returns the type of the event to log
func (cmd PullCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PULL
}

// Exec executes the command
func (cmd PullCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(commandStr, pullShortDesc, pullLongDesc, pullSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	branch := dEnv.RepoState.CwbHeadRef()

	var verr errhand.VerboseError
	var remoteName string
	if apr.NArg() > 1 {
		verr = errhand.BuildDError("").SetPrintUsage().Build()
	} else {
		if apr.NArg() == 1 {
			remoteName = apr.Arg(0)
		}

		var refSpecs []ref.RemoteRefSpec
		refSpecs, verr = dEnv.GetRefSpecs(remoteName)

		if verr == nil {
			if len(refSpecs) == 0 {
				verr = errhand.BuildDError("error: no refspec for remote").Build()
			} else {
				remote := dEnv.RepoState.Remotes[refSpecs[0].GetRemote()]

				for _, refSpec := range refSpecs {
					if remoteTrackRef := refSpec.DestRef(branch); remoteTrackRef != nil {
						verr = pullRemoteBranch(ctx, dEnv, remote, branch, remoteTrackRef)

						if verr != nil {
							break
						}
					}
				}
			}
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pullRemoteBranch(ctx context.Context, dEnv *env.DoltEnv, r env.Remote, srcRef, destRef ref.DoltRef) errhand.VerboseError {
	srcDB, err := r.GetRemoteDB(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return errhand.BuildDError("error: failed to get remote db").AddCause(err).Build()
	}

	verr := fetchRemoteBranch(ctx, dEnv, r, srcDB, dEnv.DoltDB, srcRef, destRef)

	if verr != nil {
		return verr
	}

	return mergeBranch(ctx, dEnv, destRef)
}
