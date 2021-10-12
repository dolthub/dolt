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
	"fmt"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
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
	ap := cli.CreatePullArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, pullDocs, ap))
}

func (cmd PullCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(cli.SquashParam, "", "Merges changes to the working set without updating the commit history")
	return ap
}

// EventType returns the type of the event to log
func (cmd PullCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PULL
}

// Exec executes the command
func (cmd PullCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {

	ap := cli.CreatePullArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, pullDocs, ap))

	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 1 {
		verr := errhand.VerboseErrorFromError(actions.ErrInvalidPullArgs)
		return HandleVErrAndExitCode(verr, usage)
	}

	var remoteName string
	if apr.NArg() == 1 {
		remoteName = apr.Arg(0)
	}

	pullSpec, err := env.NewPullSpec(ctx, dEnv.RepoStateReader(), remoteName, apr.Contains(cli.SquashParam), apr.Contains(cli.NoFFParam), apr.Contains(cli.ForceFlag))
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	err = pullHelper(ctx, dEnv, pullSpec)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

// pullHelper splits pull into fetch, prepare merge, and merge to interleave printing
func pullHelper(ctx context.Context, dEnv *env.DoltEnv, pullSpec *env.PullSpec) error {
	srcDB, err := pullSpec.Remote.GetRemoteDBWithoutCaching(ctx, dEnv.DoltDB.ValueReadWriter().Format())

	if err != nil {
		return fmt.Errorf("failed to get remote db; %w", err)
	}
	for _, refSpec := range pullSpec.RefSpecs {
		remoteTrackRef := refSpec.DestRef(pullSpec.Branch)

		if remoteTrackRef != nil {

			srcDBCommit, err := actions.FetchRemoteBranch(ctx, dEnv.TempTableFilesDir(), pullSpec.Remote, srcDB, dEnv.DoltDB, pullSpec.Branch, remoteTrackRef, runProgFuncs, stopProgFuncs)
			if err != nil {
				return err
			}

			err = dEnv.DoltDB.FastForward(ctx, remoteTrackRef, srcDBCommit)
			if err != nil {
				return fmt.Errorf("fetch failed; %w", err)
			}

			t := doltdb.CommitNowFunc()

			roots, err := dEnv.Roots(ctx)
			if err != nil {
				return err
			}

			mergeSpec, ok, err := merge.NewMergeSpec(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, roots, "", "", pullSpec.Msg, remoteTrackRef.String(), pullSpec.Squash, pullSpec.Noff, pullSpec.Force, t)
			if err != nil {
				return err
			}
			err = mergePrinting(ctx, dEnv, mergeSpec)
			if !ok {
				return nil
			}
			if err != nil {
				return err
			}

			stats, err := merge.MergeCommitSpec(ctx, dEnv, mergeSpec)
			printSuccessStats(stats)
			if err != nil {
				return err
			}
		}
	}

	if err != nil {
		return err
	}
	err = actions.FetchFollowTags(ctx, dEnv.TempTableFilesDir(), srcDB, dEnv.DoltDB, runProgFuncs, stopProgFuncs)

	if err != nil {
		return err
	}
	return nil
}
