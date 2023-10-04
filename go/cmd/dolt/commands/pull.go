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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
)

var pullDocs = cli.CommandDocumentationContent{
	ShortDesc: "Fetch from and integrate with another repository or a local branch",
	LongDesc: `Incorporates changes from a remote repository into the current branch. In its default mode, {{.EmphasisLeft}}dolt pull{{.EmphasisRight}} is shorthand for {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} followed by {{.EmphasisLeft}}dolt merge <remote>/<branch>{{.EmphasisRight}}.

More precisely, dolt pull runs {{.EmphasisLeft}}dolt fetch{{.EmphasisRight}} with the given parameters and calls {{.EmphasisLeft}}dolt merge{{.EmphasisRight}} to merge the retrieved branch {{.EmphasisLeft}}HEAD{{.EmphasisRight}} into the current branch.
`,
	Synopsis: []string{
		`[{{.LessThan}}remote{{.GreaterThan}}, [{{.LessThan}}remoteBranch{{.GreaterThan}}]]`,
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

func (cmd PullCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreatePullArgParser()
	return cli.NewCommandDocumentation(pullDocs, ap)
}

func (cmd PullCmd) ArgParser() *argparser.ArgParser {
	return cli.CreatePullArgParser()
}

// EventType returns the type of the event to log
func (cmd PullCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PULL
}

// Exec executes the command
func (cmd PullCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreatePullArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pullDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 2 {
		verr := errhand.VerboseErrorFromError(actions.ErrInvalidPullArgs)
		return HandleVErrAndExitCode(verr, usage)
	}
	if apr.ContainsAll(cli.CommitFlag, cli.NoCommitFlag) {
		verr := errhand.VerboseErrorFromError(errors.New(fmt.Sprintf(ErrConflictingFlags, cli.CommitFlag, cli.NoCommitFlag)))
		return HandleVErrAndExitCode(verr, usage)
	}
	if apr.ContainsAll(cli.SquashParam, cli.NoFFParam) {
		verr := errhand.VerboseErrorFromError(errors.New(fmt.Sprintf(ErrConflictingFlags, cli.SquashParam, cli.NoFFParam)))
		return HandleVErrAndExitCode(verr, usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.Println(err.Error())
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltPullQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	errChan := make(chan error)
	go func() {
		defer close(errChan)
		schema, rowIter, err := queryist.Query(sqlCtx, query)
		if err != nil {
			errChan <- err
			return
		}

		_, err = sql.RowIterToRows(sqlCtx, schema, rowIter)
		if err != nil {
			errChan <- err
			return
		}
	}()

	spinner := TextSpinner{}
	cli.Print(spinner.next() + " Pulling...")
	defer func() {
		cli.DeleteAndPrint(len(" Pulling...")+1, "")
	}()

	for {
		select {
		case err := <-errChan:
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		case <-ctx.Done():
			if ctx.Err() != nil {
				switch ctx.Err() {
				case context.DeadlineExceeded:
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("timeout exceeded")), usage)
				case context.Canceled:
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("pull cancelled by force")), usage)
				default:
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("error cancelling context: "+ctx.Err().Error())), usage)
				}
			}
			return HandleVErrAndExitCode(nil, usage)
		case <-time.After(time.Millisecond * 50):
			cli.DeleteAndPrint(len(" Pulling...")+1, spinner.next()+" Pulling...")
		}
	}
}

// constructInterpolatedDoltPullQuery constructs the sql query necessary to call the DOLT_PULL() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltPullQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}
	var args []string

	for _, arg := range apr.Args {
		args = append(args, "?")
		params = append(params, arg)
	}

	if apr.Contains(cli.SquashParam) {
		args = append(args, "'--squash'")
	}
	if apr.Contains(cli.NoFFParam) {
		args = append(args, "'--no-ff'")
	}
	if apr.Contains(cli.ForceFlag) {
		args = append(args, "'--force'")
	}
	if apr.Contains(cli.CommitFlag) {
		args = append(args, "'--commit'")
	}
	if apr.Contains(cli.NoCommitFlag) {
		args = append(args, "'--no-commit'")
	}
	if apr.Contains(cli.NoEditFlag) {
		args = append(args, "'--no-edit'")
	}
	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		args = append(args, "'--user'")
		args = append(args, "?")
		params = append(params, user)
	}

	query := "call dolt_pull(" + strings.Join(args, ", ") + ")"

	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}

// pullHelper splits pull into fetch, prepare merge, and merge to interleave printing
func pullHelper(
	ctx context.Context,
	sqlCtx *sql.Context,
	queryist cli.Queryist,
	dEnv *env.DoltEnv,
	pullSpec *env.PullSpec,
	cliCtx cli.CliContext,
) error {
	srcDB, err := pullSpec.Remote.GetRemoteDBWithoutCaching(ctx, dEnv.DoltDB.ValueReadWriter().Format(), dEnv)
	if err != nil {
		return fmt.Errorf("failed to get remote db; %w", err)
	}

	// Fetch all references
	branchRefs, err := srcDB.GetHeadRefs(ctx)
	if err != nil {
		return fmt.Errorf("%w: %s", env.ErrFailedToReadDb, err.Error())
	}

	_, hasBranch, err := srcDB.HasBranch(ctx, pullSpec.Branch.GetPath())
	if err != nil {
		return err
	}
	if !hasBranch {
		return fmt.Errorf("branch %q not found on remote", pullSpec.Branch.GetPath())
	}

	// Go through every reference and every branch in each reference
	for _, rs := range pullSpec.RefSpecs {
		rsSeen := false // track invalid refSpecs
		for _, branchRef := range branchRefs {
			remoteTrackRef := rs.DestRef(branchRef)
			if remoteTrackRef == nil {
				continue
			}

			rsSeen = true
			tmpDir, err := dEnv.TempTableFilesDir()
			if err != nil {
				return err
			}
			srcDBCommit, err := actions.FetchRemoteBranch(ctx, tmpDir, pullSpec.Remote, srcDB, dEnv.DoltDB, branchRef, buildProgStarter(downloadLanguage), stopProgFuncs)
			if err != nil {
				return err
			}

			err = dEnv.DoltDB.FastForward(ctx, remoteTrackRef, srcDBCommit)
			if errors.Is(err, datas.ErrMergeNeeded) {
				// If the remote tracking branch has diverged from the local copy, we just overwrite it
				h, err := srcDBCommit.HashOf()
				if err != nil {
					return err
				}
				err = dEnv.DoltDB.SetHead(ctx, remoteTrackRef, h)
				if err != nil {
					return err
				}
			} else if err != nil {
				return fmt.Errorf("fetch failed: %w", err)
			}

			// Merge iff branch is current branch and there is an upstream set (pullSpec.Branch is set to nil if there is no upstream)
			if branchRef != pullSpec.Branch {
				continue
			}

			t := datas.CommitterDate()

			roots, err := dEnv.Roots(ctx)
			if err != nil {
				return err
			}

			name, email, configErr := env.GetNameAndEmail(dEnv.Config)
			// If the name and email aren't set we can set them to empty values for now. This is only valid for ff
			// merges which we detect later.
			if configErr != nil {
				if pullSpec.NoFF {
					return configErr
				}
				name, email = "", ""
			}

			// Begin merge of working and head with the remote head
			mergeSpec, err := merge.NewMergeSpec(ctx, dEnv.RepoStateReader(), dEnv.DoltDB, roots, name, email, remoteTrackRef.String(), t, merge.WithPullSpecOpts(pullSpec))
			if err != nil {
				return err
			}
			if mergeSpec == nil {
				return nil
			}

			// If configurations are not set and a ff merge are not possible throw an error.
			if configErr != nil {
				canFF, err := mergeSpec.HeadC.CanFastForwardTo(ctx, mergeSpec.MergeC)
				if err != nil && err != doltdb.ErrUpToDate {
					return err
				}

				if !canFF {
					return configErr
				}
			}

			err = validateMergeSpec(ctx, mergeSpec)
			if err != nil {
				return err
			}

			headRef, err := dEnv.RepoStateReader().CWBHeadRef()
			if err != nil {
				return err
			}

			suggestedMsg := fmt.Sprintf(
				"Merge branch '%s' of %s into %s",
				pullSpec.Branch.GetPath(),
				pullSpec.Remote.Url,
				headRef.GetPath(),
			)
			tblStats, err := performMerge(ctx, sqlCtx, queryist, dEnv, mergeSpec, suggestedMsg, cliCtx)
			printSuccessStats(tblStats)
			if err != nil {
				return err
			}
		}
		if !rsSeen {
			return fmt.Errorf("%w: '%s'", ref.ErrInvalidRefSpec, rs.GetRemRefToLocal())
		}
	}

	if err != nil {
		return err
	}
	tmpDir, err := dEnv.TempTableFilesDir()
	if err != nil {
		return err
	}
	err = actions.FetchFollowTags(ctx, tmpDir, srcDB, dEnv.DoltDB, buildProgStarter(downloadLanguage), stopProgFuncs)
	if err != nil {
		return err
	}

	return nil
}
