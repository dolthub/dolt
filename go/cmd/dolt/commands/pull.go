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

	"github.com/dolthub/dolt/go/store/util/outputpager"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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
		// allows pulls (merges) that create conflicts to stick
		_, _, err = queryist.Query(sqlCtx, "set @@dolt_force_transaction_commit = 1")
		if err != nil {
			errChan <- err
			return
		}
		remoteHash, remoteHashErr := getRemoteHashForPull(apr, sqlCtx, queryist) // get the hash of the remote ref before the pull

		schema, rowIter, err := queryist.Query(sqlCtx, query)
		if err != nil {
			errChan <- err
			return
		}
		// if merge is called with '--no-commit', we need to commit the sql transaction or the staged changes will be lost
		_, _, err = queryist.Query(sqlCtx, "COMMIT")
		if err != nil {
			errChan <- err
			return
		}
		rows, err := sql.RowIterToRows(sqlCtx, schema, rowIter)
		if err != nil {
			errChan <- err
			return
		}

		if remoteHashErr != nil {
			cli.Println("pull finished, but failed to get hash of remote ref")
		}

		if apr.Contains(cli.ForceFlag) {
			headHash, headhHashErr := getHashOf(queryist, sqlCtx, "HEAD")
			if headhHashErr != nil {
				cli.Println("merge finished, but failed to get hash of HEAD")
				cli.Println(headhHashErr.Error())
			}
			if remoteHashErr == nil && headhHashErr == nil {
				cli.Println("Updating", headHash+".."+remoteHash)
			}
			commit, err := getCommitInfo(queryist, sqlCtx, "HEAD")
			if err != nil {
				cli.Println("merge finished, but failed to get commit info")
				cli.Println(err.Error())
				return
			}
			if cli.ExecuteWithStdioRestored != nil {
				cli.ExecuteWithStdioRestored(func() {
					pager := outputpager.Start()
					defer pager.Stop()

					PrintCommitInfo(pager, 0, false, "auto", commit)
				})
			}
		} else {
			success := printMergeStats(rows, apr, queryist, sqlCtx, usage, remoteHash, remoteHashErr)
			if success == 1 {
				errChan <- errors.New(" ") //return a non-nil error for the correct exit code but no further messages to print
				return
			}
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

// getRemoteHashForPull gets the hash of the remote branch being merged in
func getRemoteHashForPull(apr *argparser.ArgParseResults, sqlCtx *sql.Context, queryist cli.Queryist) (string, error) {
	var remote, branch string
	var err error
	if apr.NArg() == 0 {
		remote, err = getDefaultRemote(sqlCtx, queryist)
		if err != nil {
			return "", err
		}
	} else if apr.NArg() == 1 {
		remote = apr.Arg(0)
	} else if apr.NArg() == 2 {
		remote = apr.Arg(0)
		branch = apr.Arg(1)
	}
	if branch == "" {
		branch, err = getActiveBranchName(sqlCtx, queryist)
		if err != nil {
			return "", err
		}
	}

	remoteHash, err := getHashOf(queryist, sqlCtx, remote+"/"+branch)
	if err != nil {
		return "", err
	}
	return remoteHash, nil
}
