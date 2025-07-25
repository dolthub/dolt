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

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/config"
	"github.com/dolthub/dolt/go/store/util/outputpager"
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
	// This command may create a commit, so we need user identity
	if !cli.CheckUserNameAndEmail(cliCtx.Config()) {
		bdr := errhand.BuildDError("Could not determine name and/or email.")
		bdr.AddDetails("Log into DoltHub: dolt login")
		bdr.AddDetails("OR add name to config: dolt config [--global|--local] --add %[1]s \"FIRST LAST\"", config.UserNameKey)
		bdr.AddDetails("OR add email to config: dolt config [--global|--local] --add %[1]s \"EMAIL_ADDRESS\"", config.UserEmailKey)
		return HandleVErrAndExitCode(bdr.Build(), usage)
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
		_, _, _, err = queryist.Query(sqlCtx, "set @@dolt_force_transaction_commit = 1")
		if err != nil {
			errChan <- err
			return
		}
		// save current head for diff summaries after pull
		headHash, err := getHashOf(queryist, sqlCtx, "HEAD")
		if err != nil {
			cli.Println("failed to get hash of HEAD, pull not started")
			errChan <- err
		}

		_, rowIter, _, err := queryist.Query(sqlCtx, query)
		if err != nil {
			errChan <- err
			return
		}
		// if merge is called with '--no-commit', we need to commit the sql transaction or the staged changes will be lost
		_, _, _, err = queryist.Query(sqlCtx, "COMMIT")
		if err != nil {
			errChan <- err
			return
		}
		rows, err := sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			errChan <- err
			return
		}
		if len(rows) != 1 {
			err = fmt.Errorf("Runtime error: merge operation returned unexpected number of rows: %d", len(rows))
			errChan <- err
			return
		}
		row := rows[0]

		remoteHash, remoteRef, err := getRemoteHashForPull(apr, sqlCtx, queryist)
		if err != nil {
			cli.Println("pull finished, but failed to get hash of remote ref")
			cli.Println(err.Error())
		}

		if apr.Contains(cli.ForceFlag) {
			if remoteHash != "" && headHash != "" {
				cli.Println("Updating", headHash+".."+remoteHash)
			}
			commit, err := getCommitInfo(queryist, sqlCtx, "HEAD")
			if err != nil {
				cli.Println("pull finished, but failed to get commit info")
				cli.Println(err.Error())
				return
			}
			if cli.ExecuteWithStdioRestored != nil {
				cli.ExecuteWithStdioRestored(func() {
					pager := outputpager.Start()
					defer pager.Stop()

					PrintCommitInfo(pager, 0, false, false, "auto", commit)
				})
			}
		} else {
			fastFwd := getFastforward(row, dprocedures.PullProcFFIndex)

			var success int
			if apr.Contains(cli.NoCommitFlag) {
				success = printMergeStats(fastFwd, apr, queryist, sqlCtx, usage, headHash, remoteHash, "HEAD", "STAGED")
			} else {
				success = printMergeStats(fastFwd, apr, queryist, sqlCtx, usage, headHash, remoteHash, "HEAD", remoteRef)
			}
			if success == 1 {
				errChan <- errors.New(" ") //return a non-nil error for the correct exit code but no further messages to print
				return
			}
		}
	}()

	spinner := TextSpinner{}
	if !apr.Contains(cli.SilentFlag) {
		cli.Print(spinner.next() + " Pulling...")
		defer func() {
			cli.DeleteAndPrint(len(" Pulling...")+1, "")
		}()
	}

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
			if !apr.Contains(cli.SilentFlag) {
				cli.DeleteAndPrint(len(" Pulling...")+1, spinner.next()+" Pulling...")
			}
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
	if apr.Contains(cli.PruneFlag) {
		args = append(args, "'--prune'")
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

// getRemoteHashForPull gets the hash of the remote branch being merged in and the ref to the remote head
func getRemoteHashForPull(apr *argparser.ArgParseResults, sqlCtx *sql.Context, queryist cli.Queryist) (remoteHash, remoteRef string, err error) {
	var remote, branch string

	if apr.NArg() < 2 {
		if apr.NArg() == 0 {
			remote, err = getDefaultRemote(sqlCtx, queryist)
			if err != nil {
				return "", "", err
			}
		} else {
			remote = apr.Args[0]
		}

		rows, err := GetRowsForSql(queryist, sqlCtx, "select name from dolt_remote_branches")
		if err != nil {
			return "", "", err
		}
		for _, row := range rows {
			ref := row[0].(string)
			if ref == "remotes/"+remote+"/main" {
				branch = "main"
				break
			}
		}
		if branch == "" {
			ref := rows[0][0].(string)
			branch = strings.TrimPrefix(ref, "remotes/"+remote+"/")
		}
	} else {
		remote = apr.Args[0]
		branch = apr.Args[1]
	}

	if remote == "" || branch == "" {
		return "", "", errors.New("pull finished successfully but remote and/or branch provided is empty")
	}

	remoteHash, err = getHashOf(queryist, sqlCtx, remote+"/"+branch)
	if err != nil {
		return "", "", err
	}
	return remoteHash, remote + "/" + branch, nil
}

// getDefaultRemote gets the name of the default remote.
func getDefaultRemote(sqlCtx *sql.Context, queryist cli.Queryist) (string, error) {
	rows, err := GetRowsForSql(queryist, sqlCtx, "select name from dolt_remotes")
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", env.ErrNoRemote
	}
	if len(rows) == 1 {
		return rows[0][0].(string), nil
	}
	for _, row := range rows {
		if row[0].(string) == "origin" {
			return "origin", nil
		}
	}
	return rows[0][0].(string), nil
}
