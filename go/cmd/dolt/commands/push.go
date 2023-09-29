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
	"strings"
	"sync"
	"time"

	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/dprocedures"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"github.com/dustin/go-humanize"
	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/remotestorage"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/datas"
	"github.com/dolthub/dolt/go/store/datas/pull"
)

var pushDocs = cli.CommandDocumentationContent{
	ShortDesc: "Update remote refs along with associated objects",
	LongDesc: `Updates remote refs using local refs, while sending objects necessary to complete the given refs.

When the command line does not specify where to push with the {{.LessThan}}remote{{.GreaterThan}} argument, an attempt is made to infer the remote.  If only one remote exists it will be used, if multiple remotes exists, a remote named 'origin' will be attempted.  If there is more than one remote, and none of them are named 'origin' then the command will fail and you will need to specify the correct remote explicitly.

When the command line does not specify what to push with {{.LessThan}}refspec{{.GreaterThan}}... then the current branch will be used.

A remote's branch can be deleted by pushing an empty source ref: ` + "`dolt push origin :branch`" + `

When neither the command-line does not specify what to push, the default behavior is used, which corresponds to the current branch being pushed to the corresponding upstream branch, but as a safety measure, the push is aborted if the upstream branch does not have the same name as the local one.
`,

	Synopsis: []string{
		"[-u | --set-upstream] [{{.LessThan}}remote{{.GreaterThan}}] [{{.LessThan}}refspec{{.GreaterThan}}]",
	},
}

type PushCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd PushCmd) Name() string {
	return "push"
}

// Description returns a description of the command
func (cmd PushCmd) Description() string {
	return "Push to a dolt remote."
}

func (cmd PushCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(pushDocs, ap)
}

func (cmd PushCmd) ArgParser() *argparser.ArgParser {
	return cli.CreatePushArgParser()
}

// EventType returns the type of the event to log
func (cmd PushCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_PUSH
}

// Exec executes the command
func (cmd PushCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, pushDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltPushQuery(apr)
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

		sqlRows, err := sql.RowIterToRows(sqlCtx, schema, rowIter)
		if err != nil {
			errChan <- err
			return
		}
		cli.Println(sqlRows)
		if sqlRows[0][0].(int64) == 0 && len(sqlRows[0]) > 1 {
			if sqlRows[0][1].(string) == dprocedures.UpToDateMessage {
				cli.Println(dprocedures.UpToDateMessage)
			}
		}
	}()

	spinner := TextSpinner{}
	cli.Print(spinner.next() + " Uploading...")
	defer func() {
		cli.DeleteAndPrint(len(" Uploading...")+1, "")
	}()

	for {
		select {
		case err := <-errChan:
			return handlePushError(err, usage, apr, queryist, sqlCtx)
		case <-ctx.Done():
			return 0
		case <-time.After(time.Millisecond * 50):
			cli.DeleteAndPrint(len(" Uploading...")+1, spinner.next()+" Uploading...")
		}
	}
}

// constructInterpolatedDoltPushQuery generates the sql query necessary to call the DOLT_PUSH() function
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltPushQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}
	var args []string

	if setUpstream := apr.Contains(cli.SetUpstreamFlag); setUpstream {
		args = append(args, "'--set-upstream'")
	}
	if force := apr.Contains(cli.ForceFlag); force {
		args = append(args, "'--force'")
	}
	for _, arg := range apr.Args {
		args = append(args, "?")
		params = append(params, arg)
	}

	query := fmt.Sprintf("call dolt_push(%s)", strings.Join(args, ", "))
	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
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
	return "", env.ErrCantDetermineDefault
}

// processFetchSpecs takes a string of fetch specs and returns the destination ref and remote ref
// Assumes the fetch specs look something like: ["refs/heads/*:refs/remotes/origin/*"]
func processFetchSpecs(fetchSpecs string, branch string) (destRef, remoteRef string) {
	// TODO: replace string parsing with better logic when more JSONDocument functionality is available
	destAndRemoteRefs := strings.Split(fetchSpecs, ":")
	destRef = destAndRemoteRefs[0]
	destRef = strings.TrimPrefix(destRef, "[\"")
	destRef = strings.ReplaceAll(destRef, "*", branch)

	remoteRef = destAndRemoteRefs[1]
	remoteRef = strings.TrimSuffix(remoteRef, "\"]")
	remoteRef = strings.ReplaceAll(remoteRef, "*", branch)

	return
}

// handlePushError prints the appropriate error message and returns the exit code
func handlePushError(err error, usage cli.UsagePrinter, apr *argparser.ArgParseResults, queryist cli.Queryist, sqlCtx *sql.Context) int {
	if err == nil {
		return 0
	}

	var verr errhand.VerboseError
	switch err {
	case env.ErrNoUpstreamForBranch:
		rows, err := GetRowsForSql(queryist, sqlCtx, "select active_branch()")
		if err != nil {
			verr = errhand.BuildDError("fatal: The current branch could not be identified").AddCause(err).Build()
		} else {
			currentBranch := rows[0][0].(string)
			remoteName := "<remote>"
			if defRemote, verr := getDefaultRemote(sqlCtx, queryist); verr == nil {
				remoteName = defRemote
			}
			verr = errhand.BuildDError("fatal: The current branch " + currentBranch + " has no upstream branch.\n" +
				"To push the current branch and set the remote as upstream, use\n" +
				"\tdolt push --set-upstream " + remoteName + " " + currentBranch + "\n" +
				"To have this happen automatically for branches without a tracking\n" +
				"upstream, see 'push.autoSetupRemote' in 'dolt config --help'.").Build()
		}
	case env.ErrInvalidSetUpstreamArgs:
		verr = errhand.BuildDError("error: --set-upstream requires <remote> and <refspec> params.").SetPrintUsage().Build()
	case doltdb.ErrIsAhead, actions.ErrCantFF, datas.ErrMergeNeeded:
		rows, err := GetRowsForSql(queryist, sqlCtx, fmt.Sprintf("select url, fetch_specs from dolt_remotes where name = '%s'", apr.Arg(0)))
		if err != nil {
			verr = errhand.BuildDError("could not identify remote").AddCause(err).Build()
		} else {
			remoteUrl := rows[0][0].(string)
			fetchSpecs := rows[0][1].(types.JSONDocument)
			fetchSpecsStr, err := fetchSpecs.ToString(sqlCtx)
			if err != nil {
				verr = errhand.BuildDError("could not identify destination remote").AddCause(err).Build()
			}
			var branch string
			if apr.NArg() > 1 {
				branch = apr.Arg(1)
			} else {
				rows, err := GetRowsForSql(queryist, sqlCtx, "select active_branch()")
				if err != nil {
					verr = errhand.BuildDError("could not identify current branch").AddCause(err).Build()
				} else {
					branch = rows[0][0].(string)
				}
			}
			destRef, remoteRef := processFetchSpecs(fetchSpecsStr, branch)

			cli.Printf("To %s\n", remoteUrl)
			cli.Printf("! [rejected]          %s -> %s (non-fast-forward)\n", destRef, remoteRef)
			cli.Printf("error: failed to push some refs to '%s'\n", remoteUrl)
			cli.Println("hint: Updates were rejected because the tip of your current branch is behind")
			cli.Println("hint: its remote counterpart. Integrate the remote changes (e.g.")
			cli.Println("hint: 'dolt pull ...') before pushing again.")
			verr = errhand.BuildDError("").Build()
		}
	case actions.ErrUnknownPushErr:
		s, ok := status.FromError(err)
		if ok && s.Code() == codes.PermissionDenied {
			cli.Println("hint: have you logged into DoltHub using 'dolt login'?")
			cli.Println("hint: check that user.email in 'dolt config --list' has write perms to DoltHub repo")
		}
		if rpcErr, ok := err.(*remotestorage.RpcError); ok {
			verr = errhand.BuildDError("error: push failed").AddCause(err).AddDetails(rpcErr.FullDetails()).Build()
		} else {
			verr = errhand.BuildDError("error: push failed").AddCause(err).Build()
		}
	default:
		verr = errhand.VerboseErrorFromError(err)
	}

	return HandleVErrAndExitCode(verr, usage)
}

func pullerProgFunc(ctx context.Context, statsCh chan pull.Stats, language progLanguage) {
	p := cli.NewEphemeralPrinter()

	for {
		select {
		case <-ctx.Done():
			return
		case stats, ok := <-statsCh:
			if !ok {
				return
			}
			if language == downloadLanguage {
				p.Printf("Downloaded %s chunks, %s @ %s/s.",
					humanize.Comma(int64(stats.FetchedSourceChunks)),
					humanize.Bytes(stats.FetchedSourceBytes),
					humanize.SIWithDigits(stats.FetchedSourceBytesPerSec, 2, "B"),
				)
			} else {
				p.Printf("Uploaded %s of %s @ %s/s.",
					humanize.Bytes(stats.FinishedSendBytes),
					humanize.Bytes(stats.BufferedSendBytes),
					humanize.SIWithDigits(stats.SendBytesPerSec, 2, "B"),
				)
			}
			p.Display()
		}
	}
}

// progLanguage is the language to use when displaying progress for a pull from a src db to a sink db.
type progLanguage int

const (
	defaultLanguage progLanguage = iota
	downloadLanguage
)

func buildProgStarter(language progLanguage) actions.ProgStarter {
	return func(ctx context.Context) (*sync.WaitGroup, chan pull.Stats) {
		statsCh := make(chan pull.Stats, 128)
		wg := &sync.WaitGroup{}

		wg.Add(1)
		go func() {
			defer wg.Done()
			pullerProgFunc(ctx, statsCh, language)
		}()

		return wg, statsCh
	}
}

func stopProgFuncs(cancel context.CancelFunc, wg *sync.WaitGroup, statsCh chan pull.Stats) {
	cancel()
	close(statsCh)
	wg.Wait()
}
