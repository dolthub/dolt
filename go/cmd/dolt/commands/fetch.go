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
	"strings"
	"time"

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var fetchDocs = cli.CommandDocumentationContent{
	ShortDesc: "Download objects and refs from another repository",
	LongDesc: `Fetch refs, along with the objects necessary to complete their histories and update remote-tracking branches.

By default dolt will attempt to fetch from a remote named {{.EmphasisLeft}}origin{{.EmphasisRight}}.  The {{.LessThan}}remote{{.GreaterThan}} parameter allows you to specify the name of a different remote you wish to pull from by the remote's name.

When no refspec(s) are specified on the command line, the fetch_specs for the default remote are used.
`,

	Synopsis: []string{
		"[{{.LessThan}}remote{{.GreaterThan}}] [{{.LessThan}}refspec{{.GreaterThan}} ...]",
	},
}

type FetchCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd FetchCmd) Name() string {
	return "fetch"
}

// Description returns a description of the command
func (cmd FetchCmd) Description() string {
	return "Update the database from a remote data repository."
}

// EventType returns the type of the event to log
func (cmd FetchCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_FETCH
}

func (cmd FetchCmd) Docs() *cli.CommandDocumentation {
	ap := cli.CreateFetchArgParser()
	return cli.NewCommandDocumentation(fetchDocs, ap)
}

func (cmd FetchCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateFetchArgParser()
}

func (cmd FetchCmd) RequiresRepo() bool {
	return false
}

// Exec executes the command
func (cmd FetchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cli.CreateFetchArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, fetchDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		cli.PrintErrln(err)
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	query, err := constructInterpolatedDoltFetchQuery(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	errChan := make(chan error)
	go func() {
		defer close(errChan)
		_, _, _, err = queryist.Query(sqlCtx, query)
		if err != nil {
			errChan <- err
			return
		}
	}()

	spinner := TextSpinner{}
	if !apr.Contains(cli.SilentFlag) {
		cli.Print(spinner.next() + " Fetching...")
		defer func() {
			cli.DeleteAndPrint(len(" Fetching...")+1, "")
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
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("fetch cancelled by force")), usage)
				default:
					return HandleVErrAndExitCode(errhand.VerboseErrorFromError(errors.New("error cancelling context: "+ctx.Err().Error())), usage)
				}
			}
			return HandleVErrAndExitCode(nil, usage)
		case <-time.After(time.Millisecond * 50):
			if !apr.Contains(cli.SilentFlag) {
				cli.DeleteAndPrint(len(" Fetching...")+1, spinner.next()+" Fetching...")
			}
		}
	}
}

// constructInterpolatedDoltFetchQuery constructs the sql query necessary to call the DOLT_FETCH() function.
// Also interpolates this query to prevent sql injection.
func constructInterpolatedDoltFetchQuery(apr *argparser.ArgParseResults) (string, error) {
	var params []interface{}
	var args []string

	if apr.Contains(cli.PruneFlag) {
		args = append(args, "'--prune'")
	}
	if user, hasUser := apr.GetValue(cli.UserFlag); hasUser {
		args = append(args, "'--user'")
		args = append(args, "?")
		params = append(params, user)
	}
	for _, arg := range apr.Args {
		args = append(args, "?")
		params = append(params, arg)
	}

	query := "call dolt_fetch(" + strings.Join(args, ", ") + ")"

	interpolatedQuery, err := dbr.InterpolateForDialect(query, params, dialect.MySQL)
	if err != nil {
		return "", err
	}

	return interpolatedQuery, nil
}
