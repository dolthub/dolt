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

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions"
	"github.com/dolthub/dolt/go/libraries/doltcore/ref"
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

// Exec executes the command
func (cmd FetchCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cli.CreateFetchArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, fetchDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	r, refSpecs, err := env.NewFetchOpts(apr.Args, dEnv.RepoStateReader())
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	updateMode := ref.UpdateMode{Force: apr.Contains(cli.ForceFlag)}

	srcDB, err := r.GetRemoteDBWithoutCaching(ctx, dEnv.DbData().Ddb.ValueReadWriter().Format(), dEnv)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	err = actions.FetchRefSpecs(ctx, dEnv.DbData(), srcDB, refSpecs, r, updateMode, buildProgStarter(downloadLanguage), stopProgFuncs)
	switch err {
	case doltdb.ErrUpToDate:
		return HandleVErrAndExitCode(nil, usage)
	case actions.ErrCantFF:
		verr := errhand.BuildDError("error: fetch failed, can't fast forward remote tracking ref").AddCause(err).Build()
		return HandleVErrAndExitCode(verr, usage)
	}
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	return HandleVErrAndExitCode(nil, usage)
}
