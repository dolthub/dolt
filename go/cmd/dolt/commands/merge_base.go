// Copyright 2021 Dolthub, Inc.
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

	"github.com/gocraft/dbr/v2"
	"github.com/gocraft/dbr/v2/dialect"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var mergeBaseDocs = cli.CommandDocumentationContent{
	ShortDesc: `Find the common ancestor of two commits.`,
	LongDesc:  `Find the common ancestor of two commits, and return the ancestor's commit hash.'`,
	Synopsis: []string{
		`{{.LessThan}}commit spec{{.GreaterThan}} {{.LessThan}}commit spec{{.GreaterThan}}`,
	},
}

type MergeBaseCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MergeBaseCmd) Name() string {
	return "merge-base"
}

// Description returns a description of the command
func (cmd MergeBaseCmd) Description() string {
	return mergeBaseDocs.ShortDesc
}

func (cmd MergeBaseCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(mergeBaseDocs, ap)
}

func (cmd MergeBaseCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	return ap
}

// EventType returns the type of the event to log
func (cmd MergeBaseCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TYPE_UNSPECIFIED
}

// Exec executes the command
func (cmd MergeBaseCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, mergeBaseDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	var verr errhand.VerboseError
	if apr.NArg() != 2 {
		verr = errhand.BuildDError("%s takes exactly 2 args", cmd.Name()).Build()
		return HandleVErrAndExitCode(verr, usage)
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	interpolatedQuery, err := dbr.InterpolateForDialect("SELECT DOLT_MERGE_BASE(?, ?)", []interface{}{apr.Arg(0), apr.Arg(1)}, dialect.MySQL)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	row, err := GetRowsForSql(queryist, sqlCtx, interpolatedQuery)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cli.Println(row[0].GetValue(0).(string))

	return 0
}
