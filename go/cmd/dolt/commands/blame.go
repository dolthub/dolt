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
	"regexp"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	ref2 "github.com/dolthub/dolt/go/libraries/doltcore/ref"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"
)

const (
	blameQueryTemplate = "SELECT * FROM dolt_blame_%s AS OF '%s'"
)

var blameDocs = cli.CommandDocumentationContent{
	ShortDesc: `Show what revision and author last modified each row of a table`,
	LongDesc:  `Annotates each row in the given table with information from the revision which last modified the row. Optionally, start annotating from the given revision.`,
	Synopsis: []string{
		`[{{.LessThan}}rev{{.GreaterThan}}] {{.LessThan}}tablename{{.GreaterThan}}`,
	},
}

type BlameCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd BlameCmd) Name() string {
	return "blame"
}

// Description returns a description of the command
func (cmd BlameCmd) Description() string {
	return "Show what revision and author last modified each row of a table."
}

func (cmd BlameCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(blameDocs, ap)
}

func (cmd BlameCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	return ap
}

func (cmd BlameCmd) RequiresRepo() bool {
	return false
}

var _ cli.RepoNotRequiredCommand = BlameCmd{}

// EventType returns the type of the event to log
func (cmd BlameCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_BLAME
}

// Exec implements the `dolt blame` command. Blame annotates each row in the given table with information
// from the revision which last modified the row, optionally starting from a given revision.
//
// Blame is computed as follows:
//
// First, a blame graph is initialized with one node for every row in the table at the given commit (defaulting
// to HEAD of the currently checked-out branch).
//
// Starting from the given commit, walk backwards through the commit graph (currently by following each commit's
// first parent, though this may change in the future).
//
// For each adjacent pair of commits `old` and `new`, check each remaining unblamed node to see if the row it represents
// changed between the commits. If so, mark it with `new` as the blame origin and continue to the next node without blame.
//
// When all nodes have blame information, stop iterating through commits and print the blame graph.
// Exec executes the command
func (cmd BlameCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, blameDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 2 || apr.NArg() == 0 {
		usage()
		return 1
	}

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		iohelp.WriteLine(cli.CliOut, err.Error())
		return 1
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	var schema sql.Schema
	var ri sql.RowIter
	if apr.NArg() == 1 {
		schema, ri, _, err = queryist.Query(sqlCtx, fmt.Sprintf(blameQueryTemplate, apr.Arg(0), "HEAD"))
	} else {
		// validate input
		ref := apr.Arg(0)
		if !ref2.IsValidTagName(ref) && !doltdb.IsValidCommitHash(ref) && !isValidHeadRef(ref) {
			iohelp.WriteLine(cli.CliOut, "Invalid reference provided")
			return 1
		}

		schema, ri, _, err = queryist.Query(sqlCtx, fmt.Sprintf(blameQueryTemplate, apr.Arg(1), apr.Arg(0)))
	}
	if err != nil {
		iohelp.WriteLine(cli.CliOut, err.Error())
		return 1
	}

	err = engine.PrettyPrintResults(sqlCtx, engine.FormatTabular, schema, ri, false, false, true)
	if err != nil {
		iohelp.WriteLine(cli.CliOut, err.Error())
		return 1
	}

	return 0
}

func isValidHeadRef(s string) bool {
	var refRegex = regexp.MustCompile(`(?i)^head[\~\^0-9]*$`)
	return refRegex.MatchString(s)
}
