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

package tblcmds

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/gocraft/dbr/v2"
)

var tblCpDocs = cli.CommandDocumentationContent{
	ShortDesc: "Makes a copy of a table",
	LongDesc: `The dolt table cp command makes a copy of a table at a given commit. If a commit is not specified the copy is made of the table from the current working set.

If a table exists at the target location this command will fail unless the {{.EmphasisLeft}}--force|-f{{.EmphasisRight}} flag is provided.  In this case the table at the target location will be overwritten with the copied table.

All changes will be applied to the working tables and will need to be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.
`,
	Synopsis: []string{
		"[-f] {{.LessThan}}oldtable{{.GreaterThan}} {{.LessThan}}newtable{{.GreaterThan}}",
	},
}

type CpCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CpCmd) Name() string {
	return "cp"
}

// Description returns a description of the command
func (cmd CpCmd) Description() string {
	return "Copies a table"
}

func (cmd CpCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(tblCpDocs, ap)
}

func (cmd CpCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"oldtable", "The table being copied."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"newtable", "The destination where the table is being copied to."})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	return ap
}

// EventType returns the type of the event to log
func (cmd CpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_CP
}

// Exec executes the command
func (cmd CpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tblCpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	oldTbl, newTbl := apr.Arg(0), apr.Arg(1)
	force := apr.Contains(forceParam)

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return handleStatusVErr(err)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	err = copyTable(queryist, sqlCtx, oldTbl, newTbl, force)
	if err != nil {
		return handleStatusVErr(err)
	}

	return 0
}

func handleStatusVErr(err error) int {
	cli.PrintErrln(errhand.VerboseErrorFromError(err).Verbose())
	return 1
}

func copyTable(queryist cli.Queryist, sqlCtx *sql.Context, old, new string, force bool) error {
	oldTable := dbr.I(old)
	newTable := dbr.I(new)

	var err error
	if force {
		_, err = commands.InterpolateAndRunQuery(queryist, sqlCtx, "DROP TABLE IF EXISTS ?", newTable)
		if err != nil {
			return fmt.Errorf("error dropping table %s: %w", newTable, err)
		}
	}

	_, err = commands.InterpolateAndRunQuery(queryist, sqlCtx, "CREATE TABLE ? LIKE ?", newTable, oldTable)
	if err != nil {
		return fmt.Errorf("error creating table %s: %w", newTable, err)
	}

	_, err = commands.InterpolateAndRunQuery(queryist, sqlCtx, "INSERT INTO ? SELECT * FROM ?", newTable, oldTable)
	if err != nil {
		return fmt.Errorf("error copying table %s to %s: %w", oldTable, newTable, err)
	}

	return nil
}
