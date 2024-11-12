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
	"io"

	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var tblRmDocs = cli.CommandDocumentationContent{
	ShortDesc: "Removes table(s) from the working set of tables.",
	LongDesc:  "{{.EmphasisLeft}}dolt table rm{{.EmphasisRight}} removes table(s) from the working set.  These changes can be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}",
	Synopsis: []string{
		"{{.LessThan}}table{{.GreaterThan}}...",
	},
}

type RmCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd RmCmd) Name() string {
	return "rm"
}

// Description returns a description of the command
func (cmd RmCmd) Description() string {
	return "Deletes a table"
}

// EventType returns the type of the event to log
func (cmd RmCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_RM
}

func (cmd RmCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(tblRmDocs, ap)
}

func (cmd RmCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithVariableArgs(cmd.Name())
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table to remove"})
	return ap
}

// Exec executes the command
func (cmd RmCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tblRmDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	for _, tableName := range apr.Args {
		if doltdb.IsReadOnlySystemTable(tableName, doltdb.HasDoltPrefix(tableName)) {
			return commands.HandleVErrAndExitCode(
				errhand.BuildDError("error removing table %s", tableName).AddCause(doltdb.ErrSystemTableCannotBeModified).Build(), usage)
		}
	}

	queryStr := ""
	for _, tableName := range apr.Args {
		queryStr = fmt.Sprintf("%sDROP TABLE `%s`;", queryStr, tableName)
	}

	cli.CliOut = io.Discard // display nothing on success
	return commands.SqlCmd{}.Exec(ctx, "", []string{
		fmt.Sprintf(`--%s`, commands.QueryFlag),
		queryStr,
	}, dEnv, cliCtx)
}
