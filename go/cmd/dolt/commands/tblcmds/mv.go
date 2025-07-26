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

	eventsapi "github.com/dolthub/eventsapi_schema/dolt/services/eventsapi/v1alpha1"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var tblMvDocs = cli.CommandDocumentationContent{
	ShortDesc: "Renames a table",
	LongDesc: `
The dolt table mv command will rename a table. If a table exists with the target name this command will 
fail unless the {{.EmphasisLeft}}--force|-f{{.EmphasisRight}} flag is provided.  In that case the table at the target location will be overwritten 
by the table being renamed.

The result is equivalent of running {{.EmphasisLeft}}dolt table cp <old> <new>{{.EmphasisRight}} followed by {{.EmphasisLeft}}dolt table rm <old>{{.EmphasisRight}}, resulting 
in a new table and a deleted table in the working set. These changes can be staged using {{.EmphasisLeft}}dolt add{{.EmphasisRight}} and committed
using {{.EmphasisLeft}}dolt commit{{.EmphasisRight}}.`,

	Synopsis: []string{
		"[-f] {{.LessThan}}oldtable{{.GreaterThan}} {{.LessThan}}newtable{{.GreaterThan}}",
	},
}

type MvCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MvCmd) Name() string {
	return "mv"
}

// Description returns a description of the command
func (cmd MvCmd) Description() string {
	return "Moves a table"
}

func (cmd MvCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(tblMvDocs, ap)
}

func (cmd MvCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 2)
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"oldtable", "The table being moved."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"newtable", "The new name of the table"})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	return ap
}

// EventType returns the type of the event to log
func (cmd MvCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_MV
}

// Exec executes the command
func (cmd MvCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, tblMvDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	oldName := apr.Arg(0)
	newName := apr.Arg(1)

	queryStr := ""
	if force := apr.Contains(forceParam); force {
		queryStr = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", newName)
	}
	queryStr = fmt.Sprintf("%sRENAME TABLE `%s` TO `%s`;", queryStr, oldName, newName)

	return commands.SqlCmd{}.Exec(ctx, "", []string{
		fmt.Sprintf(`--%s`, commands.QueryFlag),
		queryStr,
	}, dEnv, cliCtx)
}
