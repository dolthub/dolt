// Copyright 2019 Liquidata, Inc.
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
	"io/ioutil"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
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

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CpCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, tblCpDocs, ap))
}

func (cmd CpCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
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
func (cmd CpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblCpDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	oldTbl, newTbl := apr.Arg(0), apr.Arg(1)

	queryStr := ""
	if force := apr.Contains(forceParam); force {
		queryStr = fmt.Sprintf("DROP TABLE IF EXISTS `%s`;", newTbl)
	}
	queryStr = fmt.Sprintf("%sCREATE TABLE `%s` LIKE `%s`;", queryStr, newTbl, oldTbl)
	queryStr = fmt.Sprintf("%sINSERT INTO `%s` SELECT * FROM `%s`;", queryStr, newTbl, oldTbl)

	cli.CliOut = ioutil.Discard // display nothing on success
	return commands.SqlCmd{}.Exec(ctx, "", []string{
		fmt.Sprintf("--%s", commands.BatchFlag),
		fmt.Sprintf(`--%s`, commands.QueryFlag),
		queryStr,
	}, dEnv)
}
