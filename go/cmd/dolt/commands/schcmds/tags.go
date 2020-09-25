// Copyright 2020 Liquidata, Inc.
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

package schcmds

import (
	"context"
	"fmt"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

//SELECT table_name AS 'table', column_name AS 'column', SUBSTR(extra, 5) AS tag FROM information_schema.columns WHERE table_name = 'XXX';

var tblTagsDocs = cli.CommandDocumentationContent{
	ShortDesc: "Shows the column tags of one or more tables.",
	LongDesc: `{{.EmphasisLeft}}dolt schema tags{{.EmphasisRight}} displays the column tags of tables on the working set.

A list of tables can optionally be provided.  If it is omitted then all tables will be shown. If a given table does not exist, then it is ignored.`,
	Synopsis: []string{
		"[-r {{.LessThan}}result format{{.GreaterThan}}] [{{.LessThan}}table{{.GreaterThan}}...]",
	},
}

type TagsCmd struct{}

var _ cli.Command = TagsCmd{}

func (cmd TagsCmd) Name() string {
	return "tags"
}

func (cmd TagsCmd) Description() string {
	return "Shows the column tags of one or more tables."
}

func (cmd TagsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, tblTagsDocs, ap))
}

func (cmd TagsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table(s) whose tags will be displayed."})
	ap.SupportsString(commands.FormatFlag, "r", "result output format", "How to format result output. Valid values are tabular, csv, json. Defaults to tabular.")
	return ap
}

func (cmd TagsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, tblTagsDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	tables := apr.Args()
	if len(tables) == 0 {
		root, verr := commands.GetWorkingWithVErr(dEnv)
		if verr != nil {
			return commands.HandleVErrAndExitCode(verr, usage)
		}
		var err error
		tables, err = root.GetTableNames(ctx)

		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("unable to get table names.").AddCause(err).Build(), usage)
		}

		tables = commands.RemoveDocsTbl(tables)
		if len(tables) == 0 {
			cli.Println("No tables in working set")
			return 0
		}
	}
	for i := 0; i < len(tables); i++ {
		tables[i] = fmt.Sprintf("'%s'", tables[i])
	}

	//TODO: implement REGEXP_SUBSTR in go-mysql-server and use it here instead of SUBSTR, as this will eventually break
	queryStr := fmt.Sprintf("SELECT table_name AS 'table', column_name AS 'column', "+
		"SUBSTR(extra, 5) AS tag FROM information_schema.columns WHERE table_name IN (%s)", strings.Join(tables, ","))

	if formatStr, ok := apr.GetValue(commands.FormatFlag); ok {
		return commands.SqlCmd{}.Exec(ctx, "", []string{
			fmt.Sprintf(`--%s=%s`, commands.FormatFlag, formatStr),
			fmt.Sprintf(`--%s`, commands.QueryFlag),
			queryStr + ";",
		}, dEnv)
	} else {
		return commands.SqlCmd{}.Exec(ctx, "", []string{
			fmt.Sprintf(`--%s`, commands.QueryFlag),
			queryStr + ";",
		}, dEnv)
	}
}
