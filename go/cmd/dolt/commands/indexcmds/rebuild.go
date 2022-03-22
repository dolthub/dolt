// Copyright 2020 Dolthub, Inc.
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

package indexcmds

import (
	"context"
	"io"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var rebuildDocs = cli.CommandDocumentationContent{
	ShortDesc: `Rebuild the contents of an index`,
	LongDesc: IndexCmdWarning + `
This command will clear the contents that are currently in an index, and rebuild them from the working set. If the index were to ever get out of sync (which is a bug), this would allow for a temporary fix to get the index functioning properly again, while the root cause is being debugged.

In most cases, running this command should not have any overall effect, as the rebuilt index will be the same as the current index.`,
	Synopsis: []string{
		`{{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
	},
}

type RebuildCmd struct{}

func (cmd RebuildCmd) Name() string {
	return "rebuild"
}

func (cmd RebuildCmd) Description() string {
	return "Internal debugging command to rebuild the contents of an index."
}

func (cmd RebuildCmd) CreateMarkdown(_ io.Writer, _ string) error {
	return nil
}

func (cmd RebuildCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index to rebuild."})
	return ap
}

func (cmd RebuildCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, rebuildDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 0
	} else if apr.NArg() != 2 {
		return HandleErr(errhand.BuildDError("Both the table and index names must be provided.").Build(), usage)
	}

	working, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}

	tableName := apr.Arg(0)
	indexName := apr.Arg(1)

	table, ok, err := working.GetTable(ctx, tableName)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get table `%s`.", tableName).AddCause(err).Build(), nil)
	}
	if !ok {
		return HandleErr(errhand.BuildDError("The table `%s` does not exist.", tableName).Build(), nil)
	}
	opts := editor.Options{Deaf: dEnv.DbEaFactory(), Tempdir: dEnv.TempTableFilesDir()}
	indexRowData, err := editor.RebuildIndex(ctx, table, indexName, opts)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to rebuild index `%s` on table `%s`.", indexName, tableName).AddCause(err).Build(), nil)
	}
	updatedTable, err := table.SetNomsIndexRows(ctx, indexName, indexRowData)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to set rebuilt index.").AddCause(err).Build(), nil)
	}
	working, err = working.PutTable(ctx, tableName, updatedTable)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to set the table for the rebuilt index.").AddCause(err).Build(), nil)
	}
	err = dEnv.UpdateWorkingRoot(ctx, working)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to update the working set.").AddCause(err).Build(), nil)
	}

	return 0
}
