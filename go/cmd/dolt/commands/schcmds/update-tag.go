// Copyright 2022 Dolthub, Inc.
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
	"strconv"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/types"
)

// TODO: Update tag should be migrated to call the new dolt_update_column_tag() stored procedure

var updateTagDocs = cli.CommandDocumentationContent{
	ShortDesc: "Update the tag of the specified column",
	LongDesc: `{{.EmphasisLeft}}dolt schema update-tag{{.EmphasisRight}}

Update tag of the specified column. Useful to fix a merge that is throwing a
schema tag conflict.
`,
	Synopsis: []string{
		"{{.LessThan}}table{{.GreaterThan}} {{.LessThan}}column{{.GreaterThan}} {{.LessThan}}tag{{.GreaterThan}}",
	},
}

type UpdateTagCmd struct{}

var _ cli.Command = UpdateTagCmd{}

func (cmd UpdateTagCmd) Name() string {
	return "update-tag"
}

func (cmd UpdateTagCmd) Description() string {
	return "Update a column's tag"
}

func (cmd UpdateTagCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(updateTagDocs, ap)
}

func (cmd UpdateTagCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateUpdateTagArgParser()
}

func (cmd UpdateTagCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, updateTagDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if !types.IsFormat_DOLT(dEnv.DoltDB(ctx).Format()) {
		verr := errhand.BuildDError("update-tag is only available in storage format __DOLT__").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	if len(apr.Args) != 3 {
		verr := errhand.BuildDError("must provide <table> <column> <tag>").Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	tableName, columnName, tagStr := apr.Args[0], apr.Args[1], apr.Args[2]

	tag, err := strconv.ParseUint(tagStr, 10, 64)
	if err != nil {
		verr := errhand.BuildDError("failed to parse tag").AddCause(err).Build()
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	tbl, tName, ok, err := doltdb.GetTableInsensitive(ctx, root, doltdb.TableName{Name: tableName})
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to get table").Build(), usage)
	}
	if !ok {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("table %s does not exist", tableName).Build(), usage)
	}

	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to get schema").Build(), usage)
	}

	newSch, err := updateColumnTag(sch, columnName, tag)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to update column tag").AddCause(err).Build(), usage)
	}

	tbl, err = tbl.UpdateSchema(ctx, newSch)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to update table schema").AddCause(err).Build(), usage)
	}

	root, err = root.PutTable(ctx, doltdb.TableName{Name: tName}, tbl)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("failed to put table in root").AddCause(err).Build(), usage)
	}

	verr = commands.UpdateWorkingWithVErr(dEnv, root)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	return commands.HandleVErrAndExitCode(nil, usage)
}

func updateColumnTag(sch schema.Schema, name string, tag uint64) (schema.Schema, error) {
	var found bool
	columns := sch.GetAllCols().GetColumns()
	// Find column and update its tag
	for i, col := range columns {
		if col.Name == name {
			col.Tag = tag
			columns[i] = col
			found = true
			break
		}
	}

	if !found {
		return nil, fmt.Errorf("column %s does not exist", name)
	}

	newSch, err := schema.SchemaFromCols(schema.NewColCollection(columns...))
	if err != nil {
		return nil, err
	}

	err = newSch.SetPkOrdinals(sch.GetPkOrdinals())
	if err != nil {
		return nil, err
	}
	newSch.SetCollation(sch.GetCollation())

	return newSch, nil
}
