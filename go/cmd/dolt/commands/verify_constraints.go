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

package commands

import (
	"context"
	"errors"
	"strings"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
)

var verifyConstraintsDocs = cli.CommandDocumentationContent{
	ShortDesc: `Verifies a table's constraints'`,
	LongDesc:  `This command verifies that the defined constraints on the given table(s)—such as a foreign key—are correct and satisfied.`,
	Synopsis:  []string{`{{.LessThan}}table{{.GreaterThan}}...`},
}

type VerifyConstraintsCmd struct{}

var _ cli.Command = VerifyConstraintsCmd{}
var _ cli.HiddenCommand = VerifyConstraintsCmd{}

func (cmd VerifyConstraintsCmd) Name() string {
	return "verify-constraints"
}

func (cmd VerifyConstraintsCmd) Description() string {
	return "Command to verify that the constraints on the given table(s) are satisfied."
}

func (cmd VerifyConstraintsCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	return nil
}

func (cmd VerifyConstraintsCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table to check constraints on."})
	return ap
}

func (cmd VerifyConstraintsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, verifyConstraintsDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 0
	}
	tableNames := apr.Args()
	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}
	fkColl, err := working.GetForeignKeyCollection(ctx)
	if err != nil {
		return HandleVErrAndExitCode(errhand.BuildDError("Unable to get foreign keys.").AddCause(err).Build(), nil)
	}

	var accumulatedConstraintErrors []string
	for _, givenTableName := range tableNames {
		tbl, tableName, ok, err := working.GetTableInsensitive(ctx, givenTableName)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("Unable to get table %s.", givenTableName).AddCause(err).Build(), nil)
		}
		if !ok {
			return HandleVErrAndExitCode(errhand.BuildDError("Table %s does not exist.", givenTableName).Build(), nil)
		}
		tblSch, err := tbl.GetSchema(ctx)
		if err != nil {
			return HandleVErrAndExitCode(errhand.BuildDError("Unable to get schema for %s.", tableName).AddCause(err).Build(), nil)
		}
		fks, _ := fkColl.KeysForTable(tableName)

		for _, fk := range fks {
			childIdx := tblSch.Indexes().GetByName(fk.TableIndex)
			childIdxRowData, err := tbl.GetIndexRowData(ctx, fk.TableIndex)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("Unable to get index data for %s.", fk.TableIndex).AddCause(err).Build(), nil)
			}

			parentTbl, _, ok, err := working.GetTableInsensitive(ctx, fk.ReferencedTableName)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("Unable to get table %s.", fk.ReferencedTableName).AddCause(err).Build(), nil)
			}
			if !ok {
				return HandleVErrAndExitCode(errhand.BuildDError("Table %s does not exist.", fk.ReferencedTableName).Build(), nil)
			}
			parentTblSch, err := parentTbl.GetSchema(ctx)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("Unable to get schema for %s.", fk.ReferencedTableName).AddCause(err).Build(), nil)
			}
			parentIdx := parentTblSch.Indexes().GetByName(fk.ReferencedTableIndex)
			parentIdxRowData, err := parentTbl.GetIndexRowData(ctx, fk.ReferencedTableIndex)
			if err != nil {
				return HandleVErrAndExitCode(errhand.BuildDError("Unable to get index data for %s.", fk.ReferencedTableIndex).AddCause(err).Build(), nil)
			}

			err = table.ForeignKeyIsSatisfied(ctx, fk, childIdxRowData, parentIdxRowData, childIdx, parentIdx)
			if err != nil {
				accumulatedConstraintErrors = append(accumulatedConstraintErrors, err.Error())
			}
		}
	}

	if len(accumulatedConstraintErrors) > 0 {
		dErr := errhand.BuildDError("All constraints are not satisfied.")
		dErr = dErr.AddCause(errors.New(strings.Join(accumulatedConstraintErrors, "\n")))
		return HandleVErrAndExitCode(dErr.Build(), nil)
	}
	return 0
}

func (cmd VerifyConstraintsCmd) Hidden() bool {
	return true
}
