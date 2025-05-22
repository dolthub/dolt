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

package cvcmds

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/commands/engine"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/merge"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/types"
)

var verifyConstraintsDocs = cli.CommandDocumentationContent{
	ShortDesc: `Verifies that working set changes satisfy table constraints`,
	LongDesc: `Verifies that inserted or modified rows in the working set satisfy the defined table constraints.
               If any constraints are violated, they are documented in the dolt_constraint_violations system table.
               By default, this command does not consider row changes that have been previously committed.`,
	Synopsis: []string{`[--all] [--output-only] [{{.LessThan}}table{{.GreaterThan}}...]`},
}

type VerifyConstraintsCmd struct{}

var _ cli.Command = VerifyConstraintsCmd{}

func (cmd VerifyConstraintsCmd) Name() string {
	return "verify"
}

func (cmd VerifyConstraintsCmd) Description() string {
	return "Command to verify that the constraints on the given table(s) are satisfied."
}

func (cmd VerifyConstraintsCmd) GatedForNBF(nbf *types.NomsBinFormat) bool {
	return false
}

func (cmd VerifyConstraintsCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(verifyConstraintsDocs, ap)
}

func (cmd VerifyConstraintsCmd) ArgParser() *argparser.ArgParser {
	return cli.CreateVerifyConstraintsArgParser(cmd.Name())
}

func (cmd VerifyConstraintsCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, _ := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, verifyConstraintsDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	verifyAllRows := apr.Contains(cli.AllFlag)
	outputOnly := apr.Contains(cli.OutputOnlyFlag)
	working, err := dEnv.WorkingRoot(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}
	tableNames := apr.Args
	if len(tableNames) == 0 {
		tableNames, err = working.GetTableNames(ctx, doltdb.DefaultSchemaName)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to read table names.").AddCause(err).Build(), nil)
		}
	}
	tableSet := doltdb.NewTableNameSet(nil)

	// TODO: schema names
	for _, tableName := range tableNames {
		tableSet.Add(doltdb.TableName{Name: tableName})
	}

	comparingRoot, err := dEnv.HeadRoot(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head root.").AddCause(err).Build(), nil)
	}
	if verifyAllRows {
		comparingRoot, err = doltdb.EmptyRootValue(ctx, comparingRoot.VRW(), comparingRoot.NodeStore())
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to create an empty root.").AddCause(err).Build(), nil)
		}
	}

	cm, err := dEnv.HeadCommit(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head commit.").AddCause(err).Build(), nil)
	}
	h, err := cm.HashOf()
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to get head commit hash.").AddCause(err).Build(), nil)
	}

	endRoot, tablesWithViolations, err := merge.AddForeignKeyViolations(ctx, working, comparingRoot, tableSet, h)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to process constraint violations.").AddCause(err).Build(), nil)
	}

	err = dEnv.UpdateWorkingRoot(ctx, endRoot)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to update working root.").AddCause(err).Build(), nil)
	}

	if tablesWithViolations.Size() > 0 {
		cli.PrintErrln("All constraints are not satisfied.")
		eng, dbName, err := engine.NewSqlEngineForEnv(ctx, dEnv)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to build sql engine.").AddCause(err).Build(), nil)
		}
		sqlCtx, err := eng.NewLocalContext(ctx)
		if err != nil {
			return commands.HandleVErrAndExitCode(errhand.BuildDError("Failed to build sql context.").AddCause(err).Build(), nil)
		}
		defer sql.SessionEnd(sqlCtx.Session)
		sql.SessionCommandBegin(sqlCtx.Session)
		defer sql.SessionCommandEnd(sqlCtx.Session)
		sqlCtx.SetCurrentDatabase(dbName)

		for _, tableName := range tablesWithViolations.AsSortedSlice() {
			tbl, ok, err := endRoot.GetTable(sqlCtx, tableName)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Error loading table.").AddCause(err).Build(), nil)
			}
			if !ok {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to load table '%s'.", tableName).Build(), nil)
			}
			cli.Println("")
			cli.Println(doltdb.DoltConstViolTablePrefix + tableName.Name)
			dErr := printViolationsForTable(sqlCtx, dbName, tableName.Name, tbl, eng)
			if dErr != nil {
				return commands.HandleVErrAndExitCode(dErr, nil)
			}
		}

		if outputOnly {
			err = dEnv.UpdateWorkingRoot(sqlCtx, working)
			if err != nil {
				return commands.HandleVErrAndExitCode(errhand.BuildDError("Unable to undo written constraint violations").AddCause(err).Build(), nil)
			}
		}

		return 1
	}

	return 0
}

func printViolationsForTable(ctx *sql.Context, dbName, tblName string, tbl *doltdb.Table, eng *engine.SqlEngine) errhand.VerboseError {
	sch, err := tbl.GetSchema(ctx)
	if err != nil {
		return errhand.BuildDError("Error loading table schema").AddCause(err).Build()
	}

	colNames := strings.Join(sch.GetAllCols().GetColumnNames(), ", ")
	query := fmt.Sprintf("SELECT violation_type, %s, violation_info from dolt_constraint_violations_%s", colNames, tblName)

	sqlSch, sqlItr, _, err := eng.Query(ctx, query)
	if err != nil {
		return errhand.BuildDError("Error querying constraint violations").AddCause(err).Build()
	}

	limitItr := &sqlLimitIter{itr: sqlItr, limit: 50}

	err = engine.PrettyPrintResults(ctx, engine.FormatTabular, sqlSch, limitItr, false, false)
	if err != nil {
		return errhand.BuildDError("Error outputting rows").AddCause(err).Build()
	}

	if limitItr.hitLimit {
		cli.Printf("Over 50 constraint violations were found. Please query '%s' to see them all.\n", doltdb.DoltConstViolTablePrefix+tblName)
	}

	return nil
}

// returns io.EOF and sets hitLimit when |limit|+1 rows have been iterated from |itr|.
type sqlLimitIter struct {
	itr      sql.RowIter
	limit    uint64
	count    uint64
	hitLimit bool
}

var _ sql.RowIter = &sqlLimitIter{}

func (itr *sqlLimitIter) Next(ctx *sql.Context) (sql.Row, error) {
	r, err := itr.itr.Next(ctx)
	if err != nil {
		return nil, err
	}
	itr.count++
	if itr.count > itr.limit {
		itr.hitLimit = true
		return nil, io.EOF
	}
	return r, nil
}

func (itr *sqlLimitIter) Close(ctx *sql.Context) error {
	return itr.itr.Close(ctx)
}
