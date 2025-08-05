// Copyright 2025 Dolthub, Inc.
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

package ci

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/dolthub/go-mysql-server/sql"
	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var runDocs = cli.CommandDocumentationContent{
	ShortDesc: "Run a Dolt CI workflow",
	LongDesc:  "Run a Dolt CI workflow by executing all saved queries and validating their results",
	Synopsis: []string{
		"{{.LessThan}}workflow name{{.GreaterThan}}",
	},
}

type RunCmd struct{}

// Name implements cli.Command.
func (cmd RunCmd) Name() string {
	return "run"
}

// Description implements cli.Command.
func (cmd RunCmd) Description() string {
	return runDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd RunCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd RunCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(runDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd RunCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

// Exec implements cli.Command.
func (cmd RunCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, runDocs, ap))
	_ = cli.ParseArgsOrDie(ap, args, help)

	if len(args) == 0 {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("must specify workflow name")), usage)
	}
	workflowName := args[0]

	queryist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	hasTables, err := dolt_ci.HasDoltCITables(queryist, sqlCtx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if !hasTables {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("dolt ci has not been initialized, please initialize with: dolt ci init")), usage)
	}

	name, email, err := env.GetNameAndEmail(cliCtx.Config())
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	wm := dolt_ci.NewWorkflowManager(name, email, queryist.Query)

	config, err := wm.GetWorkflowConfig(sqlCtx, workflowName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	savedQueries, err := getSavedQueries(sqlCtx, queryist)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	cli.Println(color.CyanString("Running workflow: %s", workflowName))
	queryAndPrint(sqlCtx, queryist, config, savedQueries)

	return 0
}

// queryAndPrint iterates through the jobs and steps for the given config, then runs each saved query and given assertion
func queryAndPrint(sqlCtx *sql.Context, queryist cli.Queryist, config *dolt_ci.WorkflowConfig, savedQueries map[string]string) {
	for _, job := range config.Jobs {
		cli.Println(color.GreenString("Running job: %s", job.Name.Value))
		for _, step := range job.Steps {
			cli.Printf("Step: %s - ", step.Name.Value)

			// We break up the query running and assertions into two steps.
			// If either fails, we'll set error and let the user know that the step failed, then give an error message
			query := savedQueries[step.SavedQueryName.Value]
			rows, err := runCIQuery(queryist, sqlCtx, step, query)
			if err == nil {
				err = assertQueries(rows, step.ExpectedRows.Value, step.ExpectedColumns.Value, query)
			}

			if err != nil {
				cli.Println("FAIL")
				cli.Println(color.RedString("%s", err))
			} else {
				cli.Println("PASS")
			}
		}
	}
}

func runCIQuery(queryist cli.Queryist, sqlCtx *sql.Context, step dolt_ci.Step, query string) ([]sql.Row, error) {
	if query == "" {
		return nil, fmt.Errorf("Could not find saved query: %s", step.SavedQueryName.Value)
	}

	rows, err := commands.GetRowsForSql(queryist, sqlCtx, query)
	if err != nil {
		statementErr := fmt.Sprintf("Ran query: %s", query)
		queryErr := fmt.Sprintf("Query error: %s", err.Error())
		err = errors.New(strings.Join([]string{statementErr, queryErr}, "\n"))

		return nil, err
	}

	return rows, nil
}

// assertQueries takes in the result of a saved query execution, and the unparsed assertions,
// then returns if the assertions failed
func assertQueries(rows []sql.Row, expectedRowsAndComparison string, expectedColumnsAndComparison string, query string) error {
	var colCount int64
	var errs []string
	rowCount := int64(len(rows))
	if rowCount > 0 {
		colCount = int64(len(rows[0]))
	}

	colCompType, expectedCols, err := dolt_ci.ParseSavedQueryExpectedResultString(expectedColumnsAndComparison)
	if colCompType != dolt_ci.WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified {
		err = dolt_ci.ValidateQueryExpectedRowOrColumnCount(colCount, expectedCols, colCompType, "column")
		if err != nil {
			errStr := fmt.Sprintf("Assertion failed: %s", err.Error())
			errs = append(errs, errStr)
		}
	}
	rowCompType, expectedRows, err := dolt_ci.ParseSavedQueryExpectedResultString(expectedRowsAndComparison)
	if rowCompType != dolt_ci.WorkflowSavedQueryExpectedRowColumnComparisonTypeUnspecified {
		err = dolt_ci.ValidateQueryExpectedRowOrColumnCount(rowCount, expectedRows, rowCompType, "row")
		if err != nil {
			errStr := fmt.Sprintf("Assertion failed: %s", err.Error())
			errs = append(errs, errStr)
		}
	}

	if len(errs) > 0 {
		statementErr := fmt.Sprintf("Ran query: %s", query)
		errs := append([]string{statementErr}, errs...)
		return errors.New(strings.Join(errs, "\n"))
	}
	return nil
}
