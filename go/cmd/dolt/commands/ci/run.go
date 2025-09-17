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

	queryist, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	hasTables, err := dolt_ci.HasDoltCITables(queryist.Queryist, queryist.Context)
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

	wm := dolt_ci.NewWorkflowManager(name, email, queryist.Queryist.Query)

	config, err := wm.GetWorkflowConfig(queryist.Context, workflowName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	savedQueries, err := getSavedQueries(queryist.Context, queryist.Queryist)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
    cli.Println(color.CyanString("Running workflow: %s", workflowName))
    failed := queryAndPrint(queryist.Context, queryist.Queryist, config, savedQueries)

    if failed {
        return 1
    }
    return 0
}

// queryAndPrint iterates through the jobs and steps for the given config, then runs each saved query and given assertion
func queryAndPrint(sqlCtx *sql.Context, queryist cli.Queryist, config *dolt_ci.WorkflowConfig, savedQueries map[string]string) bool {
    // returns true if any job had failures
    overallFailed := false
	for _, job := range config.Jobs {
        cli.Println(color.CyanString("Running job: %s", job.Name.Value))

		jobFailures := make([]string, 0)
		for _, step := range job.Steps {
            // Print a step header; details will follow on subsequent lines
            _, isDoltTest := step.(*dolt_ci.DoltTestStep)
            _, isSavedQuery := step.(*dolt_ci.SavedQueryStep)
            cli.Println(color.CyanString("  Step: %s", step.GetName()))

			var err error
			var details string
            if sq, ok := step.(*dolt_ci.SavedQueryStep); ok {
                query := savedQueries[sq.SavedQueryName.Value]
                rows, qErr := runCIQuery(queryist, sqlCtx, sq, query)
                if qErr == nil {
                    err = assertQueries(rows, sq.ExpectedRows.Value, sq.ExpectedColumns.Value, query)
                } else {
                    err = qErr
                }
                details = formatSavedQueryDetails(sq.SavedQueryName.Value, query, err)
			} else if dt, ok := step.(*dolt_ci.DoltTestStep); ok {
				details, err = runDoltTestStep(sqlCtx, queryist, dt)
			} else {
				err = fmt.Errorf("unsupported step type")
			}

            // Print details for DoltTest and SavedQuery steps; they do not emit PASS/FAIL inline
            if (isDoltTest || isSavedQuery) && details != "" {
                cli.Println(indentLines(details, "  "))
			}

			// Unified failure handling
			if err != nil {
				jobFailures = append(jobFailures, fmt.Sprintf("step '%s': %s", step.GetName(), err.Error()))
			}
		}
        if len(jobFailures) > 0 {
            cli.Println(color.CyanString("Result of '%s':", job.Name.Value) + " " + color.RedString("FAIL"))
            overallFailed = true
		} else {
            cli.Println(color.CyanString("Result of '%s':", job.Name.Value) + " " + color.GreenString("PASS"))
		}
	}
    return overallFailed
}

// indentLines prefixes every line in s with the given prefix.
func indentLines(s, prefix string) string {
    if s == "" {
        return s
    }
    parts := strings.Split(s, "\n")
    for i := range parts {
        parts[i] = prefix + parts[i]
    }
    return strings.Join(parts, "\n")
}

// formatSavedQueryDetails returns indented detail lines for SavedQuery steps to match DoltTest formatting.
// It always prints the query line if available, and on error it prints additional error/info lines excluding
// any redundant "Ran query:" prefix since the query line is already shown.
func formatSavedQueryDetails(savedQueryName, query string, err error) string {
    // First line always shows the saved query name and status
    status := "PASS"
    if err != nil {
        status = "FAIL"
    }
    statusColored := status
    if status == "PASS" {
        statusColored = color.GreenString(status)
    } else {
        statusColored = color.RedString(status)
    }

    lines := []string{fmt.Sprintf("  - %s - %s", savedQueryName, statusColored)}

    // Only include query and error details on failure
    if err != nil {
        if strings.TrimSpace(query) != "" {
            lines = append(lines, fmt.Sprintf("    - query: %s", query))
        }
        // Summarize error by removing any redundant 'Ran query:' line
        var parts []string
        for _, l := range strings.Split(err.Error(), "\n") {
            if strings.HasPrefix(l, "Ran query: ") {
                continue
            }
            trimmed := strings.TrimSpace(l)
            if trimmed != "" {
                parts = append(parts, trimmed)
            }
        }
        if len(parts) > 0 {
            lines = append(lines, fmt.Sprintf("    - error: %s", color.RedString(strings.Join(parts, "; "))))
        }
    }
    return strings.Join(lines, "\n")
}

func runCIQuery(queryist cli.Queryist, sqlCtx *sql.Context, step *dolt_ci.SavedQueryStep, query string) ([]sql.Row, error) {
	if query == "" {
		return nil, fmt.Errorf("Could not find saved query: %s", step.SavedQueryName.Value)
	}

	rows, err := cli.GetRowsForSql(queryist, sqlCtx, query)
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
