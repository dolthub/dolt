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
	"github.com/dolthub/dolt/go/store/val"
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
	queryAndPrint(queryist.Context, queryist.Queryist, config, savedQueries)

	return 0
}

// queryAndPrint iterates through the jobs and steps for the given config, then runs each saved query and given assertion
func queryAndPrint(sqlCtx *sql.Context, queryist cli.Queryist, config *dolt_ci.WorkflowConfig, savedQueries map[string]string) {
	for _, job := range config.Jobs {
		cli.Println(color.GreenString("Running job: %s", job.Name.Value))

		jobFailures := make([]string, 0)
		for _, step := range job.Steps {
			// For DoltTest steps we print a header and list individual tests before summary
			_, isDoltTest := step.(*dolt_ci.DoltTestStep)
			if isDoltTest {
				cli.Println(fmt.Sprintf("Step: %s", step.GetName()))
			} else {
				cli.Printf("Step: %s - ", step.GetName())
			}

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
			} else if dt, ok := step.(*dolt_ci.DoltTestStep); ok {
				details, err = runDoltTestStep(sqlCtx, queryist, dt)
			} else {
				err = fmt.Errorf("unsupported step type")
			}

			if isDoltTest {
				if details != "" {
					cli.Println(details)
				}
				if err != nil {
					jobFailures = append(jobFailures, fmt.Sprintf("step '%s': %s", step.GetName(), err.Error()))
				}
			} else {
				if err != nil {
					cli.Println("FAIL")
					cli.Println(color.RedString("%s", err))
					jobFailures = append(jobFailures, fmt.Sprintf("step '%s': %s", step.GetName(), err.Error()))
				} else {
					cli.Println("PASS")
				}
			}
		}
		if len(jobFailures) > 0 {
			cli.Println(color.RedString("Result: FAIL"))
			cli.Println(color.RedString("%s", strings.Join(jobFailures, "; ")))
		} else {
			cli.Println(color.GreenString("Result: PASS"))
		}
	}
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

// runDoltTestStep evaluates a Dolt Test step per selection rules and requires all selected tests to PASS
func runDoltTestStep(sqlCtx *sql.Context, queryist cli.Queryist, dt *dolt_ci.DoltTestStep) (string, error) {
	testsProvided := len(dt.Tests) > 0
	groupsProvided := len(dt.TestGroups) > 0
	testsWildcard := testsProvided && len(dt.Tests) == 1 && strings.TrimSpace(dt.Tests[0].Value) == "*"
	groupsWildcard := groupsProvided && len(dt.TestGroups) == 1 && strings.TrimSpace(dt.TestGroups[0].Value) == "*"

	fetch := func(selector string) ([]sql.Row, error) {
		q := fmt.Sprintf("SELECT * FROM dolt_test_run('%s')", strings.ReplaceAll(selector, "'", "''"))
		return cli.GetRowsForSql(queryist, sqlCtx, q)
	}

	// Case 1: no explicit args → run all tests
	if !testsProvided && !groupsProvided {
		rows, err := cli.GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_test_run()")
		if err != nil {
			return "", err
		}
		details, failures, err := formatDoltTestRows(sqlCtx, rows)
		if err != nil {
			return "", err
		}
		if len(failures) > 0 {
			return details, errors.New(strings.Join(failures, "; "))
		}
		return details, nil
	}

	if testsProvided && !groupsProvided {
		// normalize tests: if any wildcard present, treat as wildcard only
		hasStar := false
		for _, t := range dt.Tests {
			if t.Value == "*" {
				hasStar = true
				break
			}
		}
		if hasStar {
			rows, err := cli.GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_test_run()")
			if err != nil {
				return "", err
			}
			details, failures, err := formatDoltTestRows(sqlCtx, rows)
			if err != nil {
				return "", err
			}
			if len(failures) > 0 {
				return details, errors.New(strings.Join(failures, "; "))
			}
			return details, nil
		}
		allRows := make([]sql.Row, 0)
		for _, t := range dt.Tests {
			rows, err := fetch(t.Value)
			if err != nil {
				return "", err
			}
			if len(rows) == 0 {
				return "", fmt.Errorf("test '%s' not found", t.Value)
			}
			allRows = append(allRows, rows...)
		}
		details, failures, err := formatDoltTestRows(sqlCtx, allRows)
		if err != nil {
			return "", err
		}
		if len(failures) > 0 {
			return details, errors.New(strings.Join(failures, "; "))
		}
		return details, nil
	}

	if groupsProvided && !testsProvided {
		// normalize groups: if any wildcard present, treat as wildcard only
		hasStar := false
		for _, g := range dt.TestGroups {
			if g.Value == "*" {
				hasStar = true
				break
			}
		}
		if hasStar {
			rows, err := cli.GetRowsForSql(queryist, sqlCtx, "SELECT * FROM dolt_test_run()")
			if err != nil {
				return "", err
			}
			details, failures, err := formatDoltTestRows(sqlCtx, rows)
			if err != nil {
				return "", err
			}
			if len(failures) > 0 {
				return details, errors.New(strings.Join(failures, "; "))
			}
			return details, nil
		}
		allRows := make([]sql.Row, 0)
		for _, g := range dt.TestGroups {
			rows, err := fetch(g.Value)
			if err != nil {
				return "", err
			}
			if len(rows) == 0 {
				return "", fmt.Errorf("group '%s' not found", g.Value)
			}
			allRows = append(allRows, rows...)
		}
		details, failures, err := formatDoltTestRows(sqlCtx, allRows)
		if err != nil {
			return "", err
		}
		if len(failures) > 0 {
			return details, errors.New(strings.Join(failures, "; "))
		}
		return details, nil
	}

	// both provided
	var allRows []sql.Row
	if testsWildcard && !groupsWildcard {
		// Run all tests for the specified groups (intersection of all tests with group selector)
		for _, g := range dt.TestGroups {
			rows, err := fetch(g.Value)
			if err != nil {
				return "", err
			}
			if len(rows) == 0 {
				return "", fmt.Errorf("group '%s' not found", g.Value)
			}
			allRows = append(allRows, rows...)
		}
	} else if groupsWildcard && !testsWildcard {
		// Run only the specified test names (across all groups)
		for _, t := range dt.Tests {
			rows, err := fetch(t.Value)
			if err != nil {
				return "", err
			}
			if len(rows) == 0 {
				return "", fmt.Errorf("test '%s' not found", t.Value)
			}
			allRows = append(allRows, rows...)
		}
	} else {
		// Neither is wildcard: for each group, run only the specified tests present in that group
		for _, g := range dt.TestGroups {
			rows, err := fetch(g.Value)
			if err != nil {
				return "", err
			}
			if len(rows) == 0 {
				return "", fmt.Errorf("group '%s' not found", g.Value)
			}
			groupTests := make(map[string]bool)
			for _, r := range rows {
				tName, err := getStringFromCol(sqlCtx, r[0])
				if err != nil {
					return "", err
				}
				groupTests[tName] = true
			}
			for _, t := range dt.Tests {
				if !groupTests[t.Value] {
					return "", fmt.Errorf("test '%s' not found in group '%s'", t.Value, g.Value)
				}
			}
			for _, r := range rows {
				tName, err := getStringFromCol(sqlCtx, r[0])
				if err != nil {
					return "", err
				}
				for _, t := range dt.Tests {
					if tName == t.Value {
						allRows = append(allRows, r)
					}
				}
			}
		}
	}
	details, failures, err := formatDoltTestRows(sqlCtx, allRows)
	if err != nil {
		return "", err
	}
	if len(failures) > 0 {
		return details, errors.New(strings.Join(failures, "; "))
	}
	return details, nil
}

// formatDoltTestRows returns a formatted summary of all tests and a list of failure messages
func formatDoltTestRows(sqlCtx *sql.Context, rows []sql.Row) (string, []string, error) {
	lines := make([]string, 0, len(rows)+1)
	failures := make([]string, 0)
	for _, row := range rows {
		tName, err := getStringFromCol(sqlCtx, row[0])
		if err != nil {
			return "", nil, err
		}
		gName, err := getStringFromCol(sqlCtx, row[1])
		if err != nil {
			return "", nil, err
		}
		status, err := getStringFromCol(sqlCtx, row[3])
		if err != nil {
			return "", nil, err
		}
		message, err := getStringFromCol(sqlCtx, row[4])
		if err != nil {
			return "", nil, err
		}
		statusUpper := strings.ToUpper(status)
		statusColored := statusUpper
		if statusUpper == "PASS" {
			statusColored = color.GreenString(statusUpper)
		} else if statusUpper == "FAIL" {
			statusColored = color.RedString(statusUpper)
		}
		line := fmt.Sprintf("  - test: %s (group: %s) - %s", tName, gName, statusColored)
		if statusUpper != "PASS" {
			if message == "" {
				message = "failed"
			}
			line = line + fmt.Sprintf(": %s", message)
			failures = append(failures, fmt.Sprintf("%s: %s", tName, message))
		}
		lines = append(lines, line)
	}
	return strings.Join(lines, "\n"), failures, nil
}

func getStringFromCol(sqlCtx *sql.Context, v interface{}) (string, error) {
	if ts, ok := v.(*val.TextStorage); ok {
		return ts.Unwrap(sqlCtx)
	} else if s, ok := v.(string); ok {
		return s, nil
	} else if v == nil {
		return "", nil
	} else {
		return "", fmt.Errorf("unexpected type %T, expected string", v)
	}
}
