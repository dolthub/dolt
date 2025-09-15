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
	"fmt"

	"github.com/dolthub/go-mysql-server/sql"
	"gopkg.in/yaml.v3"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	dtablefunctions "github.com/dolthub/dolt/go/libraries/doltcore/sqle/dtablefunctions"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/val"
)

var viewDocs = cli.CommandDocumentationContent{
	ShortDesc: "View details of a specific Dolt CI workflow",
	LongDesc:  "View details of a specific Dolt CI workflow including steps, configuration, and status",
	Synopsis: []string{
		"{{.LessThan}}workflow name{{.GreaterThan}}",
		"{{.LessThan}}workflow name{{.GreaterThan}} --job {{.LessThan}}job name{{.GreaterThan}}",
	},
}

type ViewCmd struct{}

// Name implements cli.Command.
func (cmd ViewCmd) Name() string {
	return "view"
}

// Description implements cli.Command.
func (cmd ViewCmd) Description() string {
	return viewDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd ViewCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd ViewCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(viewDocs, ap)
}

// ArgParser implements cli.Command.
func (cmd ViewCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	ap.SupportsString(cli.JobFlag, "j", "job", "View workflow details for the given {{.LessThan}}job name{{.GreaterThan}}")
	return ap
}

// Exec implements cli.Command.
func (cmd ViewCmd) Exec(ctx context.Context, commandStr string, args []string, _ *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, viewDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	// Check if workflow name provided
	if len(args) == 0 {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("workflow name is required")),
			usage)
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

	// Check if --job was used. A jobVal of "" will be used if the flag was not set, and toPrint will get the full workflow.
	jobVal, _ := apr.GetValue(cli.JobFlag)
	toPrint, err := updateConfigQueryStatements(config, savedQueries, jobVal)

	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	err = printWorkflowConfig(toPrint)

	return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
}

func printWorkflowConfig(toPrint interface{}) error {
	b, err := yaml.Marshal(toPrint)
	if err != nil {
		return err
	}

	cli.Println(string(b))
	return nil
}

func updateConfigQueryStatements(config *dolt_ci.WorkflowConfig, savedQueries map[string]string, jobName string) (interface{}, error) {
	for i, job := range config.Jobs {
		for j := range job.Steps {
			step := job.Steps[j]
			if sq, ok := step.(*dolt_ci.SavedQueryStep); ok {
				if name := savedQueries[sq.SavedQueryName.Value]; name != "" {
					// Replace the element in-place with updated statement
					sq.SavedQueryStatement = yaml.Node{
						Kind:  yaml.ScalarNode,
						Style: yaml.DoubleQuotedStyle,
						Value: name,
					}
					config.Jobs[i].Steps[j] = sq
				} else {
					sq.SavedQueryStatement = yaml.Node{
						Kind:  yaml.ScalarNode,
						Style: yaml.DoubleQuotedStyle,
						Value: "saved query not found",
					}
					config.Jobs[i].Steps[j] = sq
				}
			} else if dt, ok := step.(*dolt_ci.DoltTestStep); ok {
				// For dolt test steps, populate dolt_test_statements to show users what will be run
				stmts, err := previewDoltTestStatements(dt)
				if err != nil {
					return nil, err
				}
				dt.DoltTestStatements = nil
				for _, s := range stmts {
					dt.DoltTestStatements = append(dt.DoltTestStatements, yaml.Node{Kind: yaml.ScalarNode, Style: yaml.DoubleQuotedStyle, Value: s})
				}
				config.Jobs[i].Steps[j] = dt
			}
		}

		if job.Name.Value == jobName {
			return job, nil
		}
	}

	if jobName != "" {
		return nil, fmt.Errorf("cannot find job with name: %s", jobName)
	}

	return config, nil
}

// previewDoltTestStatements returns the SQL queries that would be executed by dolt_test_run
// for the given DoltTestStep selection (groups and tests). We use the same logic as dolt_test_run
// to resolve selections against the dolt_tests system table. If both groups and tests are empty,
// an empty list is returned.
func previewDoltTestStatements(dt *dolt_ci.DoltTestStep) ([]string, error) {
	// Build selection args: normalize wildcards so that presence of '*' in a field ignores other entries
	// If none provided, we'll show wildcard as a preview.
	args := make([]string, 0, len(dt.Tests)+len(dt.TestGroups))
	hasStarTests := false
	for _, t := range dt.Tests {
		if t.Value == "*" {
			hasStarTests = true
			break
		}
	}
	hasStarGroups := false
	for _, g := range dt.TestGroups {
		if g.Value == "*" {
			hasStarGroups = true
			break
		}
	}

	testsProvided := len(dt.Tests) > 0
	groupsProvided := len(dt.TestGroups) > 0

	switch {
	case testsProvided && groupsProvided:
		// If tests is wildcard and groups are specific → preview groups
		if hasStarTests && !hasStarGroups {
			for _, g := range dt.TestGroups {
				args = append(args, g.Value)
			}
			break
		}
		// If groups is wildcard and tests are specific → preview tests
		if hasStarGroups && !hasStarTests {
			for _, t := range dt.Tests {
				args = append(args, t.Value)
			}
			break
		}
		// Both wildcard (should be invalid by validation), but fall back to single wildcard
		if hasStarTests && hasStarGroups {
			args = []string{"*"}
			break
		}
		// Neither wildcard → preview both sets
		for _, t := range dt.Tests {
			args = append(args, t.Value)
		}
		for _, g := range dt.TestGroups {
			args = append(args, g.Value)
		}
	case testsProvided:
		if hasStarTests {
			args = []string{"*"}
		} else {
			for _, t := range dt.Tests {
				args = append(args, t.Value)
			}
		}
	case groupsProvided:
		if hasStarGroups {
			args = []string{"*"}
		} else {
			for _, g := range dt.TestGroups {
				args = append(args, g.Value)
			}
		}
	default:
		args = []string{"*"}
	}

	// Use the same helper the table function relies on to locate rows. We cannot instantiate a full engine here,
	// so we mirror the lookup strings that dolt_test_run uses for previewing: test names and group names.
	// We will format preview strings like dolt_test_run would accept: each arg stands alone.
	// Since we can't query here without engine, return the args themselves as indicative selectors.
	// Consumers can run: SELECT * FROM dolt_test_run('<arg>') to see full detail.
	// To provide better UX, if no args provided, show wildcard.
	// args already normalized above

	// Represent each selection as a dolt_test_run invocation for clarity
	stmts := make([]string, 0, len(args))
	for _, a := range args {
		stmts = append(stmts, fmt.Sprintf("SELECT * FROM %s('%s')", (&dtablefunctions.TestsRunTableFunction{}).Name(), a))
	}
	return stmts, nil
}

func getSavedQueries(sqlCtx *sql.Context, queryist cli.Queryist) (map[string]string, error) {
	savedQueries := make(map[string]string)
	resetFunc, err := cli.SetSystemVar(queryist, sqlCtx, true)
	if err != nil {
		return nil, err
	}

	_, rowIter, _, err := queryist.Query(sqlCtx, "SHOW TABLES LIKE 'dolt_query_catalog'")
	if err != nil {
		return nil, err
	}
	if resetFunc != nil {
		err = resetFunc()
		if err != nil {
			return nil, err
		}
	}

	rows, err := sql.RowIterToRows(sqlCtx, rowIter)
	if len(rows) > 0 {
		_, rowIter, _, err = queryist.Query(sqlCtx, "SELECT * FROM dolt_query_catalog")
		if err != nil {
			return nil, err
		}
		rows, err = sql.RowIterToRows(sqlCtx, rowIter)
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			var queryName, queryStatement string
			queryName, err = getStringColAsString(sqlCtx, row[2])
			if err != nil {
				return nil, err
			}
			queryStatement, err := getStringColAsString(sqlCtx, row[3])
			if err != nil {
				return nil, err
			}
			savedQueries[queryName] = queryStatement
		}
	}
	return savedQueries, nil
}

// The dolt_query_catalog system table returns *val.TextStorage types under certain situations,
// so we use a special parser to get the correct string values
func getStringColAsString(sqlCtx *sql.Context, tableValue interface{}) (string, error) {
	if ts, ok := tableValue.(*val.TextStorage); ok {
		return ts.Unwrap(sqlCtx)
	} else if str, ok := tableValue.(string); ok {
		return str, nil
	} else {
		return "", fmt.Errorf("unexpected type %T, was expecting string", tableValue)
	}
}
