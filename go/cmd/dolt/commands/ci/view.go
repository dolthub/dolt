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
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
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

// Hidden should return true if this command should be hidden from the help text
func (cmd ViewCmd) Hidden() bool {
	return false
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
			if name := savedQueries[step.SavedQueryName.Value]; name != "" {
				config.Jobs[i].Steps[j].SavedQueryStatement = yaml.Node{
					Kind:  yaml.ScalarNode,
					Style: yaml.DoubleQuotedStyle,
					Value: name,
				}
			} else {
				config.Jobs[i].Steps[j].SavedQueryStatement = yaml.Node{
					Kind:  yaml.ScalarNode,
					Style: yaml.DoubleQuotedStyle,
					Value: "saved query not found",
				}
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

func getSavedQueries(sqlCtx *sql.Context, queryist cli.Queryist) (map[string]string, error) {
	savedQueries := make(map[string]string)
	resetFunc, err := commands.SetSystemVar(queryist, sqlCtx, true)
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
			savedQueries[row[2].(string)] = row[3].(string)
		}
	}

	return savedQueries, nil
}
