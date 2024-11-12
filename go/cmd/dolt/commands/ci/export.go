// Copyright 2024 Dolthub, Inc.
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
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var exportDocs = cli.CommandDocumentationContent{
	ShortDesc: "Export a Dolt continuous integration workflow file by name",
	LongDesc:  "Export a Dolt continuous integration workflow file by name",
	Synopsis: []string{
		"{{.LessThan}}workflow name{{.GreaterThan}}",
	},
}

type ExportCmd struct{}

// Name implements cli.Command.
func (cmd ExportCmd) Name() string {
	return "export"
}

// Description implements cli.Command.
func (cmd ExportCmd) Description() string {
	return exportDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd ExportCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd ExportCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(exportDocs, ap)
}

// Hidden should return true if this command should be hidden from the help text
func (cmd ExportCmd) Hidden() bool {
	return true
}

// ArgParser implements cli.Command.
func (cmd ExportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

// Exec implements cli.Command.
func (cmd ExportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, exportDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if !cli.CheckEnvIsValid(dEnv) {
		return 1
	}

	var verr errhand.VerboseError
	verr = validateExportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	workflowName := apr.Arg(0)

	querist, sqlCtx, closeFunc, err := cliCtx.QueryEngine(ctx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	if closeFunc != nil {
		defer closeFunc()
	}

	user, email, err := env.GetNameAndEmail(dEnv.Config)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	hasTables, err := dolt_ci.HasDoltCITables(sqlCtx)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if !hasTables {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(fmt.Errorf("dolt ci has not been initialized, please initialize with: dolt ci init")), usage)
	}

	wr := dolt_ci.NewWorkflowManager(user, email, querist.Query)

	db, err := newDatabase(sqlCtx, sqlCtx.GetCurrentDatabase(), dEnv, false)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	config, err := wr.GetWorkflowConfig(sqlCtx, db, workflowName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	outpath, err := writeWorkflowConfig(config, cwd)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	cli.Println(color.CyanString(fmt.Sprintf("Dolt CI Workflow '%s' exported to %s.", config.Name.Value, outpath)))
	return 0
}

func writeWorkflowConfig(config *dolt_ci.WorkflowConfig, dir string) (outpath string, err error) {
	filename := strings.Replace(config.Name.Value, " ", "_", -1)
	outpath = filepath.Join(dir, fmt.Sprintf("%s.yaml", filename))
	var f *os.File
	f, err = os.Create(outpath)
	if err != nil {
		return
	}
	defer func() {
		rerr := f.Close()
		if err == nil {
			err = rerr
		}
	}()

	var r io.Reader
	r, err = dolt_ci.WorkflowConfigToYaml(config)
	if err != nil {
		return
	}

	_, err = io.Copy(f, r)
	return
}

func validateExportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 1 {
		return errhand.BuildDError("expected 1 argument").SetPrintUsage().Build()
	}
	return nil
}
