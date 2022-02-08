// Copyright 2019 Dolthub, Inc.
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
	"io"
	"os"

	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	dsqle "github.com/dolthub/dolt/go/libraries/doltcore/sqle"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var schExportDocs = cli.CommandDocumentationContent{
	ShortDesc: "Exports table schemas as SQL DDL statements.",
	LongDesc: "Exports table schemas as SQL DDL statements, which can then be executed to recreate tables." + `

If ` + "`table`" + ` is given, only that table's schema will be exported, otherwise all table schemas will be exported.

If ` + "`file`" + ` is given, the exported schemas will be written to that file, otherwise they will be written to standard out.`,
	Synopsis: []string{
		"[{{.LessThan}}table{{.GreaterThan}}] [{{.LessThan}}file{{.GreaterThan}}]",
	},
}

type ExportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ExportCmd) Name() string {
	return "export"
}

// Description returns a description of the command
func (cmd ExportCmd) Description() string {
	return "Exports a table's schema in SQL form."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ExportCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, schExportDocs, ap))
}

func (cmd ExportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "table whose schema is being exported."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "the file to which the schema will be exported."})
	return ap
}

// EventType returns the type of the event to log
func (cmd ExportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_SCHEMA
}

// Exec executes the command
func (cmd ExportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, schExportDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr = exportSchemas(ctx, apr, root, dEnv)
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}

func exportSchemas(ctx context.Context, apr *argparser.ArgParseResults, root *doltdb.RootValue, dEnv *env.DoltEnv) errhand.VerboseError {
	var tblName string
	var fileName string
	switch apr.NArg() {
	case 0: // write all tables to stdout
	case 1:
		if doltdb.IsValidTableName(apr.Arg(0)) {
			tblName = apr.Arg(0)
		} else {
			fileName = apr.Arg(0)
		}
	case 2:
		tblName = apr.Arg(0)
		fileName = apr.Arg(1)
	default:
		return errhand.BuildDError("schema export takes at most two parameters.").SetPrintUsage().Build()
	}

	var wr io.Writer
	if fileName != "" {
		wc, err := dEnv.FS.OpenForWrite(fileName, os.ModePerm)
		if err != nil {
			return errhand.BuildDError("unable to open file %s for export", fileName).AddCause(err).Build()
		}
		defer wc.Close()
		wr = wc
	} else {
		wr = cli.CliOut
	}

	var tablesToExport []string
	var err error
	if tblName != "" {
		if doltdb.HasDoltPrefix(tblName) {
			return errhand.BuildDError("%s not found", tblName).Build()
		}
		tablesToExport = []string{tblName}
	} else {
		tablesToExport, err = doltdb.GetNonSystemTableNames(ctx, root)
		if err != nil {
			return errhand.BuildDError("error retrieving table names").AddCause(err).Build()
		}
	}

	for _, tn := range tablesToExport {
		opts := editor.Options{Deaf: dEnv.DbEaFactory()}
		verr := exportTblSchema(ctx, tn, root, wr, opts)
		if verr != nil {
			return verr
		}
	}

	return nil
}

func exportTblSchema(ctx context.Context, tblName string, root *doltdb.RootValue, wr io.Writer, opts editor.Options) errhand.VerboseError {
	sqlCtx, engine, _ := dsqle.PrepareCreateTableStmt(ctx, dsqle.NewUserSpaceDatabase(root, opts))

	stmt, err := dsqle.GetCreateTableStmt(sqlCtx, engine, tblName)
	if err != nil {
		return errhand.VerboseErrorFromError(err)
	}

	_, err = fmt.Fprintln(wr, stmt)
	return errhand.BuildIf(err, "error writing schema for table %s", tblName).AddCause(err).Build()
}
