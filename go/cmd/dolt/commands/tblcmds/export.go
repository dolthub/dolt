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

package tblcmds

import (
	"context"
	"io"
	"os"
	"path/filepath"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
)

var exportDocs = cli.CommandDocumentationContent{
	ShortDesc: `Export the contents of a table to a file.`,
	LongDesc: `{{.EmphasisLeft}}dolt table export{{.EmphasisRight}} will export the contents of {{.LessThan}}table{{.GreaterThan}} to {{.LessThan}}|file{{.GreaterThan}}

See the help for {{.EmphasisLeft}}dolt table import{{.EmphasisRight}} as the options are the same.
`,
	Synopsis: []string{
		"[-f] [-pk {{.LessThan}}field{{.GreaterThan}}] [-schema {{.LessThan}}file{{.GreaterThan}}] [-map {{.LessThan}}file{{.GreaterThan}}] [-continue] [-file-type {{.LessThan}}type{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}file{{.GreaterThan}}",
	},
}

type exportOptions struct {
	tableName  string
	force      bool
	dest       mvdata.DataLocation
	srcOptions interface{}
}

func (m exportOptions) checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS) (bool, error) {
	if _, isStream := m.dest.(mvdata.StreamDataLocation); isStream {
		return false, nil
	}
	if !m.force {
		return m.dest.Exists(ctx, root, fs)
	}
	return false, nil
}

func (m exportOptions) IsBatched() bool {
	return false
}

func (m exportOptions) WritesToTable() bool {
	return false
}

func (m exportOptions) SrcName() string {
	return m.tableName
}

func (m exportOptions) DestName() string {
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

// getExportDestination returns an export destination corresponding to the input parameters
func getExportDestination(apr *argparser.ArgParseResults) mvdata.DataLocation {
	path := ""
	if apr.NArg() > 1 {
		path = apr.Arg(1)
	}

	fType, _ := apr.GetValue(fileTypeParam)
	destLoc := mvdata.NewDataLocation(path, fType)

	switch val := destLoc.(type) {
	case mvdata.FileDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			cli.PrintErrln(
				color.RedString("Could not infer type file '%s'\n", path),
				"File extensions should match supported file types, or should be explicitly defined via the file-type parameter")
			return nil
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			destLoc = val
		} else if val.Format != mvdata.CsvFile && val.Format != mvdata.PsvFile {
			cli.PrintErrln(color.RedString("Cannot export this format to stdout"))
			return nil
		}
	}

	return destLoc
}

func parseExportArgs(ap *argparser.ArgParser, commandStr string, args []string) (*exportOptions, errhand.VerboseError) {
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, exportDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return nil, errhand.BuildDError("missing required argument").Build()
	} else if apr.NArg() > 2 {
		usage()
		return nil, errhand.BuildDError("too many arguments").Build()
	}

	tableName := apr.Arg(0)
	if !doltdb.IsValidTableName(tableName) {
		usage()
		cli.PrintErrln(
			color.RedString("'%s' is not a valid table name\n", tableName),
			"table names must match the regular expression:", doltdb.TableNameRegexStr)
		return nil, errhand.BuildDError("invalid table name").Build()
	}

	fileLoc := getExportDestination(apr)

	if fileLoc == nil {
		return nil, errhand.BuildDError("could not validate table export args").Build()
	}

	return &exportOptions{
		tableName: tableName,
		force:     apr.Contains(forceParam),
		dest:      fileLoc,
	}, nil
}

type ExportCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd ExportCmd) Name() string {
	return "export"
}

// Description returns a description of the command
func (cmd ExportCmd) Description() string {
	return "Export a table to a file."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd ExportCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.ArgParser()
	return commands.CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, exportDocs, ap))
}

func (cmd ExportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table being exported."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being output to."})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	return ap
}

// EventType returns the type of the event to log
func (cmd ExportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_EXPORT
}

// Exec executes the command
func (cmd ExportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	_, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, exportDocs, ap))

	exOpts, verr := parseExportArgs(ap, commandStr, args)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	rd, err := mvdata.NewSqlEngineReader(ctx, dEnv, exOpts.tableName)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Error creating reader for %s.", exOpts.SrcName()).AddCause(err).Build(), usage)
	}

	wr, verr := getTableWriter(ctx, root, dEnv, rd.GetSchema(), exOpts)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	pipeline := mvdata.NewDataMoverPipeline(ctx, rd, wr)

	err = pipeline.Execute()
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.BuildDError("Error opening writer for %s.", exOpts.DestName()).AddCause(err).Build(), usage)
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))
	return 0
}

func getTableWriter(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, rdSchema schema.Schema, exOpts *exportOptions) (table.SqlTableWriter, errhand.VerboseError) {
	ow, err := exOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}
	if ow {
		return nil, errhand.BuildDError("%s already exists. Use -f to overwrite.", exOpts.DestName()).Build()
	}

	err = dEnv.FS.MkDirs(filepath.Dir(exOpts.DestName()))
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	filePath, err := dEnv.FS.Abs(exOpts.DestName())
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}

	writer, err := dEnv.FS.OpenForWrite(filePath, os.ModePerm)
	if err != nil {
		return nil, errhand.BuildDError("Error opening writer for %s.", exOpts.DestName()).AddCause(err).Build()
	}

	wr, err := exOpts.dest.NewCreatingWriter(ctx, exOpts, root, rdSchema, editor.Options{Deaf: dEnv.DbEaFactory()}, writer)
	if err != nil {
		return nil, errhand.BuildDError("Error opening writer for %s.", exOpts.DestName()).AddCause(err).Build()
	}

	return wr, nil
}
