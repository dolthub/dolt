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
	"os"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/funcitr"
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
	tableName   string
	contOnErr   bool
	force       bool
	schFile     string
	mappingFile string
	primaryKeys []string
	src         mvdata.TableDataLocation
	dest        mvdata.DataLocation
	srcOptions  interface{}
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

func (m exportOptions) WritesToTable() bool {
	return false
}

func (m exportOptions) SrcName() string {
	return m.src.Name
}

func (m exportOptions) DestName() string {
	if t, tblDest := m.dest.(mvdata.TableDataLocation); tblDest {
		return t.Name
	}
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

// validateExportArgs validates the input from the arg parser, and returns the tuple:
// (table name to export, data location of table to export, data location to export to)
func validateExportArgs(apr *argparser.ArgParseResults, usage cli.UsagePrinter) (string, mvdata.TableDataLocation, mvdata.DataLocation) {
	if apr.NArg() == 0 || apr.NArg() > 2 {
		usage()
		return "", mvdata.TableDataLocation{}, nil
	}

	tableName := apr.Arg(0)
	if !doltdb.IsValidTableName(tableName) {
		cli.PrintErrln(
			color.RedString("'%s' is not a valid table name\n", tableName),
			"table names must match the regular expression:", doltdb.TableNameRegexStr)
		return "", mvdata.TableDataLocation{}, nil
	}

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
			return "", mvdata.TableDataLocation{}, nil
		}

	case mvdata.StreamDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			val = mvdata.StreamDataLocation{Format: mvdata.CsvFile, Reader: os.Stdin, Writer: iohelp.NopWrCloser(cli.CliOut)}
			destLoc = val
		} else if val.Format != mvdata.CsvFile && val.Format != mvdata.PsvFile {
			cli.PrintErrln(color.RedString("Cannot export this format to stdout"))
			return "", mvdata.TableDataLocation{}, nil
		}
	}

	tableLoc := mvdata.TableDataLocation{Name: tableName}

	return tableName, tableLoc, destLoc
}

func parseExportArgs(ap *argparser.ArgParser, commandStr string, args []string) (*exportOptions, errhand.VerboseError) {
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, exportDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	tableName, tableLoc, fileLoc := validateExportArgs(apr, usage)

	if fileLoc == nil || len(tableLoc.Name) == 0 {
		return nil, errhand.BuildDError("could not validate table export args").Build()
	}

	schemaFile, _ := apr.GetValue(schemaParam)
	mappingFile, _ := apr.GetValue(mappingFileParam)

	val, _ := apr.GetValue(primaryKeyParam)
	pks := funcitr.MapStrings(strings.Split(val, ","), strings.TrimSpace)
	pks = funcitr.FilterStrings(pks, func(s string) bool { return s != "" })

	return &exportOptions{
		tableName:   tableName,
		contOnErr:   apr.Contains(contOnErrParam),
		force:       apr.Contains(forceParam),
		schFile:     schemaFile,
		mappingFile: mappingFile,
		primaryKeys: pks,
		src:         tableLoc,
		dest:        fileLoc,
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
func (cmd ExportCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, exportDocs, ap))
}

func (cmd ExportCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table being exported."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being output to."})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsFlag(contOnErrParam, "", "Continue exporting when row export errors are encountered.")
	ap.SupportsString(schemaParam, "s", "schema_file", "The schema for the output data.")
	ap.SupportsString(mappingFileParam, "m", "mapping_file", "A file that lays out how fields should be mapped from input data to output data.")
	ap.SupportsString(primaryKeyParam, "pk", "primary_key", "Explicitly define the name of the field in the schema which should be used as the primary key.")
	ap.SupportsString(fileTypeParam, "", "file_type", "Explicitly define the type of the file if it can't be inferred from the file extension.")
	return ap
}

// EventType returns the type of the event to log
func (cmd ExportCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_TABLE_EXPORT
}

// Exec executes the command
func (cmd ExportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	_, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, exportDocs, ap))

	exOpts, verr := parseExportArgs(ap, commandStr, args)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	mover, verr := NewExportDataMover(ctx, root, dEnv, exOpts, importStatsCB)

	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	skipped, verr := mvdata.MoveData(ctx, dEnv, mover, exOpts)

	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))
	return 0
}

func NewExportDataMover(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, exOpts *exportOptions, statsCB noms.StatsCB) (*mvdata.DataMover, errhand.VerboseError) {
	var rd table.TableReadCloser
	var err error

	ow, err := exOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return nil, errhand.VerboseErrorFromError(err)
	}
	if ow {
		return nil, errhand.BuildDError("%s already exists. Use -f to overwrite.", exOpts.DestName()).Build()
	}

	rd, srcIsSorted, err := exOpts.src.NewReader(ctx, root, dEnv.FS, exOpts.srcOptions)

	if err != nil {
		return nil, errhand.BuildDError("Error creating reader for %s.", exOpts.SrcName()).AddCause(err).Build()
	}

	// close on err exit
	defer func() {
		if rd != nil {
			rd.Close(ctx)
		}
	}()

	inSch := rd.GetSchema()
	outSch := inSch

	wr, err := exOpts.dest.NewCreatingWriter(ctx, exOpts, dEnv, root, srcIsSorted, outSch, statsCB, true)

	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", exOpts.tableName).AddCause(err).Build()
	}

	emptyTransColl := pipeline.NewTransformCollection()

	imp := &mvdata.DataMover{Rd: rd, Transforms: emptyTransColl, Wr: wr, ContOnErr: exOpts.contOnErr}
	rd = nil

	return imp, nil
}
