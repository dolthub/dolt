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

package commands

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

const (
	forceParam    = "force"
	directoryFlag = "directory"
	filenameFlag  = "file-name"
)

var dumpDocs = cli.CommandDocumentationContent{
	ShortDesc: `Export all tables.`,
	LongDesc: `{{.EmphasisLeft}}dolt dump{{.EmphasisRight}} will export {{.LessThan}}table{{.GreaterThan}} to {{.LessThan}}file{{.GreaterThan}}. 
If a dump file already exists then the operation will fail,unless the {{.EmphasisLeft}}--force | -f{{.EmphasisRight}} flag 
is provided. The force flag forces the existing dump file to be overwritten.
`,

	Synopsis: []string{
		"[options] [{{.LessThan}}commit{{.GreaterThan}}]",
		"[-f] [-r {{.LessThan}}result-format{{.GreaterThan}}] ",
	},
}

type DumpCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd DumpCmd) Name() string {
	return "dump"
}

// Description returns a description of the command
func (cmd DumpCmd) Description() string {
	return "Export all tables in the working set into a file."
}

// CreateMarkdown creates a markdown file containing the help text for the command at the given path
func (cmd DumpCmd) CreateMarkdown(wr io.Writer, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(wr, cli.GetCommandDocumentation(commandStr, lsDocs, ap))
}

func (cmd DumpCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsString(FormatFlag, "r", "result_file_type", "Define the type of the output file. Valid values are sql and csv. Defaults to sql.")
	ap.SupportsString(filenameFlag, "", "file_name", "Define file name for dump file. Valid value is sql only.")
	ap.SupportsString(directoryFlag, "", "directory_name", "Define directory name to dump the files in. Valid file types are csv and json. Defaults to csv.")

	return ap
}

// EventType returns the type of the event to log
func (cmd DumpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DUMP
}

// Exec executes the command
func (cmd DumpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, dumpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 0 {
		return HandleVErrAndExitCode(errhand.BuildDError("too many arguments").SetPrintUsage().Build(), usage)
	}

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	tblNames, err := doltdb.GetNonSystemTableNames(ctx, root)
	if err != nil {
		errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
	}
	if len(tblNames) == 0 {
		cli.Println("No tables to export.")
		return 0
	}

	force := apr.Contains(forceParam)
	resultFormat, _ := apr.GetValue(FormatFlag)
	fn, fnOk := apr.GetValue(filenameFlag)
	dn, dnOk := apr.GetValue(directoryFlag)

	switch resultFormat {
	case "", "sql", ".sql":
		if dnOk {
			return HandleVErrAndExitCode(errhand.BuildDError("no directory name needed for .sql type").SetPrintUsage().Build(), usage)
		}

		var fileName string
		if fnOk {
			fileName = fn
			if fileName[len(fileName)-4:] != ".sql" {
				fileName += ".sql"
			}
		} else {
			fileName = "doltdump.sql"
		}

		dumpOpts := getDumpOptions(fileName, resultFormat)
		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, fileName)
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}

		for _, tbl := range tblNames {
			tblOpts := newTableArgs(tbl, dumpOpts.dest)

			err = dumpTable(ctx, root, dEnv, tblOpts, fPath)
			if err != nil {
				return HandleVErrAndExitCode(err, usage)
			}
		}
	case "csv", ".csv":
		if fnOk {
			return HandleVErrAndExitCode(errhand.BuildDError("no filename needed for .csv").SetPrintUsage().Build(), usage)
		}

		err = dumpTables(ctx, root, dEnv, force, tblNames, ".csv", dn+"/")
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	case "json", ".json":
		if fnOk {
			return HandleVErrAndExitCode(errhand.BuildDError("no filename needed for .json").SetPrintUsage().Build(), usage)
		}

		err = dumpTables(ctx, root, dEnv, force, tblNames, ".json", dn+"/")
		if err != nil {
			return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
		}
	default:
		return HandleVErrAndExitCode(errhand.BuildDError("invalid result format").SetPrintUsage().Build(), usage)
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))

	return 0
}

type dumpOptions struct {
	format string
	dest   mvdata.DataLocation
}

type tableOptions struct {
	tableName  string
	src        mvdata.TableDataLocation
	dest       mvdata.DataLocation
	srcOptions interface{}
}

func (m tableOptions) WritesToTable() bool {
	return false
}

func (m tableOptions) SrcName() string {
	return m.src.Name
}

func (m tableOptions) DestName() string {
	if t, tblDest := m.dest.(mvdata.TableDataLocation); tblDest {
		return t.Name
	}
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

func (m dumpOptions) DumpDestName() string {
	if t, tblDest := m.dest.(mvdata.TableDataLocation); tblDest {
		return t.Name
	}
	if f, fileDest := m.dest.(mvdata.FileDataLocation); fileDest {
		return f.Path
	}
	return m.dest.String()
}

// dumpTable dumps table in file given specific table and file location info
func dumpTable(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, tblOpts *tableOptions, filePath string) errhand.VerboseError {
	mover, verr := NewDumpDataMover(ctx, root, dEnv, tblOpts, importStatsCB, filePath)
	if verr != nil {
		return verr
	}

	skipped, verr := mvdata.MoveData(ctx, dEnv, mover, tblOpts)
	if verr != nil {
		return verr
	}
	if skipped > 0 {
		cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
	}

	return nil
}

// checkAndCreateOpenDestFile returns filePath to created dest file after checking for any existing file and handles it
func checkAndCreateOpenDestFile(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, force bool, dumpOpts *dumpOptions, fileName string) (string, errhand.VerboseError) {
	ow, err := checkOverwrite(ctx, root, dEnv.FS, force, dumpOpts.dest)
	if err != nil {
		return "", errhand.VerboseErrorFromError(err)
	}
	if ow {
		return "", errhand.BuildDError("%s already exists. Use -f to overwrite.", fileName).Build()
	}

	// create new file
	err = dEnv.FS.MkDirs(filepath.Dir(dumpOpts.DumpDestName()))
	if err != nil {
		return "", errhand.VerboseErrorFromError(err)
	}

	filePath, err := dEnv.FS.Abs(fileName)
	if err != nil {
		return "", errhand.VerboseErrorFromError(err)
	}

	os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	return filePath, nil
}

// checkOverwrite returns TRUE if the file exists and force flag not given and
// FALSE if the file is stream data / file does not exist / file exists and force flag is given
func checkOverwrite(ctx context.Context, root *doltdb.RootValue, fs filesys.ReadableFS, force bool, dest mvdata.DataLocation) (bool, error) {
	if _, isStream := dest.(mvdata.StreamDataLocation); isStream {
		return false, nil
	}
	if !force {
		return dest.Exists(ctx, root, fs)
	}
	return false, nil
}

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	displayStr := fmt.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

// getDumpDestination returns a dump destination corresponding to the input parameters
func getDumpDestination(path string) mvdata.DataLocation {
	destLoc := mvdata.NewDataLocation(path, "")

	switch val := destLoc.(type) {
	case mvdata.FileDataLocation:
		if val.Format == mvdata.InvalidDataFormat {
			cli.PrintErrln(
				color.RedString("Could not infer type file '%s'\n", path),
				"File extensions should match supported file types, or should be explicitly defined via the result-format parameter")
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

// getDumpArgs returns dumpOptions of result format and dest file location corresponding to the input parameters
func getDumpOptions(fileName string, rf string) *dumpOptions {
	fileLoc := getDumpDestination(fileName)

	return &dumpOptions{
		format: rf,
		dest:   fileLoc,
	}
}

// newTableArgs returns tableOptions of table name and src table location and dest file location
// corresponding to the input parameters
func newTableArgs(tblName string, destination mvdata.DataLocation) *tableOptions {
	return &tableOptions{
		tableName: tblName,
		src:       mvdata.TableDataLocation{Name: tblName},
		dest:      destination,
	}
}

// dumpTables returns nil if all tables is dumped successfully, and it returns err if there is one.
// It handles only csv and json file types(rf).
func dumpTables(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, force bool, tblNames []string, rf string, dirName string) errhand.VerboseError {
	var fName string
	if dirName == "/" {
		dirName = "doltdump/"
	}
	for _, tbl := range tblNames {
		fName = dirName + tbl + rf
		dumpOpts := getDumpOptions(fName, rf)

		fPath, err := checkAndCreateOpenDestFile(ctx, root, dEnv, force, dumpOpts, fName)
		if err != nil {
			return err
		}

		tblOpts := newTableArgs(tbl, dumpOpts.dest)

		err = dumpTable(ctx, root, dEnv, tblOpts, fPath)
		if err != nil {
			return err
		}
	}
	return nil
}

// NewDumpDataMover returns dataMover with tableOptions given source table and destination file info
func NewDumpDataMover(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, tblOpts *tableOptions, statsCB noms.StatsCB, filePath string) (retDataMover *mvdata.DataMover, retErr errhand.VerboseError) {
	var err error

	rd, srcIsSorted, err := tblOpts.src.NewReader(ctx, root, dEnv.FS, tblOpts.srcOptions)
	if err != nil {
		return nil, errhand.BuildDError("Error creating reader for %s.", tblOpts.SrcName()).AddCause(err).Build()
	}

	// close on err exit
	defer func() {
		if rd != nil {
			rErr := rd.Close(ctx)
			if rErr != nil {
				retErr = errhand.BuildDError("Could not close reader for %s.", tblOpts.SrcName()).AddCause(rErr).Build()
			}
		}
	}()

	inSch := rd.GetSchema()
	outSch := inSch

	opts := editor.Options{Deaf: dEnv.DbEaFactory()}

	writer, err := dEnv.FS.OpenForWriteAppend(filePath, os.ModePerm)
	if err != nil {
		return nil, errhand.BuildDError("Error opening writer for %s.", tblOpts.DestName()).AddCause(err).Build()
	}

	wr, err := tblOpts.dest.NewCreatingWriter(ctx, tblOpts, root, srcIsSorted, outSch, statsCB, opts, writer)
	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", tblOpts.tableName).AddCause(err).Build()
	}

	emptyTransColl := pipeline.NewTransformCollection()

	imp := &mvdata.DataMover{Rd: rd, Transforms: emptyTransColl, Wr: wr, ContOnErr: false}
	rd = nil

	return imp, nil
}
