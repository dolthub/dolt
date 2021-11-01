package commands

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	eventsapi "github.com/dolthub/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/mvdata"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/editor"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
	"github.com/fatih/color"
	"os"
)

const (
	forceParam       = "force"
	formatParam	 	 = "result-format"
)

var dumpDocs = cli.CommandDocumentationContent{
	ShortDesc: `Export all tables to a file.`,
	LongDesc: `{{.EmphasisLeft}}dolt dump{{.EmphasisRight}} will export the contents of all {{.LessThan}}table{{.GreaterThan}} to {{.LessThan}}|file{{.GreaterThan}}
`,

	Synopsis: []string{
		"[--options] [{{.LessThan}}commit{{.GreaterThan}}]",
	},
}

type exportOptions struct {
	tableName   string
	force       bool
	format 		string
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

// getExportDestination returns an export destination corresponding to the input parameters
func getExportDestination(path string) mvdata.DataLocation {

	destLoc := mvdata.NewDataLocation(path, "")

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
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, dumpDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() > 0 {
		usage()
		return nil, errhand.BuildDError("too many arguments").Build()
	}

	var fileName string
	resultFormat, _ := apr.GetValue(formatParam)

	switch resultFormat {
	case "", "sql", ".sql":
		fileName = "doltdump.sql"
	case "csv", ".csv":
		//handle CSV filetype
		//maybe create dir 'doltdump' and put all the csv dump files into it
	default:
		usage()
		return nil, errhand.BuildDError("invalid result format").Build()
	}

	//fileLoc := getExportDestination(apr.Arg(0))
	fileLoc := getExportDestination(fileName)

	return &exportOptions{
		//tableName:   tableName,
		force:       apr.Contains(forceParam),
		format:		 resultFormat,
		//src:         tableLoc,
		dest:        fileLoc,
	}, nil
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
func (cmd DumpCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, lsDocs, ap))
}

func (cmd DumpCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	//ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"file", "The file being output to."})
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the force flag will allow the target to be overwritten.")
	ap.SupportsString(formatParam, "r", "result_file_type", "Define the type of the output file. Valid values are sql and csv. Defaults to sql.")

	return ap
}

func importStatsCB(stats types.AppliedEditStats) {
	noEffect := stats.NonExistentDeletes + stats.SameVal
	total := noEffect + stats.Modifications + stats.Additions
	displayStr := fmt.Sprintf("Rows Processed: %d, Additions: %d, Modifications: %d, Had No Effect: %d", total, stats.Additions, stats.Modifications, noEffect)
	displayStrLen = cli.DeleteAndPrint(displayStrLen, displayStr)
}

// EventType returns the type of the event to log
func (cmd DumpCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_DUMP
}

// Exec executes the command
func (cmd DumpCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	_, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, dumpDocs, ap))

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	tblNames, err := doltdb.GetNonSystemTableNames(ctx, root)
	if err != nil {
		errhand.BuildDError("error: failed to get tables").AddCause(err).Build()
	}

	if len(tblNames) == 0 {
		cli.Println("No tables to export\n")
		return 0
	}

	exOpts, verr := parseExportArgs(ap, commandStr, args)
	if verr != nil {
		return HandleVErrAndExitCode(verr, usage)
	}

	ow, err := exOpts.checkOverwrite(ctx, root, dEnv.FS)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	if ow {
		return HandleVErrAndExitCode(errhand.BuildDError("%s already exists. Use -f to overwrite.", exOpts.DestName()).Build(), usage)
	}

	// create new file
	err = dEnv.FS.MkDirs(filepath.Dir(exOpts.DestName()))
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	filePath, _ := dEnv.FS.Abs(exOpts.DestName())
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, os.ModePerm)

	//cli.Printf("Tables exporting:\n")
	for _, tbl := range tblNames {
		//cli.Println("\t", tbl)

		exOpts.tableName = tbl
		exOpts.src = mvdata.TableDataLocation{Name: tbl}

		mover, verr := NewExportDataMover(ctx, root, dEnv, exOpts, importStatsCB, filePath)
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}

		skipped, verr := mvdata.MoveData(ctx, dEnv, mover, exOpts)
		if skipped > 0 {
			cli.PrintErrln(color.YellowString("Lines skipped: %d", skipped))
		}
		if verr != nil {
			return HandleVErrAndExitCode(verr, usage)
		}
	}

	cli.PrintErrln(color.CyanString("Successfully exported data."))

	return 0
}

func NewExportDataMover(ctx context.Context, root *doltdb.RootValue, dEnv *env.DoltEnv, exOpts *exportOptions, statsCB noms.StatsCB, filePath string) (*mvdata.DataMover, errhand.VerboseError) {
	var rd table.TableReadCloser
	var err error

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

	opts := editor.Options{Deaf: dEnv.DbEaFactory()}

	writer, wErr := dEnv.FS.OpenForWriteAppend(filePath, os.ModePerm)
	if wErr != nil {
		return nil, errhand.BuildDError("Error opening writer for %s.", exOpts.DestName()).AddCause(wErr).Build()
	}

	wr, err := exOpts.dest.NewCreatingWriter(ctx, exOpts, root, srcIsSorted, outSch, statsCB, opts, writer)
	if err != nil {
		return nil, errhand.BuildDError("Could not create table writer for %s", exOpts.tableName).AddCause(err).Build()
	}

	emptyTransColl := pipeline.NewTransformCollection()

	imp := &mvdata.DataMover{Rd: rd, Transforms: emptyTransColl, Wr: wr, ContOnErr: false}
	rd = nil

	return imp, nil
}
