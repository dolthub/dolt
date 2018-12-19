package edit

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/edit/mvdata"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/table/typed/noms"
	"os"
)

const (
	tableParam       = "table"
	outSchemaParam   = "schema"
	mappingFileParam = "map"
	forceParam       = "force"
	contOnErrParam   = "continue"
	primaryKeyParam  = "pk"
	fileTypeParam    = "file-type"
)

func createUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func invalidOptions(fs *flag.FlagSet, errFmt string, args ...interface{}) (*mvdata.DataLocation, *mvdata.DataLocation) {
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, color.RedString(errFmt))
	} else {
		fmt.Fprintln(os.Stderr, color.RedString(errFmt, args...))
	}
	fs.Usage()
	return nil, nil
}

func initCreateFlagSet(commandStr string) (*flag.FlagSet, *cli.StrArgMap, *cli.BoolFlagMap) {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = createUsage(fs)

	argMap := cli.NewStrArgMap(fs, map[string]string{
		outSchemaParam:   "The schema for the output data.",
		mappingFileParam: "A file that lays out how fields should be mapped from input data to output data",
		tableParam:       "Destination of where the new data should be imported to.",
		primaryKeyParam:  "Explicitly define the name of the field in the schema which should be used as the primary key.",
		fileTypeParam:    "Explicitly define the type of the file if it can't be inferred from the file extension",
	})

	flagMap := cli.NewBoolFlagMap(fs, map[string]string{
		forceParam:     "If a create operation is being executed, data already exists in the destination, the Force flag will allow the target to be overwritten",
		contOnErrParam: "Continue importing when row import errors are encountered."})

	return fs, argMap, flagMap
}

func validateCreateOrExportArgs(fs *flag.FlagSet, args []string, argMap *cli.StrArgMap, flagMap *cli.BoolFlagMap) (*mvdata.DataLocation, *mvdata.DataLocation) {
	fs.Parse(args)

	argMap.Update()
	emptyArgs := argMap.GetEmpty()

	for _, required := range []string{tableParam} {
		if emptyArgs.Contains(required) {
			return invalidOptions(fs, "Missing required paramater -%s", required)
		}
	}

	tableName := argMap.Get(tableParam)
	if !doltdb.IsValidTableName(tableName) {
		fmt.Fprintln(
			os.Stderr,
			color.RedString("\"%s\" is not a valid table name\n", tableName),
			"table names must match the regular expression", tableParam)
		return nil, nil
	}

	if fs.NArg() != 1 {
		return invalidOptions(fs, "Exactly one file must be provided.")
	}

	path := fs.Arg(0)
	fileLoc := mvdata.NewDataLocation(path, argMap.Get(fileTypeParam))

	if fileLoc.Format == mvdata.InvalidDataFormat {
		return invalidOptions(fs, "Could not infer type from parameter %s.  Should be a valid table name or a supported file type.", path)
	}

	tableLoc := &mvdata.DataLocation{tableName, mvdata.DoltDB}

	return fileLoc, tableLoc
}

func Create(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	force, mvOpts := parseCreateArgs(commandStr, args)

	if mvOpts == nil {
		return 1
	}

	return executeMove(cliEnv, force, mvOpts)
}

func parseCreateArgs(commandStr string, args []string) (bool, *mvdata.MoveOptions) {
	fs, argMap, flagMap := initCreateFlagSet(commandStr)
	fileLoc, tableLoc := validateCreateOrExportArgs(fs, args, argMap, flagMap)

	if fileLoc == nil || tableLoc == nil {
		return false, nil
	}

	return flagMap.Get(forceParam), &mvdata.MoveOptions{
		mvdata.OverwriteOp,
		flagMap.Get(contOnErrParam),
		argMap.Get(outSchemaParam),
		argMap.Get(mappingFileParam),
		argMap.Get(primaryKeyParam),
		fileLoc,
		tableLoc,
	}
}

func executeMove(cliEnv *env.DoltCLIEnv, force bool, mvOpts *mvdata.MoveOptions) int {
	root, err := cliEnv.WorkingRoot()

	if err != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to get the working root value for this data repository."))
		return 1
	}

	if !force && mvOpts.Dest.Exists(root, cliEnv.FS) {
		fmt.Fprintln(os.Stderr, color.RedString("The data already exists in %s.  Use -f to overwrite.", mvOpts.Dest.Path))
		return 1
	}

	mover, verr := mvdata.NewDataMover(root, cliEnv.FS, mvOpts)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	err = mover.Move()

	if err != nil {
		fmt.Fprintln(os.Stderr, "An error occurred moving data.", err.Error())
		return 1
	}

	if nomsWr, ok := mover.Wr.(noms.NomsMapWriteCloser); ok {
		err = cliEnv.PutTableToWorking(*nomsWr.GetMap(), nomsWr.GetSchema(), mvOpts.Dest.Path)

		if err != nil {
			fmt.Fprintln(os.Stderr, color.RedString("Failed to update the working value."))
			return 1
		}
	}

	return 0
}
