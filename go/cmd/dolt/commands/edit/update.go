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

func updateUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func initUpdateFlagSet(commandStr string) (*flag.FlagSet, *cli.StrArgMap, *cli.BoolFlagMap) {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = updateUsage(fs)

	argMap := cli.NewStrArgMap(fs, map[string]string{
		mappingFileParam: "A file that lays out how fields should be mapped from input data to output data",
		tableParam:       "Destination of where the new data should be imported to.",
		fileTypeParam:    "Explicitly define the type of the file if it can't be inferred from the file extension"})

	flagMap := cli.NewBoolFlagMap(fs, map[string]string{
		contOnErrParam: "Continue importing when row import errors are encountered."})

	return fs, argMap, flagMap
}

func validateUpdateArgs(fs *flag.FlagSet, args []string, argMap *cli.StrArgMap, flagMap *cli.BoolFlagMap) (*mvdata.DataLocation, *mvdata.DataLocation) {
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
		return invalidOptions(fs, "Exactly one file must be provided to import.")
	}

	path := fs.Arg(0)
	fileLoc := mvdata.NewDataLocation(path, argMap.Get(fileTypeParam))

	if fileLoc.Format == mvdata.InvalidDataFormat {
		return invalidOptions(fs, "Could not infer type from parameter %s.  Should be a valid table name or a supported file type.", path)
	}

	tableLoc := &mvdata.DataLocation{tableName, mvdata.DoltDB}

	return fileLoc, tableLoc
}

func parseUpdateArgs(commandStr string, args []string) *mvdata.MoveOptions {
	fs, argMap, flagMap := initUpdateFlagSet(commandStr)
	fileLoc, tableLoc := validateUpdateArgs(fs, args, argMap, flagMap)

	if fileLoc == nil || tableLoc == nil {
		return nil
	}

	return &mvdata.MoveOptions{
		mvdata.UpdateOp,
		flagMap.Get(contOnErrParam),
		"",
		argMap.Get(mappingFileParam),
		"",
		fileLoc,
		tableLoc,
	}
}

func Update(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	mvOpts := parseUpdateArgs(commandStr, args)

	if mvOpts == nil {
		return 1
	}

	root, err := cliEnv.WorkingRoot()

	if err != nil {
		fmt.Fprintln(os.Stderr, color.RedString("Unable to get working value."))
		return 1
	}

	if !mvOpts.Dest.Exists(root, cliEnv.FS) {
		fmt.Fprintln(os.Stderr, color.RedString("Cannot find the table %s", mvOpts.Dest.Path))
		return 1
	}

	mover, verr := mvdata.NewDataMover(root, cliEnv.FS, mvOpts)

	if verr != nil {
		fmt.Fprintln(os.Stderr, verr.Verbose())
		return 1
	}

	err = mover.Move()

	if err != nil {
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
