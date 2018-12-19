package edit

import (
	"flag"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands/edit/mvdata"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/env"
)

func exportUsage(fs *flag.FlagSet) func() {
	return func() {
		fs.PrintDefaults()
	}
}

func initExportFlagSet(commandStr string) (*flag.FlagSet, *cli.StrArgMap, *cli.BoolFlagMap) {
	fs := flag.NewFlagSet(commandStr, flag.ExitOnError)
	fs.Usage = exportUsage(fs)

	argMap := cli.NewStrArgMap(fs, map[string]string{
		outSchemaParam:   "The schema for the output data.",
		mappingFileParam: "A file that lays out how fields should be mapped from input data to output data",
		tableParam:       "Source table being exported to a file",
		primaryKeyParam:  "Explicitly define the name of the field in the schema which should be used as the primary key.",
		fileTypeParam:    "Explicitly define the type of the file if it can't be inferred from the file extension"})

	flagMap := cli.NewBoolFlagMap(fs, map[string]string{
		forceParam:     "If a create operation is being executed, data already exists in the destination, the Force flag will allow the target to be overwritten",
		contOnErrParam: "Continue exporting when row export errors are encountered."})

	return fs, argMap, flagMap
}

func Export(commandStr string, args []string, cliEnv *env.DoltCLIEnv) int {
	fs, argMap, flagMap := initExportFlagSet(commandStr)
	fileLoc, tableLoc := validateCreateOrExportArgs(fs, args, argMap, flagMap)

	if fileLoc == nil || tableLoc == nil {
		return 1
	}

	force := flagMap.Get(forceParam)
	mvOpts := &mvdata.MoveOptions{
		mvdata.OverwriteOp,
		flagMap.Get(contOnErrParam),
		argMap.Get(outSchemaParam),
		argMap.Get(mappingFileParam),
		argMap.Get(primaryKeyParam),
		tableLoc,
		fileLoc,
	}

	result := executeMove(cliEnv, force, mvOpts)

	if result == 0 {
		fmt.Println(color.CyanString("Successfully exported data."))
	}

	return result
}
