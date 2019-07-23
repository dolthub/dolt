package tblcmds

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var tblRmShortDesc = "Removes table(s) from the working set of tables."
var tblRmLongDesc = "dolt table rm removes table(s) from the working set.  These changes can be staged using " +
	"<b>dolt add</b> and committed using <b>dolt commit</b>"
var tblRmSynopsis = []string{
	"<table>...",
}

func Rm(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "The table to remove"
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblRmShortDesc, tblRmLongDesc, tblRmSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 1
	}

	working, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		verr := commands.ValidateTablesWithVErr(apr.Args(), working)

		if verr == nil {
			verr = removeTables(dEnv, apr.Args(), working)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func removeTables(dEnv *env.DoltEnv, tables []string, working *doltdb.RootValue) errhand.VerboseError {
	working, err := working.RemoveTables(context.TODO(), tables...)

	if err != nil {
		return errhand.BuildDError("Unable to remove table(s)").AddCause(err).Build()
	}

	return commands.UpdateWorkingWithVErr(dEnv, working)
}
