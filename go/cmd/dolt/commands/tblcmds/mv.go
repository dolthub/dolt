package tblcmds

import (
	"context"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var tblMvShortDesc = "Renames a table"
var tblMvLongDesc = `
The dolt table mv command will rename a table. If a table exists with the target name this command will 
fail unless the <b>--force|-f</b> flag is provided.  In that case the table at the target location will be overwritten 
by the table being renamed.

The result is equivalent of running <b>dolt table cp <old> <new></b> followed by <b>dolt table rm <old></b>, resulting 
in a new table and a deleted table in the working set. These changes can be staged using <b>dolt add</b> and committed
using <b>dolt commit</b>.`

var tblMvSynopsis = []string{
	"[-f] <oldtable> <newtable>",
}

func Mv(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["oldtable"] = "The table being moved."
	ap.ArgListHelp["newtable"] = "The new name of the table"
	ap.SupportsFlag(forceParam, "f", "If data already exists in the destination, the Force flag will allow the target to be overwritten.")
	help, usage := cli.HelpAndUsagePrinters(commandStr, tblMvShortDesc, tblMvLongDesc, tblMvSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() != 2 {
		usage()
		return 1
	}

	force := apr.Contains(forceParam)
	working, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		old := apr.Arg(0)
		new := apr.Arg(1)
		if verr == nil {
			tbl, ok := working.GetTable(context.TODO(), old)
			if ok {
				if !force && working.HasTable(context.TODO(), new) {
					verr = errhand.BuildDError("Data already exists in '%s'.  Use -f to overwrite.", new).Build()
				} else {
					working = working.PutTable(context.Background(), dEnv.DoltDB, new, tbl)
					working, err := working.RemoveTables(context.TODO(), []string{old})

					if err != nil {
						verr = errhand.BuildDError("Unable to remove '%s'", old).Build()
					} else {
						verr = commands.UpdateWorkingWithVErr(dEnv, working)
					}
				}
			} else {
				verr = errhand.BuildDError("Table '%s' not found.", old).Build()
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
