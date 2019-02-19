package cnfcmds

import (
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/merge"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
)

var resShortDesc = "Removes rows from list of conflicts"
var resLongDesc = "When a merge operation finds conflicting changes, the rows with the conflicts are added to list " +
	"of conflicts that must be resolved.  Once the value for the row is resolved in the working set of tables, then " +
	"the conflict should be resolved.\n" +
	"\n" +
	"In it's first form <b>dolt conflicts resolve <table> <key>...</b>, resolve runs in manual merge mode resolving " +
	"the conflicts whose keys are provided.\n" +
	"\n" +
	"In it's second form <b>dolt conflicts resolve --ours|--theirs <table>...</b>, resolve runs in auto resolve mode. " +
	"where conflicts are resolved using a rule to determine which version of a row should be used."
var resSynopsis = []string{
	"<table> <key>...",
	"--ours|--theirs <table>...",
}

const (
	oursFlag   = "ours"
	theirsFlag = "theirs"
)

var autoResolvers = map[string]merge.AutoResolver{
	oursFlag:   merge.Ours,
	theirsFlag: merge.Theirs,
}

var autoResolverParams []string

func init() {
	autoResolverParams = make([]string, 0, len(autoResolvers))
	for k := range autoResolvers {
		autoResolverParams = append(autoResolverParams, k)
	}
}

func Resolve(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "List of tables to be printed. When in auto-resolve mode, '.' can be used to resolve all tables."
	ap.ArgListHelp["key"] = "key(s) of rows within a table whose conflicts have been resolved"
	ap.SupportsFlag("ours", "", "For all conflicts, take the version from our branch and resolve the conflict")
	ap.SupportsFlag("theirs", "", "Fol all conflicts, take the version from our branch and resolve the conflict")
	help, usage := cli.HelpAndUsagePrinters(commandStr, resShortDesc, resLongDesc, resSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)

	if apr.ContainsAny(autoResolverParams...) {
		return autoResolve(usage, apr, dEnv)
	} else {
		return manualResolve(usage, apr, dEnv)
	}
}

func autoResolve(usage cli.UsagePrinter, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) int {
	funcFlags := apr.FlagsEqualTo(autoResolverParams, true)

	if funcFlags.Size() > 1 {
		usage()
		return 1
	} else if apr.NArg() == 0 {
		usage()
		return 1
	}

	autoResolveFlag := funcFlags.AsSlice()[0]
	autoResolveFunc := autoResolvers[autoResolveFlag]

	var err error
	tbls := apr.Args()
	if len(tbls) == 1 && tbls[0] == "." {
		err = actions.AutoResolveAll(dEnv, autoResolveFunc)
	} else {
		err = actions.AutoResolveTables(dEnv, autoResolveFunc, tbls)
	}

	if err != nil {
		if err == doltdb.ErrNoConflicts {
			cli.Println("no conflicts to resolve.")
			return 0
		}

		panic(err) // todo: fix
		return 1
	}

	return 0
}

func manualResolve(usage cli.UsagePrinter, apr *argparser.ArgParseResults, dEnv *env.DoltEnv) int {
	args := apr.Args()

	if len(args) < 2 {
		usage()
		return 1
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		tblName := args[0]
		if root.HasTable(tblName) {
			tbl, ok := root.GetTable(tblName)

			if !ok {
				verr = errhand.BuildDError("fatal: table not found - " + tblName).Build()
			} else {
				invalid, notFound, updatedTbl, err := tbl.ResolveConflicts(args[1:])

				if err != nil {
					verr = errhand.BuildDError("fatal: Failed to resolve conflicts").AddCause(err).Build()
				} else {
					for _, key := range invalid {
						cli.Println(key, "is not a valid key")
					}

					for _, key := range notFound {
						cli.Println(key, "is not the primary key of a conflicting row")
					}

					if updatedTbl.HashOf() != tbl.HashOf() {
						root := root.PutTable(dEnv.DoltDB, tblName, updatedTbl)
						verr = commands.UpdateWorkingWithVErr(dEnv, root)
					}
				}
			}
		}
	}

	return commands.HandleVErrAndExitCode(verr, usage)
}
