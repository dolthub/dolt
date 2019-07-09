package cnfcmds

import (
	"context"

	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/merge"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
)

var catShortDesc = "print conflicts"
var catLongDesc = `The dolt conflicts cat command reads table conflicts and writes them to the standard output.`
var catSynopsis = []string{
	"[<commit>] <table>...",
}

func Cat(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.ArgListHelp["table"] = "List of tables to be printed. '.' can be used to print conflicts for all tables."
	help, usage := cli.HelpAndUsagePrinters(commandStr, catShortDesc, catLongDesc, catSynopsis, ap)
	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	if len(args) == 0 {
		cli.Println("No tables specified")
		cli.Println(" Maybe you wanted to say 'dolt conflicts cat .'?")
		usage()
		return 1
	}

	root, verr := commands.GetWorkingWithVErr(dEnv)

	if verr == nil {
		var cm *doltdb.Commit
		cm, verr = commands.MaybeGetCommitWithVErr(dEnv, args[0])

		if verr == nil {
			if cm != nil {
				args = args[1:]
				root = cm.GetRootValue()
			}

			if len(args) == 0 {
				usage()
				return 1
			}

			verr = printConflicts(root, args)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func printConflicts(root *doltdb.RootValue, tblNames []string) errhand.VerboseError {
	if len(tblNames) == 1 && tblNames[0] == "." {
		tblNames = actions.AllTables(context.TODO(), root)
	}

	var verr errhand.VerboseError
	for _, tblName := range tblNames {
		func() {
			if !root.HasTable(context.TODO(), tblName) {
				verr = errhand.BuildDError("error: unknown table '%s'", tblName).Build()
				return
			}

			tbl, _ := root.GetTable(context.TODO(), tblName)
			cnfRd, err := merge.NewConflictReader(context.TODO(), tbl)

			if err == doltdb.ErrNoConflicts {
				return
			} else if err != nil {
				panic(err)
			}

			defer cnfRd.Close()

			cnfWr := merge.NewConflictSink(iohelp.NopWrCloser(cli.CliOut), cnfRd.GetSchema(), " | ")
			defer cnfWr.Close()

			nullPrinter := nullprinter.NewNullPrinter(cnfRd.GetSchema())
			fwtTr := fwt.NewAutoSizingFWTTransformer(cnfRd.GetSchema(), fwt.HashFillWhenTooLong, 1000)
			transforms := pipeline.NewTransformCollection(
				pipeline.NewNamedTransform(nullprinter.NULL_PRINTING_STAGE, nullPrinter.ProcessRow),
				pipeline.NamedTransform{Name: "fwt", Func: fwtTr.TransformToFWT},
			)

			// TODO: Pipeline should be contextified.
			srcProcFunc := pipeline.ProcFuncForSourceFunc(func() (row.Row, pipeline.ImmutableProperties, error) { return cnfRd.NextConflict(context.TODO()) })
			sinkProcFunc := pipeline.ProcFuncForSinkFunc(cnfWr.ProcRowWithProps)
			p := pipeline.NewAsyncPipeline(srcProcFunc, sinkProcFunc, transforms, func(failure *pipeline.TransformRowFailure) (quit bool) {
				panic("")
			})

			p.InjectRow("fwt", untyped.NewRowFromTaggedStrings(tbl.Format(), cnfRd.GetSchema(), schema.ExtractAllColNames(cnfRd.GetSchema())))

			p.Start()
			p.Wait()
		}()
	}

	return verr
}
