// Copyright 2019 Liquidata, Inc.
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

package cnfcmds

import (
	"context"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/commands"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env/actions"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/merge"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
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

				var err error
				root, err = cm.GetRootValue()

				if err != nil {
					verr = errhand.BuildDError("unable to get the root value").AddCause(err).Build()
				}
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
		var err error
		tblNames, err = actions.AllTables(context.TODO(), root)

		if err != nil {
			 return errhand.BuildDError("unable to read tables").AddCause(err).Build()
		}
	}

	for _, tblName := range tblNames {
		verr := func() errhand.VerboseError {
			if has, err := root.HasTable(context.TODO(), tblName); err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			} else if !has{
				return errhand.BuildDError("error: unknown table '%s'", tblName).Build()
			}

			tbl, _, err := root.GetTable(context.TODO(), tblName)

			if err != nil {

			}

			cnfRd, err := merge.NewConflictReader(context.TODO(), tbl)

			if err == doltdb.ErrNoConflicts {
				return nil
			} else if err != nil {
				return errhand.BuildDError("failed to read conflicts").AddCause(err).Build()
			}

			defer cnfRd.Close()

			cnfWr, err := merge.NewConflictSink(iohelp.NopWrCloser(cli.CliOut), cnfRd.GetSchema(), " | ")
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

			colNames, err := schema.ExtractAllColNames(cnfRd.GetSchema())

			if err != nil {
				return errhand.BuildDError("error: failed to read columns from schema").AddCause(err).Build()
			}
			r, err := untyped.NewRowFromTaggedStrings(tbl.Format(), cnfRd.GetSchema(), colNames)

			if err != nil {
				return errhand.BuildDError("error: failed to create header row for printing").AddCause(err).Build()
			}

			p.InjectRow("fwt", r)

			p.Start()
			p.Wait()

			return nil
		}()

		if verr != nil {
			return verr
		}
	}

	return nil
}
