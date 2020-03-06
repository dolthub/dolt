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

	eventsapi "github.com/liquidata-inc/dolt/go/gen/proto/dolt/services/eventsapi/v1alpha1"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"

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


var catDocs = cli.CommandDocumentationContent{
	ShortDesc: "print conflicts",
	LongDesc: `The dolt conflicts cat command reads table conflicts and writes them to the standard output.`,
	Synopsis: []string{
		"[{{.LessThan}}commit{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}}...",
	},
}

type CatCmd struct{}

// Name is returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd CatCmd) Name() string {
	return "cat"
}

// Description returns a description of the command
func (cmd CatCmd) Description() string {
	return "Writes out the table conflicts."
}

// CreateMarkdown creates a markdown file containing the helptext for the command at the given path
func (cmd CatCmd) CreateMarkdown(fs filesys.Filesys, path, commandStr string) error {
	ap := cmd.createArgParser()
	return commands.CreateMarkdown(fs, path, cli.GetCommandDocumentation(commandStr, catDocs, ap))
}

// EventType returns the type of the event to log
func (cmd CatCmd) EventType() eventsapi.ClientEventType {
	return eventsapi.ClientEventType_CONF_CAT
}

func (cmd CatCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "List of tables to be printed. '.' can be used to print conflicts for all tables."})

	return ap
}

// Exec executes the command
func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, catDocs, ap))
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

			verr = printConflicts(ctx, root, args)
		}
	}

	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	return 0
}

func printConflicts(ctx context.Context, root *doltdb.RootValue, tblNames []string) errhand.VerboseError {
	if len(tblNames) == 1 && tblNames[0] == "." {
		var err error
		tblNames, err = actions.AllTables(ctx, root)

		if err != nil {
			return errhand.BuildDError("unable to read tables").AddCause(err).Build()
		}
	}

	for _, tblName := range tblNames {
		verr := func() errhand.VerboseError {
			if has, err := root.HasTable(ctx, tblName); err != nil {
				return errhand.BuildDError("error: unable to read database").AddCause(err).Build()
			} else if !has {
				return errhand.BuildDError("error: unknown table '%s'", tblName).Build()
			}

			tbl, _, err := root.GetTable(ctx, tblName)

			if err != nil {

			}

			cnfRd, err := merge.NewConflictReader(ctx, tbl)

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
			srcProcFunc := pipeline.ProcFuncForSourceFunc(func() (row.Row, pipeline.ImmutableProperties, error) { return cnfRd.NextConflict(ctx) })
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
