// Copyright 2020 Liquidata, Inc.
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

package indexcmds

import (
	"context"

	"github.com/fatih/color"

	"github.com/liquidata-inc/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/liquidata-inc/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/liquidata-inc/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/dolt/go/libraries/utils/filesys"
	"github.com/liquidata-inc/dolt/go/libraries/utils/iohelp"
	"github.com/liquidata-inc/dolt/go/store/types"
)

var catDocs = cli.CommandDocumentationContent{
	ShortDesc: `Display the contents of an index`,
	LongDesc: IndexCmdWarning + `
This command displays the contents of the given index. If indexes were tables, it would be equivalent to running "select * from {{.LessThan}}index{{.GreaterThan}}".`,
	Synopsis: []string{
		`{{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
	},
}

type CatCmd struct{}

func (cmd CatCmd) Name() string {
	return "cat"
}

func (cmd CatCmd) Description() string {
	return "Internal debugging command to display the contents of an index."
}

func (cmd CatCmd) CreateMarkdown(filesys.Filesys, string, string) error {
	return nil
}

func (cmd CatCmd) createArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index that belongs to the given table."})
	return ap
}

func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.createArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, catDocs, ap))
	apr := cli.ParseArgs(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 0
	} else if apr.NArg() != 2 {
		return HandleErr(errhand.BuildDError("Both the table and index names must be provided.").Build(), usage)
	}

	working, err := dEnv.WorkingRoot(context.Background())
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get working.").AddCause(err).Build(), nil)
	}

	tableName := apr.Arg(0)
	indexName := apr.Arg(1)

	table, ok, err := working.GetTable(ctx, tableName)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get table `%s`.", tableName).AddCause(err).Build(), nil)
	}
	if !ok {
		return HandleErr(errhand.BuildDError("The table `%s` does not exist.", tableName).Build(), nil)
	}
	tblSch, err := table.GetSchema(ctx)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to get schema for `%s`.", tableName).AddCause(err).Build(), nil)
	}
	index := tblSch.Indexes().Get(indexName)
	if index == nil {
		return HandleErr(errhand.BuildDError("The index `%s` does not exist on table `%s`.", indexName, tableName).Build(), nil)
	}
	indexRowData, err := table.GetIndexRowData(ctx, index.Name())
	if err != nil {
		return HandleErr(errhand.BuildDError("The index `%s` does not have a data map.", indexName).Build(), nil)
	}

	transforms := pipeline.NewTransformCollection()
	p, err := cmd.createIndexPrintPipeline(ctx, index, indexRowData, transforms)
	if err != nil {
		return HandleErr(errhand.BuildDError("error: failed to setup pipeline").AddCause(err).Build(), nil)
	}

	p.Start()
	err = p.Wait()
	if err != nil {
		return HandleErr(errhand.BuildDError("error: error processing results").AddCause(err).Build(), nil)
	}

	return 0
}

func (cmd CatCmd) createIndexPrintPipeline(ctx context.Context, index schema.Index, indexRowData types.Map, transforms *pipeline.TransformCollection) (*pipeline.Pipeline, error) {
	indexSch := index.Schema()
	colColl := indexSch.GetAllCols()
	unkeyedSch := schema.UnkeyedSchemaFromCols(colColl)
	untypedSch, err := untyped.UntypeUnkeySchema(unkeyedSch)
	if err != nil {
		return nil, err
	}
	mapping, err := rowconv.TagMapping(indexSch, untypedSch)
	if err != nil {
		return nil, err
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	transforms.AppendTransforms(pipeline.NewNamedTransform("map", rowconv.GetRowConvTransformFunc(rConv)))
	outSch := mapping.DestSch

	nullPrinter := nullprinter.NewNullPrinter(outSch)
	transforms.AppendTransforms(pipeline.NewNamedTransform("null printing", nullPrinter.ProcessRow))
	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.PrintAllWhenTooLong, 10000)
	transforms.AppendTransforms(pipeline.NamedTransform{Name: "fwt", Func: autoSizeTransform.TransformToFWT})

	rd, err := noms.NewNomsMapReader(ctx, indexRowData, indexSch)
	if err != nil {
		return nil, err
	}
	wr, err := tabular.NewTextTableWriter(iohelp.NopWrCloser(cli.CliOut), outSch)
	if err != nil {
		return nil, err
	}

	p := pipeline.NewAsyncPipeline(
		pipeline.ProcFuncForReader(ctx, rd),
		pipeline.ProcFuncForWriter(ctx, wr),
		transforms,
		func(tff *pipeline.TransformRowFailure) (quit bool) {
			cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, outSch)))
			return true
		},
	)
	p.RunAfter(func() { _ = rd.Close(ctx) })
	p.RunAfter(func() { _ = wr.Close(ctx) })

	colNames, err := schema.ExtractAllColNames(outSch)
	if err != nil {
		return nil, err
	}
	r, err := untyped.NewRowFromTaggedStrings(indexRowData.Format(), outSch, colNames)
	if err != nil {
		return nil, err
	}

	p.InjectRow("fwt", r)

	return p, nil
}
