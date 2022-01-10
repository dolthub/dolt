// Copyright 2020 Dolthub, Inc.
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
	"fmt"
	"io"
	"strings"

	"github.com/fatih/color"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/table"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/typed/json"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/nullprinter"
	"github.com/dolthub/dolt/go/libraries/doltcore/table/untyped/tabular"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/libraries/utils/iohelp"
	"github.com/dolthub/dolt/go/store/types"
)

var catDocs = cli.CommandDocumentationContent{
	ShortDesc: `Display the contents of an index`,
	LongDesc: IndexCmdWarning + `
This command displays the contents of the given index. If indexes were tables, it would be equivalent to running "select * from {{.LessThan}}index{{.GreaterThan}}".`,
	Synopsis: []string{
		`[-r {{.LessThan}}result format{{.GreaterThan}}] {{.LessThan}}table{{.GreaterThan}} {{.LessThan}}index{{.GreaterThan}}`,
	},
}

const formatFlag = "result-format"

type resultFormat byte

const (
	formatTabular resultFormat = iota
	formatCsv
	formatJson
)

type CatCmd struct {
	resultFormat resultFormat
}

func (cmd CatCmd) Name() string {
	return "cat"
}

func (cmd CatCmd) Description() string {
	return "Internal debugging command to display the contents of an index."
}

func (cmd CatCmd) CreateMarkdown(_ io.Writer, _ string) error {
	return nil
}

func (cmd CatCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParser()
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"table", "The table that the given index belongs to."})
	ap.ArgListHelp = append(ap.ArgListHelp, [2]string{"index", "The name of the index that belongs to the given table."})
	ap.SupportsString(formatFlag, "r", "result format", "How to format the resulting output. Valid values are tabular, csv, json. Defaults to tabular.")
	return ap
}

func (cmd CatCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.GetCommandDocumentation(commandStr, catDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	if apr.NArg() == 0 {
		usage()
		return 0
	} else if apr.NArg() != 2 {
		return HandleErr(errhand.BuildDError("Both the table and index names must be provided.").Build(), usage)
	}

	cmd.resultFormat = formatTabular
	if formatSr, ok := apr.GetValue(formatFlag); ok {
		switch strings.ToLower(formatSr) {
		case "tabular":
			cmd.resultFormat = formatTabular
		case "csv":
			cmd.resultFormat = formatCsv
		case "json":
			cmd.resultFormat = formatJson
		default:
			return HandleErr(errhand.BuildDError("Invalid argument for --result-format. Valid values are tabular, csv, json").Build(), usage)
		}
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
	index := tblSch.Indexes().GetByName(indexName)
	if index == nil {
		return HandleErr(errhand.BuildDError("The index `%s` does not exist on table `%s`.", indexName, tableName).Build(), nil)
	}
	indexRowData, err := table.GetNomsIndexRowData(ctx, index.Name())
	if err != nil {
		return HandleErr(errhand.BuildDError("The index `%s` does not have a data map.", indexName).Build(), nil)
	}

	err = cmd.prettyPrintResults(ctx, index.Schema(), indexRowData)
	if err != nil {
		return HandleErr(errhand.BuildDError("Unable to display data for `%s`.", indexName).AddCause(err).Build(), nil)
	}

	return 0
}

// TODO: merge this with the cmd/sql.go code, which is what this was modified from
func (cmd CatCmd) prettyPrintResults(ctx context.Context, doltSch schema.Schema, rowData types.Map) error {
	nbf := types.Format_Default

	untypedSch, err := untyped.UntypeUnkeySchema(doltSch)
	if err != nil {
		return err
	}

	rowChannel := make(chan row.Row)
	p := pipeline.NewPartialPipeline(pipeline.InFuncForChannel(rowChannel))

	switch cmd.resultFormat {
	case formatCsv:
		nullPrinter := nullprinter.NewNullPrinterWithNullString(untypedSch, "")
		p.AddStage(pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow))

	case formatTabular:
		nullPrinter := nullprinter.NewNullPrinter(untypedSch)
		p.AddStage(pipeline.NewNamedTransform(nullprinter.NullPrintingStage, nullPrinter.ProcessRow))
		autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
		p.AddStage(pipeline.NamedTransform{Name: "fwt", Func: autoSizeTransform.TransformToFWT})
	}

	cliWr := iohelp.NopWrCloser(cli.CliOut)

	var wr table.TableWriteCloser

	switch cmd.resultFormat {
	case formatTabular:
		wr, err = tabular.NewTextTableWriter(cliWr, untypedSch)
	case formatCsv:
		wr, err = csv.NewCSVWriter(cliWr, untypedSch, csv.NewCSVInfo())
	case formatJson:
		wr, err = json.NewJSONWriter(cliWr, untypedSch)
	default:
		panic("unimplemented output format type")
	}

	if err != nil {
		return err
	}

	p.RunAfter(func() { _ = wr.Close(ctx) })

	cliSink := pipeline.ProcFuncForWriter(ctx, wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(ctx, tff.Row, untypedSch)))
		return true
	})

	colNames, err := schema.ExtractAllColNames(untypedSch)

	if err != nil {
		return err
	}

	r, err := untyped.NewRowFromTaggedStrings(nbf, untypedSch, colNames)

	if err != nil {
		return err
	}

	if cmd.resultFormat == formatTabular {
		p.InjectRow("fwt", r)
	}

	var rowFn func(key types.Value, value types.Value) (row.Row, error)
	switch cmd.resultFormat {
	case formatJson:
		rowFn = func(key types.Value, value types.Value) (row.Row, error) {
			return row.FromNoms(doltSch, key.(types.Tuple), value.(types.Tuple))
		}
	default:
		rowFn = func(key types.Value, _ types.Value) (row.Row, error) {
			taggedValues := make(row.TaggedValues)
			tplIter, err := key.(types.Tuple).Iterator()
			if err != nil {
				return nil, err
			}
			for tplIter.HasMore() {
				_, tagVal, err := tplIter.Next()
				if err != nil {
					return nil, err
				}
				_, val, err := tplIter.Next()
				if err != nil {
					return nil, err
				}
				if val != types.NullValue {
					tag := uint64(tagVal.(types.Uint))
					strPtr, err := doltSch.GetAllCols().TagToCol[tag].TypeInfo.FormatValue(val)
					if err != nil {
						return nil, err
					}
					taggedValues[tag] = types.String(*strPtr)
				}
			}
			return row.New(nbf, untypedSch, taggedValues)
		}
	}

	go func() {
		defer close(rowChannel)
		_ = rowData.IterAll(ctx, func(k, v types.Value) error {
			r, iterErr := rowFn(k, v)
			if iterErr == nil {
				rowChannel <- r
			}
			return nil
		})
	}()

	p.Start()
	if err := p.Wait(); err != nil {
		return fmt.Errorf("error processing results: %v", err)
	}

	return nil
}
