package commands

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/xwb1989/sqlparser"
)

var sqlShortDesc = "EXPERIMENTAL: Runs a SQL query"
var sqlLongDesc = "EXPERIMENTAL: Runs a SQL query you specify. By default, begins an interactive session to run " +
	"queries and view the results. With the -q option, runs the given query and prints any results, then exits."
var sqlSynopsis = []string{
	"[options] -q query_string",
	"[options]",
}
var fwtStageName = "fwt"

const (
	queryFlag = "query"
)

// Struct to represent the salient results of parsing a select statement.
type selectStatement struct {
	tableName string
	colNames  []string
	filterFn  filterFn
	limit     int
}

type filterFn = func(r row.Row) (matchesFilter bool)

type selectTransform struct {
	p      *pipeline.Pipeline
	filter filterFn
	limit  int
	count  int
}

func (st *selectTransform) limitAndFilter(inRow row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	if st.limit == -1 || st.count < st.limit {
		if st.filter(inRow) {
			st.count++
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		}
	} else {
		st.p.NoMore()
	}

	return nil, ""
}

func Sql(commandStr string, args []string, dEnv *env.DoltEnv) int {
	ap := argparser.NewArgParser()
	ap.SupportsString(queryFlag, "q", "SQL query to run", "Runs a single query and exits")
	help, usage := cli.HelpAndUsagePrinters(commandStr, sqlShortDesc, sqlLongDesc, sqlSynopsis, ap)

	apr := cli.ParseArgs(ap, args, help)
	args = apr.Args()

	query, ok := apr.GetValue(queryFlag)
	if !ok {
		color.RedString("No query string found")
		usage()
		return 1
	}

	sqlStatement, err := sqlparser.Parse(query)
	if err != nil {
		cli.PrintErrln(color.RedString("Error parsing SQL: %v.", err.Error()))
		return 1
	}

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		cli.PrintErrln(verr.Verbose())
		return 1
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Select:
		p, outSch, err := sql.BuildQueryPipeline(root, s, query)
		if err != nil {
			cli.PrintErrln(color.RedString(err.Error(), args))
			return 1
		}

		wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), outSch, &csv.CSVFileInfo{Delim: '|'})
		p.RunAfter(func() { wr.Close() })

		cliSink := pipeline.ProcFuncForWriter(wr)
		p.SetOutput(cliSink)

		p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
			cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(tff.Row, outSch)))
			return true
		})

		colNames := schema.ExtractAllColNames(outSch)
		autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.PrintAllWhenTooLong, 10000)
		p.AddStage(pipeline.NamedTransform{fwtStageName, autoSizeTransform.TransformToFWT})

		// Insert the table header row at the appropriate stage
		p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(outSch, colNames))

		p.Start()
		err = p.Wait()
		if err != nil {
			return quitErr("error processing results: %v", err)
		}

		return 0
	default:
		return quitErr("Unhandled SQL statement: %v.", query)
		return 1
	}
}

// Writes an error message to the CLI and returns 1
func quitErr(fmtMsg string, args ...interface{}) int {
	cli.PrintErrln(color.RedString(fmtMsg, args))
	return 1
}

func addSizingTransform(outSch schema.Schema, transforms *pipeline.TransformCollection) {
	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.PrintAllWhenTooLong, 10000)
	transforms.AppendTransforms(pipeline.NamedTransform{fwtStageName, autoSizeTransform.TransformToFWT})
}
