package commands

import (
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
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
		return quitErr("Error parsing SQL: %v.", err.Error())
	}

	root, verr := GetWorkingWithVErr(dEnv)
	if verr != nil {
		return quitErr(verr.Verbose())
	}

	switch s := sqlStatement.(type) {
	case *sqlparser.Select:
		return sqlSelect(root, s, query)
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return quitErr("Error parsing SQL: %v.", err.Error())
		}
		return sqlDDL(dEnv, root, s, query, usage)
	case *sqlparser.Insert:
		return sqlInsert(root, s, query)
	default:
		return quitErr("Unhandled SQL statement: %v.", query)
	}
}

// Executes a SQL insert statement and prints the result to the CLI.
func sqlInsert(value *doltdb.RootValue, insert *sqlparser.Insert, s string) int {
	// TODO: fill in
	return 0
}

// Executes a SQL select statement and prints the result to the CLI.
func sqlSelect(root *doltdb.RootValue, s *sqlparser.Select, query string) int {
	p, outSch, err := sql.BuildSelectQueryPipeline(root, s, query)
	if err != nil {
		cli.PrintErrln(color.RedString(err.Error()))
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
}

// Executes a SQL DDL statement (create, update, etc.) and prints the result to the CLI.
func sqlDDL(dEnv *env.DoltEnv, root *doltdb.RootValue, ddl *sqlparser.DDL, query string, usage cli.UsagePrinter) int {
	switch ddl.Action {
	case sqlparser.CreateStr:
		root, _, err := sql.ExecuteCreate(dEnv.DoltDB, root, ddl, query)
		if err != nil {
			return quitErr("Error creating table: %v", err)
		}
		verr := UpdateWorkingWithVErr(dEnv, root)
		return HandleVErrAndExitCode(verr, usage)
	case sqlparser.AlterStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.DropStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.RenameStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.TruncateStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.CreateVindexStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.AddColVindexStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	case sqlparser.DropColVindexStr:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	default:
		return quitErr("Unhandled DDL action %v in query %v", ddl.Action, query)
	}
}

// Writes an error message to the CLI and returns 1
func quitErr(fmtMsg string, args ...interface{}) int {
	cli.PrintErrln(color.RedString(fmtMsg, args))
	return 1
}