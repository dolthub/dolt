package commands

import (
	"context"
	"fmt"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/sql"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/tabular"
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
	case *sqlparser.Insert:
		return sqlInsert(dEnv, root, s, query, usage)
	case *sqlparser.Update:
		return sqlUpdate(dEnv, root, s, query, usage)
	case *sqlparser.Delete:
		return sqlDelete(dEnv, root, s, query, usage)
	case *sqlparser.DDL:
		_, err := sqlparser.ParseStrictDDL(query)
		if err != nil {
			return quitErr("Error parsing SQL: %v.", err.Error())
		}
		return sqlDDL(dEnv, root, s, query, usage)
	default:
		return quitErr("Unhandled SQL statement: %v.", query)
	}
}

// Executes a SQL select statement and prints the result to the CLI.
func sqlSelect(root *doltdb.RootValue, s *sqlparser.Select, query string) int {

	p, statement, err := sql.BuildSelectQueryPipeline(context.TODO(), root, s)
	if err != nil {
		cli.PrintErrln(color.RedString(err.Error()))
		return 1
	}

	// Now that we have the output schema, we do two additional steps in the pipeline:
	// 1) Coerce all the values in each row into strings
	// 2) Run them through a fixed width transformer to make them print pretty
	untypedSch, untypingTransform := NewUntypingTransformer(statement.ResultSetSchema.Schema())
	p.AddStage(untypingTransform)
	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(untypedSch, fwt.PrintAllWhenTooLong, 10000)
	p.AddStage(pipeline.NamedTransform{fwtStageName, autoSizeTransform.TransformToFWT})

	// Redirect outtput to the CLI
	cliWr := iohelp.NopWrCloser(cli.CliOut)

	wr := tabular.NewTextTableWriter(cliWr, statement.ResultSetSchema.Schema())
	p.RunAfter(func() { wr.Close(context.TODO()) })

	cliSink := pipeline.ProcFuncForWriter(context.TODO(), wr)
	p.SetOutput(cliSink)

	p.SetBadRowCallback(func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(context.Background(), tff.Row, statement.ResultSetSchema.Schema())))
		return true
	})

	// Insert the table header row at the appropriate stage
	p.InjectRow(fwtStageName, untyped.NewRowFromTaggedStrings(untypedSch, schema.ExtractAllColNames(statement.ResultSetSchema.Schema())))

	p.Start()
	err = p.Wait()
	if err != nil {
		return quitErr("error processing results: %v", err)
	}

	return 0
}

// Executes a SQL insert statement and prints the result to the CLI.
func sqlInsert(dEnv *env.DoltEnv, root *doltdb.RootValue, stmt *sqlparser.Insert, query string, usage cli.UsagePrinter) int {
	result, err := sql.ExecuteInsert(context.Background(), dEnv.DoltDB, root, stmt, query)
	if err != nil {
		return quitErr("Error inserting rows: %v", err.Error())
	}

	verr := UpdateWorkingWithVErr(dEnv, result.Root)

	if verr == nil {
		cli.Println(fmt.Sprintf("Rows inserted: %v", result.NumRowsInserted))
		if result.NumRowsUpdated > 0 {
			cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
		}
		if result.NumErrorsIgnored > 0 {
			cli.Println(fmt.Sprintf("Errors ignored: %v", result.NumErrorsIgnored))
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

// Executes a SQL update statement and prints the result to the CLI.
func sqlUpdate(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Update, query string, usage cli.UsagePrinter) int {
	result, err := sql.ExecuteUpdate(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return quitErr("Error during update: %v", err.Error())
	}

	verr := UpdateWorkingWithVErr(dEnv, result.Root)

	if verr == nil {
		cli.Println(fmt.Sprintf("Rows updated: %v", result.NumRowsUpdated))
		if result.NumRowsUnchanged > 0 {
			cli.Println(fmt.Sprintf("Rows matched but unchanged: %v", result.NumRowsUnchanged))
		}
	}

	return HandleVErrAndExitCode(verr, usage)
}

// Executes a SQL delete statement and prints the result to the CLI.
func sqlDelete(dEnv *env.DoltEnv, root *doltdb.RootValue, update *sqlparser.Delete, query string, usage cli.UsagePrinter) int {
	result, err := sql.ExecuteDelete(context.Background(), dEnv.DoltDB, root, update, query)
	if err != nil {
		return quitErr("Error during update: %v", err.Error())
	}

	verr := UpdateWorkingWithVErr(dEnv, result.Root)

	if verr == nil {
		cli.Println(fmt.Sprintf("Rows deleted: %v", result.NumRowsDeleted))
	}

	return HandleVErrAndExitCode(verr, usage)
}

// Executes a SQL DDL statement (create, update, etc.) and prints the result to the CLI.
func sqlDDL(dEnv *env.DoltEnv, root *doltdb.RootValue, ddl *sqlparser.DDL, query string, usage cli.UsagePrinter) int {
	switch ddl.Action {
	case sqlparser.CreateStr:
		root, _, err := sql.ExecuteCreate(context.Background(), dEnv.DoltDB, root, ddl, query)
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

// Returns a new untyping transformer for the schema given.
// TODO: move this somewhere more appropriate. Import cycles currently make this difficult.
func NewUntypingTransformer(sch schema.Schema) (schema.Schema, pipeline.NamedTransform) {
	untypedSch := untyped.UntypeUnkeySchema(sch)
	mapping, err := rowconv.TagMapping(sch, untypedSch)

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	return untypedSch, pipeline.NewNamedTransform("untype", rowconv.GetRowConvTransformFunc(rConv))
}
