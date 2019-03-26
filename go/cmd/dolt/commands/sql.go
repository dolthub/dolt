package commands

import (
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/fatih/color"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/cli"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/env"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/csv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/fwt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/argparser"
	"github.com/liquidata-inc/ld/dolt/go/libraries/utils/iohelp"
	"github.com/xwb1989/sqlparser"
	"strconv"
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

	var tableName string

	switch s := sqlStatement.(type) {
	case *sqlparser.Select:
		tableExprs := s.From
		if len(tableExprs) > 1 {
			cli.PrintErrln(color.RedString("Only selecting from a single table is supported"))
		}
		tableExpr := tableExprs[0]
		switch te := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch e := te.Expr.(type) {
			case sqlparser.TableName:
				tableName = e.Name.String()
				//cli.Println("Table name was " + tableName)

			case *sqlparser.Subquery:
				return quitErr("Subqueries are not supported: %v.", query)
			}
		case *sqlparser.ParenTableExpr:
			return quitErr("Parenthetical table expressions are not supported: %v,", query)
		case *sqlparser.JoinTableExpr:
			return quitErr("Joins are not supported: %v,", query)
		default:
			return quitErr("Unsupported select statement: %v", query)
		}

		root, verr := GetWorkingWithVErr(dEnv)
		if verr != nil {
			cli.PrintErrln(verr.Verbose())
			return 1
		}

		tbl, tableExists := root.GetTable(tableName)
		if !tableExists {
			return quitErr("Unrecognized table %v", tableName)
		}

		tableSch := tbl.GetSchema()

		whereClause := s.Where
		if whereClause.Type != sqlparser.WhereStr {
			return quitErr("Having clause not supported: %v", query)
		}

		switch expr := whereClause.Expr.(type) {
		case *sqlparser.ComparisonExpr:
			var columns []string
			colSelections := s.SelectExprs
			for _, colSelection := range colSelections {
				switch selectExpr := colSelection.(type) {
				case *sqlparser.StarExpr:
					tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
						columns = append(columns, col.Name)
						return false
					})
				case *sqlparser.AliasedExpr:
					fmt.Printf("colSelection: %v", colSelection)
					return quitErr("Aliased expression not supported: %v", selectExpr, columns)
				case sqlparser.Nextval:
					return quitErr("Next value is not supported: %v", query)
				}
			}

			left := expr.Left
			right := expr.Right
			op := expr.Operator
			switch op {
			case sqlparser.EqualStr:

				colExpr := left
				valExpr := right

				colName, ok := colExpr.(*sqlparser.ColName)
				if !ok {
					colExpr = right
					valExpr = left
				}

				colName, ok = colExpr.(*sqlparser.ColName)
				if !ok {
					return quitErr("Only column names and value literals are supported")
				}

				colNameStr := colName.Name.String()

				var sqlVal string
				switch r := valExpr.(type) {
				case *sqlparser.SQLVal:
					switch r.Type {
					// String-like values will print with quotes or other markers by default, so use the naked asci
					// bytes coerced into a string for them
					case sqlparser.HexVal:
						fallthrough
					case sqlparser.BitVal:
						fallthrough
					case sqlparser.StrVal:
						sqlVal = string(r.Val)
					default:
						// Default is to use the string value of the SQL node and hope it works
						sqlVal = nodeToString(valExpr)
					}
				default:
					// Default is to use the string value of the SQL node and hope it works
					sqlVal = nodeToString(valExpr)
				}

				col, ok := tableSch.GetAllCols().GetByName(colNameStr)
				if !ok {
					return quitErr("%v is not a known column", colNameStr)
				}

				tag := col.Tag
				convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
				val, err := convFunc(types.String(string(sqlVal)))
				if err != nil {
					return quitErr("Couldn't convert column to string: %v", col)
				}

				filterFn := func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)

					if !ok {
						return false
					}

					return val.Equals(rowVal)
				}

				selectStmt := &selectStatement{filterFn: filterFn, tableName: tableName, limit: -1}

				runQueryAndPrintResults(root, selectStmt)

			case sqlparser.LessThanStr:
				colExpr := left
				valExpr := right

				colName, ok := colExpr.(*sqlparser.ColName)
				if !ok {
					colExpr = right
					valExpr = left
				}

				colName, ok = colExpr.(*sqlparser.ColName)
				if !ok {
					return quitErr("Only column names and value literals are supported")
				}

				colNameStr := colName.Name.String()

				var sqlVal string
				switch r := valExpr.(type) {
				case *sqlparser.SQLVal:
					switch r.Type {
					// String-like values will print with quotes or other markers by default, so use the naked asci
					// bytes coerced into a string for them
					case sqlparser.HexVal:
						fallthrough
					case sqlparser.BitVal:
						fallthrough
					case sqlparser.StrVal:
						sqlVal = string(r.Val)
					default:
						// Default is to use the string value of the SQL node and hope it works
						sqlVal = nodeToString(valExpr)
					}
				default:
					// Default is to use the string value of the SQL node and hope it works
					sqlVal = nodeToString(valExpr)
				}

				col, ok := tableSch.GetAllCols().GetByName(colNameStr)
				if !ok {
					return quitErr("%v is not a known column", colNameStr)
				}

				tag := col.Tag
				convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
				val, err := convFunc(types.String(string(sqlVal)))
				if err != nil {
					return quitErr("Couldn't convert column to string: %v", col)
				}

				filterFn := func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)

					if !ok {
						return false
					}

					return rowVal.Less(val)
				}

				selectStmt := &selectStatement{filterFn: filterFn, tableName: tableName, limit: -1}

				runQueryAndPrintResults(root, selectStmt)

			case sqlparser.GreaterThanStr:

				colExpr := left
				valExpr := right

				colName, ok := colExpr.(*sqlparser.ColName)
				if !ok {
					colExpr = right
					valExpr = left
				}

				colName, ok = colExpr.(*sqlparser.ColName)
				if !ok {
					return quitErr("Only column names and value literals are supported")
				}

				colNameStr := colName.Name.String()

				var sqlVal string
				switch r := valExpr.(type) {
				case *sqlparser.SQLVal:
					switch r.Type {
					// String-like values will print with quotes or other markers by default, so use the naked asci
					// bytes coerced into a string for them
					case sqlparser.HexVal:
						fallthrough
					case sqlparser.BitVal:
						fallthrough
					case sqlparser.StrVal:
						sqlVal = string(r.Val)
					default:
						// Default is to use the string value of the SQL node and hope it works
						sqlVal = nodeToString(valExpr)
					}
				default:
					// Default is to use the string value of the SQL node and hope it works
					sqlVal = nodeToString(valExpr)
				}

				col, ok := tableSch.GetAllCols().GetByName(colNameStr)
				if !ok {
					return quitErr("%v is not a known column", colNameStr)
				}

				tag := col.Tag
				convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
				val, err := convFunc(types.String(string(sqlVal)))
				if err != nil {
					return quitErr("Couldn't convert column to string: %v", col)
				}

				filterFn := func(r row.Row) bool {
					rowVal, ok := r.GetColVal(tag)

					if !ok {
						return false
					}

					return val.Less(rowVal)
				}

				selectStmt := &selectStatement{filterFn: filterFn, tableName: tableName, limit: -1}

				runQueryAndPrintResults(root, selectStmt)

			case sqlparser.LessEqualStr:
				return quitErr("<= operation not supported")
			case sqlparser.GreaterEqualStr:
				return quitErr(">= operation not supported")
			case sqlparser.NotEqualStr:
				return quitErr("<> operation not supported")
			case sqlparser.NullSafeEqualStr:
				return quitErr("null safe operation not supported")
			case sqlparser.InStr:
				return quitErr("in keyword not supported")
			case sqlparser.NotInStr:
				return quitErr("in keyword not supported")
			case sqlparser.LikeStr:
				return quitErr("like keyword not supported")
			case sqlparser.NotLikeStr:
				return quitErr("like keyword not supported")
			case sqlparser.RegexpStr:
				return quitErr("regular expressions not supported")
			case sqlparser.NotRegexpStr:
				return quitErr("regular expressions not supported")
			case sqlparser.JSONExtractOp:
				return quitErr("json not supported")
			case sqlparser.JSONUnquoteExtractOp:
				return quitErr("json not supported")
			}

		case *sqlparser.AndExpr:
			return quitErr("And expressions not supported: %v", query)
		case *sqlparser.OrExpr:
			return quitErr("Or expressions not supported: %v", query)
		case *sqlparser.NotExpr:
			return quitErr("Not expressions not supported: %v", query)
		case *sqlparser.ParenExpr:
			return quitErr("Parenthetical expressions not supported: %v", query)
		case *sqlparser.RangeCond:
			return quitErr("Range expressions not supported: %v", query)
		case *sqlparser.IsExpr:
			return quitErr("Is expressions not supported: %v", query)
		case *sqlparser.ExistsExpr:
			return quitErr("Exists expressions not supported: %v", query)
		case *sqlparser.SQLVal:
			return quitErr("Not expressions not supported: %v", query)
		case *sqlparser.NullVal:
			return quitErr("NULL expressions not supported: %v", query)
		case *sqlparser.BoolVal:
			return quitErr("Bool expressions not supported: %v", query)
		case *sqlparser.ColName:
			return quitErr("Column name expressions not supported: %v", query)
		case *sqlparser.ValTuple:
			return quitErr("Tuple expressions not supported: %v", query)
		case *sqlparser.Subquery:
			return quitErr("Subquery expressions not supported: %v", query)
		case *sqlparser.ListArg:
			return quitErr("List expressions not supported: %v", query)
		case *sqlparser.BinaryExpr:
			return quitErr("Binary expressions not supported: %v", query)
		case *sqlparser.UnaryExpr:
			return quitErr("Unary expressions not supported: %v", query)
		case *sqlparser.IntervalExpr:
			return quitErr("Interval expressions not supported: %v", query)
		case *sqlparser.CollateExpr:
			return quitErr("Collate expressions not supported: %v", query)
		case *sqlparser.FuncExpr:
			return quitErr("Function expressions not supported: %v", query)
		case *sqlparser.CaseExpr:
			return quitErr("Case expressions not supported: %v", query)
		case *sqlparser.ValuesFuncExpr:
			return quitErr("Values func expressions not supported: %v", query)
		case *sqlparser.ConvertExpr:
			return quitErr("Conversion expressions not supported: %v", query)
		case *sqlparser.SubstrExpr:
			return quitErr("Substr expressions not supported: %v", query)
		case *sqlparser.ConvertUsingExpr:
			return quitErr("Convert expressions not supported: %v", query)
		case *sqlparser.MatchExpr:
			return quitErr("Match expressions not supported: %v", query)
		case *sqlparser.GroupConcatExpr:
			return quitErr("Group concat expressions not supported: %v", query)
		case *sqlparser.Default:
			return quitErr("Unrecognized expression: %v", query)
		}

		return 0
	default:
		return quitErr("Unhandled SQL statement: %v.", query)
		return 1
	}
}

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Extracts an int from ascii bytes
func extractIntFromAsciiBytes(bytes []byte) (int, error) {
	s := string(bytes)
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, err
	}
	return i, nil
}

// Writes an error message to the CLI and returns 1
func quitErr(fmtMsg string, args ...interface{}) int {
	cli.PrintErrln(color.RedString(fmtMsg, args))
	return 1
}

// Runs the a selection pipeline for the query and prints the table of resultant values, returning any error encountered.
func runQueryAndPrintResults(root *doltdb.RootValue, statement *selectStatement) errhand.VerboseError {
	var verr errhand.VerboseError
	if !root.HasTable(statement.tableName) {
		return errhand.BuildDError("error: unknown table '%s'", statement.tableName).Build()
	}

	tbl, _ := root.GetTable(statement.tableName)
	tblSch := tbl.GetSchema()

	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	transforms := pipeline.NewTransformCollection(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))
	outSch, verr := addMapTransform(statement, tblSch, transforms)

	if verr != nil {
		return verr
	}

	p := createPipeline(tbl, tblSch, outSch, transforms)
	selTrans.p = p

	p.Start()
	err := p.Wait()

	if err != nil {
		return errhand.BuildDError("error: error processing results").AddCause(err).Build()
	}

	return nil
}

// Creates a pipeline to select and print rows from the table given. Adds a fixed-width printing transform to the
// collection of transformations given.
func createPipeline(tbl *doltdb.Table, tblSch schema.Schema, outSch schema.Schema, transforms *pipeline.TransformCollection) *pipeline.Pipeline {
	colNames := schema.ExtractAllColNames(outSch)
	addSizingTransform(outSch, transforms)

	rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
	wr, _ := csv.NewCSVWriter(iohelp.NopWrCloser(cli.CliOut), outSch, &csv.CSVFileInfo{Delim: '|'})

	badRowCallback := func(tff *pipeline.TransformRowFailure) (quit bool) {
		cli.PrintErrln(color.RedString("error: failed to transform row %s.", row.Fmt(tff.Row, outSch)))
		return true
	}

	rdProcFunc := pipeline.ProcFuncForReader(rd)
	wrProcFunc := pipeline.ProcFuncForWriter(wr)

	p := pipeline.NewAsyncPipeline(rdProcFunc, wrProcFunc, transforms, badRowCallback)
	p.RunAfter(func() { rd.Close() })
	p.RunAfter(func() { wr.Close() })

	// Insert the table header row at the appropriate stage
	p.InsertRow(fwtStageName, untyped.NewRowFromTaggedStrings(outSch, colNames))

	return p
}

func addSizingTransform(outSch schema.Schema, transforms *pipeline.TransformCollection) {
	autoSizeTransform := fwt.NewAutoSizingFWTTransformer(outSch, fwt.PrintAllWhenTooLong, 10000)
	transforms.AppendTransforms(pipeline.NamedTransform{fwtStageName, autoSizeTransform.TransformToFWT})
}

func addMapTransform(statement *selectStatement, sch schema.Schema, transforms *pipeline.TransformCollection) (schema.Schema, errhand.VerboseError) {
	colColl := sch.GetAllCols()

	if len(statement.colNames) > 0 {
		cols := make([]schema.Column, 0, len(statement.colNames)+1)


		for _, name := range statement.colNames {
			if col, ok := colColl.GetByName(name); !ok {
				return nil, errhand.BuildDError("error: unknown column '%s'", name).Build()
			} else {
				cols = append(cols, col)
			}
		}

		colColl, _ = schema.NewColCollection(cols...)
	}

	outSch := schema.SchemaFromCols(colColl)
	mapping, err := rowconv.TagMapping(sch, untyped.UntypeSchema(outSch))

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	transform := pipeline.NewNamedTransform("map", rowconv.GetRowConvTransformFunc(rConv))
	transforms.AppendTransforms(transform)

	return mapping.DestSch, nil
}