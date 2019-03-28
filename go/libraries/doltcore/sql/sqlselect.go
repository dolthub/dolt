package sql

import (
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped"
	"github.com/xwb1989/sqlparser"
	"strconv"
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

// BuildSelectQueryPipeline interprets the select statement given, builds a pipeline to execute it, and returns the pipeline
// for the caller to mutate and execute, as well as the schema of the result set. The pipeline will not have any output
// set; one must be assigned before execution.
func BuildSelectQueryPipeline(root *doltdb.RootValue, s *sqlparser.Select, query string) (*pipeline.Pipeline, schema.Schema, error) {
	tableExprs := s.From
	if len(tableExprs) > 1 {
		return quitErr("Only selecting from a single table is supported")
	}

	var tableName string

	tableExpr := tableExprs[0]
	switch te := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := te.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			quitErr("Subqueries are not supported: %v.", query)
		default:
			quitErr("Unrecognized expression: %v", nodeToString(e))
		}
	case *sqlparser.ParenTableExpr:
		return quitErr("Parenthetical table expressions are not supported: %v,", query)
	case *sqlparser.JoinTableExpr:
		return quitErr("Joins are not supported: %v,", query)
	default:
		return quitErr("Unsupported select statement: %v", query)
	}

	if !root.HasTable(tableName) {
		return quitErr("error: unknown table '%s'", tableName)
	}
	tbl, _:= root.GetTable(tableName)

	selectStmt := &selectStatement{tableName: tableName}
	tableSch := tbl.GetSchema()

	// Process the columns selected
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
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:
				columns = append(columns, colExpr.Name.String())
			default:
				return quitErr("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return quitErr("Next value is not supported: %v", query)
		}
	}
	selectStmt.colNames = columns

	// Include a limit if asked for
	if s.Limit != nil && s.Limit.Rowcount != nil {
		limitVal, ok := s.Limit.Rowcount.(*sqlparser.SQLVal)
		if !ok {
			return quitErr("Couldn't parse limit clause: %v", query)
		}
		limitInt, err := strconv.Atoi(nodeToString(limitVal))
		if err != nil {
			return quitErr("Couldn't parse limit clause: %v", query)
		}
		selectStmt.limit = limitInt
	} else {
		selectStmt.limit = -1
	}

	err := processWhereClause(selectStmt, s, query, tableSch)
	if err != nil {
		return nil, nil, err
	}

	return createPipeline(root, selectStmt)
}

// Processes the where clause by applying an appropriate filter fn to the selectStatement given. Returns an error if the
// where clause can't be processed.
func processWhereClause(selectStmt *selectStatement, s *sqlparser.Select, query string, tableSch schema.Schema) error {
	whereClause := s.Where

	// nil where clause gets an always-true filter function
	if whereClause == nil {
		selectStmt.filterFn = func(r row.Row) bool {
			return true
		}
		return nil
	}

	if whereClause.Type != sqlparser.WhereStr {
		errors.New(fmt.Sprintf("Having clause not supported: %v", query))
	}

	switch expr := whereClause.Expr.(type) {
	case *sqlparser.ComparisonExpr:

		left := expr.Left
		right := expr.Right
		op := expr.Operator

		colExpr := left
		valExpr := right

		// Swap the column and value expr as necessary
		colName, ok := colExpr.(*sqlparser.ColName)
		if !ok {
			colExpr = right
			valExpr = left
		}

		colName, ok = colExpr.(*sqlparser.ColName)
		if !ok {
			errors.New(fmt.Sprintf("Only column names and value literals are supported"))
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
			errors.New(fmt.Sprintf("%v is not a known column", colNameStr))
		}

		tag := col.Tag
		convFunc := doltcore.GetConvFunc(types.StringKind, col.Kind)
		comparisonVal, err := convFunc(types.String(string(sqlVal)))

		if err != nil {
			errors.New(fmt.Sprintf("Couldn't convert column to string: %v", err))
		}

		// All the operations differ only in their filter logic
		switch op {
		case sqlparser.EqualStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return comparisonVal.Equals(rowVal)
			}
		case sqlparser.LessThanStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return rowVal.Less(comparisonVal)
			}
		case sqlparser.GreaterThanStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return comparisonVal.Less(rowVal)
			}
		case sqlparser.LessEqualStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return rowVal.Less(comparisonVal) || rowVal.Equals(comparisonVal)
			}
		case sqlparser.GreaterEqualStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return comparisonVal.Less(rowVal) || comparisonVal.Equals(rowVal)
			}
		case sqlparser.NotEqualStr:
			selectStmt.filterFn = func(r row.Row) bool {
				rowVal, ok := r.GetColVal(tag)
				if !ok {
					return false
				}
				return !comparisonVal.Equals(rowVal)
			}
		case sqlparser.NullSafeEqualStr:
			return errors.New(fmt.Sprintf("null safe equal operation not supported"))
		case sqlparser.InStr:
			return errors.New(fmt.Sprintf("in keyword not supported"))
		case sqlparser.NotInStr:
			return errors.New(fmt.Sprintf("in keyword not supported"))
		case sqlparser.LikeStr:
			return errors.New(fmt.Sprintf("like keyword not supported"))
		case sqlparser.NotLikeStr:
			return errors.New(fmt.Sprintf("like keyword not supported"))
		case sqlparser.RegexpStr:
			return errors.New(fmt.Sprintf("regular expressions not supported"))
		case sqlparser.NotRegexpStr:
			return errors.New(fmt.Sprintf("regular expressions not supported"))
		case sqlparser.JSONExtractOp:
			return errors.New(fmt.Sprintf("json not supported"))
		case sqlparser.JSONUnquoteExtractOp:
			return errors.New(fmt.Sprintf("json not supported"))
		}
	case *sqlparser.AndExpr:
		return errors.New(fmt.Sprintf("And expressions not supported: %v", query))
	case *sqlparser.OrExpr:
		return errors.New(fmt.Sprintf("Or expressions not supported: %v", query))
	case *sqlparser.NotExpr:
		return errors.New(fmt.Sprintf("Not expressions not supported: %v", query))
	case *sqlparser.ParenExpr:
		return errors.New(fmt.Sprintf("Parenthetical expressions not supported: %v", query))
	case *sqlparser.RangeCond:
		return errors.New(fmt.Sprintf("Range expressions not supported: %v", query))
	case *sqlparser.IsExpr:
		return errors.New(fmt.Sprintf("Is expressions not supported: %v", query))
	case *sqlparser.ExistsExpr:
		return errors.New(fmt.Sprintf("Exists expressions not supported: %v", query))
	case *sqlparser.SQLVal:
		return errors.New(fmt.Sprintf("Not expressions not supported: %v", query))
	case *sqlparser.NullVal:
		return errors.New(fmt.Sprintf("NULL expressions not supported: %v", query))
	case *sqlparser.BoolVal:
		return errors.New(fmt.Sprintf("Bool expressions not supported: %v", query))
	case *sqlparser.ColName:
		return errors.New(fmt.Sprintf("Column name expressions not supported: %v", query))
	case *sqlparser.ValTuple:
		return errors.New(fmt.Sprintf("Tuple expressions not supported: %v", query))
	case *sqlparser.Subquery:
		return errors.New(fmt.Sprintf("Subquery expressions not supported: %v", query))
	case *sqlparser.ListArg:
		return errors.New(fmt.Sprintf("List expressions not supported: %v", query))
	case *sqlparser.BinaryExpr:
		return errors.New(fmt.Sprintf("Binary expressions not supported: %v", query))
	case *sqlparser.UnaryExpr:
		return errors.New(fmt.Sprintf("Unary expressions not supported: %v", query))
	case *sqlparser.IntervalExpr:
		return errors.New(fmt.Sprintf("Interval expressions not supported: %v", query))
	case *sqlparser.CollateExpr:
		return errors.New(fmt.Sprintf("Collate expressions not supported: %v", query))
	case *sqlparser.FuncExpr:
		return errors.New(fmt.Sprintf("Function expressions not supported: %v", query))
	case *sqlparser.CaseExpr:
		return errors.New(fmt.Sprintf("Case expressions not supported: %v", query))
	case *sqlparser.ValuesFuncExpr:
		return errors.New(fmt.Sprintf("Values func expressions not supported: %v", query))
	case *sqlparser.ConvertExpr:
		return errors.New(fmt.Sprintf("Conversion expressions not supported: %v", query))
	case *sqlparser.SubstrExpr:
		return errors.New(fmt.Sprintf("Substr expressions not supported: %v", query))
	case *sqlparser.ConvertUsingExpr:
		return errors.New(fmt.Sprintf("Convert expressions not supported: %v", query))
	case *sqlparser.MatchExpr:
		return errors.New(fmt.Sprintf("Match expressions not supported: %v", query))
	case *sqlparser.GroupConcatExpr:
		return errors.New(fmt.Sprintf("Group concat expressions not supported: %v", query))
	case *sqlparser.Default:
		return errors.New(fmt.Sprintf("Unrecognized expression: %v", query))
	default:
		return errors.New(fmt.Sprintf("Unrecognized expression: %v", query))
	}

	return nil
}

// Turns a node to a string
func nodeToString(node sqlparser.SQLNode) string {
	buffer := sqlparser.NewTrackedBuffer(nil)
	node.Format(buffer)
	return buffer.String()
}

// Returns an error with the message specified. Return type includes a nil Pipeline object to conform to the needs of
// BuildSelectQueryPipeline.
func quitErr(fmtMsg string, args ...interface{}) (*pipeline.Pipeline, schema.Schema, error) {
	return nil, nil, errors.New(fmt.Sprintf(fmtMsg, args))
}

// createPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createPipeline(root *doltdb.RootValue, statement *selectStatement) (*pipeline.Pipeline, schema.Schema, errhand.VerboseError) {
	tbl, _ := root.GetTable(statement.tableName)
	tblSch := tbl.GetSchema()

	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	transforms := pipeline.NewTransformCollection(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))
	outSchema, verr := addColumnMapTransform(statement, tblSch, transforms)

	if verr != nil {
		return nil, nil, verr
	}

	rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
	rdProcFunc := pipeline.ProcFuncForReader(rd)

	p := pipeline.NewPartialPipeline(rdProcFunc, transforms)
	p.RunAfter(func() { rd.Close() })

	return p, outSchema, nil
}

// Adds a transformation that maps column names in a result set to a new set of columns.
func addColumnMapTransform(statement *selectStatement, tableSch schema.Schema, transforms *pipeline.TransformCollection) (schema.Schema, errhand.VerboseError) {
	colColl := tableSch.GetAllCols()

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
	mapping, err := rowconv.TagMapping(tableSch, untyped.UntypeSchema(outSch))

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	transform := pipeline.NewNamedTransform("map", rowconv.GetRowConvTransformFunc(rConv))
	transforms.AppendTransforms(transform)

	return mapping.DestSch, nil
}