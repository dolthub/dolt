package sql

import (
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/cmd/dolt/errhand"
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
	tableName       string
	selectedColTags []uint64
	colAliases      map[uint64]string
	filterFn        rowFilterFn
	limit           int
}

type selectTransform struct {
	noMoreCallback func()
	filter         rowFilterFn
	limit          int
	count          int
}

// Limits and filter the rows returned by a query
func (st *selectTransform) limitAndFilter(inRow row.Row, props pipeline.ReadableMap) ([]*pipeline.TransformedRowResult, string) {
	if st.limit == -1 || st.count < st.limit {
		if st.filter(inRow) {
			st.count++
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		}
	} else if st.count == st.limit {
		st.noMoreCallback()
	}

	return nil, ""
}

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteSelect(root *doltdb.RootValue, s *sqlparser.Select, query string) ([]row.Row, schema.Schema, error) {
	p, schema, err := BuildSelectQueryPipeline(root, s, query)
	if err != nil {
		return nil, nil, err
	}

	var rows []row.Row // your boat
	rowSink := pipeline.ProcFuncForSinkFunc(
		func(r row.Row, props pipeline.ReadableMap) error {
			rows = append(rows, r)
			return nil
		})

	var executionErr error
	errSink := func(failure *pipeline.TransformRowFailure) (quit bool) {
		executionErr = errors.New(fmt.Sprintf("Query execution failed at stage %v for row %v: error was %v",
			failure.TransformName, failure.Row, failure.Details))
		return true
	}

	p.SetOutput(rowSink)
	p.SetBadRowCallback(errSink)

	p.Start()
	err = p.Wait()
	if err != nil {
		return nil, nil, err
	}
	if executionErr != nil {
		return nil, nil, executionErr
	}

	return rows, schema, nil
}

// BuildSelectQueryPipeline interprets the select statement given, builds a pipeline to execute it, and returns the pipeline
// for the caller to mutate and execute, as well as the schema of the result set. The pipeline will not have any output
// set; one must be assigned before execution.
func BuildSelectQueryPipeline(root *doltdb.RootValue, s *sqlparser.Select, query string) (*pipeline.Pipeline, schema.Schema, error) {
	tableExprs := s.From
	if len(tableExprs) > 1 {
		return errSelect("Only selecting from a single table is supported")
	}

	var tableName string

	tableExpr := tableExprs[0]
	switch te := tableExpr.(type) {
	case *sqlparser.AliasedTableExpr:
		switch e := te.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			return errSelect("Subqueries are not supported: %v.", query)
		default:
			return errSelect("Unrecognized expression: %v", nodeToString(e))
		}
	case *sqlparser.ParenTableExpr:
		return errSelect("Parenthetical table expressions are not supported: %v,", query)
	case *sqlparser.JoinTableExpr:
		return errSelect("Joins are not supported: %v,", query)
	default:
		return errSelect("Unsupported select statement: %v", query)
	}

	if !root.HasTable(tableName) {
		return errSelect("Unknown table '%s'", tableName)
	}
	tbl, _:= root.GetTable(tableName)

	selectStmt := &selectStatement{tableName: tableName}
	tableSch := tbl.GetSchema()

	columnAliases := make(map[uint64]string)

	// Process the columns selected
	var columns []uint64
	colSelections := s.SelectExprs
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			tableSch.GetAllCols().Iter(func(tag uint64, col schema.Column) (stop bool) {
				columns = append(columns, tag)
				return false
			})
		case *sqlparser.AliasedExpr:
			colAlias := selectExpr.As.String()
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:
				colName := colExpr.Name.String()
				column, ok := tableSch.GetAllCols().GetByName(colName)
				if !ok {
					return errSelect("Unknown column %v", colName)
				}

				// an absent column alias will be empty
				if colAlias != "" {
					columnAliases[column.Tag] = colAlias
				}
				columns = append(columns, column.Tag)
			default:
				return errSelect("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return errSelect("Next value is not supported: %v", query)
		}
	}
	selectStmt.selectedColTags = columns
	selectStmt.colAliases = columnAliases

	// Include a limit if asked for
	if s.Limit != nil && s.Limit.Rowcount != nil {
		limitVal, ok := s.Limit.Rowcount.(*sqlparser.SQLVal)
		if !ok {
			return errSelect("Couldn't parse limit clause: %v", query)
		}
		limitInt, err := strconv.Atoi(nodeToString(limitVal))
		if err != nil {
			return errSelect("Couldn't parse limit clause: %v", query)
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
	filter, err := createFilterForWhere(s.Where, tableSch)
	if err != nil {
		return err
	}

	selectStmt.filterFn = filter
	return nil
}

// Returns an error with the message specified. Return type includes a nil Pipeline object to conform to the needs of
// BuildSelectQueryPipeline.
func errSelect(fmtMsg string, args ...interface{}) (*pipeline.Pipeline, schema.Schema, error) {
	return nil, nil, errors.New(fmt.Sprintf(fmtMsg, args...))
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
	selTrans.noMoreCallback = func() {p.NoMore()}

	p.RunAfter(func() { rd.Close() })

	return p, outSchema, nil
}

// Adds a transformation that maps column names in a result set to a new set of columns.
func addColumnMapTransform(statement *selectStatement, tableSch schema.Schema, transforms *pipeline.TransformCollection) (schema.Schema, errhand.VerboseError) {
	colColl := tableSch.GetAllCols()

	if len(statement.selectedColTags) > 0 {
		cols := make([]schema.Column, 0, len(statement.selectedColTags))

		for _, tag := range statement.selectedColTags {
			if col, ok := colColl.GetByTag(tag); !ok {
				panic("unknown tag " + string(tag))
			} else {
				if alias, ok := statement.colAliases[col.Tag]; ok {
					col.Name = alias
				}
				cols = append(cols, col)
			}
		}

		colColl, _ = schema.NewColCollection(cols...)
	}

	outSch := schema.UnkeyedSchemaFromCols(colColl)
	mapping, err := rowconv.TagMapping(tableSch, untyped.UntypeUnkeySchema(outSch))

	if err != nil {
		panic(err)
	}

	rConv, _ := rowconv.NewRowConverter(mapping)
	transform := pipeline.NewNamedTransform("map", rowconv.GetRowConvTransformFunc(rConv))
	transforms.AppendTransforms(transform)

	return mapping.DestSch, nil
}