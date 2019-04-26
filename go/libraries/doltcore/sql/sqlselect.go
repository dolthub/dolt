package sql

import (
	"errors"
	"fmt"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/rowconv"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/xwb1989/sqlparser"
	"io"
	"strconv"
)

// Struct to represent the salient results of parsing a select statement.
type SelectStatement struct {
	// Result set schema
	ResultSetSchema *resultset.ResultSetSchema
	// Input tables in order of selection
	inputTables []string
	// Input table schemas, keyed by table name
	inputSchemas map[string]schema.Schema
	// Columns to be selected
	selectedCols []QualifiedColumn
	// Aliases for columns and tables
	aliases *Aliases
	// Filter function for the result set
	filterFn rowFilterFn
	// Limit of results returned
	limit int
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
func ExecuteSelect(root *doltdb.RootValue, s *sqlparser.Select) ([]row.Row, schema.Schema, error) {
	p, statement, err := BuildSelectQueryPipeline(root, s)
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

	return rows, statement.ResultSetSchema.Schema(), nil
}

// BuildSelectQueryPipeline interprets the select statement given, builds a pipeline to execute it, and returns the pipeline
// for the caller to mutate and execute, as well as the schema of the result set. The pipeline will not have any output
// set; one must be assigned before execution.
func BuildSelectQueryPipeline(root *doltdb.RootValue, s *sqlparser.Select) (*pipeline.Pipeline, *SelectStatement, error) {
	selectStmt := &SelectStatement{aliases: NewAliases(), inputSchemas: make(map[string]schema.Schema)}

	if err := processFromClause(root, selectStmt, s.From); err != nil {
		return nil, nil, err
	}

	if err := processSelectedColumns(root, selectStmt, s.SelectExprs); err != nil {
		return nil, nil, err
	}

	if err := processLimitClause(s, selectStmt); err != nil {
		return nil, nil, err
	}

	if err := createResultSetSchema(selectStmt); err != nil {
		return nil, nil, err
	}

	if err := processWhereClause(selectStmt, s); err != nil {
		return nil, nil, err
	}

	p, err := createPipeline(root, selectStmt)
	if err != nil {
		return nil, nil, err
	}

	return p, selectStmt, nil
}

// Processed the limit clause of the SQL select statement given, storing the result of the analysis in the selectStmt
// given or returning any error encountered.
func processLimitClause(s *sqlparser.Select, selectStmt *SelectStatement) error {
	if s.Limit != nil && s.Limit.Rowcount != nil {
		limitVal, ok := s.Limit.Rowcount.(*sqlparser.SQLVal)
		if !ok {
			return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
		}
		limitInt, err := strconv.Atoi(nodeToString(limitVal))
		if err != nil {
			return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
		}
		selectStmt.limit = limitInt
	} else {
		selectStmt.limit = -1
	}

	return nil
}

// Processes the from clause of the select statement, storing the result of the analysis in the selectStmt given or
// returning any error encountered.
func processFromClause(root *doltdb.RootValue, selectStmt *SelectStatement, from sqlparser.TableExprs) error {
	for _, tableExpr := range from {
		var tableName string
		switch te := tableExpr.(type) {
		case *sqlparser.AliasedTableExpr:
			switch e := te.Expr.(type) {
			case sqlparser.TableName:
				tableName = e.Name.String()
			case *sqlparser.Subquery:
				return errFmt("Subqueries are not supported: %v.", nodeToString(e))
			default:
				return errFmt("Unrecognized expression: %v", nodeToString(e))
			}

			if !root.HasTable(tableName) {
				return errFmt("Unknown table '%s'", tableName)
			}

			if !te.As.IsEmpty() {
				selectStmt.aliases.AddTableAlias(tableName, te.As.String())
			}
			selectStmt.aliases.AddTableAlias(tableName, tableName)
			selectStmt.inputSchemas[tableName] = mustGetSchema(root, tableName)
			selectStmt.inputTables = append(selectStmt.inputTables, tableName)

		case *sqlparser.ParenTableExpr:
			return errFmt("Parenthetical table expressions are not supported: %v,", nodeToString(te))
		case *sqlparser.JoinTableExpr:
			return errFmt("Joins expressions are not supported: %v,", nodeToString(te))
		default:
			return errFmt("Unsupported select statement: %v", nodeToString(te))
		}
	}

	return nil
}

// Processes the select expression (columns to return from the query). Adds the results to the SelectStatement given,
// or returns an error if it cannot. All table aliases must be established in the SelectStatement.
func processSelectedColumns(root *doltdb.RootValue, selectStmt *SelectStatement, colSelections sqlparser.SelectExprs) error {
	var columns []QualifiedColumn
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:

			for _, tableName := range selectStmt.inputTables {
				tableSch := selectStmt.inputSchemas[tableName]
				tableSch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
					columns = append(columns, QualifiedColumn{tableName, col.Name})
					return false
				})
			}
		case *sqlparser.AliasedExpr:
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:
				var tableSch schema.Schema
				var tableName string
				colName := colExpr.Name.String()
				if !colExpr.Qualifier.IsEmpty() {
					columnTableName := colExpr.Qualifier.Name.String()
					var ok bool
					if tableName, ok = selectStmt.aliases.TablesByAlias[columnTableName]; ok{
						tableSch = mustGetSchema(root, tableName)
					} else {
						return errFmt("Unknown table " + columnTableName)
					}
				} else {
					qc, err := resolveColumn(colName, selectStmt.inputSchemas, selectStmt.aliases)
					if err != nil {
						return err
					}
					tableName = qc.TableName
					tableSch = selectStmt.inputSchemas[tableName]
				}

				_, ok := tableSch.GetAllCols().GetByName(colName)
				if !ok {
					return errFmt("Unknown column '%v'", colName)
				}

				// an absent column alias will be empty
				if selectExpr.As.String() != "" {
					selectStmt.aliases.AddColumnAlias(tableName, colName, selectExpr.As.String())
				}
				columns = append(columns, QualifiedColumn{tableName, colName})
			default:
				return errFmt("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return errFmt("Next value is not supported: %v", nodeToString(selectExpr))
	 	}
	}

	selectStmt.selectedCols = columns
	return nil
}

// Gets the schema for the table name given. Will cause a panic if the table doesn't exist.
func mustGetSchema(root *doltdb.RootValue, tableName string) schema.Schema {
	tbl, _:= root.GetTable(tableName)
	return tbl.GetSchema()
}

// Processes the where clause by applying an appropriate filter fn to the SelectStatement given. Returns an error if the
// where clause can't be processed.
func processWhereClause(selectStmt *SelectStatement, s *sqlparser.Select) error {
	filter, err := createFilterForWhere(s.Where, selectStmt.inputSchemas, selectStmt.aliases)
	if err != nil {
		return err
	}

	selectStmt.filterFn = filter
	return nil
}

type singleTablePipelineResult struct {
	p    *pipeline.Pipeline
	rows []row.Row
	err  error
}

// createPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createPipeline(root *doltdb.RootValue, statement *SelectStatement) (*pipeline.Pipeline, error) {

	if len(statement.inputSchemas) == 1 {
		var tableName string
		for name := range statement.inputSchemas {
			tableName = name
		}
		return createSingleTablePipeline(root, statement, tableName, true)
	}

	pipelines := make(map[string]*singleTablePipelineResult)
	for tableName := range statement.inputSchemas {
		p, err := createSingleTablePipeline(root, statement, tableName, false)
		if err != nil {
			return nil, err
		}

		result := &singleTablePipelineResult{p: p, rows: make([]row.Row, 0)}
		pipelines[tableName] = result

		rowSink := pipeline.ProcFuncForSinkFunc(
			func(r row.Row, props pipeline.ReadableMap) error {
				result.rows = append(result.rows, r)
				return nil
			})

		errSink := func(failure *pipeline.TransformRowFailure) (quit bool) {
			result.err = errors.New(fmt.Sprintf("Query execution failed at stage %v for row %v: error was %v",
				failure.TransformName, failure.Row, failure.Details))
			return true
		}

		p.SetOutput(rowSink)
		p.SetBadRowCallback(errSink)
		p.Start()
	}

	results := make([]resultset.TableResult, 0)
	for _, tableName := range statement.inputTables {
		result := pipelines[tableName]
		if err := result.p.Wait(); err != nil || result.err != nil {
			return nil, err
		}
		results = append(results, resultset.TableResult{
			Schema: statement.inputSchemas[tableName],
			Rows: result.rows,
		})
	}

	crossProduct := statement.ResultSetSchema.CrossProduct(results)
	source := sourceFuncForRows(crossProduct)

	p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})
	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	selTrans.noMoreCallback = func() { p.NoMore() }
	p.AddStage(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))

	return p, nil
}

func sourceFuncForRows(rows []row.Row) pipeline.SourceFunc {
	idx := 0
	return func() (row.Row, pipeline.ImmutableProperties, error) {
		if idx >= len(rows) {
			return nil, pipeline.NoProps, io.EOF
		}
		r := rows[idx]
		idx++
		return r, pipeline.NoProps, nil
	}
}

// Creates a pipeline to return results from a single table.
func createSingleTablePipeline(root *doltdb.RootValue, statement *SelectStatement, tableName string, isTerminal bool) (*pipeline.Pipeline, error) {
	tblSch := statement.inputSchemas[tableName]
	tbl, _ := root.GetTable(tableName)

	rd := noms.NewNomsMapReader(tbl.GetRowData(), tblSch)
	rdProcFunc := pipeline.ProcFuncForReader(rd)
	p := pipeline.NewPartialPipeline(rdProcFunc, &pipeline.TransformCollection{})
	p.RunAfter(func() { rd.Close() })

	selTrans := &selectTransform{nil, statement.filterFn, statement.limit, 0}
	selTrans.noMoreCallback = func() {p.NoMore()}

	if isTerminal {
		p.AddStage(pipeline.NewNamedTransform("select", selTrans.limitAndFilter))
		p.AddStage(createOutputSchemaMappingTransform(tblSch, statement.ResultSetSchema))
	}

	return p, nil
}

func createOutputSchemaMappingTransform(tableSch schema.Schema, rss *resultset.ResultSetSchema) pipeline.NamedTransform {
	mapping := rss.Mapping(tableSch)
	rConv, _ := rowconv.NewRowConverter(mapping)
	return pipeline.NewNamedTransform("mapToResultSet", rowconv.GetRowConvTransformFunc(rConv))
}

// Returns a ResultSetSchema for the given select statement, which contains a target schema and mappings to get there
// from the individual table schemas.
func createResultSetSchema(statement *SelectStatement) error {
	// Iterate over the columns twice: first to get an ordered list to use to create an output schema with
	cols := make([]schema.Column, 0, len(statement.selectedCols))
	for _, selectedCol := range statement.selectedCols {
		colName := selectedCol.ColumnName
		tableName := selectedCol.TableName

		if tableSch, ok := statement.inputSchemas[tableName]; !ok {
			panic ("Unknown table " + tableName)
		} else {
			if col, ok := tableSch.GetAllCols().GetByName(colName); !ok {
				panic("Unknown column " + colName)
			} else {
				if alias, ok := statement.aliases.AliasesByColumn[selectedCol]; ok {
					col.Name = alias
				}
				cols = append(cols, col)
			}
		}
	}

	rss, err := resultset.NewFromColumns(cols...)
	if err != nil {
		return err
	}

	// Then a second time, to create a mapping from the source schema to the column in the result set.
	for _, selectedCol := range statement.selectedCols {
		tableSch := statement.inputSchemas[selectedCol.TableName]
		col, _ := tableSch.GetAllCols().GetByName(selectedCol.ColumnName)

		err = rss.AddColumn(tableSch, col)
		if err != nil {
			return err
		}
	}

	// Finally set the result set schema in the statement
	statement.ResultSetSchema = rss

	return nil
}