package sql

import (
	"context"
	"errors"
	"fmt"
	"github.com/attic-labs/noms/go/types"
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

// No limit marker for limit statements in select
const noLimit = -1

// Struct to represent the salient results of parsing a select statement.
type SelectStatement struct {
	// Result set schema
	ResultSetSchema *resultset.ResultSetSchema
	// Intermediate result set schema, used for processing results
	intermediateRss *resultset.ResultSetSchema
	// Input tables in order of selection
	inputTables []string
	// Input table schemas, keyed by table name
	inputSchemas map[string]schema.Schema
	// Columns to be selected
	selectedCols []QualifiedColumn
	// Referenced columns (selected or in where / having clause)
	referencedCols []QualifiedColumn
	// Join expressions
	joins []*sqlparser.JoinTableExpr
	// Aliases for columns and tables
	aliases *Aliases
	// Filter function for the result set
	filterFn rowFilterFn
	// Ordering function for the result set
	orderFn rowLesserFn
	// Limit of results returned
	limit int
}

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteSelect(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select) ([]row.Row, schema.Schema, error) {
	p, statement, err := BuildSelectQueryPipeline(ctx, root, s)
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
func BuildSelectQueryPipeline(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select) (*pipeline.Pipeline, *SelectStatement, error) {
	selectStmt := &SelectStatement{
		aliases:      NewAliases(),
		inputSchemas: make(map[string]schema.Schema),
		joins:        make([]*sqlparser.JoinTableExpr, 0),
	}

	if err := processFromClause(ctx, root, selectStmt, s.From); err != nil {
		return nil, nil, err
	}

	if err := processSelectedColumns(ctx, root, selectStmt, s.SelectExprs); err != nil {
		return nil, nil, err
	}

	if err := processReferencedColumns(selectStmt, s.Where); err != nil {
		return nil, nil, err
	}

	if err := createResultSetSchema(selectStmt); err != nil {
		return nil, nil, err
	}

	if err := processOrderByClause(selectStmt, s.OrderBy); err != nil {
		return nil, nil, err
	}

	if err := processLimitClause(s, selectStmt); err != nil {
		return nil, nil, err
	}

	p, err := createPipeline(ctx, root, s, selectStmt)
	if err != nil {
		return nil, nil, err
	}

	return p, selectStmt, nil
}

type orderDirection bool
const (
	asc  orderDirection = true
	desc orderDirection = false
)

// Returns the appropriate less value for sorting, reversing the sort order for desc orders.
func (od orderDirection) lessVal(less bool) bool {
	switch od {
	case asc:
		return less
	case desc:
		return !less
	}
	panic("impossible")
}

// order-by struct
type ob struct {
	qc            QualifiedColumn
	comparisonTag uint64
	direction     orderDirection
}

// Processes the order by clause and applies the result to the select statement given, or returns an error if it cannot.
func processOrderByClause(statement *SelectStatement, orderBy sqlparser.OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}

	obs := make([]ob, len(orderBy))
	for i, o := range orderBy {
		qcs, err := resolveColumnsInWhereExpr(o.Expr, statement.inputSchemas, statement.aliases)
		if err != nil {
			return err
		}

		if len(qcs) == 0 {
			return errFmt("Found no column in order by clause: %v", nodeToString(o))
		} else if len(qcs) > 1 {
			return errFmt("Found more than one column in order by clause: %v", nodeToString(o))
		}

		qc := qcs[0]

		tableSch, ok := statement.inputSchemas[qc.TableName]
		if !ok {
			return errFmt("Unresolved table %v", qc.TableName)
		}

		column, ok := tableSch.GetAllCols().GetByName(qc.ColumnName)
		if !ok {
			return errFmt(UnknownColumnErrFmt, qc.ColumnName)
		}

		comparisonTag := statement.ResultSetSchema.Mapping(tableSch).SrcToDest[column.Tag]

		dir := asc
		if o.Direction == sqlparser.DescScr {
			dir = desc
		}

		obs[i] = ob{qc, comparisonTag, dir}
	}

	// less function for sorting, returns whether left < right
	statement.orderFn = func(rLeft, rRight row.Row) bool {
		for _, ob := range obs {
			leftVal, _ := rLeft.GetColVal(ob.comparisonTag)
			rightVal, _ := rRight.GetColVal(ob.comparisonTag)

			// MySQL behavior is that nulls sort first in asc, last in desc
			if types.IsNull(leftVal) {
				return  ob.direction.lessVal(true)
			} else if types.IsNull(rightVal) {
				return ob.direction.lessVal(false)
			}

			if leftVal.Less(rightVal) {
				return ob.direction.lessVal(true)
			} else if rightVal.Less(leftVal) {
				return ob.direction.lessVal(false)
			}
		}

		return false
	}

	return nil
}

// Processes the referenced columns, those appearing either in the select list, the where clause, or the join statement.
func processReferencedColumns(selectStmt *SelectStatement, where *sqlparser.Where) error {
	cols := make([]QualifiedColumn, 0)
	cols = append(cols, selectStmt.selectedCols...)

	referencedCols, err := resolveColumnsInWhereClause(where, selectStmt.inputSchemas, selectStmt.aliases)
	if err != nil {
		return err
	}

	joinCols, err := resolveColumnsInJoins(selectStmt.joins, selectStmt.inputSchemas, selectStmt.aliases)
	if err != nil {
		return err
	}

	for _, col := range referencedCols {
		if !contains(col, cols) {
			cols = append(cols, col)
		}
	}
	for _, col := range joinCols {
		if !contains(col, cols) {
			cols = append(cols, col)
		}
	}

	selectStmt.referencedCols = cols
	return nil
}

func contains(column QualifiedColumn, cols []QualifiedColumn) bool {
	for _, col := range cols {
		if AreColumnsEqual(col, column) {
			return true
		}
	}
	return false
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
		selectStmt.limit = noLimit
	}

	return nil
}

// Processes the from clause of the select statement, storing the result of the analysis in the selectStmt given or
// returning any error encountered.
func processFromClause(ctx context.Context, root *doltdb.RootValue, selectStmt *SelectStatement, from sqlparser.TableExprs) error {
	for _, tableExpr := range from {
		if err := processTableExpression(ctx, root, selectStmt, tableExpr); err != nil {
			return err
		}
	}

	return nil
}

// Processes the a single table expression from a from (or join) clause
func processTableExpression(ctx context.Context, root *doltdb.RootValue, selectStmt *SelectStatement, expr sqlparser.TableExpr) error {
	switch te := expr.(type) {
	case *sqlparser.AliasedTableExpr:
		var tableName string
		switch e := te.Expr.(type) {
		case sqlparser.TableName:
			tableName = e.Name.String()
		case *sqlparser.Subquery:
			return errFmt("Subqueries are not supported: %v.", nodeToString(e))
		default:
			return errFmt("Unrecognized expression: %v", nodeToString(e))
		}

		if !root.HasTable(ctx, tableName) {
			return errFmt("Unknown table '%s'", tableName)
		}

		if !te.As.IsEmpty() {
			selectStmt.aliases.AddTableAlias(tableName, te.As.String())
		}
		selectStmt.aliases.AddTableAlias(tableName, tableName)

		selectStmt.inputSchemas[tableName] = mustGetSchema(ctx, root, tableName)
		selectStmt.inputTables = append(selectStmt.inputTables, tableName)

	case *sqlparser.JoinTableExpr:
		switch te.Join {
		case sqlparser.JoinStr: // ok
		default:
			return errFmt("Unsupported join type: %v", te.Join)
		}

		// We'll need the full condition when filtering results later in the analysis
		selectStmt.joins = append(selectStmt.joins, te)

		// Join expressions can also define aliases, which we will fill in here recursively
		if err := processTableExpression(ctx, root, selectStmt, te.LeftExpr); err != nil {
			return err
		}
		if err := processTableExpression(ctx, root, selectStmt, te.RightExpr); err != nil {
			return err
		}
	case *sqlparser.ParenTableExpr:
		return errFmt("Parenthetical table expressions are not supported: %v,", nodeToString(te))
	default:
		return errFmt("Unsupported table expression: %v", nodeToString(te))
	}

	return nil
}

// Processes the select expression (columns to return from the query). Adds the results to the SelectStatement given,
// or returns an error if it cannot. All table aliases must be established in the SelectStatement.
func processSelectedColumns(ctx context.Context, root *doltdb.RootValue, selectStmt *SelectStatement, colSelections sqlparser.SelectExprs) error {
	var columns []QualifiedColumn
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			if !selectExpr.TableName.IsEmpty() {
				var targetTable string
				if aliasedTableName, ok := selectStmt.aliases.TablesByAlias[selectExpr.TableName.Name.String()]; ok {
					targetTable = aliasedTableName
				} else {
					targetTable = selectExpr.TableName.Name.String()
				}
				tableSch := selectStmt.inputSchemas[targetTable]
				tableSch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
					columns = append(columns, QualifiedColumn{targetTable, col.Name})
					return false
				})
			} else {
				for _, tableName := range selectStmt.inputTables {
					tableSch := selectStmt.inputSchemas[tableName]
					tableSch.GetAllCols().IterInSortedOrder(func(tag uint64, col schema.Column) (stop bool) {
						columns = append(columns, QualifiedColumn{tableName, col.Name})
						return false
					})
				}
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
					if tableName, ok = selectStmt.aliases.TablesByAlias[columnTableName]; ok {
						tableSch = mustGetSchema(ctx, root, tableName)
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
					return errFmt(UnknownColumnErrFmt, colName)
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
func mustGetSchema(ctx context.Context, root *doltdb.RootValue, tableName string) schema.Schema {
	tbl, _ := root.GetTable(ctx, tableName)
	return tbl.GetSchema(ctx)
}

// Fills in the result set filter function in selectStmt using the conditions in the where clause and the join.
func createResultSetFilter(selectStmt *SelectStatement, s *sqlparser.Select, rss *resultset.ResultSetSchema) error {
	var rowFilter rowFilterFn
	whereFilter, err := createFilterForWhere(s.Where, selectStmt.inputSchemas, selectStmt.aliases, rss)
	if err != nil {
		return err
	}
	rowFilter = whereFilter

	if len(selectStmt.joins) > 0 {
		joinFilter, err := createFilterForJoins(selectStmt.joins, selectStmt.inputSchemas, selectStmt.aliases, rss)
		if err != nil {
			return err
		}
		rowFilter = func(r row.Row) (matchesFilter bool) {
			return whereFilter(r) && joinFilter(r)
		}
	}

	selectStmt.filterFn = rowFilter
	return nil
}

// The result of running a single select pipeline.
type singleTablePipelineResult struct {
	p    *pipeline.Pipeline
	rows []row.Row
	err  error
}

// createPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createPipeline(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select, selectStmt *SelectStatement) (*pipeline.Pipeline, error) {

	if len(selectStmt.inputSchemas) == 1 {
		var tableName string
		var tableSch schema.Schema
		for name, sch := range selectStmt.inputSchemas {
			tableName = name
			tableSch = sch
		}

		// The field mapping used by where clause filtering is different depending on whether we filter the rows before
		// or after we convert them to the result set schema. For single table selects, we use the schema of the single
		// table for where clause filtering, then convert those rows to the result set schema.
		if err := createResultSetFilter(selectStmt, s, resultset.Identity(tableSch)); err != nil {
			return nil, err
		}

		return createSingleTablePipeline(ctx, root, selectStmt, tableName, true)
	}

	pipelines := make(map[string]*singleTablePipelineResult)
	for tableName := range selectStmt.inputSchemas {
		p, err := createSingleTablePipeline(ctx, root, selectStmt, tableName, false)
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
	for _, tableName := range selectStmt.inputTables {
		result := pipelines[tableName]
		if err := result.p.Wait(); err != nil || result.err != nil {
			return nil, err
		}
		results = append(results, resultset.TableResult{
			Schema: selectStmt.inputSchemas[tableName],
			Rows:   result.rows,
		})
	}

	crossProduct := selectStmt.intermediateRss.CrossProduct(results)
	source := sourceFuncForRows(crossProduct)

	if err := createResultSetFilter(selectStmt, s, selectStmt.intermediateRss); err != nil {
		return nil, err
	}

	p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})
	p.AddStage(pipeline.NewNamedTransform("where", createWhereFn(selectStmt)))
	if selectStmt.limit != noLimit {
		p.AddStage(pipeline.NewNamedTransform("limit", createLimitFn(selectStmt, p)))
	}

	reductionTransform, err := schemaReductionTransform(selectStmt)
	if err != nil {
		return nil, err
	}
	p.AddStage(reductionTransform)

	return p, nil
}

// Returns a transform stage to reduce the intermediate schema to the final one.
// TODO: this is unnecessary for many queries and could be skipped.
func schemaReductionTransform(selectStmt *SelectStatement) (pipeline.NamedTransform, error) {
	mapping, err := rowconv.TagMapping(selectStmt.intermediateRss.Schema(), selectStmt.ResultSetSchema.Schema())
	if err != nil {
		return pipeline.NamedTransform{}, err
	}
	rConv, _ := rowconv.NewRowConverter(mapping)
	return pipeline.NewNamedTransform("transform to result schema", rowconv.GetRowConvTransformFunc(rConv)), nil
}

// Returns a source func that yields the rows given in order.
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

// Returns a transform function to limit the set of rows to the value specified by the statement. Must only be applied
// if a limit > 0 is specified.
func createLimitFn(statement *SelectStatement, p *pipeline.Pipeline) pipeline.TransformRowFunc {
	if statement.limit == noLimit {
		panic("Called createLimitFn without a limit specified")
	}

	var count int
	return func(inRow row.Row, props pipeline.ReadableMap) (results []*pipeline.TransformedRowResult, s string) {
		if count < statement.limit {
			count++
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		} else if count == statement.limit {
			p.NoMore()
		}
		return nil, ""
	}
}

// Returns a transform function to filter the set of rows to match the where / join clauses.
func createWhereFn(statement *SelectStatement) pipeline.TransformRowFunc {
	if statement.filterFn == nil {
		panic("Called createWhereFn without a filterFn specified")
	}

	return func(inRow row.Row, props pipeline.ReadableMap) (results []*pipeline.TransformedRowResult, s string) {
		if statement.filterFn(inRow) {
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		}
		return nil, ""
	}
}

// Creates a pipeline to return results from a single table.
func createSingleTablePipeline(ctx context.Context, root *doltdb.RootValue, statement *SelectStatement, tableName string, isTerminal bool) (*pipeline.Pipeline, error) {
	tblSch := statement.inputSchemas[tableName]
	tbl, _ := root.GetTable(ctx, tableName)

	rd := noms.NewNomsMapReader(ctx, tbl.GetRowData(ctx), tblSch)
	rdProcFunc := pipeline.ProcFuncForReader(ctx, rd)
	p := pipeline.NewPartialPipeline(rdProcFunc, &pipeline.TransformCollection{})
	p.RunAfter(func() { rd.Close(ctx) })

	if isTerminal {
		p.AddStage(pipeline.NewNamedTransform("where", createWhereFn(statement)))
		if statement.limit != noLimit {
			p.AddStage(pipeline.NewNamedTransform("limit", createLimitFn(statement, p)))
		}
		p.AddStage(createOutputSchemaMappingTransform(tblSch, statement.ResultSetSchema))
	}

	return p, nil
}

func createOutputSchemaMappingTransform(tableSch schema.Schema, rss *resultset.ResultSetSchema) pipeline.NamedTransform {
	mapping := rss.Mapping(tableSch)
	rConv, _ := rowconv.NewRowConverter(mapping)
	return pipeline.NewNamedTransform("mapToResultSet", rowconv.GetRowConvTransformFunc(rConv))
}

// Fills in a ResultSetSchema for the given select statement, which contains a target schema and mappings to get there
// from the individual table schemas. Also fills in an intermediate schema, which for some queries contains additional
// columns necessary to evaluate where clauses but not part of the final result set.
func createResultSetSchema(statement *SelectStatement) error {
	rss, err := rssFromColumns(statement.selectedCols, statement.inputSchemas, statement.aliases)
	if err != nil {
		return err
	}
	statement.ResultSetSchema = rss

	rss, err = rssFromColumns(statement.referencedCols, statement.inputSchemas, statement.aliases)
	if err != nil {
		return err
	}
	statement.intermediateRss = rss

	return nil
}

// Returns a result set schema created from the columns given
func rssFromColumns(columns []QualifiedColumn, inputSchemas map[string]schema.Schema, aliases *Aliases) (*resultset.ResultSetSchema, error) {
	cols := make([]resultset.ColWithSchema, len(columns))
	for i, selectedCol := range columns {
		colName := selectedCol.ColumnName
		tableName := selectedCol.TableName

		if tableSch, ok := inputSchemas[tableName]; !ok {
			panic("Unknown table " + tableName)
		} else {
			if col, ok := tableSch.GetAllCols().GetByName(colName); !ok {
				panic(fmt.Sprintf(UnknownColumnErrFmt, colName))
			} else {
				// Rename any aliased columns for output. Column names only matter for the end result, not any
				// intermediate ones.
				if alias, ok := aliases.AliasesByColumn[selectedCol]; ok {
					col.Name = alias
				}
				cols[i] = resultset.ColWithSchema{Col: col, Sch: tableSch}
			}
		}
	}

	return resultset.NewFromColumns(cols...)
}
