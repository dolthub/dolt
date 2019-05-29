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
	"sort"
	"strconv"
)

// No limit marker for limit statements in select
const noLimit = -1

// Boolean lesser function for rows. Returns whether rLeft < rRight
type rowLesserFn func(rLeft row.Row, rRight row.Row) bool

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
	selectedCols []SelectedColumn
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
	// Offset for results (skip N)
	offset int
}

// A SelectedColumn is a column in the result set. It has a name and a way to extract it from an intermediate row.
type SelectedColumn struct {
	Name   string
	Col    QualifiedColumn // TODO: remove me
	Getter RowValGetter
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

	if err := processSelectedColumns(selectStmt, s.SelectExprs); err != nil {
		return nil, nil, err
	}

	if err := processReferencedColumns(selectStmt, s.SelectExprs, s.Where, s.OrderBy); err != nil {
		return nil, nil, err
	}

	if err := createResultSetSchema(selectStmt); err != nil {
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
func processOrderByClause(statement *SelectStatement, orderBy sqlparser.OrderBy, rss *resultset.ResultSetSchema) error {
	if len(orderBy) == 0 {
		return nil
	}

	obs := make([]ob, len(orderBy))
	for i, o := range orderBy {
		qcs, err := resolveColumnsInExpr(o.Expr, statement.inputSchemas, statement.aliases)
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

		comparisonTag := rss.Mapping(tableSch).SrcToDest[column.Tag]

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
func processReferencedColumns(selectStmt *SelectStatement, colSelections sqlparser.SelectExprs, where *sqlparser.Where, orderBy sqlparser.OrderBy) error {
	cols := make([]QualifiedColumn, 0)
	var selectedCols, whereCols, joinCols, orderByCols []QualifiedColumn
	var err error
	
	if selectedCols, err = resolveColumnsInSelectClause(colSelections, selectStmt.inputTables, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
		return err
	}

	if whereCols, err = resolveColumnsInWhereClause(where, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
		return err
	}

	if joinCols, err = resolveColumnsInJoins(selectStmt.joins, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
		return err
	}

	if orderByCols, err = resolveColumnsInOrderBy(orderBy, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
		return err
	}

	for _, refCols := range [][]QualifiedColumn {selectedCols, whereCols, joinCols, orderByCols} {
		for _, col := range refCols {
			if !contains(col, cols) {
				cols = append(cols, col)
			}
		}
	}

	selectStmt.referencedCols = cols
	return nil
}

// Returns whether the given column is in the slice
func contains(column QualifiedColumn, cols []QualifiedColumn) bool {
	for _, col := range cols {
		if ColumnsEqual(col, column) {
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

			if limitInt < 0 {
				return errFmt("Limit must be >= 0 if supplied: '%v'", nodeToString(s.Limit.Rowcount))
			}

			selectStmt.limit = limitInt

			if s.Limit.Offset != nil {
				offsetVal, ok := s.Limit.Offset.(*sqlparser.SQLVal)
				if !ok {
					return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
				}
				offsetInt, err := strconv.Atoi(nodeToString(offsetVal))
				if err != nil {
					return errFmt("Couldn't parse limit clause: %v", nodeToString(s.Limit))
				}

				if offsetInt < 0 {
					return errFmt("Offset must be >= 0 if supplied: '%v'", nodeToString(s.Limit.Offset))
				}

				selectStmt.offset = offsetInt
			}

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

	return validateTablesAndAliases(selectStmt)
}

// Validates that the tables and aliases from the select have unique names.
func validateTablesAndAliases(selectStatement *SelectStatement) error {
	seen := make(map[string]bool)
	for alias := range selectStatement.aliases.TablesByAlias {
		seen[alias] = true
	}
	for tableName := range selectStatement.inputSchemas {
		if seen[tableName] {
			return errFmt("Non-unique table name / alias: '%v'", tableName)
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

		if tableName == DUAL {
			return errFmt("Selects without a table are not supported: %v", nodeToString(te))
		}

		canonicalTableName, err := resolveTable(tableName, root.GetTableNames(ctx), NewAliases())
		if err != nil {
			return err
		}

		if !te.As.IsEmpty() {
			if err := selectStmt.aliases.AddTableAlias(canonicalTableName, te.As.String()); err != nil {
				return err
			}
		}

		if _, ok := selectStmt.inputSchemas[canonicalTableName]; !ok {
			selectStmt.inputSchemas[canonicalTableName] = mustGetSchema(ctx, root, canonicalTableName)
		}
		selectStmt.inputTables = append(selectStmt.inputTables, canonicalTableName)

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
func processSelectedColumns(selectStmt *SelectStatement, colSelections sqlparser.SelectExprs) error {
	var columns []QualifiedColumn
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			if qcs, err := resolveColumnsInStarExpr(selectExpr, selectStmt.inputTables, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
				return err
			} else {
				columns = append(columns, qcs...)
			}
		case *sqlparser.AliasedExpr:
			switch colExpr := selectExpr.Expr.(type) {
			case *sqlparser.ColName:

				qc, err := resolveColumn(getColumnNameString(colExpr), selectStmt.inputSchemas, selectStmt.aliases)
				if err != nil {
					return err
				}

				// an absent column alias will be empty
				if selectExpr.As.String() != "" {
					selectStmt.aliases.AddColumnAlias(qc, selectExpr.As.String())
				} else {
					// This isn't a true alias, but we want the column header to exactly match the original select statement, even
					// if we found a case-insensitive match for the column name.
					selectStmt.aliases.AddColumnAlias(qc, colExpr.Name.String())
				}

				columns = append(columns, qc)
			default:
				return errFmt("Only column selections or * are supported")
			}
		case sqlparser.Nextval:
			return errFmt("Next value is not supported: %v", nodeToString(selectExpr))
		}
	}

	selectedCols := make([]SelectedColumn, len(columns))
	for i, col := range columns {
		name := col.ColumnName
		if alias, ok := selectStmt.aliases.GetColumnAlias(col); ok {
			name = alias
		}
		// TODO: getters
		selectedCols[i] = SelectedColumn{Name: name, Col: col, Getter: RowValGetter{}}
	}

	selectStmt.selectedCols = selectedCols
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

		if err := processOrderByClause(selectStmt, s.OrderBy, resultset.Identity(tableSch)); err != nil {
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

	if err := processOrderByClause(selectStmt, s.OrderBy, selectStmt.intermediateRss); err != nil {
		return nil, err
	}

	p := pipeline.NewPartialPipeline(pipeline.ProcFuncForSourceFunc(source), &pipeline.TransformCollection{})
	p.AddStage(pipeline.NewNamedTransform("where", createWhereFn(selectStmt)))
	if selectStmt.orderFn != nil {
		p.AddStage(pipeline.NamedTransform{Name: "order by", Func: newSortingTransform(selectStmt.orderFn)})
	}
	if selectStmt.limit != noLimit {
		p.AddStage(pipeline.NewNamedTransform("limit", createLimitAndOffsetFn(selectStmt, p)))
	}

	reductionTransform, err := schemaReductionTransform(selectStmt)
	if err != nil {
		return nil, err
	}
	p.AddStage(reductionTransform)

	return p, nil
}

// Returns a sorting transform for the rowLesserFn given. The transform will necessarily block until it receives all
// input rows before sending rows to the rest of the pipeline.
func newSortingTransform(lesser rowLesserFn) pipeline.TransformFunc {
	rows := make([]pipeline.RowWithProps, 0)

	sortAndWrite := func(outChan chan<- pipeline.RowWithProps) {
		sort.Slice(rows, func(i, j int) bool {
			return lesser(rows[i].Row, rows[j].Row)
		})
		for _, r := range rows {
			outChan <- r
		}
	}

	return func(inChan <-chan pipeline.RowWithProps, outChan chan<- pipeline.RowWithProps, badRowChan chan<- *pipeline.TransformRowFailure, stopChan <-chan struct{}) {
		for {
			select {
			case <-stopChan:
				sortAndWrite(outChan)
				return
			default:
			}

			select {
			case r, ok := <-inChan:
				if ok {
					rows = append(rows, r)
				} else {
					sortAndWrite(outChan)
					return
				}

			case <-stopChan:
				sortAndWrite(outChan)
				return
			}
		}
	}
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
func createLimitAndOffsetFn(statement *SelectStatement, p *pipeline.Pipeline) pipeline.TransformRowFunc {
	if statement.limit == noLimit {
		panic("Called createLimitAndOffsetFn without a limit specified")
	}

	var skipped, returned int
	return func(inRow row.Row, props pipeline.ReadableMap) (results []*pipeline.TransformedRowResult, s string) {
		if skipped >= statement.offset && returned < statement.limit {
			returned++
			return []*pipeline.TransformedRowResult{{inRow, nil}}, ""
		} else if returned == statement.limit {
			p.NoMore()
		} else {
			skipped++
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
		if statement.orderFn != nil {
			p.AddStage(pipeline.NamedTransform{Name: "order by", Func: newSortingTransform(statement.orderFn)})
		}
		if statement.limit != noLimit {
			p.AddStage(pipeline.NewNamedTransform("limit", createLimitAndOffsetFn(statement, p)))
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
	rss, err := rssFromSelectedColumns(statement.selectedCols, statement.inputSchemas, statement.aliases)
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
func rssFromSelectedColumns(columns []SelectedColumn, inputSchemas map[string]schema.Schema, aliases *Aliases) (*resultset.ResultSetSchema, error) {
	qcs := make([]QualifiedColumn, len(columns))
	for i, col := range columns {
		qcs[i] = col.Col
	}

	return rssFromColumns(qcs, inputSchemas, aliases)
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
				// Rename any aliased columns for output. Column names only matter for the end result, not any intermediate ones.
				if alias, ok := aliases.GetColumnAlias(selectedCol); ok {
					col.Name = alias
				}
				cols[i] = resultset.ColWithSchema{Col: col, Sch: tableSch}
			}
		}
	}

	return resultset.NewFromColumns(cols...)
}