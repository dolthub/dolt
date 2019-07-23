package sql

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"vitess.io/vitess/go/vt/sqlparser"

	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/doltdb"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/row"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/schema"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/pipeline"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/typed/noms"
	"github.com/liquidata-inc/ld/dolt/go/libraries/doltcore/table/untyped/resultset"
	"github.com/liquidata-inc/ld/dolt/go/store/types"
)

// No limit marker for limit statements in select
const noLimit = -1

// Struct to represent the salient results of parsing a select statement.
type SelectStatement struct {
	// Result set schema
	ResultSetSchema schema.Schema
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
	// Filter for the where clause
	whereFilter *RowFilter
	// Filter for the join clauses
	joinFilter *RowFilter
	// Sorter for the result set
	orderBy *RowSorter
	// Limit of results returned
	limit int
	// Offset for results (skip N)
	offset int
}

// A SelectedColumn is a column in the result set. It has a name and a way to extract it from an intermediate row.
type SelectedColumn struct {
	Name   string
	Getter *RowValGetter
}

// ExecuteSelect executes the given select query and returns the resultant rows accompanied by their output schema.
func ExecuteSelect(ctx context.Context, root *doltdb.RootValue, s *sqlparser.Select) ([]row.Row, schema.Schema, error) {
	p, statement, err := BuildSelectQueryPipeline(ctx, root, s)
	if err != nil {
		return nil, nil, err
	}

	rows := make([]row.Row, 0)
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

	return rows, statement.ResultSetSchema, nil
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

	if err := processWhereClause(selectStmt, s.Where); err != nil {
		return nil, nil, err
	}

	if err := processJoins(selectStmt); err != nil {
		return nil, nil, err
	}

	if err := processOrderByClause(root.VRW().Format(), selectStmt, s.OrderBy); err != nil {
		return nil, nil, err
	}

	if err := processLimitClause(s, selectStmt); err != nil {
		return nil, nil, err
	}

	if err := createResultSetSchema(selectStmt); err != nil {
		return nil, nil, err
	}

	if err := createIntermediateSchema(selectStmt); err != nil {
		return nil, nil, err
	}

	p, err := createSelectPipeline(ctx, root, selectStmt)
	if err != nil {
		return nil, nil, err
	}

	return p, selectStmt, nil
}

// Creates the final result set schema and applies it to the statement given.
func createResultSetSchema(selectStmt *SelectStatement) error {
	cols := make([]schema.Column, len(selectStmt.selectedCols))
	for i, selectedCol := range selectStmt.selectedCols {
		cols[i] = schema.NewColumn(selectedCol.Name, uint64(i), selectedCol.Getter.NomsKind, false)
	}

	collection, err := schema.NewColCollection(cols...)
	if err != nil {
		return err
	}

	selectStmt.ResultSetSchema = schema.UnkeyedSchemaFromCols(collection)
	return nil
}

// Processes the order by clause and applies the result to the select statement given, or returns an error if it cannot.
func processOrderByClause(nbf *types.NomsBinFormat, statement *SelectStatement, orderBy sqlparser.OrderBy) error {
	if len(orderBy) == 0 {
		return nil
	}

	sorter, err := createRowSorter(nbf, statement, orderBy)
	if err != nil {
		return err
	}

	statement.orderBy = sorter
	return nil
}

// Processes the joins previously recorded in the statement and applies an appropriate filter to the statement.
func processJoins(selectStmt *SelectStatement) error {
	joinFilter, err := createFilterForJoins(selectStmt.joins, selectStmt.inputSchemas, selectStmt.aliases)
	if err != nil {
		return err
	}

	selectStmt.joinFilter = joinFilter
	return nil
}

// Processes the where clause and applies an appropriate filter to the statement.
func processWhereClause(selectStmt *SelectStatement, where *sqlparser.Where) error {
	if whereFilter, err := createFilterForWhere(where, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
		return err
	} else {
		selectStmt.whereFilter = whereFilter
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

	for _, refCols := range [][]QualifiedColumn{selectedCols, whereCols, joinCols, orderByCols} {
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
	for tableName := range selectStatement.inputSchemas {
		if _, ok := selectStatement.aliases.GetTableByAlias(tableName); ok {
			return errFmt(NonUniqueTableNameErrFmt, tableName)
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
	var selected []SelectedColumn
	for _, colSelection := range colSelections {
		switch selectExpr := colSelection.(type) {
		case *sqlparser.StarExpr:
			if qcs, err := resolveColumnsInStarExpr(selectExpr, selectStmt.inputTables, selectStmt.inputSchemas, selectStmt.aliases); err != nil {
				return err
			} else {
				for _, qc := range qcs {
					getter, err := getterForColumn(qc, selectStmt.inputSchemas)
					if err != nil {
						return nil
					}
					selected = append(selected, SelectedColumn{Name: qc.ColumnName, Getter: getter})
				}
			}
		case *sqlparser.AliasedExpr:

			getter, err := getterFor(selectExpr.Expr, selectStmt.inputSchemas, selectStmt.aliases.TableAliasesOnly())
			if err != nil {
				return err
			}

			colName := ""
			// an absent column alias will be empty
			if selectExpr.As.String() != "" {
				colName = selectExpr.As.String()
			} else {
				// not a true alias, but makes the column name expression available to the rest of the query
				colName = resultSetColumnName(selectExpr.Expr)
			}
			selectStmt.aliases.AddColumnAlias(colName, getter)

			selected = append(selected, SelectedColumn{Name: colName, Getter: getter})

		case sqlparser.Nextval:
			return errFmt("Next value is not supported: %v", nodeToString(selectExpr))
		}
	}

	selectStmt.selectedCols = selected
	return nil
}

// resultSetColumnName returns the appropriate name for a result set column that doesn't have an alias assigned
func resultSetColumnName(expr sqlparser.Expr) string {
	switch ce := expr.(type) {
	case *sqlparser.ColName:
		return ce.Name.String()
	default:
		return nodeToString(expr)
	}
}

// Gets the schema for the table name given. Will cause a panic if the table doesn't exist.
func mustGetSchema(ctx context.Context, root *doltdb.RootValue, tableName string) schema.Schema {
	tbl, _ := root.GetTable(ctx, tableName)
	return tbl.GetSchema(ctx)
}

// Binds the tag numbers used by the various components in the statement to those provided by the given resolver.
func bindTagNumbers(statement *SelectStatement, resolver TagResolver) error {
	var initVals []InitValue
	for _, col := range statement.selectedCols {
		initVals = append(initVals, col.Getter)
	}
	initVals = append(initVals, statement.joinFilter, statement.whereFilter, statement.orderBy)
	return ComposeInits(initVals...)(resolver)
}

// The result of running a single select pipeline.
type singleTablePipelineResult struct {
	p       *pipeline.Pipeline
	outChan chan row.Row
	err     error
}

// createSelectPipeline constructs a pipeline to execute the statement and returns it. The constructed pipeline doesn't have
// an output set, and must be supplied one before execution.
func createSelectPipeline(ctx context.Context, root *doltdb.RootValue, selectStmt *SelectStatement) (*pipeline.Pipeline, error) {

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
		if err := bindTagNumbers(selectStmt, resultset.Identity(tableName, tableSch)); err != nil {
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

		result := &singleTablePipelineResult{p: p, outChan: make(chan row.Row)}
		pipelines[tableName] = result

		rowSink := pipeline.ProcFuncForSinkFunc(
			func(r row.Row, props pipeline.ReadableMap) error {
				result.outChan <- r
				return nil
			})

		errSink := func(failure *pipeline.TransformRowFailure) (quit bool) {
			result.err = errors.New(fmt.Sprintf("Query execution failed at stage %v for row %v: error was %v",
				failure.TransformName, failure.Row, failure.Details))
			return true
		}

		p.RunAfter(func() {
			close(result.outChan)
		})

		// TODO: this adds an unnecessary stage to the pipeline
		p.SetOutput(rowSink)
		p.SetBadRowCallback(errSink)
		p.Start()
	}

	results := make([]*resultset.TableResult, 0)
	for _, tableName := range selectStmt.inputTables {
		result := pipelines[tableName]
		results = append(results, resultset.NewTableResult(result.outChan, selectStmt.inputSchemas[tableName]))
	}

	cpChan := make(chan row.Row)
	cb := func(r row.Row) {
		cpChan <- r
	}
	go func() {
		defer close(cpChan)
		selectStmt.intermediateRss.CrossProduct(root.VRW().Format(), results, cb)
	}()

	// TODO: we need to check errors in pipeline execution without blocking
	// for _, result := range pipelines {
	// 	if err := result.p.Wait(); err != nil {
	// 		return nil, err
	// 	}
	// }

	if err := bindTagNumbers(selectStmt, selectStmt.intermediateRss); err != nil {
		return nil, err
	}

	p := pipeline.NewPartialPipeline(pipeline.InFuncForChannel(cpChan))
	p.AddStage(pipeline.NewNamedTransform("where", createWhereFn(selectStmt)))
	if selectStmt.orderBy != nil {
		p.AddStage(pipeline.NamedTransform{Name: "order by", Func: newSortingTransform(selectStmt.orderBy.Less)})
	}
	if selectStmt.limit != noLimit {
		p.AddStage(pipeline.NewNamedTransform("limit", createLimitAndOffsetFn(selectStmt, p)))
	}

	p.AddStage(createOutputSchemaMappingTransform(root.VRW().Format(), selectStmt))

	return p, nil
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

// Fills in the result set filter function in selectStmt using the conditions in the where clause and the join.
func createResultSetFilter(selectStmt *SelectStatement) *RowFilter {
	return newRowFilter(func(r row.Row) (matchesFilter bool) {
		return selectStmt.whereFilter.filter(r) && selectStmt.joinFilter.filter(r)
	})
}

// Returns a transform function to filter the set of rows to match the where / join clauses.
func createWhereFn(statement *SelectStatement) pipeline.TransformRowFunc {
	rowFilter := createResultSetFilter(statement)

	return func(inRow row.Row, props pipeline.ReadableMap) (results []*pipeline.TransformedRowResult, s string) {
		if rowFilter.filter(inRow) {
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
	p := pipeline.NewPartialPipeline(rdProcFunc)
	p.RunAfter(func() { rd.Close(ctx) })

	if isTerminal {
		p.AddStage(pipeline.NewNamedTransform("where", createWhereFn(statement)))
		if statement.orderBy != nil {
			p.AddStage(pipeline.NamedTransform{Name: "order by", Func: newSortingTransform(statement.orderBy.Less)})
		}
		if statement.limit != noLimit {
			p.AddStage(pipeline.NewNamedTransform("limit", createLimitAndOffsetFn(statement, p)))
		}
		p.AddStage(createOutputSchemaMappingTransform(root.VRW().Format(), statement))
	}

	return p, nil
}

func createOutputSchemaMappingTransform(nbf *types.NomsBinFormat, selectStmt *SelectStatement) pipeline.NamedTransform {
	var transformFunc pipeline.TransformRowFunc
	transformFunc = func(inRow row.Row, props pipeline.ReadableMap) (rowData []*pipeline.TransformedRowResult, badRowDetails string) {
		taggedVals := make(row.TaggedValues)
		for i, selectedCol := range selectStmt.selectedCols {
			val := selectedCol.Getter.Get(inRow)
			if !types.IsNull(val) {
				taggedVals[uint64(i)] = val
			}
		}
		r := row.New(nbf, selectStmt.ResultSetSchema, taggedVals)
		return []*pipeline.TransformedRowResult{{r, nil}}, ""
	}

	return pipeline.NewNamedTransform("create result set", transformFunc)
}

// Fills in a an intermediate ResultSetSchema for the given select statement, which contains the schema of the
// intermediate result set.
func createIntermediateSchema(statement *SelectStatement) error {
	rss, err := rssFromColumns(statement.referencedCols, statement.inputSchemas)
	if err != nil {
		return err
	}
	statement.intermediateRss = rss

	return nil
}

// Returns a result set schema created from the columns given
func rssFromColumns(columns []QualifiedColumn, inputSchemas map[string]schema.Schema) (*resultset.ResultSetSchema, error) {
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
				cols[i] = resultset.ColWithSchema{Col: col, Sch: tableSch}
			}
		}
	}

	return resultset.NewFromColumns(inputSchemas, cols...)
}
